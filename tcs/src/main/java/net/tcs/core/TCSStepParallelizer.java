package net.tcs.core;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;

import com.task.coordinator.message.utils.TCSConstants;

import net.tcs.db.JobInstanceDAO;
import net.tcs.db.TaskInstanceDAO;
import net.tcs.db.adapter.JobInstanceDBAdapter;
import net.tcs.db.adapter.TaskInstanceDBAdapter;
import net.tcs.task.JobDefinition;

public class TCSStepParallelizer {
    private final TaskInstanceDBAdapter taskDBAdapter;

    private final JobInstanceDBAdapter jobInstanceDBAdapter;

    public TCSStepParallelizer(TaskInstanceDBAdapter taskDBAdapter, JobInstanceDBAdapter jobInstanceDBAdapterr) {
        this.taskDBAdapter = taskDBAdapter;
        this.jobInstanceDBAdapter = jobInstanceDBAdapterr;
    }

    /**
     * Parallelization is needed if at-least one task is marked as the first
     * task of a step, and the parallel-count key is injected in the JobContext
     * key.
     *
     * @param jobStateMachineContext
     * @param jobContext
     * @param tasks
     * @return
     */
    public boolean needParallelization(final JobStateMachineContextBase jobStateMachineContext,
            Map<String, String> jobContext, List<TaskStateMachineContextBase> tasks) {

        if (!jobStateMachineContext.hasSteps) {
            return false;
        }

        for (final TaskStateMachineContextBase task : tasks) {
            if (task.isFirstTaskInStep() && !StringUtils.isEmpty(task.getStep())
                    && (task.parallelExecutionIndex == 0)) {
                final String step = task.getStep();
                final String parallelCountKey = String.format(TCSConstants.STEP_PARALLEL_COUNT_KEY_FORMAT, step);
                if (jobContext.containsKey(parallelCountKey)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Create parallel tasks, if needed (save in DB) and return the list of
     * tasks.
     *
     * If the task is not part of a step, or not the first task of a step, or
     * the parallel count is 0, then just return the task.
     *
     * @param task
     * @param jobContext
     * @param jobStateMachineContext
     * @param jobDAO
     * @param jobDef
     * @return
     */
    public List<TaskStateMachineContextBase> createParallelTasks(TaskStateMachineContextBase task,
            Map<String, String> jobContext, final JobStateMachineContextBase jobStateMachineContext, JobInstanceDAO jobDAO,
            final JobDefinition jobDef) {
        final int parallelCount = getParallelCount(task, jobContext);
        if (parallelCount <= 0) {
            return Collections.singletonList(task);
        } else {
            final List<String> tasksInStep = jobDef.getTasksForStep(task.getStep());

            saveAndInjectParallelExecutionTasks(parallelCount, tasksInStep, jobStateMachineContext, jobDAO);

            return jobStateMachineContext.getParallelTaskStateMachineContextsByName(task.getName(), parallelCount);
        }
    }

    /**
     * Return parallel count of a task if all the following conditions are true:
     *
     * a. Task belongs to a step.
     *
     * b. Task is the first task in the step.
     *
     * c. ParallelCount for the step is specified in the JobContext KV map.
     *
     * @param task
     * @param jobContext
     * @return
     */
    private int getParallelCount(TaskStateMachineContextBase task, Map<String, String> jobContext) {
        if (task.isFirstTaskInStep() && !StringUtils.isEmpty(task.getStep()) && (task.parallelExecutionIndex == 0)) {
            final String step = task.getStep();
            final String parallelCountKey = String.format(TCSConstants.STEP_PARALLEL_COUNT_KEY_FORMAT, step);
            if (jobContext.containsKey(parallelCountKey)) {
                try {
                    return Integer.parseInt(jobContext.get(parallelCountKey));
                } catch (final NumberFormatException ex) {
                    return 0;
                }
            }
        }
        return 0;
    }

    /**
     * For a step, create parallelTasks, (a) Save parallel tasks in DB and (b)
     * Inject parallel tasks to the In-memory DAG.
     *
     * @param parallelCount
     * @param tasks
     * @param jobStateMachineContext
     * @param jobDAO
     */
    private void saveAndInjectParallelExecutionTasks(int parallelCount, List<String> tasks,
            final JobStateMachineContextBase jobStateMachineContext, JobInstanceDAO jobDAO) {

        final LinkedHashMap<String, String> taskNameToIdMap = jobStateMachineContext.getTaskInstanceIds(tasks, 0);

        final LinkedHashMap<String, String> taskInputs = taskDBAdapter.getInputForTask(taskNameToIdMap.values());

        final LinkedHashMap<String, String> nameToInputMap = new LinkedHashMap<>();
        for (final Entry<String, String> entry : taskNameToIdMap.entrySet()) {
            final String input = taskInputs.get(entry.getValue());
            nameToInputMap.put(entry.getKey(), input);
        }

        final Map<TaskKey, String> taskMap = jobInstanceDBAdapter.saveParallelTasks(jobDAO, parallelCount,
                nameToInputMap);

        final StepJobStateMachineContext jobSM = (StepJobStateMachineContext) jobStateMachineContext;
        jobSM.injectParallelTasksIntoGraph(tasks, taskMap, parallelCount);
    }

    /**
     * During TCS Job recovery, perform the following:
     *
     * 1. Determine the ParallelCount for all the steps, by inspecting the Tasks
     * from DB.
     *
     * 2. For each step, inject parallel tasks to the In-memory DAG.
     *
     * @param jobDef
     * @param taskDAOs
     * @param jobStateMachineContext
     */
    public void injectParallelTasksIntoGraphDuringRecovery(JobDefinition jobDef, List<TaskInstanceDAO> taskDAOs,
            JobStateMachineContext jobStateMachineContext) {
        final Map<String, Integer> stepToMaxParallelIndexMap = new HashMap<>();

        for (final TaskInstanceDAO taskDAO : taskDAOs) {
            final String taskName = taskDAO.getName();

            final String stepName = jobDef.getStep(taskName);
            if (!StringUtils.EMPTY.equals(stepName)) {
                final Integer count = stepToMaxParallelIndexMap.get(stepName);
                if (count == null) {
                    stepToMaxParallelIndexMap.put(stepName, taskDAO.getParallelExecutionIndex());
                } else {
                    if (taskDAO.getParallelExecutionIndex() > count) {
                        stepToMaxParallelIndexMap.put(stepName, taskDAO.getParallelExecutionIndex());
                    }
                }
            }
        }

        if (stepToMaxParallelIndexMap.isEmpty()) {
            return;
        }

        final Map<TaskKey, String> taskMap = new HashMap<>();

        for (final TaskInstanceDAO taskDAO : taskDAOs) {
            taskMap.put(new TaskKey(taskDAO.getName(), taskDAO.getParallelExecutionIndex()), taskDAO.getInstanceId());
        }

        final StepJobStateMachineContext jobSM = (StepJobStateMachineContext) jobStateMachineContext;

        for (final String step : stepToMaxParallelIndexMap.keySet()) {
            final int parallelCount = stepToMaxParallelIndexMap.get(step) + 1;

            jobSM.injectParallelTasksIntoGraph(jobDef.getTasksForStep(step), taskMap, parallelCount);
        }
    }
}
