package net.tcs.core;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import net.tcs.task.JobDefinition;
import net.tcs.task.TaskDefinition;

/**
 * In-memory representation of the state of a running Job instance that has
 * steps.
 */
public class StepJobStateMachineContext extends JobStateMachineContext {

    public StepJobStateMachineContext(String jobName, String instanceId, String shardId, String jobNotificationURI) {
        super(jobName, instanceId, shardId, true, jobNotificationURI);
    }

    /**
     * Create TaskStateMachineContext (for parallel tasks in a step), and inject
     * into the In-memory DAG.
     *
     * 1. For the first task in the step, wire all the parallel task SMs to have
     * the same set of predecessors. Also, update the predecessors to point back
     * to these parallel tasks as successors.
     *
     * 2. For the remaining tasks in the step, wire all the parallel task SMs to
     * have predecessor point to the task above and successor point to the task
     * below.
     *
     * 3. For the last task in the step, wire all the parallel task SMs to have
     * the same set of successors. Also, update the successors to point back to
     * these parallel tasks as predecessors.
     *
     * @param tasks
     *            Linear tasks belonging to a particular step.
     * @param taskMap
     *            Map of {TaskKey => TaskInstanceId}
     * @param parallelCount
     *            Parallel count for the Step, as specified in the JobContext
     */
    public void injectParallelTasksIntoGraph(List<String> tasks, Map<TaskKey, String> taskMap, int parallelCount) {

        for (int index = 0; index < tasks.size(); index++) {
            final String taskName = tasks.get(index);
            final TaskStateMachineContextBase taskAt0 = taskExecutionDataMap.get(new TaskKey(taskName, 0));

            for (int i = 1; i < parallelCount; i++) {

                final TaskKey taskKeyAtI = new TaskKey(taskName, i);
                final String taskInstanceId = taskMap.get(taskKeyAtI);

                final boolean inheritPredecessors = (index == 0);
                final TaskStateMachineContext taskAtI = new TaskStateMachineContext(taskAt0, taskInstanceId, i,
                        inheritPredecessors);

                if (inheritPredecessors) {
                    for (final TaskKey pred : taskAt0.predecessors) {
                        final TaskStateMachineContextBase predTask = taskExecutionDataMap.get(pred);
                        predTask.successors.add(taskKeyAtI);
                    }
                } else {
                    final TaskKey pred = new TaskKey(tasks.get(index - 1), i);
                    taskAtI.predecessors.add(pred);

                    final TaskStateMachineContextBase predTask = taskExecutionDataMap.get(pred);
                    predTask.successors.add(taskKeyAtI);
                }

                taskExecutionDataMap.put(taskKeyAtI, taskAtI);
            }
        }

        /*
         * Rewire the last tasks with successors
         */
        final int lastIndex = tasks.size() - 1;
        final String taskName = tasks.get(lastIndex);

        final TaskStateMachineContextBase taskAt0 = taskExecutionDataMap.get(new TaskKey(taskName, 0));

        if (!taskAt0.successors.isEmpty()) {
            for (int i = 1; i < parallelCount; i++) {

                final TaskKey taskKeyAtI = new TaskKey(taskName, i);

                final TaskStateMachineContextBase taskAtI = taskExecutionDataMap.get(taskKeyAtI);
                taskAtI.successors.addAll(taskAt0.successors);

                for (final TaskKey succ : taskAtI.successors) {
                    final TaskStateMachineContextBase succTask = taskExecutionDataMap.get(succ);
                    succTask.predecessors.add(taskKeyAtI);
                }
            }
        }
    }

    /**
     * Create and initialize TaskStateMachineContexts for a JobSpec that has
     * steps. No parallel tasks in a step are created at this point. They are
     * created dynamically when the parallelCount is specified for the step in
     * the Job key-value context.
     */
    @Override
    protected void createTaskStateMachineContexts(JobDefinition job) {
        for (final TaskDefinition taskDef : job.getTaskMap().values()) {
            final TaskStateMachineContextBase taskExecData = new TaskStateMachineContext(taskDef);
            final String stepName = job.getStep(taskDef.getTaskName());

            if (!StringUtils.EMPTY.equals(stepName)) {
                taskExecData.setStep(job.getStep(taskDef.getTaskName()));
                taskExecData.setFirstTaskInStep(job.isFirstTask(taskDef.getTaskName(), stepName));
            }

            taskExecutionDataMap.put(new TaskKey(taskDef.getTaskName(), 0), taskExecData);
        }
    }
}
