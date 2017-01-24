package net.tcs.core;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.tcs.db.TaskInstanceDAO;
import net.tcs.db.adapter.TaskInstanceDBAdapter;
import net.tcs.task.JobDefinition;
import net.tcs.task.TCSTaskInput;
import net.tcs.task.TaskDefinition;

/**
 * In-memory representation of the state of a Job instance being rolled-back
 */
public class JobRollbackStateMachineContext extends JobStateMachineContextBase {

    public JobRollbackStateMachineContext(String jobName, String instanceId, String shardId, boolean hasSteps,
            String jobNotificationURI) {
        super(jobName, instanceId, shardId, hasSteps, jobNotificationURI);
    }

    /**
     * Build a Reverse-DAG with only completed task nodes REVISIT TODO, fix for
     * parallel tasks
     *
     * @param job
     * @param completedTaskDAOs
     */
    public void initializeFromJobDefinition(JobDefinition job, List<TaskInstanceDAO> completedTaskDAOs) {
        super.initializeFromJobDefinition(job);

        final Set<TaskKey> completedTasks = new HashSet<>();
        final Set<String> completedTaskNames = new HashSet<>();

        for (final TaskInstanceDAO taskDAO : completedTaskDAOs) {
            completedTasks.add(new TaskKey(taskDAO.getName(), taskDAO.getParallelExecutionIndex()));
            completedTaskNames.add(taskDAO.getName());
        }

        for (final TaskDefinition taskDef : job.getTaskMap().values()) {
            if (completedTaskNames.contains(taskDef.getTaskName())) {
                final TaskRollbackStateMachineContext taskExecData = new TaskRollbackStateMachineContext(taskDef,
                        completedTasks);
                taskExecutionDataMap.put(new TaskKey(taskDef.getTaskName(), 0), taskExecData);
            }
        }

        for (final TaskStateMachineContextBase task : taskExecutionDataMap.values()) {
            for (final TaskKey successor : task.successors) {
                final TaskStateMachineContextBase successorTask = taskExecutionDataMap.get(successor);
                if (successorTask != null) {
                    successorTask.predecessors.add(new TaskKey(task.getName(), 0));
                }
            }
        }
    }

    @Override
    public TCSTaskInput getTaskInputForExecution(TaskStateMachineContextBase task, TaskInstanceDBAdapter taskDBAdapter) {
        return null;
    }
}
