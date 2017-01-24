package net.tcs.core;

import java.util.HashMap;
import java.util.Map;

import net.tcs.db.adapter.TaskInstanceDBAdapter;
import net.tcs.task.JobDefinition;
import net.tcs.task.TCSTaskInput;
import net.tcs.task.TaskDefinition;

/**
 * In-memory representation of the state of a running Job instance.
 */
public class JobStateMachineContext extends JobStateMachineContextBase {

    public JobStateMachineContext(String jobName, String instanceId, String shardId,
            String jobNotificationURI) {
        super(jobName, instanceId, shardId, false, jobNotificationURI);
    }

    protected JobStateMachineContext(String jobName, String instanceId, String shardId, boolean hasSteps,
            String jobNotificationURI) {
        super(jobName, instanceId, shardId, hasSteps, jobNotificationURI);
    }

    @Override
    public void initializeFromJobDefinition(JobDefinition job) {
        super.initializeFromJobDefinition(job);

        createTaskStateMachineContexts(job);

        for (final TaskStateMachineContextBase task : taskExecutionDataMap.values()) {
            for (final TaskKey par : task.predecessors) {
                final TaskStateMachineContextBase predecessor = taskExecutionDataMap.get(par);
                predecessor.successors.add(new TaskKey(task.getName(), task.getParallelExecutionIndex()));
            }
        }
    }

    @Override
    public TCSTaskInput getTaskInputForExecution(TaskStateMachineContextBase task,
            TaskInstanceDBAdapter taskDBAdapter) {
        final Map<String, TaskKey> predecessorIdToNameMap = new HashMap<>();
        for (final TaskKey pred : task.predecessors) {
            final TaskStateMachineContextBase predecessorTaskContext = taskExecutionDataMap.get(pred);
            predecessorIdToNameMap.put(predecessorTaskContext.getInstanceId(), pred);
        }

        final TCSTaskInput taskInput = taskDBAdapter.getInputForTask(task.getInstanceId(), predecessorIdToNameMap);
        return taskInput;
    }

    protected void createTaskStateMachineContexts(JobDefinition job) {
        for (final TaskDefinition taskDef : job.getTaskMap().values()) {
            final TaskStateMachineContextBase taskExecData = new TaskStateMachineContext(taskDef);
            taskExecutionDataMap.put(new TaskKey(taskDef.getTaskName(), 0), taskExecData);
        }
    }
}
