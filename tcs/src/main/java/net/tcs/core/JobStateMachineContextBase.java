package net.tcs.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import net.tcs.db.TaskInstanceDAO;
import net.tcs.db.adapter.TaskInstanceDBAdapter;
import net.tcs.task.JobDefinition;
import net.tcs.task.TCSTaskInput;

/**
 * In-memory representation of the state of a running Job instance.
 */
public abstract class JobStateMachineContextBase {

    protected final String name;
    protected final String instanceId;
    protected final String shardId;
    protected final boolean hasSteps;
    protected String registrant;
    protected final Map<TaskKey, TaskStateMachineContextBase> taskExecutionDataMap = new HashMap<>();

    public String getName() {
        return name;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public String getShardId() {
        return shardId;
    }

    public JobStateMachineContextBase(String jobName, String instanceId, String shardId, boolean hasSteps,
            String jobNotificationURI) {
        this.name = jobName;
        this.instanceId = instanceId;
        this.shardId = shardId;
        this.hasSteps = hasSteps;
        this.registrant = jobNotificationURI;
    }

    public void initializeFromJobDefinition(JobDefinition job) {
        if (registrant == null) {
            registrant = job.getRegistrant();
        }
    }

    public void updateGraph(List<TaskInstanceDAO> taskDAOs) {
        for (final TaskInstanceDAO taskDAO : taskDAOs) {
            final TaskKey key = new TaskKey(taskDAO.getName(), taskDAO.getParallelExecutionIndex());
            final TaskStateMachineContextBase taskContext = taskExecutionDataMap.get(key);
            if (taskContext != null) {
                taskContext.setInstanceId(taskDAO.getInstanceId());
                taskContext.initializeFromDB(taskDAO);
            }
        }
    }

    public LinkedHashMap<String, String> getTaskInstanceIds(List<String> taskNames, int parallelExecutionIndex) {
        final LinkedHashMap<String, String> output = new LinkedHashMap<>();
        for (final String taskName : taskNames) {
            final TaskStateMachineContextBase sm = taskExecutionDataMap
                    .get(new TaskKey(taskName, parallelExecutionIndex));
            if (sm != null) {
                output.put(taskName, sm.getInstanceId());
            }
        }
        return output;
    }

    /**
     * Get all tasks that are ready to execute, after a Shard recovery
     *
     * @return
     */
    public List<TaskStateMachineContextBase> getTasksReadyToExecute() {

        final List<TaskStateMachineContextBase> readyToExecuteTasks = new ArrayList<>();

        for (final TaskStateMachineContextBase taskContext : taskExecutionDataMap.values()) {
            if (taskContext.isReadyToExecute()) {
                if (areAllPredecessorTasksComplete(taskContext)) {
                    readyToExecuteTasks.add(taskContext);
                }
            }
        }
        return readyToExecuteTasks;
    }

    /**
     * Get the root task(s) for the Job
     *
     * @return
     */
    public List<TaskStateMachineContextBase> getRootTasks() {
        final List<TaskStateMachineContextBase> tasks = new ArrayList<>();
        for (final TaskStateMachineContextBase task : taskExecutionDataMap.values()) {
            if (task.predecessors.isEmpty() && task.isReadyToExecute()) {
                tasks.add(task);
            }
        }
        return tasks;
    }

    /**
     * Determine if the Task is ready to execute. A task is ready to execute if
     * all its parent tasks are complete.
     *
     * @param taskName
     * @return
     */
    private boolean areAllPredecessorTasksComplete(TaskStateMachineContextBase task) {
        for (final TaskKey predecessor : task.predecessors) {
            if (!taskExecutionDataMap.get(predecessor).isComplete()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Marks a Task as complete, and then gets all the children tasks that are
     * ready to execute.
     *
     * @param taskName
     * @return
     */
    public List<TaskStateMachineContextBase> taskComplete(String taskName, int parallelExecutionIndex) {
        final TaskStateMachineContextBase taskExecData = taskExecutionDataMap
                .get(new TaskKey(taskName, parallelExecutionIndex));
        taskExecData.markComplete();

        final List<TaskStateMachineContextBase> readyToExecuteTasks = new ArrayList<>();

        for (final TaskKey child : taskExecData.successors) {
            final TaskStateMachineContextBase childTask = taskExecutionDataMap.get(child);
            if (areAllPredecessorTasksComplete(childTask)) {
                readyToExecuteTasks.add(childTask);
            }
        }
        return readyToExecuteTasks;
    }

    public abstract TCSTaskInput getTaskInputForExecution(TaskStateMachineContextBase task,
            TaskInstanceDBAdapter taskDBAdapter);

    public Set<String> getTargetExecutionURIs() {
        final Set<String> uris = new HashSet<>();

        for (final TaskStateMachineContextBase taskContext : taskExecutionDataMap.values()) {
            uris.add(taskContext.getTaskExecutionTarget());
        }
        return uris;
    }

    public boolean areAllTasksComplete() {
        for (final TaskStateMachineContextBase taskContext : taskExecutionDataMap.values()) {
            if (!taskContext.isComplete()) {
                return false;
            }
        }
        return true;
    }

    public List<TaskStateMachineContextBase> getAllCompletedTasks() {
        final List<TaskStateMachineContextBase> completedTasks = new ArrayList<>();
        for (final TaskStateMachineContextBase taskContext : taskExecutionDataMap.values()) {
            if (taskContext.isComplete()) {
                completedTasks.add(taskContext);
            }
        }
        return completedTasks;
    }

    public TaskStateMachineContextBase getTaskExecutionContext(String taskName, int parallelExecutionIndex) {
        return taskExecutionDataMap.get(new TaskKey(taskName, parallelExecutionIndex));
    }

    public String getRegistrant() {
        return registrant;
    }

    public void setRegistrant(String registrant) {
        this.registrant = registrant;
    }

    public List<TaskStateMachineContextBase> getParallelTaskStateMachineContextsByName(String taskName, int parallelCount) {
        final List<TaskStateMachineContextBase> taskSMs = new ArrayList<>();

        for (int index = 0; index < parallelCount; index++) {
            final TaskKey key = new TaskKey(taskName, index);
            final TaskStateMachineContextBase task = taskExecutionDataMap.get(key);
            if (task != null) {
                taskSMs.add(task);
            }
        }
        return taskSMs;
    }

    /*
     * For TestNG test only
     */
    @VisibleForTesting
    TaskStateMachineContextBase getTaskStateMachine(String taskName, int parallelExecutionIndex) {
        return taskExecutionDataMap.get(new TaskKey(taskName, parallelExecutionIndex));
    }
}
