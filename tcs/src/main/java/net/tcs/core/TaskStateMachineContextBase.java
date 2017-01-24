package net.tcs.core;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import net.tcs.db.TaskInstanceDAO;
import net.tcs.task.TaskDefinition;

/**
 * In-memory representation of a running task instance.
 */
public abstract class TaskStateMachineContextBase {

    public String getTaskExecutionTarget() {
        return taskExecutionTarget;
    }

    public String getName() {
        return name;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public int getParallelExecutionIndex() {
        return parallelExecutionIndex;
    }

    public void setParallelExecutionIndex(int parallelExecutionIndex) {
        this.parallelExecutionIndex = parallelExecutionIndex;
    }

    public boolean isRootTask() {
        return predecessors.isEmpty();
    }

    public String getStep() {
        return step;
    }

    public void setStep(String step) {
        this.step = step;
    }

    public boolean isFirstTaskInStep() {
        return firstTaskInStep;
    }

    public void setFirstTaskInStep(boolean firstTaskInStep) {
        this.firstTaskInStep = firstTaskInStep;
    }

    public TaskStateMachineContextBase(TaskDefinition taskDef) {
        this.name = taskDef.getTaskName();
        this.taskExecutionTarget = taskDef.getTaskExecutionTarget();
    }

    /**
     * This constructor is used to create TaskStateMachineContext instances
     * during step parallelization.
     *
     * @param task
     * @param taskInstanceId
     * @param parallelIndex
     */
    public TaskStateMachineContextBase(TaskStateMachineContextBase task, String taskInstanceId, int parallelIndex) {
        this.name = task.name;
        this.instanceId = taskInstanceId;
        this.taskExecutionTarget = task.taskExecutionTarget;
        this.parallelExecutionIndex = parallelIndex;
    }

    public abstract void initializeFromDB(TaskInstanceDAO taskDAO);

    public abstract boolean isReadyToExecute();

    public abstract boolean isComplete();

    public abstract boolean isInProgress();

    public abstract void markInProgress();

    public abstract void revertMarkInProgress();

    public abstract void markComplete();

    protected String instanceId;

    protected int parallelExecutionIndex = 0;

    protected final String name;

    private String step = StringUtils.EMPTY;

    private boolean firstTaskInStep = false;

    protected String taskExecutionTarget;

    protected final Set<TaskKey> predecessors = new HashSet<>();

    protected final Set<TaskKey> successors = new HashSet<>();
}
