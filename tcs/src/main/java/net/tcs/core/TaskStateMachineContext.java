package net.tcs.core;

import net.tcs.db.TaskInstanceDAO;
import net.tcs.state.TaskState;
import net.tcs.task.TaskDefinition;

/**
 * In-memory representation of a running task instance.
 */
public class TaskStateMachineContext extends TaskStateMachineContextBase {

    private TaskState taskState;

    public TaskStateMachineContext(TaskDefinition taskDef) {
        super(taskDef);
        for (final String parent : taskDef.getParents()) {
            this.predecessors.add(new TaskKey(parent, 0));
        }
        this.taskState = TaskState.INIT;
    }

    /**
     * This constructor is used to create TaskStateMachineContext instances
     * during step parallelization.
     *
     * @param task
     *            task at parallelIndex-0
     * @param taskInstanceId
     * @param parallelIndex
     * @param inheritPredecessors
     *            true for first task in the step, false otherwise
     */
    public TaskStateMachineContext(TaskStateMachineContextBase task, String taskInstanceId, int parallelIndex,
            boolean inheritPredecessors) {
        super(task, taskInstanceId, parallelIndex);
        this.taskState = TaskState.INIT;

        if (inheritPredecessors) {
            this.predecessors.addAll(task.predecessors);
        }
    }

    @Override
    public boolean isReadyToExecute() {
        return (taskState == TaskState.INIT);
    }

    @Override
    public boolean isComplete() {
        return (taskState == TaskState.COMPLETE);
    }

    @Override
    public void markInProgress() {
        taskState = TaskState.INPROGRESS;
    }

    @Override
    public void markComplete() {
        taskState = TaskState.COMPLETE;
    }

    @Override
    public void initializeFromDB(TaskInstanceDAO taskDAO) {
        taskState = TaskState.get(taskDAO.getState());
    }

    @Override
    public void revertMarkInProgress() {
        taskState = TaskState.INIT;
    }

    @Override
    public boolean isInProgress() {
        return (taskState == TaskState.INPROGRESS);
    }
}
