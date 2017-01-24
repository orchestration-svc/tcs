package net.tcs.core;

import java.util.Set;

import net.tcs.db.TaskInstanceDAO;
import net.tcs.state.TaskRollbackState;
import net.tcs.task.TaskDefinition;

/**
 * In-memory representation of a task instance processing Rollback
 */
public class TaskRollbackStateMachineContext extends TaskStateMachineContextBase {

    private TaskRollbackState taskState;

    public TaskRollbackStateMachineContext(TaskDefinition taskDef, Set<TaskKey> completedTasks) {
        super(taskDef);

        /*
         * Reverse-DAG with only completed task nodes.
         */
        final Set<String> predecessorNames = taskDef.getParents();
        for (final TaskKey completedTask : completedTasks) {
            if (predecessorNames.contains(completedTask.taskName)) {
                this.successors.add(completedTask);
            }
        }

        this.taskState = TaskRollbackState.ROLLBACK_READY;
    }

    @Override
    public boolean isReadyToExecute() {
        return (taskState == TaskRollbackState.ROLLBACK_READY);
    }

    @Override
    public boolean isComplete() {
        return (taskState == TaskRollbackState.ROLLBACK_COMPLETE || taskState == TaskRollbackState.ROLLBACK_DISALLOWED);
    }

    @Override
    public void markInProgress() {
        taskState = TaskRollbackState.ROLLBACK_INPROGRESS;
    }

    @Override
    public void markComplete() {
        taskState = TaskRollbackState.ROLLBACK_COMPLETE;
    }

    @Override
    public void initializeFromDB(TaskInstanceDAO taskDAO) {
        taskState = TaskRollbackState.get(taskDAO.getRollbackState());
    }

    @Override
    public void revertMarkInProgress() {
        taskState = TaskRollbackState.ROLLBACK_READY;
    }

    @Override
    public boolean isInProgress() {
        return (taskState == TaskRollbackState.ROLLBACK_INPROGRESS);
    }
}
