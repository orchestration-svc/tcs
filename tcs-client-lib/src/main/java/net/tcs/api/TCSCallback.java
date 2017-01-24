package net.tcs.api;

import java.util.Map;

import net.tcs.task.ParentTaskOutput;

/**
 * A TCS client must implement this interface to receive Task/Job related
 * notifications from TCS.
 */
public interface TCSCallback {
    /**
     * Called by Task Coordination Service (TCS) to start a Task.
     *
     * @param taskExecutionContext
     * @param taskInput
     * @param parentTasksOutput
     * @param jobContext
     */
    public void startTask(TCSTaskContext taskExecutionContext, byte[] taskInput, ParentTaskOutput parentTasksOutput,
            Map<String, String> jobContext);

    /**
     * Called by Task Coordination Service (TCS) to roll-back a previously
     * succeeded Task.
     *
     * @param taskExecutionContext
     * @param taskOutput
     */
    public void rollbackTask(TCSTaskContext taskExecutionContext, byte[] taskOutput);
}
