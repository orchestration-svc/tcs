package net.tcs.task;

import java.util.Map;

import net.tcs.task.ParentTaskInfo;

public class TCSTaskInput {
    public String getTaskInput() {
        return taskInput;
    }

    public Map<String, ParentTaskInfo> getParentTaskOutput() {
        return parentTaskOutput;
    }

    public TCSTaskInput(String taskInput,
            Map<String, ParentTaskInfo> parentTaskOutput) {
        this.taskInput = taskInput;
        this.parentTaskOutput = parentTaskOutput;
    }

    private final String taskInput;

    private final Map<String, ParentTaskInfo> parentTaskOutput;
}
