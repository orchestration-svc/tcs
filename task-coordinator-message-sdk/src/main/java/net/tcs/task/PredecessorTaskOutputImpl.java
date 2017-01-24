package net.tcs.task;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class PredecessorTaskOutputImpl implements ParentTaskOutput {
    public PredecessorTaskOutputImpl(Map<String, ParentTaskInfo> parentTaskOutput) {
        this.parentTaskOutput = parentTaskOutput;
    }

    public PredecessorTaskOutputImpl() {
    }

    public Map<String, ParentTaskInfo> getParentTasks() {
        return parentTaskOutput;
    }

    public void setParentTasks(Map<String, ParentTaskInfo> parentTasks) {
        this.parentTaskOutput = parentTasks;
    }

    private Map<String, ParentTaskInfo> parentTaskOutput;

    @JsonIgnore
    @Override
    public Set<String> getPredecessors() {
        if (parentTaskOutput != null) {
            return parentTaskOutput.keySet();
        } else {
            return Collections.emptySet();
        }
    }

    @JsonIgnore
    @Override
    public ParentTaskInfo getPredecessorTaskInfo(String parentTaskName) {
        if (parentTaskOutput != null) {
            return parentTaskOutput.get(parentTaskName);
        } else {
            return null;
        }
    }
}
