package com.task.coordinator.dto;

import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import net.tcs.task.TaskDefinition;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(Include.NON_NULL)
@JsonPropertyOrder({ "taskName", "taskExecutionURI", "predecessors" })
public class TaskDefinitionDTO {
    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String name) {
        this.taskName = name;
    }

    public String getStepName() {
        return stepName;
    }

    public void setStepName(String stepName) {
        this.stepName = stepName;
    }

    public Set<String> getPredecessors() {
        return new HashSet<>(predecessors);
    }

    public void setPredecessors(Set<String> parents) {
        this.predecessors.clear();
        this.predecessors.addAll(parents);
    }

    public String getTaskExecutionURI() {
        return taskExecutionURI;
    }

    public void setTaskExecutionURI(String taskExecutionURI) {
        this.taskExecutionURI = taskExecutionURI;
    }

    public TaskDefinitionDTO() {
    }

    public TaskDefinitionDTO(TaskDefinition taskDef) {
        taskName = taskDef.getTaskName();
        taskExecutionURI = taskDef.getTaskExecutionTarget();
        predecessors.addAll(taskDef.getParents());
    }

    private String taskName;

    private String stepName;

    private final Set<String> predecessors = new HashSet<>();

    private String taskExecutionURI;
}
