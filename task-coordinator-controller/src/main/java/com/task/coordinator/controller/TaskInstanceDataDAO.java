package com.task.coordinator.controller;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "TaskInstanceData")
public class TaskInstanceDataDAO {

    public TaskInstanceDataDAO() {
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getTaskInput() {
        return taskInput;
    }

    public void setTaskInput(String taskInput) {
        this.taskInput = taskInput;
    }

    public String getTaskOutput() {
        return taskOutput;
    }

    public void setTaskOutput(String taskOutput) {
        this.taskOutput = taskOutput;
    }

    @Id
    @Column(name = "instanceId")
    private String instanceId;

    @Column(name = "taskinput", columnDefinition = "TEXT")
    private String taskInput;

    @Column(name = "taskoutput", columnDefinition = "TEXT")
    private String taskOutput;
}
