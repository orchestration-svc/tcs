package com.task.coordinator.dto;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import net.tcs.task.JobDefinition;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(Include.NON_NULL)
@JsonPropertyOrder({ "jobName", "tasks", "steps" })
public class JobDefinitionDTO {
    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public List<TaskDefinitionDTO> getTasks() {
        return new ArrayList<>(tasks);
    }

    public void setTasks(List<TaskDefinitionDTO> tasks) {
        this.tasks.clear();
        this.tasks.addAll(tasks);
    }

    public Map<String, List<String>> getSteps() {
        return new HashMap<>(steps);
    }

    public void setSteps(Map<String, List<String>> steps) {
        this.steps.clear();
        this.steps.putAll(steps);
    }

    @JsonIgnore
    public void addTask(TaskDefinitionDTO task) {
        this.tasks.add(task);
    }

    public JobDefinitionDTO() {
    }

    public JobDefinitionDTO(JobDefinition job) {
        jobName = job.getJobName();
        steps.putAll(job.getSteps());
    }

    private String jobName;

    private final List<TaskDefinitionDTO> tasks = new ArrayList<>();

    private final Map<String, List<String>> steps = new HashMap<>();
}
