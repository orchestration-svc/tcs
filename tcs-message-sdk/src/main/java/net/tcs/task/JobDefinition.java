package net.tcs.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_NULL)
public class JobDefinition implements JobSpec {

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("JobDefinition [jobName=" + jobName + "]");
        sb.append(System.lineSeparator());
        sb.append("------------------------------------------------------------");
        sb.append(System.lineSeparator());
        for (final TaskDefinition taskDef : taskMap.values()) {
            sb.append(taskDef.toString());
        }
        return sb.toString();
    }

    private String jobName;
    private final Map<String, TaskDefinition> taskMap = new HashMap<>();
    private String registrant;

    private final Map<String, List<String>> steps = new HashMap<>();

    private final Map<String, String> taskToStepMap = new HashMap<>();

    public JobDefinition() {
    }

    public JobDefinition(String jobName) {
        this.jobName = jobName;
    }

    @Override
    public String getJobName() {
        return jobName;
    }

    @Override
    @JsonIgnore
    public JobSpec addTask(String taskName, String taskExecutionTarget, Set<String> parentTasks) {
        if (taskMap.containsKey(taskName)) {
            throw new IllegalArgumentException("Task: " + taskName + " already exists");
        }

        if (parentTasks != null) {
            for (final String parentTask : parentTasks) {
                if (!taskMap.containsKey(parentTask)) {
                    throw new IllegalArgumentException("Parent Task: " + parentTask + " does not exist");
                }
            }
        }
        final TaskDefinition task = new TaskDefinition(taskName, taskExecutionTarget);
        taskMap.put(taskName, task);
        task.setParents(parentTasks);
        return this;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public Map<String, TaskDefinition> getTaskMap() {
        final Map<String, TaskDefinition> map = new HashMap<>();
        map.putAll(taskMap);
        return map;
    }

    public void setTaskMap(HashMap<String, TaskDefinition> taskMap) {
        this.taskMap.clear();
        this.taskMap.putAll(taskMap);
    }

    public Map<String, List<String>> getSteps() {
        final Map<String, List<String>> steps = new HashMap<>();
        steps.putAll(this.steps);
        return steps;
    }

    public void setSteps(Map<String, List<String>> steps) {
        this.steps.clear();
        this.steps.putAll(steps);
    }

    @Override
    @JsonIgnore
    public Set<String> getTasks() {
        return taskMap.keySet();
    }

    public String getRegistrant() {
        return registrant;
    }

    public void setRegistrant(String registrant) {
        this.registrant = registrant;
    }

    @Override
    @JsonIgnore
    public TaskSpec getTaskSpec(String taskName) {
        return taskMap.get(taskName);
    }

    @JsonIgnore
    @Override
    public void markStep(String stepName, List<String> tasks) {
        if (steps.containsKey(stepName)) {
            throw new IllegalArgumentException(stepName + " already registered");
        }

        for (final String task : tasks) {
            if (taskToStepMap.containsKey(task)) {
                throw new IllegalArgumentException(task + " already marked with step: " + taskToStepMap.get(task));
            }
        }

        for (int i = 1; i < tasks.size(); i++) {
            final String task = tasks.get(i);
            final TaskDefinition taskDef = taskMap.get(task);
            if (taskDef == null) {
                throw new IllegalArgumentException("Task is not aded to JobSpec: " + task);
            }

            final Set<String> predecessors = taskDef.getParents();
            if (predecessors.isEmpty()) {
                throw new IllegalArgumentException("Task: " + task + " has no predecessors");
            }

            if (predecessors.size() > 1) {
                throw new IllegalArgumentException("Task: " + task + " has more than one predecessor");
            }

            if (!predecessors.contains(tasks.get(i - 1))) {
                throw new IllegalArgumentException(
                        "Task: " + task + " must have " + tasks.get(i - 1) + " as a predecessor");
            }
        }

        final List<String> associatedTasks = new ArrayList<>(tasks);
        steps.put(stepName, associatedTasks);

        for (final String task : tasks) {
            taskToStepMap.put(task, stepName);
        }
    }

    @JsonIgnore
    public void validateSteps() {
        if (steps == null || steps.isEmpty()) {
            return;
        }

        taskToStepMap.clear();

        for (final Entry<String, List<String>> entry : steps.entrySet()) {
            for (final String task : entry.getValue()) {
                if (taskToStepMap.containsKey(task)) {
                    throw new IllegalArgumentException(
                            task + " already marked with step: " + taskToStepMap.get(task));
                }
                taskToStepMap.put(task, entry.getKey());
            }
        }

        for (final List<String> tasks : steps.values()) {

            for (int i = 1; i < tasks.size(); i++) {
                final String task = tasks.get(i);
                final TaskDefinition taskDef = taskMap.get(task);
                if (taskDef == null) {
                    throw new IllegalArgumentException("Task is not aded to JobSpec: " + task);
                }

                final Set<String> predecessors = taskDef.getParents();
                if (predecessors.isEmpty()) {
                    throw new IllegalArgumentException("Task: " + task + " has no predecessors");
                }

                if (predecessors.size() > 1) {
                    throw new IllegalArgumentException("Task: " + task + " has more than one predecessor");
                }

                if (!predecessors.contains(tasks.get(i - 1))) {
                    throw new IllegalArgumentException(
                            "Task: " + task + " must have " + tasks.get(i - 1) + " as a predecessor");
                }
            }
        }
    }

    @JsonIgnore
    public final String getStep(final String task) {
        if (taskToStepMap.containsKey(task)) {
            return taskToStepMap.get(task);
        } else {
            return "";
        }
    }

    @JsonIgnore
    public final boolean isFirstTask(final String task, final String step) {
        final List<String> tasks = steps.get(step);
        if (tasks == null || tasks.isEmpty()) {
            return false;
        }

        return (task.equals(tasks.get(0)));
    }

    @JsonIgnore
    public List<String> getTasksForStep(String step) {
        final List<String> tasks = new ArrayList<>(steps.get(step));
        return tasks;
    }

    @JsonIgnore
    public boolean hasSteps() {
        return (!steps.isEmpty());
    }
}
