package net.tcs.task;

import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_NULL)
public class TaskDefinition implements TaskSpec {

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("   TaskName:            " + taskName);
        sb.append(System.lineSeparator());
        sb.append("   TaskExecutionTarget: " + taskExecutionTarget);
        sb.append(System.lineSeparator());

        sb.append("   Predecessors:        [ ");
        if (!parents.isEmpty()) {
            for (final String parent : parents) {
                sb.append(parent + "  ");
            }
        }
        sb.append("]");
        sb.append(System.lineSeparator());
        sb.append("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        sb.append(System.lineSeparator());
        return sb.toString();
    }

    private String taskName;
    private String taskExecutionTarget;
    private final Set<String> parents = new HashSet<String>();

    public TaskDefinition(String taskName, String taskExecutionTarget) {
        this.taskName = taskName;
        this.taskExecutionTarget = taskExecutionTarget;
    }

    public TaskDefinition() {

    }

    @Override
    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    @Override
    public String getTaskExecutionTarget() {
        return taskExecutionTarget;
    }

    public void setTaskExecutionTarget(String taskExecutionTarget) {
        this.taskExecutionTarget = taskExecutionTarget;
    }

    @Override
    public Set<String> getParents() {
        final Set<String> pars = new HashSet<String>();
        pars.addAll(parents);
        return pars;
    }

    public void setParents(Set<String> parents) {
        if (parents != null) {
            this.parents.clear();
            this.parents.addAll(parents);
        }
    }
}
