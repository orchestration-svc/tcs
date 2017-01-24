package net.tcs.core;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import net.tcs.task.JobDefinition;
import net.tcs.task.TaskDefinition;

public class JobDefinitionCycleDetector {

    private final Map<String, TaskDefinition> taskMap;

    public JobDefinitionCycleDetector(JobDefinition jobSpec) {
        taskMap = jobSpec.getTaskMap();
    }

    public boolean detectCycle() {

        for (final TaskDefinition task : taskMap.values()) {

            final Set<String> visited = new HashSet<>();
            final Stack<String> path = new Stack<>();

            if (hasCycle(task.getTaskName(), task.getTaskName(), visited, path)) {
                return true;
            }
        }

        return false;
    }

    private boolean hasCycle(final String curNode, final String sourceNode, Set<String> visited,
            final Stack<String> path) {

        visited.add(curNode);
        path.push(curNode);
        final TaskDefinition curTask = taskMap.get(curNode);

        for (final String parentTaskName : curTask.getParents()) {
            if (path.contains(parentTaskName))
                return true;

            if (!visited.contains(parentTaskName)) {
                if (hasCycle(parentTaskName, curNode, visited, path)) {
                    return true;
                }
            }
        }
        path.pop();
        return false;
    }
}
