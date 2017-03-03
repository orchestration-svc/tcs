package com.task.coordinator.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import net.tcs.task.JobDefinition;
import net.tcs.task.TaskDefinition;

public class JobDefinitionTopologicalSorter {

    private static final class TaskNode {
        private final String name;
        private final Set<String> successors = new HashSet<>();

        public TaskNode(TaskDefinition task) {
            this.name = task.getTaskName();
        }
    }

    private final Map<String, TaskNode> taskNodes = new HashMap<>();
    private final Map<String, TaskDefinition> taskDefMap;
    private final Stack<String> topological = new Stack<>();
    private final Set<String> visited = new HashSet<>();

    public JobDefinitionTopologicalSorter(JobDefinition job) {
        taskDefMap = job.getTaskMap();
    }

    /**
     * Return task-names sorted topologically
     *
     * @return
     */
    public List<String> topologicalSort() {

        for (final TaskDefinition task : taskDefMap.values()) {
            taskNodes.put(task.getTaskName(), new TaskNode(task));
        }

        for (final TaskDefinition task : taskDefMap.values()) {
            final TaskNode curNode = taskNodes.get(task.getTaskName());
            final Set<String> preds = task.getParents();
            if (!preds.isEmpty()) {
                for (final String pred : preds) {
                    final TaskNode tNode = taskNodes.get(pred);
                    tNode.successors.add(curNode.name);
                }
            }
        }

        final Stack<String> topo = traverseDFS(taskNodes.keySet());

        final List<String> topoList = new ArrayList<>();
        while (!topo.isEmpty()) {
            topoList.add(topo.pop());
        }
        return topoList;
    }

    public Stack<String> traverseDFS(Set<String> taskNodes) {
        for (final String taskNode : taskNodes) {
            if (!visited.contains(taskNode)) {
                dfs(taskNode);
            }
        }

        return topological;
    }

    private void dfs(final String currentTaskName) {
        visited.add(currentTaskName);

        final TaskNode current = taskNodes.get(currentTaskName);
        for (final String successor : current.successors) {

            if (!visited.contains(successor)) {
                dfs(successor);
            }
        }
        topological.push(currentTaskName);
    }
}
