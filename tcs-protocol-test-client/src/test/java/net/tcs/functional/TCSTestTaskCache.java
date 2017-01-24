package net.tcs.functional;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.testng.Assert;

import net.tcs.task.ParentTaskInfo;
import net.tcs.task.ParentTaskOutput;
import net.tcs.task.TaskSpec;

public class TCSTestTaskCache {

    /**
     * Map of {TaskKey (jobName:taskName) => TaskInput}
     */
    private final ConcurrentMap<String, String> taskInputMap = new ConcurrentHashMap<>();

    /**
     * Map of {TaskKey (jobName:taskName) => TaskOutput}
     */
    private final ConcurrentMap<String, String> taskOutputMap = new ConcurrentHashMap<>();

    private static String getTaskKey(String jobName, String jobId, String taskName, int parallelExecutionIndex) {
        return String.format("%s:%s:%s:%d", jobName, jobId, taskName, parallelExecutionIndex);
    }

    public void saveTaskInput(String jobName, String jobId, String taskName, String taskInput,
            int parallelExecutionIndex) {
        final String key = getTaskKey(jobName, jobId, taskName, parallelExecutionIndex);
        taskInputMap.put(key, taskInput);
    }

    public void saveTaskOutput(String jobName, String jobId, String taskName, String taskOutput,
            int parallelExecutionIndex) {
        final String key = getTaskKey(jobName, jobId, taskName, parallelExecutionIndex);
        taskOutputMap.put(key, taskOutput);
    }

    public void verifyTaskInput(String jobName, String jobId, String taskName, String taskInput,
            int parallelExecutionIndex) {
        final String key = getTaskKey(jobName, jobId, taskName, parallelExecutionIndex);
        for (int i = 0; i < 5; i++) {
            final String val = taskInputMap.get(key);
            if (val != null) {
                Assert.assertEquals(taskInput, val, "Task input does not match");
                return;
            } else {
                try {
                    Thread.sleep(1000);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        Assert.assertEquals(taskInput, taskInputMap.get(key), "Task input does not match");
    }

    public void verifyParentTaskOutput(String jobName, String jobId, String taskName, int parallelExecutionIndex,
            TaskSpec taskSpec,
            ParentTaskOutput parentTasksOutput) {
        final Set<String> parentTaskNames = parentTasksOutput.getPredecessors();

        Assert.assertEquals(parentTaskNames, taskSpec.getParents(), "Parents list does not match");
        for (final String parent : parentTaskNames) {

            final ParentTaskInfo parentTaskInfo = parentTasksOutput.getPredecessorTaskInfo(parent);
            final String key = getTaskKey(jobName, jobId, parent, parallelExecutionIndex);

            Assert.assertEquals(parentTaskInfo.getOutput(), taskOutputMap.get(key),
                    "Parent task output does not match");
        }
    }

    public void cleanup() {
        taskInputMap.clear();
        taskOutputMap.clear();
    }
}
