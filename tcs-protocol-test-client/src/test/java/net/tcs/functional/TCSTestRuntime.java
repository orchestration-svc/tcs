package net.tcs.functional;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;

import com.task.coordinator.message.utils.TCSConstants;

import net.tcs.api.TCSCallback;
import net.tcs.api.TCSClient;
import net.tcs.api.TCSJobHandler;
import net.tcs.api.TCSTaskContext;
import net.tcs.task.JobDefinition;
import net.tcs.task.JobSpec;
import net.tcs.task.ParentTaskOutput;
import net.tcs.task.TaskSpec;

public class TCSTestRuntime {

    private static volatile TaskNotificationCallback taskNotifCallback;

    public static void registerTaskNotifCallback(TaskNotificationCallback taskNotifCallback) {
        TCSTestRuntime.taskNotifCallback = taskNotifCallback;
    }

    private static final ConcurrentMap<String, TCSTaskContext> inProgressTasks = new ConcurrentHashMap<>();

    private static final ConcurrentMap<String, String> jobStatusMap = new ConcurrentHashMap<>();

    private static final ConcurrentMap<String, Boolean> failTaskMap = new ConcurrentHashMap<>();

    private static final ConcurrentMap<String, Integer> retryTaskMap = new ConcurrentHashMap<>();

    private static final TCSJobHandler jobHandler = new TCSJobHandler() {

        @Override
        public void jobComplete(String jobId) {
            System.out.println("Job complete: JobId: " + jobId);
            jobStatusMap.putIfAbsent(jobId, "COMPLETE");
        }

        @Override
        public void jobFailed(String jobId) {
            System.out.println("Job failed: JobId: " + jobId);
            jobStatusMap.putIfAbsent(jobId, "FAILED");
        }

        @Override
        public void jobRollbackComplete(String jobId) {
            System.out.println("Job rolled-back: JobId: " + jobId);
            jobStatusMap.putIfAbsent(jobId, "ROLLBACK_COMPLETE");
        }

        @Override
        public void jobRollbackFailed(String jobId) {
        }
    };

    private static final class TCSTestCallback implements TCSCallback {

        public TCSTestCallback(BlockingQueue<TCSTaskContext> taskQ, TCSTestTaskCache taskCache, TCSTestJobKVContextCache jobKVContextCache,
                TCSClient tcsClientRuntime) {
            super();
            this.taskQ = taskQ;
            this.taskCache = taskCache;
            this.jobKVContextCache = jobKVContextCache;
            this.tcsClientRuntime = tcsClientRuntime;
        }

        @Override
        public void startTask(TCSTaskContext taskContext, byte[] taskInput, ParentTaskOutput parentTasksOutput,
                Map<String, String> jobContext) {

            System.out.println("Received Begin Task: JobName: " + taskContext.getJobName() + "   TaskName: "
                    + taskContext.getTaskName() + "    ParallelIndex: " + taskContext.getTaskParallelExecutionIndex()
                    + "   Shard: " + taskContext.getShardId() + "   RetryCount: "
                    + taskContext.getTaskRetryCount());

            final String taskKey = getTaskKey(taskContext.getJobName(), taskContext.getTaskName());
            if (retryTaskMap.containsKey(taskKey)) {
                final int retryCount = taskContext.getTaskRetryCount();
                retryTaskMap.replace(taskKey, retryCount);
                if (retryCount < 2) {
                    /*
                     * Drop the task, which will trigger task-retry
                     */
                    return;
                }
            }

            try {
                final String input = new String(taskInput);
                taskCache.verifyTaskInput(taskContext.getJobName(), taskContext.getJobId(), taskContext.getTaskName(),
                        input, taskContext.getTaskParallelExecutionIndex());

                final JobSpec jobSpec = jobspecMap.get(taskContext.getJobName());
                final TaskSpec taskSpec = jobSpec.getTaskSpec(taskContext.getTaskName());

                final JobDefinition jobDef = (JobDefinition) jobSpec;

                /*
                 * REVISIT TODO, assert ParentTaskOutput for tasks that belong
                 * to step.
                 */
                if (!jobDef.hasSteps()) {
                    taskCache.verifyParentTaskOutput(taskContext.getJobName(), taskContext.getJobId(),
                            taskContext.getTaskName(), taskContext.getTaskParallelExecutionIndex(), taskSpec,
                            parentTasksOutput);
                }

                if (!jobContext.isEmpty()) {
                    jobKVContextCache.checkKVInput(taskContext.getJobId(), jobContext);
                }

                inProgressTasks.put(taskContext.getTaskId(), taskContext);
                taskQ.add(taskContext);
            } catch (final AssertionError ex) {
                System.out.println("Assertion error in startTask" + ex.toString());
                ex.printStackTrace();
            }
        }

        @Override
        public void rollbackTask(TCSTaskContext taskContext, byte[] taskOutput) {
            try {
                Thread.sleep(50);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            tcsClientRuntime.taskRollbackComplete(taskContext);
            if (taskNotifCallback != null) {
                taskNotifCallback.taskRolledBack(taskContext.getTaskId());
            }
        }

        private final BlockingQueue<TCSTaskContext> taskQ;
        private final TCSTestTaskCache taskCache;
        private final TCSTestJobKVContextCache jobKVContextCache;
        private final TCSClient tcsClientRuntime;
    };

    private TCSTestCallback callback;

    private static final class TaskWorker implements Runnable {

        public TaskWorker(BlockingQueue<TCSTaskContext> taskQ, TCSClient tcsClientRuntime, TCSTestTaskCache taskCache,
                TCSTestJobKVContextCache jobKVContextCache) {
            super();
            this.taskQ = taskQ;
            this.tcsClientRuntime = tcsClientRuntime;
            this.taskCache = taskCache;
            this.jobKVContextCache = jobKVContextCache;
        }

        private final BlockingQueue<TCSTaskContext> taskQ;
        private final TCSClient tcsClientRuntime;
        private final TCSTestTaskCache taskCache;
        private final TCSTestJobKVContextCache jobKVContextCache;

        volatile boolean stop = false;
        private final Random r = new Random();

        @Override
        public void run() {
            while (!stop) {
                try {
                    final TCSTaskContext taskContext = taskQ.poll(5, TimeUnit.SECONDS);
                    if (taskContext != null) {
                        final int randval = r.nextInt(5);
                        if (randval == 3) {
                            Thread.sleep(600);
                        } else if (randval == 1) {
                            Thread.sleep(1200);
                        }
                        Thread.sleep(r.nextInt(50) + 50);
                        System.out.println("Sending task complete notification: " + taskContext.getJobName() + "  "
                                + taskContext.getTaskName());

                        final String taskKey = getTaskKey(taskContext.getJobName(), taskContext.getTaskName());
                        if (failTaskMap.containsKey(taskKey)) {
                            tcsClientRuntime.taskFailed(taskContext, RandomStringUtils.randomAlphabetic(32).getBytes());

                            if (taskNotifCallback != null) {
                                taskNotifCallback.taskFailed(taskContext.getTaskId());
                            }
                        } else {
                            final String taskOutput = RandomStringUtils.randomAlphabetic(32);
                            taskCache.saveTaskOutput(taskContext.getJobName(), taskContext.getJobId(),
                                    taskContext.getTaskName(), taskOutput, taskContext.getTaskParallelExecutionIndex());

                            final Map<String, String> jobContextKV = new HashMap<>();
                            if (r.nextBoolean()) {
                                for (int kvi = 0; kvi < 3; kvi++) {
                                    jobContextKV.put(taskContext.getTaskId() + RandomStringUtils.randomAlphabetic(8),
                                            RandomStringUtils.randomAlphabetic(16));
                                }
                                jobKVContextCache.saveKVOutput(taskContext.getJobId(), jobContextKV);
                                tcsClientRuntime.taskComplete(taskContext, taskOutput.getBytes(), jobContextKV);
                            } else {
                                tcsClientRuntime.taskComplete(taskContext, taskOutput.getBytes());
                            }

                            if (taskNotifCallback != null) {
                                taskNotifCallback.taskComplete(taskContext.getTaskId());
                            }
                        }
                        inProgressTasks.remove(taskContext.getTaskId());
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (final Exception ex) {

                }
            }
        }
    }

    private static final class InProgressTaskPinger implements Runnable {

        public InProgressTaskPinger(TCSClient tcsClientRuntime) {
            super();
            this.tcsClientRuntime = tcsClientRuntime;
        }

        volatile boolean stop = false;

        @Override
        public void run() {
            while (!stop) {
                try {
                    Thread.sleep(2000);
                    final Collection<TCSTaskContext> taskContexts = inProgressTasks.values();
                    for (final TCSTaskContext task : taskContexts) {
                        tcsClientRuntime.taskInProgress(task);
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (final Exception ex) {

                }
            }
        }

        private final TCSClient tcsClientRuntime;
    }

    private final BlockingQueue<TCSTaskContext> taskQ = new LinkedBlockingQueue<>();
    private TCSClient tcsClientRuntime;
    private ExecutorService executor;
    private final List<TaskWorker> workers = new ArrayList<>();

    private InProgressTaskPinger taskPinger;

    private static final Map<String, JobSpec> jobspecMap = new HashMap<>();

    private final TCSTestTaskCache taskCache = new TCSTestTaskCache();

    private final TCSTestJobKVContextCache jobKVContextCache = new TCSTestJobKVContextCache();

    private final Random r = new Random();

    public void initialize(TCSClient tcsClientRuntime) throws IOException {
        this.tcsClientRuntime = tcsClientRuntime;

        taskPinger = new InProgressTaskPinger(tcsClientRuntime);
        callback = new TCSTestCallback(taskQ, taskCache, jobKVContextCache, tcsClientRuntime);
        executor = Executors.newCachedThreadPool(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "TCSDispatcher");
            }
        });

        executor.submit(taskPinger);
        for (int i = 0; i < 8; i++) {
            final TaskWorker dispatcher = new TaskWorker(taskQ, tcsClientRuntime, taskCache, jobKVContextCache);
            workers.add(dispatcher);
            executor.submit(dispatcher);
        }
    }

    public void failTask(String jobName, String taskName) {
        failTaskMap.put(getTaskKey(jobName, taskName), true);
    }

    public void retryTask(String jobName, String taskName) {
        retryTaskMap.put(getTaskKey(jobName, taskName), -1);
    }

    public void verifyTaskRetry() {
        for (final Entry<String, Integer> entry : retryTaskMap.entrySet()) {
            Assert.assertTrue(entry.getValue() > 0,
                    "Task may not have been retried: retryCount: " + entry.getValue() + "  Task: " + entry.getKey());
        }
    }

    public void cleanupData() {
        taskQ.clear();
        inProgressTasks.clear();
        jobStatusMap.clear();
        taskCache.cleanup();
        jobKVContextCache.cleanup();
        failTaskMap.clear();
        retryTaskMap.clear();
    }

    public void cleanup() {

        for (final TaskWorker worker : workers) {
            worker.stop = true;
        }
        taskPinger.stop = true;

        try {
            Thread.sleep(5000);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        executor.shutdown();
        try {
            executor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        cleanupData();
    }

    public String waitForJobStatus(String jobId, int count) {
        for (int i = 0; i < count; i++) {

            final String status = jobStatusMap.get(jobId);
            if (status != null) {
                return status;
            }

            try {
                Thread.sleep(3000);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        System.err.println("Job status could not be collected after 30 secs. JobId: " + jobId);
        throw new RuntimeException("Job status could not be collected after 30 secs JobId: " + jobId);
    }

    private void prepareForTaskExecution(String job) {
        final JobSpec jobSpec = tcsClientRuntime.queryRegisteredJob(job);
        if (jobSpec != null) {
            jobspecMap.put(job, jobSpec);

            final Map<String, TCSCallback> map = new HashMap<>();
            final Set<String> tasks = jobSpec.getTasks();
            System.out.println("Task names for Job: " + job + Arrays.toString(tasks.toArray(new String[tasks.size()])));
            for (final String task : tasks) {
                map.put(task, callback);
            }

            tcsClientRuntime.prepareToExecute(job, jobHandler, map);
        }
    }

    public List<String> executeJob(String jobName, int count) {
        final List<String> jobIds = new ArrayList<>();
        prepareForTaskExecution(jobName);
        for (int i = 0; i < count; i++) {
            final JobSpec jobspec = jobspecMap.get(jobName);

            final JobDefinition jobDef = (JobDefinition) jobspec;
            jobDef.validateSteps();

            final Set<String> tasks = jobspec.getTasks();
            final Map<String, byte[]> taskInput = new HashMap<>();
            for (final String task : tasks) {

                final String input = RandomStringUtils.randomAlphabetic(32);
                taskInput.put(task, input.getBytes());
            }

            final Map<String, String> jobContextKV = new HashMap<>();

            if (jobDef.hasSteps()) {
                final Map<String, List<String>> stepToTaskMap = jobDef.getSteps();
                for (final String step : stepToTaskMap.keySet()) {
                    final String key = String.format(TCSConstants.STEP_PARALLEL_COUNT_KEY_FORMAT, step);
                    jobContextKV.put(key, "3");
                }
            }

            if (r.nextBoolean()) {
                for (int kvi = 0; kvi < 3; kvi++) {
                    jobContextKV.put(RandomStringUtils.randomAlphabetic(8), RandomStringUtils.randomAlphabetic(16));
                }
            }

            final String jobId;

            if (!jobContextKV.isEmpty()) {
                jobId = tcsClientRuntime.startJob(jobName, taskInput, jobContextKV);
            } else {
                jobId = tcsClientRuntime.startJob(jobName, taskInput);
            }

            jobKVContextCache.registerAndSaveKVOutput(jobId, jobContextKV);
            for (final Entry<String, byte[]> entry : taskInput.entrySet()) {
                final String taskName = entry.getKey();
                final String step = jobDef.getStep(taskName);
                if (StringUtils.isEmpty(step)) {
                    taskCache.saveTaskInput(jobName, jobId, entry.getKey(), new String(entry.getValue()), 0);
                } else {
                    for (int index = 0; index < 3; index++) {
                        taskCache.saveTaskInput(jobName, jobId, entry.getKey(), new String(entry.getValue()), index);
                    }
                }

            }

            System.out.println("started job : " + jobId + "    " + new Date().toString());
            jobIds.add(jobId);
        }
        return jobIds;
    }

    public void rollbackJob(String jobName, String jobId) {
        prepareForTaskExecution(jobName);
        tcsClientRuntime.rollbackJob(jobName, jobId);
        System.out.println("started rollback of job : " + jobId + "    " + new Date().toString());
    }

    private static String getTaskKey(String jobName, String taskName) {
        return String.format("%s:%s", jobName, taskName);
    }
}

