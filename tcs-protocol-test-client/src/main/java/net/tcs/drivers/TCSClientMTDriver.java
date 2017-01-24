package net.tcs.drivers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.RandomStringUtils;

import net.tcs.api.TCSCallback;
import net.tcs.api.TCSClient;
import net.tcs.api.TCSClientFactory;
import net.tcs.api.TCSJobHandler;
import net.tcs.api.TCSTaskContext;
import net.tcs.task.JobSpec;
import net.tcs.task.ParentTaskOutput;

public class TCSClientMTDriver {

    private static final LinkedBlockingQueue<TCSTaskContext> taskQ = new LinkedBlockingQueue<>();

    private static final ConcurrentMap<String, TCSTaskContext> inProgressTasks = new ConcurrentHashMap<>();

    private static final TCSJobHandler jobHandler = new TCSJobHandler() {

        @Override
        public void jobComplete(String jobId) {
            System.out.println("Job complete: JobId: " + jobId);
        }

        @Override
        public void jobFailed(String jobId) {
            System.out.println("Job failed: JobId: " + jobId);
        }

        @Override
        public void jobRollbackComplete(String jobId) {
        }

        @Override
        public void jobRollbackFailed(String jobId) {
        }
    };

    private static final TCSCallback callback = new TCSCallback() {

        @Override
        public void startTask(TCSTaskContext taskContext, byte[] taskInput, ParentTaskOutput parentTasksOutput,
                Map<String, String> jobContext) {
            inProgressTasks.put(taskContext.getTaskId(), taskContext);
            taskQ.add(taskContext);
        }

        @Override
        public void rollbackTask(TCSTaskContext taskContext, byte[] taskOutput) {
        }
    };

    private static final class TaskWorker implements Runnable {

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
                        tcsClientRuntime.taskComplete(taskContext,
                                RandomStringUtils.randomAlphabetic(32).getBytes());
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
    }

    private static TCSClient tcsClientRuntime;
    private static ExecutorService executor;
    private static List<TaskWorker> workers = new ArrayList<>();

    private static final InProgressTaskPinger taskPinger = new InProgressTaskPinger();

    private static List<String> jobs = Arrays.asList("testjob", "testjobser", "testjob2", "testjob3", "testjob4",
            "testjob5");

    private static final Map<String, JobSpec> jobspecMap = new HashMap<>();

    public static void main(String[] args) throws IOException {

        if (args == null || args.length < 2) {
            System.err.println(
                    "Must specify [numParitions] [RMQ-Broker] [numWorkers (optional)] [workerIndex (optional]");
            return;
        }

        final int numPartitions = Integer.parseInt(args[0]);
        final String rmqBroker = args[1];

        tcsClientRuntime = TCSClientFactory.createTCSClient(numPartitions, rmqBroker);
        tcsClientRuntime.initialize();

        executor = Executors.newCachedThreadPool(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "TCSDispatcher");
            }
        });

        executor.submit(taskPinger);
        for (int i = 0; i < 8; i++) {
            final TaskWorker dispatcher = new TaskWorker();
            workers.add(dispatcher);
            executor.submit(dispatcher);
        }

        if (args.length > 2) {
            final int numWorkers = Integer.parseInt(args[2]);
            final int workerIndex = Integer.parseInt(args[3]);
            prepareForTaskExecution(numWorkers, workerIndex);
        } else {
            prepareForTaskExecution();
        }

        processConsoleInput();


        System.out.println("Shutting down");

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
        tcsClientRuntime.cleanup();
        System.out.println("Shutdown");
    }

    private static void prepareForTaskExecution() {
        for (final String job : jobs) {
            final JobSpec jobSpec = tcsClientRuntime.queryRegisteredJob(job);
            if (jobSpec != null) {
                jobspecMap.put(job, jobSpec);

                final Map<String, TCSCallback> map = new HashMap<>();
                final Set<String> tasks = jobSpec.getTasks();
                System.out.println(
                        "Task names for Job: " + job + Arrays.toString(tasks.toArray(new String[tasks.size()])));
                for (final String task : tasks) {
                    map.put(task, callback);
                }

                tcsClientRuntime.prepareToExecute(job, jobHandler, map);
            }
        }

        System.out.println("Ready to execute tasks");
    }

    private static void prepareForTaskExecution(int numWorkers, int workerIndex) {
        for (final String job : jobs) {
            final JobSpec jobSpec = tcsClientRuntime.queryRegisteredJob(job);
            if (jobSpec != null) {
                jobspecMap.put(job, jobSpec);

                final Map<String, TCSCallback> map = new HashMap<>();
                final Set<String> tasks = jobSpec.getTasks();

                final String[] taskArray = tasks.toArray(new String[tasks.size()]);

                final List<String> tasksToExec = new ArrayList<>();
                for (int i = workerIndex; i < taskArray.length; i += numWorkers) {
                    tasksToExec.add(taskArray[i]);
                }

                System.out.println(
                        "Task to execute for Job: " + job + "    :"
                                + Arrays.toString(tasksToExec.toArray(new String[tasksToExec.size()])));
                for (final String task : tasksToExec) {
                    map.put(task, callback);
                }

                tcsClientRuntime.prepareToExecute(job, jobHandler, map);
            }
        }

        System.out.println("Ready to execute tasks");
    }

    protected static void processConsoleInput() throws IOException {
        final BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            System.out.println("Choose an option");
            final String s = in.readLine();
            if ((s != null) && s.length() != 0) {
                String[] argList = null;
                String cmd = null;
                if (s.indexOf(" ") != -1) {
                    argList = s.split(" ");
                    cmd = argList[0];
                } else {
                    cmd = s;
                }
                if (cmd.equalsIgnoreCase("quit") || cmd.equalsIgnoreCase("exit")) {
                    return;
                } else if (cmd.equalsIgnoreCase("runjobs")) {
                    final int count = Integer.parseInt(argList[1]);
                    runJobs(count);
                } else {
                    System.out.println("unknown command");
                }
            }
        }
    }

    private static void runJobs(int count) {
        System.out.println("submitting jobs : " + new Date().toString());
        for (int i = 0; i < count; i++) {
            for (final String jobName : jobs) {
                final JobSpec jobspec = jobspecMap.get(jobName);
                final Set<String> tasks = jobspec.getTasks();
                final Map<String, byte[]> taskInput = new HashMap<>();
                for (final String task : tasks) {
                    taskInput.put(task, RandomStringUtils.randomAlphabetic(32).getBytes());
                }
                tcsClientRuntime.startJob(jobName, taskInput);
                try {
                    Thread.sleep(20);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        System.out.println("submitted all jobs : " + new Date().toString());
    }
}
