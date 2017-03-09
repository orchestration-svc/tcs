package net.tcs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import net.tcs.api.TCSCallback;
import net.tcs.api.TCSClient;
import net.tcs.api.TCSClientFactory;
import net.tcs.api.TCSTaskContext;
import net.tcs.task.ParentTaskInfo;
import net.tcs.task.ParentTaskOutput;

public class TCSDemoTaskExecutor {

    private static class DemoTaskCallbackBase implements TCSCallback {

        protected final TCSClient tcsClientRuntime;

        public DemoTaskCallbackBase(TCSClient tcsClientRuntime) {
            super();
            this.tcsClientRuntime = tcsClientRuntime;
        }

        @Override
        public void rollbackTask(TCSTaskContext taskExecutionContext, byte[] taskOutput) {
        }

        @Override
        public void startTask(TCSTaskContext taskExecutionContext, byte[] taskInput, ParentTaskOutput parentTasksOutput,
                Map<String, String> jobContext) {

            System.out.println("==============================");
            System.out.println(taskExecutionContext.toString());
            if (taskInput != null) {
                System.out.println("Task input: " + new String(taskInput));
            }

            if (parentTasksOutput.getPredecessors() != null && !parentTasksOutput.getPredecessors().isEmpty()) {
                for (String predecessorTaskName : parentTasksOutput.getPredecessors()) {
                    ParentTaskInfo info = parentTasksOutput.getPredecessorTaskInfo(predecessorTaskName);
                    System.out.println(
                            "Output from predecessor task: " + predecessorTaskName + "   is: " + info.getOutput());
                }
            }

            if (jobContext != null && !jobContext.isEmpty()) {
                System.out.println("Job context map:");
                for (Entry<String, String> entry : jobContext.entrySet()) {
                    System.out.println("key: " + entry.getKey() + " value: " + entry.getValue());
                }
            }
        }
    }

    private static final class DemoTaskACallback extends DemoTaskCallbackBase {
        public DemoTaskACallback(TCSClient tcsClientRuntime) {
            super(tcsClientRuntime);
        }

        @Override
        public void startTask(final TCSTaskContext taskExecutionContext, byte[] taskInput, ParentTaskOutput parentTasksOutput,
                Map<String, String> jobContext) {
            super.startTask(taskExecutionContext, taskInput, parentTasksOutput, jobContext);

            new Thread(new Runnable() {

                @Override
                public void run() {

                    // simulate work
                    Random r = new Random();
                    try {
                        Thread.sleep(2000 + r.nextInt(1000));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

                    final String taskOutput = "taskA output";

                    Map<String, String> taskContextOutput = new HashMap<>();
                    taskContextOutput.put("taskA-key1", "taskA-val1");
                    taskContextOutput.put("taskA-key2", "taskA-val2");
                    tcsClientRuntime.taskComplete(taskExecutionContext, taskOutput.getBytes(), taskContextOutput);
                }
            }).start();
        }
    }

    private static final class DemoTaskBCallback extends DemoTaskCallbackBase {
        public DemoTaskBCallback(TCSClient tcsClientRuntime) {
            super(tcsClientRuntime);
        }

        @Override
        public void startTask(final TCSTaskContext taskExecutionContext, byte[] taskInput,
                ParentTaskOutput parentTasksOutput, Map<String, String> jobContext) {
            super.startTask(taskExecutionContext, taskInput, parentTasksOutput, jobContext);

            new Thread(new Runnable() {

                @Override
                public void run() {

                    // simulate work
                    Random r = new Random();
                    try {
                        Thread.sleep(2000 + r.nextInt(1000));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

                    final String taskOutput = "taskB output";
                    tcsClientRuntime.taskComplete(taskExecutionContext, taskOutput.getBytes());
                }
            }).start();
        }
    }

    private static final class DemoTaskCCallback extends DemoTaskCallbackBase {
        public DemoTaskCCallback(TCSClient tcsClientRuntime) {
            super(tcsClientRuntime);
        }

        @Override
        public void startTask(final TCSTaskContext taskExecutionContext, byte[] taskInput,
                ParentTaskOutput parentTasksOutput, Map<String, String> jobContext) {
            super.startTask(taskExecutionContext, taskInput, parentTasksOutput, jobContext);

            new Thread(new Runnable() {

                @Override
                public void run() {

                    // simulate work
                    Random r = new Random();
                    try {
                        Thread.sleep(2000 + r.nextInt(1000));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

                    final String taskOutput = "taskC output";
                    tcsClientRuntime.taskComplete(taskExecutionContext, taskOutput.getBytes());
                }
            }).start();
        }
    }

    private static final class DemoTaskDCallback extends DemoTaskCallbackBase {
        public DemoTaskDCallback(TCSClient tcsClientRuntime) {
            super(tcsClientRuntime);
        }

        @Override
        public void startTask(final TCSTaskContext taskExecutionContext, byte[] taskInput,
                ParentTaskOutput parentTasksOutput, Map<String, String> jobContext) {
            super.startTask(taskExecutionContext, taskInput, parentTasksOutput, jobContext);

            new Thread(new Runnable() {

                @Override
                public void run() {

                    // simulate work
                    Random r = new Random();
                    try {
                        Thread.sleep(2000 + r.nextInt(1000));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

                    final String taskOutput = "taskD output";
                    Map<String, String> taskContextOutput = new HashMap<>();
                    taskContextOutput.put("taskD-key1", "taskD-val1");
                    taskContextOutput.put("taskD-key2", "taskD-val2");
                    tcsClientRuntime.taskComplete(taskExecutionContext, taskOutput.getBytes(), taskContextOutput);
                }
            }).start();
        }
    }

    static final class TaskHandlerFactory {
        public static TCSCallback createTaskHandler(String taskName) {
            switch (taskName) {
            case "taskA":
                return new DemoTaskACallback(tcsClientRuntime);

            case "taskB":
                return new DemoTaskBCallback(tcsClientRuntime);

            case "taskC":
                return new DemoTaskCCallback(tcsClientRuntime);

            case "taskD":
                return new DemoTaskDCallback(tcsClientRuntime);

            default:
                return null;
            }
        }
    }

    private static TCSClient tcsClientRuntime;

    public static void main(String[] args) throws IOException {

        if (args == null || args.length < 1) {
            System.err.println("Must specify task name");
            return;
        }

        String taskName = args[0];
        final String rmqIP = System.getProperty("rmqIP", "localhost");
        tcsClientRuntime = TCSClientFactory.createTCSClient(rmqIP);
        tcsClientRuntime.initialize();

        String jobName = "tcsdemojob";

        TCSCallback taskHandler = TaskHandlerFactory.createTaskHandler(taskName);

        Map<String, TCSCallback> taskHandlers = new HashMap<>();
        taskHandlers.put(taskName, taskHandler);
        tcsClientRuntime.prepareToExecute(jobName, taskHandlers);

        System.out.println("waiting to execute tasks. Press Ctl-C to shut down TaskExecutor.");
        /*
         * Register a shutdown hook to perform graceful shutdown.
         */
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                tcsClientRuntime.cleanup();
            }
        });
    }
}
