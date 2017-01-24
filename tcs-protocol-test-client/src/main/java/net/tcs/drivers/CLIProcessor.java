package net.tcs.drivers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import net.tcs.api.TCSCallback;
import net.tcs.api.TCSJobHandler;
import net.tcs.api.TCSTaskContext;
import net.tcs.task.JobSpec;
import net.tcs.task.ParentTaskOutput;

public class CLIProcessor {

    private static final ConcurrentMap<String, TCSTaskContext> inProgressTasks = new ConcurrentHashMap<>();

    private static final ConcurrentMap<String, TCSTaskContext> inRollbackProgressTasks = new ConcurrentHashMap<>();

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
            System.out.println("Job rollback complete: JobId: " + jobId);
        }

        @Override
        public void jobRollbackFailed(String jobId) {
        }
    };

    private static final TCSCallback callback = new TCSCallback() {

        @Override
        public void startTask(TCSTaskContext taskContext, byte[] taskInput, ParentTaskOutput parentTasksOutput,
                Map<String, String> jobContext) {
            final StringBuilder sb = new StringBuilder();
            if (taskContext.getTaskRetryCount() > 0) {
                sb.append("Retry task:");
            } else {
                sb.append("Start task:");
            }
            sb.append(" taskName: " + taskContext.getTaskName());
            sb.append(" taskParallelExecutionIndex: " + taskContext.getTaskParallelExecutionIndex());
            sb.append(" taskId: " + taskContext.getTaskId());
            sb.append(" jobName: " + taskContext.getJobName());
            sb.append(" jobId: " + taskContext.getJobId());
            sb.append(" time: " + new Date().toString());
            System.out.println(sb.toString());

            if (!jobContext.isEmpty()) {
                System.out.println("Job context map");
                for (final Entry<String, String> entry : jobContext.entrySet()) {
                    System.out.println("Key: " + entry.getKey() + "   Value: " + entry.getValue());
                }
            }
            inProgressTasks.putIfAbsent(taskContext.getTaskId(), taskContext);
        }

        @Override
        public void rollbackTask(TCSTaskContext taskContext, byte[] taskOutput) {
            final StringBuilder sb = new StringBuilder();
            sb.append("Rollback task:");
            sb.append(" taskName: " + taskContext.getTaskName());
            sb.append(" taskId: " + taskContext.getTaskId());
            sb.append(" jobName: " + taskContext.getJobName());
            sb.append(" jobId: " + taskContext.getJobId());
            sb.append(" time: " + new Date().toString());
            System.out.println(sb.toString());
            inRollbackProgressTasks.putIfAbsent(taskContext.getTaskId(), taskContext);
        }
    };

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
                }
                try {
                    processCommands(cmd, argList);
                } catch (final Exception ex) {
                    ex.printStackTrace();
                    System.err.println(
                            "Exception while processing command: " + cmd + " Exception details: " + ex.toString());
                }
            }
        }
    }

    private static void processCommands(String cmd, String[] argsList) {
        System.out.println("===========================================");
        if (cmd.equalsIgnoreCase("help")) {

            help();
        } else if (cmd.equalsIgnoreCase("Prepare")) {

            processCommandPrepareForJob(argsList);
        } else if (cmd.equalsIgnoreCase("QueryJob")) {

            processCommandQueryJob(argsList);
        } else if (cmd.equalsIgnoreCase("StartJob")) {

            processCommandStartJob(argsList);
        } else if (cmd.equalsIgnoreCase("TaskComplete")) {

            processCommandTaskComplete(argsList);
        } else if (cmd.equalsIgnoreCase("TaskFailed")) {

            processCommandTaskFailed(argsList);
        } else if (cmd.equalsIgnoreCase("TaskUpdate")) {

            processCommandTaskInProgressUpdate(argsList);
        } else if (cmd.equalsIgnoreCase("RollbackJob")) {

            processCommandRollbackJob(argsList);
        } else if (cmd.equalsIgnoreCase("TaskRollbackComplete")) {

            processCommandTaskRollbackComplete(argsList);
        } else {
            System.out.println("unknown command");
        }
        System.out.println("===========================================");
    }

    private static void processCommandTaskInProgressUpdate(String[] argsList) {
        if (argsList == null || argsList.length < 2) {
            System.out.println("Insufficient args");
            return;
        }
        final String taskId = argsList[1];

        final TCSTaskContext taskContext = inProgressTasks.get(taskId);
        if (taskContext == null) {
            System.err.println("Did not find in progress task: " + taskId);
        } else {
            TCSClientDriver.getTcsClientRuntime().taskInProgress(taskContext);
        }
    }

    private static void processCommandTaskFailed(String[] argsList) {
        if (argsList == null || argsList.length < 3) {
            System.out.println("Insufficient args");
            return;
        }
        final String taskId = argsList[1];
        final String error = argsList[2];

        final TCSTaskContext taskContext = inProgressTasks.remove(taskId);
        if (taskContext == null) {
            System.err.println("Did not find in progress task: " + taskId);
        } else {
            TCSClientDriver.getTcsClientRuntime().taskFailed(taskContext, error.getBytes());
        }
    }

    private static void processCommandTaskComplete(String[] argsList) {
        if (argsList == null || argsList.length < 3) {
            System.out.println("Insufficient args");
            return;
        }
        if (argsList.length > 3 && (argsList.length % 2 == 0)) {
            System.out.println("Incorrect number of args; Must specify key/value pair for every context");
            return;
        }

        final String taskId = argsList[1];
        final String output = argsList[2];

        final Map<String, String> taskContextOutput = new HashMap<>();
        if (argsList.length > 3) {
            for (int index = 3; index < argsList.length; index += 2) {
                taskContextOutput.put(argsList[index], argsList[index + 1]);
            }
        }

        final TCSTaskContext taskExecutionContext = inProgressTasks.remove(taskId);
        if (taskExecutionContext == null) {
            System.err.println("Did not find in progress task: " + taskId);
        } else {
            if (taskContextOutput.isEmpty()) {
                TCSClientDriver.getTcsClientRuntime().taskComplete(taskExecutionContext, output.getBytes());
            } else {
                TCSClientDriver.getTcsClientRuntime().taskComplete(taskExecutionContext, output.getBytes(), taskContextOutput);
            }
        }
    }

    private static void processCommandTaskRollbackComplete(String[] argsList) {
        if (argsList == null || argsList.length < 2) {
            System.out.println("Insufficient args");
            return;
        }
        final String taskId = argsList[1];

        final TCSTaskContext taskContext = inRollbackProgressTasks.remove(taskId);
        if (taskContext == null) {
            System.err.println("Did not find in progress task: " + taskId);
        } else {
            TCSClientDriver.getTcsClientRuntime().taskRollbackComplete(taskContext);
        }
    }

    private static void processCommandStartJob(String[] argsList) {
        if (argsList == null || argsList.length < 3) {
            System.out.println("Insufficient args");
            return;
        }

        if (argsList.length > 3 && (argsList.length % 2 == 0)) {
            System.out.println("Incorrect number of args; Must specify key/value pair for every context");
            return;
        }

        final String jobName = argsList[1];
        final String input = argsList[2];

        final Map<String, String> jobContextMap = new HashMap<>();
        if (argsList.length > 3) {
            for (int index = 3; index < argsList.length; index += 2) {
                jobContextMap.put(argsList[index], argsList[index + 1]);
            }
        }

        if (jobContextMap.isEmpty()) {
            TCSClientDriver.getTcsClientRuntime().startJob(jobName, input.getBytes());
        } else {
            TCSClientDriver.getTcsClientRuntime().startJob(jobName, input.getBytes(), jobContextMap);
        }
    }

    private static void processCommandRollbackJob(String[] argsList) {
        if (argsList == null || argsList.length < 3) {
            System.out.println("Insufficient args");
            return;
        }
        final String jobName = argsList[1];
        final String jobInstanceId = argsList[2];
        TCSClientDriver.getTcsClientRuntime().rollbackJob(jobName, jobInstanceId);
    }

    private static void processCommandQueryJob(String[] argsList) {
        if (argsList == null || argsList.length < 2) {
            System.out.println("Insufficient args");
            return;
        }
        final String jobName = argsList[1];
        final JobSpec jobSpec = TCSClientDriver.getTcsClientRuntime().queryRegisteredJob(jobName);
        if (jobSpec != null) {
            System.out.println(jobSpec.toString());
        } else {
            System.err.println("Job: " + jobName + " not registered with TCS");
        }
    }

    private static void processCommandPrepareForJob(String[] argsList) {
        if (argsList == null || argsList.length < 3) {
            System.out.println("Insufficient args");
            return;
        }
        final String jobName = argsList[1];
        final Map<String, TCSCallback> map = new HashMap<>();
        for (int i = 2; i < argsList.length; i++) {
            map.put(argsList[i], callback);
        }

        TCSClientDriver.getTcsClientRuntime().prepareToExecute(jobName, jobHandler, map);
    }

    private static void help() {
        System.out.println("help");
        System.out.println("quit/exit");
        System.out.println("QueryJob [jobName]");
        System.out.println("Prepare [jobName] [TaskName]");
        System.out.println("StartJob [jobName] [Input] [<optional> k1 v1 k2 v2 ...]");
        System.out.println("TaskComplete [taskId] [TaskOutput] [<optional> k1 v1 k2 v2 ...]");
        System.out.println("TaskFailed [taskId] [Error]");
        System.out.println("TaskUpdate [taskId]");
        System.out.println("RollbackJob [jobName] [jobId]");
        System.out.println("TaskRollbackComplete [taskId]");
    }
}
