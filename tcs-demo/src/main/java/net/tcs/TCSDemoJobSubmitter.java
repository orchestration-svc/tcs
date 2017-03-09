package net.tcs;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import net.tcs.api.TCSClient;
import net.tcs.api.TCSClientFactory;
import net.tcs.api.TCSJobHandler;

public class TCSDemoJobSubmitter {
    private static TCSClient tcsClientRuntime;

    private static final ConcurrentMap<String, String> jobStatusMap = new ConcurrentHashMap<>();

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

    public static void main(String[] args) {
        final String rmqIP = System.getProperty("rmqIP", "localhost");
        tcsClientRuntime = TCSClientFactory.createTCSClient(rmqIP);
        tcsClientRuntime.initialize();

        String jobName = "tcsdemojob";

        // rmq://address/exchange/ROUTING_KEY
        String endpointURI = String.format("rmq://%s/%s/%s", rmqIP, TCSJobRegisterMain.RMQ_EXCHANGE,
                "listener-rkey-" + jobName);
        tcsClientRuntime.registerJobListener(jobName, endpointURI, jobHandler);

        String rootTaskInput = "Initial input for taskA";

        Map<String, String> jobContext = new HashMap<>();

        jobContext.put("taskA-key0", "taskA-val0");
        jobContext.put("taskB-key0", "taskB-val0");
        String jobId = tcsClientRuntime.startJob(jobName, rootTaskInput.getBytes(), jobContext);
        System.out.println("Started a Job instance for job: " + jobName + " Job InstanceId: " + jobId);

        while (jobStatusMap.isEmpty()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        String status = jobStatusMap.get(jobId);
        System.out.println("Job Status: " + status);
        tcsClientRuntime.cleanup();
    }
}
