package net.tcs;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import com.task.coordinator.endpoint.TcsTaskExecutionEndpoint;

import net.tcs.api.TCSClient;
import net.tcs.api.TCSClientFactory;
import net.tcs.task.JobSpec;

public class TCSJobRegisterMain {

    public static final String RMQ_EXCHANGE = "tcs.exchange.test";
    private static TCSClient tcsClientRuntime;

    public static void main(String[] args) throws IOException {
        final String rmqIP = System.getProperty("rmqIP", "localhost");
        tcsClientRuntime = TCSClientFactory.createTCSClient(rmqIP);

        try {
            tcsClientRuntime.initialize();
            registerJobDemoJob(rmqIP);
        } finally {
            tcsClientRuntime.cleanup();
        }
    }

    private static void registerJobDemoJob(String rmqIP) throws IOException {
        String jobName = "tcsdemojob";

        final TcsTaskExecutionEndpoint taskAEndpoint = new TcsTaskExecutionEndpoint(rmqIP, RMQ_EXCHANGE,
                "rkey.demo-serviceA");

        final TcsTaskExecutionEndpoint taskBEndpoint = new TcsTaskExecutionEndpoint(rmqIP,
                RMQ_EXCHANGE, "rkey.demo-serviceB");

        final TcsTaskExecutionEndpoint taskCEndpoint = new TcsTaskExecutionEndpoint(rmqIP,
                RMQ_EXCHANGE, "rkey.demo-serviceC");

        final TcsTaskExecutionEndpoint taskDEndpoint = new TcsTaskExecutionEndpoint(rmqIP,
                RMQ_EXCHANGE, "rkey.demo-serviceD");
        final JobSpec job = TCSClientFactory.createJobSpec(jobName);

        job.addTask("taskA", taskAEndpoint.toEndpointURI(), null)
        .addTask("taskB", taskBEndpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("taskA")))
        .addTask("taskC", taskCEndpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("taskA")))
        .addTask("taskD", taskDEndpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("taskB", "taskC")));

        System.out.println("Registering job: " + jobName);
        tcsClientRuntime.registerJob(job);
        System.out.println("Registered job: " + jobName);
    }
}
