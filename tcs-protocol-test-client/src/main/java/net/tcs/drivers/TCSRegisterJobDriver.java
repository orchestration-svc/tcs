package net.tcs.drivers;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import com.task.coordinator.endpoint.TcsTaskExecutionEndpoint;
import com.task.coordinator.message.utils.TCSConstants;

import net.tcs.api.TCSClient;
import net.tcs.api.TCSClientFactory;
import net.tcs.task.JobSpec;

public class TCSRegisterJobDriver {

    private static TCSClient tcsClientRuntime;

    public static void main(String[] args) throws IOException {
        final String brokerAddress = args[0];
        tcsClientRuntime = TCSClientFactory.createTCSClient(brokerAddress);
        tcsClientRuntime.initialize();

        registerJobTestJob1(brokerAddress);
        registerJobTestJob2(brokerAddress);
        registerJobTestJob3(brokerAddress);
        registerJobTestJob4(brokerAddress);
        registerJobTestJob5(brokerAddress);
        registerJobTestJob6(brokerAddress);
        registerJobTestJob7(brokerAddress);
        registerJobTestJob8(brokerAddress);

        tcsClientRuntime.cleanup();
    }

    public static void registerJobTestJob1(String brokerAddress) throws IOException {
        final TcsTaskExecutionEndpoint t1Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t1");

        final TcsTaskExecutionEndpoint t2Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t2");

        final TcsTaskExecutionEndpoint t3Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t3");

        final TcsTaskExecutionEndpoint t4Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t4");
        final JobSpec job = TCSClientFactory.createJobSpec("testjob");

        job.addTask("t1", t1Endpoint.toEndpointURI(), null);
        job.addTask("t2", t2Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t1")));
        job.addTask("t3", t3Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t1")));
        job.addTask("t4", t4Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t2", "t3")));

        tcsClientRuntime.registerJob(job);
    }

    public static void registerJobTestJob2(String brokerAddress) throws IOException {

        final TcsTaskExecutionEndpoint t1Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t11");

        final TcsTaskExecutionEndpoint t2Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t22");

        final TcsTaskExecutionEndpoint t3Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t33");

        final TcsTaskExecutionEndpoint t4Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t44");
        final JobSpec job = TCSClientFactory.createJobSpec("testjob2");

        job.addTask("t1", t1Endpoint.toEndpointURI(), null);
        job.addTask("t2", t2Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t1")));
        job.addTask("t3", t3Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t1")));
        job.addTask("t4", t4Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t1")));

        tcsClientRuntime.registerJob(job);
    }

    public static void registerJobTestJob3(String brokerAddress) throws IOException {

        final TcsTaskExecutionEndpoint t1Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t111");

        final TcsTaskExecutionEndpoint t2Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t222");

        final TcsTaskExecutionEndpoint t3Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t333");

        final TcsTaskExecutionEndpoint t4Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t444");

        final TcsTaskExecutionEndpoint t5Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t555");
        final JobSpec job = TCSClientFactory.createJobSpec("testjob3");

        job.addTask("t1", t1Endpoint.toEndpointURI(), null);
        job.addTask("t2", t2Endpoint.toEndpointURI(), null);
        job.addTask("t3", t3Endpoint.toEndpointURI(), null);
        job.addTask("t4", t4Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t1", "t2", "t3")));
        job.addTask("t5", t5Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t1", "t2", "t3")));

        tcsClientRuntime.registerJob(job);
    }

    public static void registerJobTestJob4(String brokerAddress) throws IOException {

        final TcsTaskExecutionEndpoint t0Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t0000");
        final TcsTaskExecutionEndpoint t1Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t1111");
        final TcsTaskExecutionEndpoint t2Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t2222");
        final TcsTaskExecutionEndpoint t3Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t3333");
        final TcsTaskExecutionEndpoint t4Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t4444");
        final TcsTaskExecutionEndpoint t5Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t5555");
        final TcsTaskExecutionEndpoint t6Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t6666");
        final TcsTaskExecutionEndpoint t7Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t7777");
        final TcsTaskExecutionEndpoint t8Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t8888");
        final TcsTaskExecutionEndpoint t9Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t9999");

        final JobSpec job = TCSClientFactory.createJobSpec("testjob4");

        job.addTask("t0", t0Endpoint.toEndpointURI(), null);
        job.addTask("t1", t1Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t0")));
        job.addTask("t3", t3Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t0")));
        job.addTask("t2", t2Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t0", "t3")));
        job.addTask("t4", t4Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t1", "t2")));
        job.addTask("t5", t5Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t3")));
        job.addTask("t6", t6Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t4")));
        job.addTask("t7", t7Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t5")));
        job.addTask("t8", t8Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t5")));
        job.addTask("t9", t9Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t6", "t7", "t8")));

        tcsClientRuntime.registerJob(job);
    }

    public static void registerJobTestJob5(String brokerAddress) throws IOException {

        final TcsTaskExecutionEndpoint t0Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t01");
        final TcsTaskExecutionEndpoint t1Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t12");
        final TcsTaskExecutionEndpoint t2Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t23");
        final TcsTaskExecutionEndpoint t3Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t34");

        final JobSpec job = TCSClientFactory.createJobSpec("testjob5");

        job.addTask("t0", t0Endpoint.toEndpointURI(), null);
        job.addTask("t1", t1Endpoint.toEndpointURI(), null);
        job.addTask("t2", t2Endpoint.toEndpointURI(), null);
        job.addTask("t3", t3Endpoint.toEndpointURI(), null);

        tcsClientRuntime.registerJob(job);
    }

    public static void registerJobTestJob6(String brokerAddress) throws IOException {

        final TcsTaskExecutionEndpoint t0Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t012");
        final TcsTaskExecutionEndpoint t1Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t123");
        final TcsTaskExecutionEndpoint t2Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t234");
        final TcsTaskExecutionEndpoint t3Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t345");

        final JobSpec job = TCSClientFactory.createJobSpec("testjobser");

        job.addTask("t0", t0Endpoint.toEndpointURI(), null);
        job.addTask("t1", t1Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t0")));
        job.addTask("t2", t2Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t1")));
        job.addTask("t3", t3Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t2")));

        tcsClientRuntime.registerJob(job);
    }

    public static void registerJobTestJob7(String brokerAddress) throws IOException {

        final TcsTaskExecutionEndpoint t0Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t012");
        final TcsTaskExecutionEndpoint t1Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t123");
        final TcsTaskExecutionEndpoint t2Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t234");
        final TcsTaskExecutionEndpoint t3Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t345");

        final JobSpec job = TCSClientFactory.createJobSpec("testjobser2");

        job.addTask("t0", t0Endpoint.toEndpointURI(), null);
        job.addTask("t1", t1Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t0")));
        job.addTask("t2", t2Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t1")));
        job.addTask("t3", t3Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t2")));

        tcsClientRuntime.registerJob(job);
    }

    public static void registerJobTestJob8(String brokerAddress) throws IOException {

        final TcsTaskExecutionEndpoint t0Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t0122");
        final TcsTaskExecutionEndpoint t1Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t1233");
        final TcsTaskExecutionEndpoint t2Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t2344");
        final TcsTaskExecutionEndpoint t3Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t3455");

        final JobSpec job = TCSClientFactory.createJobSpec("testjobwithstep");

        job.addTask("t0", t0Endpoint.toEndpointURI(), null);
        job.addTask("t1", t1Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t0")));
        job.addTask("t2", t2Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t1")));
        job.addTask("t3", t3Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t2")));

        job.markStep("teststep", Arrays.asList("t1", "t2", "t3"));

        tcsClientRuntime.registerJob(job);
    }
}
