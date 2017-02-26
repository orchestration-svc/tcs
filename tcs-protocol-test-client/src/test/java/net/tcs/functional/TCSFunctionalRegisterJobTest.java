package net.tcs.functional;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.task.coordinator.endpoint.TcsTaskExecutionEndpoint;
import com.task.coordinator.message.utils.TCSConstants;

import net.tcs.api.TCSClient;
import net.tcs.api.TCSClientFactory;
import net.tcs.task.JobSpec;

@Test(groups = "registerJobs")
public class TCSFunctionalRegisterJobTest {

    private TCSClient tcsClientRuntime;
    private TCSTestRuntime testRuntime;

    private PrintWriter fileWriter;

    @BeforeClass
    @Parameters({ "rmqIP", "partitionCount" })
    public void setup(@Optional("") String rmqIP, @Optional("4") int partitionCount) throws IOException {
        if (StringUtils.isEmpty(rmqIP)) {
            throw new SkipException("Skipping TCSFunctionalRegisterJobTest");
        }
        tcsClientRuntime = TCSClientFactory.createTCSClient(rmqIP);
        tcsClientRuntime.initialize();

        testRuntime = new TCSTestRuntime();
        testRuntime.initialize(tcsClientRuntime);

        fileWriter = new PrintWriter(new FileWriter("jobs.txt", false));
    }

    @AfterClass
    public void cleanup() {
        if (fileWriter != null) {
            fileWriter.close();
        }

        if (testRuntime != null) {
            testRuntime.cleanup();
        }

        if (tcsClientRuntime != null) {
            tcsClientRuntime.cleanup();
        }
    }

    @Test
    @Parameters({ "rmqIP" })
    public void testJobExecutionSuccess(String rmqIP) throws IOException {
        registerJobTestJob(rmqIP);
        registerJobTestJob2(rmqIP);
        registerJobTestJob3(rmqIP);
        registerJobTestJob4(rmqIP);
        registerParallelJobTest(rmqIP);
        registerJobTestJob6(rmqIP);
        registerJobTestJobWithStep(rmqIP);
    }

    private String registerJobTestJob(String brokerAddress) throws IOException {
        final String jobName = "testjob-" + UUID.randomUUID().toString();

        final TcsTaskExecutionEndpoint t1Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t1");

        final TcsTaskExecutionEndpoint t2Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t2");

        final TcsTaskExecutionEndpoint t3Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t3");

        final TcsTaskExecutionEndpoint t4Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t4");
        final JobSpec job = TCSClientFactory.createJobSpec(jobName).addTask("t1", t1Endpoint.toEndpointURI(), null)
                .addTask("t2", t2Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t1")))
                .addTask("t3", t3Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t1")))
                .addTask("t4", t4Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t2", "t3")));

        tcsClientRuntime.registerJob(job);
        fileWriter.println(jobName);
        return jobName;
    }

    private String registerJobTestJob2(String brokerAddress) throws IOException {

        final String jobName = "testjob-" + UUID.randomUUID().toString();

        final TcsTaskExecutionEndpoint t1Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t11");

        final TcsTaskExecutionEndpoint t2Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t22");

        final TcsTaskExecutionEndpoint t3Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t33");

        final TcsTaskExecutionEndpoint t4Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t44");
        final JobSpec job = TCSClientFactory.createJobSpec(jobName);

        job.addTask("t1", t1Endpoint.toEndpointURI(), null);
        job.addTask("t2", t2Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t1")));
        job.addTask("t3", t3Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t1")));
        job.addTask("t4", t4Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t1")));

        tcsClientRuntime.registerJob(job);
        fileWriter.println(jobName);
        return jobName;
    }

    private String registerJobTestJob3(String brokerAddress) throws IOException {

        final String jobName = "testjob-" + UUID.randomUUID().toString();

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
        final JobSpec job = TCSClientFactory.createJobSpec(jobName);

        job.addTask("t1", t1Endpoint.toEndpointURI(), null);
        job.addTask("t2", t2Endpoint.toEndpointURI(), null);
        job.addTask("t3", t3Endpoint.toEndpointURI(), null);
        job.addTask("t4", t4Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t1", "t2", "t3")));
        job.addTask("t5", t5Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t1", "t2", "t3")));

        tcsClientRuntime.registerJob(job);
        fileWriter.println(jobName);
        return jobName;
    }

    private String registerJobTestJob4(String brokerAddress) throws IOException {

        final String jobName = "testjob-" + UUID.randomUUID().toString();

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

        final JobSpec job = TCSClientFactory.createJobSpec(jobName);

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
        fileWriter.println(jobName);
        return jobName;
    }

    private String registerParallelJobTest(String brokerAddress) throws IOException {

        final String jobName = "testjob-" + UUID.randomUUID().toString();

        final TcsTaskExecutionEndpoint t0Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t01");
        final TcsTaskExecutionEndpoint t1Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t12");
        final TcsTaskExecutionEndpoint t2Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t23");
        final TcsTaskExecutionEndpoint t3Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t34");

        final JobSpec job = TCSClientFactory.createJobSpec(jobName);

        job.addTask("t0", t0Endpoint.toEndpointURI(), null);
        job.addTask("t1", t1Endpoint.toEndpointURI(), null);
        job.addTask("t2", t2Endpoint.toEndpointURI(), null);
        job.addTask("t3", t3Endpoint.toEndpointURI(), null);

        tcsClientRuntime.registerJob(job);
        fileWriter.println(jobName);
        return jobName;
    }

    private String registerJobTestJob6(String brokerAddress) throws IOException {

        final String jobName = "testjob-" + UUID.randomUUID().toString();

        final TcsTaskExecutionEndpoint t0Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t012");
        final TcsTaskExecutionEndpoint t1Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t123");
        final TcsTaskExecutionEndpoint t2Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t234");
        final TcsTaskExecutionEndpoint t3Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t345");

        final JobSpec job = TCSClientFactory.createJobSpec(jobName);

        job.addTask("t0", t0Endpoint.toEndpointURI(), null);
        job.addTask("t1", t1Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t0")));
        job.addTask("t2", t2Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t1")));
        job.addTask("t3", t3Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t2")));

        tcsClientRuntime.registerJob(job);
        fileWriter.println(jobName);
        return jobName;
    }

    private String registerJobTestJobWithStep(String brokerAddress) throws IOException {

        final String jobName = "testjobstep-" + UUID.randomUUID().toString();

        final TcsTaskExecutionEndpoint t0Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t01112");
        final TcsTaskExecutionEndpoint t1Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t12223");
        final TcsTaskExecutionEndpoint t2Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t23334");
        final TcsTaskExecutionEndpoint t3Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t34445");
        final TcsTaskExecutionEndpoint t4Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t45556");
        final TcsTaskExecutionEndpoint t5Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t56667");
        final TcsTaskExecutionEndpoint t6Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t67778");
        final TcsTaskExecutionEndpoint t7Endpoint = new TcsTaskExecutionEndpoint(brokerAddress,
                TCSConstants.TCS_JOB_EXEC_EXCHANGE, "tcs.rkey-t78889");

        final JobSpec job = TCSClientFactory.createJobSpec(jobName);

        job.addTask("t0", t0Endpoint.toEndpointURI(), null);
        job.addTask("t1", t1Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t0")));
        job.addTask("t2", t2Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t1")));
        job.addTask("t3", t3Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t2")));
        job.addTask("t4", t4Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t1", "t2", "t3")));
        job.addTask("t5", t5Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t4")));
        job.addTask("t6", t6Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t5")));
        job.addTask("t7", t7Endpoint.toEndpointURI(), new HashSet<String>(Arrays.asList("t6")));

        job.markStep("teststep", Arrays.asList("t4", "t5", "t6"));

        tcsClientRuntime.registerJob(job);
        fileWriter.println(jobName);
        return jobName;
    }
}
