package net.tcs.functional;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import net.tcs.task.JobSpec;

@Test(dependsOnGroups = "registerJobs")
public class TCSFunctionalJobExecutionRetryTest extends TCSTestBase {

    @Override
    @BeforeClass
    @Parameters({ "rmqIP", "partitionCount" })
    public void setup(@Optional("") String rmqIP, @Optional("4") int partitionCount) throws IOException {
        if (StringUtils.isEmpty(rmqIP)) {
            throw new SkipException("Skipping TCSFunctionalJobExecutionRetryTest");
        }
        super.setup(rmqIP, partitionCount);
    }

    @Override
    @AfterClass
    public void cleanup() {
        super.cleanup();
    }

    @Test
    public void testJobExecutionRetry() throws InterruptedException {
        final Random r = new Random();
        final List<String> jobs = new ArrayList<>();

        for (final String jobName : jobspecMap.keySet()) {
            final JobSpec jobSpec = jobspecMap.get(jobName);
            final Set<String> tasks = jobSpec.getTasks();
            final List<String> taskList = new ArrayList<>(tasks);
            final int retryTaskIndex = r.nextInt(taskList.size());
            final String retryTask = taskList.get(retryTaskIndex);

            testRuntime.retryTask(jobName, retryTask);
        }

        for (final String jobName : jobspecMap.keySet()) {
            final List<String> jobIds = testRuntime.executeJob(jobName, 1);
            jobs.addAll(jobIds);
        }

        for (final String jobId : jobs) {
            Assert.assertEquals("COMPLETE", testRuntime.waitForJobStatus(jobId, 60));
        }

        testRuntime.verifyTaskRetry();
    }
}
