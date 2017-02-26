package net.tcs.functional;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
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

@Test(dependsOnGroups = "failTest")
public class TCSFunctionalJobExecutionRollbackTest extends TCSTestBase {

    @Override
    @BeforeClass
    @Parameters({ "rmqIP" })
    public void setup(@Optional("") String rmqIP) throws IOException {
        if (StringUtils.isEmpty(rmqIP)) {
            throw new SkipException("Skipping TCSFunctionalJobExecutionRollbackTest");
        }
        super.setup(rmqIP);
    }

    @Override
    @AfterClass
    public void cleanup() {
        super.cleanup();
    }

    @Test
    public void testJobExecutionRollback() throws InterruptedException, FileNotFoundException, IOException {

        final Set<String> rolledbackTasks = new HashSet<>();

        TCSTestRuntime.registerTaskNotifCallback(new TaskNotificationCallback() {

            @Override
            public void taskComplete(String taskId) {
            }

            @Override
            public void taskFailed(String taskId) {
            }

            @Override
            public void taskRolledBack(String taskId) {
                rolledbackTasks.add(taskId);
            }
        });

        final Map<String, String> jobIdToNameMap = new HashMap<>();

        try (BufferedReader fileReader = new BufferedReader(new FileReader("failjobs.txt"))) {
            String line = null;
            while ((line = fileReader.readLine()) != null) {
                final String[] array = line.split(" ");
                jobIdToNameMap.put(array[0], array[1]);
            }
        }

        final Set<String> succeededTasks = new HashSet<>();

        try (BufferedReader fileReader = new BufferedReader(new FileReader("failtasks.txt"))) {
            String line = null;
            while ((line = fileReader.readLine()) != null) {
                succeededTasks.add(line);
            }
        }

        for (final String jobName : jobIdToNameMap.values()) {
            final JobSpec jobSpec = tcsClientRuntime.queryRegisteredJob(jobName);
            jobspecMap.put(jobName, jobSpec);
        }

        for (final Entry<String, String> jobInfo : jobIdToNameMap.entrySet()) {
            testRuntime.rollbackJob(jobInfo.getValue(), jobInfo.getKey());
        }

        for (final String jobId : jobIdToNameMap.keySet()) {
            Assert.assertEquals("ROLLBACK_COMPLETE", testRuntime.waitForJobStatus(jobId, 10));
        }

        Assert.assertTrue(rolledbackTasks.size() == succeededTasks.size());
        Assert.assertTrue(rolledbackTasks.containsAll(succeededTasks));
    }
}
