package net.tcs.functional;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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

import net.tcs.task.JobDefinition;
import net.tcs.task.JobSpec;

@Test(groups = "failTest", dependsOnGroups = "registerJobs")
public class TCSFunctionalJobExecutionFailTest extends TCSTestBase {

    private PrintWriter fw;
    private PrintWriter fwTask;

    @Override
    @BeforeClass
    @Parameters({ "rmqIP" })
    public void setup(@Optional("") String rmqIP) throws IOException {
        if (StringUtils.isEmpty(rmqIP)) {
            throw new SkipException("Skipping TCSFunctionalJobExecutionFailTest");
        }
        super.setup(rmqIP);
        fw = new PrintWriter(new FileWriter("failjobs.txt", false));
        fwTask = new PrintWriter(new FileWriter("failtasks.txt", false));
    }

    @Override
    @AfterClass
    public void cleanup() {
        super.cleanup();

        if (fw != null) {
            fw.close();
        }

        if (fwTask != null) {
            fwTask.close();
        }
    }

    @Test
    public void testJobExecutionFail() throws InterruptedException {
        TCSTestRuntime.registerTaskNotifCallback(new TaskNotificationCallback() {

            @Override
            public void taskComplete(String taskId) {
                fwTask.println(taskId);
            }

            @Override
            public void taskFailed(String taskId) {
            }

            @Override
            public void taskRolledBack(String taskId) {
            }
        });

        final Random r = new Random();
        final List<String> jobs = new ArrayList<>();

        for (final String jobName : jobspecMap.keySet()) {

            final JobSpec jobSpec = jobspecMap.get(jobName);

            final JobDefinition jobDef = (JobDefinition) jobSpec;
            if (jobDef.hasSteps()) {
                /*
                 * REVISIT TODO, enable this when Rollback is implemented for
                 * step-marked Jobs.
                 */
                continue;
            }
            final Set<String> tasks = jobSpec.getTasks();
            final List<String> taskList = new ArrayList<>(tasks);
            final int failTaskIndex = r.nextInt(taskList.size());
            final String failTask = taskList.get(failTaskIndex);

            testRuntime.failTask(jobName, failTask);
        }

        for (final String jobName : jobspecMap.keySet()) {

            final JobSpec jobSpec = jobspecMap.get(jobName);

            final JobDefinition jobDef = (JobDefinition) jobSpec;
            if (jobDef.hasSteps()) {
                /*
                 * REVISIT TODO, enable this when Rollback is implemented for
                 * step-marked Jobs.
                 */
                continue;
            }
            final List<String> jobIds = testRuntime.executeJob(jobName, 1);
            jobs.addAll(jobIds);

            for (final String jobId : jobIds) {
                fw.println(String.format("%s %s",  jobId, jobName));
            }
        }

        for (final String jobId : jobs) {
            Assert.assertEquals("FAILED", testRuntime.waitForJobStatus(jobId, 10));
        }
    }
}
