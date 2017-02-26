package net.tcs.messagehandlers;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.amqp.support.converter.MessageConverter;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.task.coordinator.base.message.TcsCtrlMessage;
import com.task.coordinator.base.message.TcsCtrlMessageResult;
import com.task.coordinator.message.utils.TCSConstants;
import com.task.coordinator.request.message.BeginJobMessage;
import com.task.coordinator.request.message.BeginTaskMessage;
import com.task.coordinator.request.message.JobCompleteMessage;
import com.task.coordinator.request.message.JobSpecRegistrationMessage;
import com.task.coordinator.request.message.JobSubmitRequestMessage;
import com.task.coordinator.request.message.TaskCompleteMessage;

import net.tcs.core.TCSDispatcher;
import net.tcs.core.TCSRollbackDispatcher;
import net.tcs.core.TaskBoard;
import net.tcs.core.TaskKey;
import net.tcs.core.TestJobDefCreateUtils;
import net.tcs.db.JobInstanceDAO;
import net.tcs.db.TaskInstanceDAO;
import net.tcs.messages.JobSubmitRequest;
import net.tcs.shard.TCSShardRecoveryManager;
import net.tcs.shard.TCSShardRunner.StopNotifier;
import net.tcs.state.JobState;
import net.tcs.task.JobDefinition;

public class TCSDispatcherWithStepAndRecoveryTest extends DBAdapterTestBase {
    private TcsJobExecSubmitListener jobHandler;

    private TcsJobExecBeginListener beginJobHandler;

    private TcsTaskExecEventListener taskHandler;

    private final Map<TaskKey, BeginTaskMessage> inProgressTasks = new HashMap<>();

    private final String shardId = "shard0";

    private final String jobName0 = "testjob-0";

    private final String jobName1 = "testjob-1";

    private final String jobName2 = "testjob-2";

    private final String jobName3 = "testjob-3";

    private TCSTestProducer producer;

    private TCSDispatcher dispatcher;

    private TaskBoard taskBoard;

    private final ExecutorService executor = Executors.newCachedThreadPool();

    @Override
    @BeforeClass
    public void setup() throws ClassNotFoundException, SQLException, IOException {
        MockitoAnnotations.initMocks(this);
        super.setup();

        registerJob(TestJobDefCreateUtils.createJobDefWithStepMarking0(jobName0));
        registerJob(TestJobDefCreateUtils.createJobDefWithStepMarking1(jobName1));
        registerJob(TestJobDefCreateUtils.createJobDefWithStepMarking2(jobName2));
        registerJob(TestJobDefCreateUtils.createJobDefWithStepMarking3(jobName3));
    }

    @Override
    @AfterClass
    public void cleanup() {
        executor.shutdown();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        super.cleanup();
    }

    @BeforeMethod
    public void before() {
        simulateRecovery();
    }

    @AfterMethod
    public void after() {
        simulateShutdown();
        inProgressTasks.clear();
    }

    @Test
    public void testJobExecutionSuccessWithZeroParallelCount() throws IOException, InterruptedException {

        final String jobId = UUID.randomUUID().toString();
        final JobSubmitRequest req = new JobSubmitRequest(jobName0, jobId, "rmq://1.2.3.4/def/rkey", null);

        final JobSubmitRequestMessage jobSubmitMessage = new JobSubmitRequestMessage();
        jobSubmitMessage.setRequest(req);
        jobHandler.handleSubmitJob(jobSubmitMessage);

        final List<JobInstanceDAO> runningJobs = jobInstanceAdapter.getAllInprogressJobsForShard(shardId);
        Assert.assertEquals(runningJobs.size(), 1);
        Assert.assertEquals(runningJobs.get(0).getInstanceId(), jobId);

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t0", 0)), Arrays.asList(tk("t0", 0)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t1", 0), tk("t2", 0), tk("t3", 0)),
                Arrays.asList(tk("t3", 0)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t1", 0), tk("t2", 0)), Arrays.asList(tk("t1", 0)));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t2", 0)), Arrays.asList(tk("t2", 0)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t4", 0)), Arrays.asList(tk("t4", 0)));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t5", 0)), Arrays.asList(tk("t5", 0)));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t6", 0)), Arrays.asList(tk("t6", 0)));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t7", 0)), Arrays.asList(tk("t7", 0)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t8", 0), tk("t9", 0)), Arrays.asList(tk("t8", 0)));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t9", 0)), Arrays.asList(tk("t9", 0)));

        final TcsCtrlMessage msg = producer.getMessage();
        Assert.assertNotNull(msg);
        final JobCompleteMessage jobCompleteMsg = (JobCompleteMessage) msg;
        Assert.assertEquals(jobCompleteMsg.getJobName(), jobName0);
        Assert.assertEquals(jobCompleteMsg.getJobId(), jobId);

        getAndAssertJobState(jobId, JobState.COMPLETE);
    }

    @Test
    public void testJobExecutionSuccessWithNonZeroParallelCount() throws IOException, InterruptedException {

        final String jobId = UUID.randomUUID().toString();
        final JobSubmitRequest req = new JobSubmitRequest(jobName0, jobId, "rmq://1.2.3.4/def/rkey", null);
        final Map<String, String> jobContext = new HashMap<>();
        final String key = String.format(TCSConstants.STEP_PARALLEL_COUNT_KEY_FORMAT, "teststep");
        jobContext.put(key, "3");
        req.setJobContext(jobContext);

        final JobSubmitRequestMessage jobSubmitMessage = new JobSubmitRequestMessage();
        jobSubmitMessage.setRequest(req);
        jobHandler.handleSubmitJob(jobSubmitMessage);

        final List<JobInstanceDAO> runningJobs = jobInstanceAdapter.getAllInprogressJobsForShard(shardId);
        Assert.assertEquals(runningJobs.size(), 1);
        Assert.assertEquals(runningJobs.get(0).getInstanceId(), jobId);

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t0", 0)), Arrays.asList(tk("t0", 0)));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t1", 0), tk("t2", 0), tk("t3", 0)),
                Arrays.asList(tk("t3", 0)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t1", 0), tk("t2", 0)), Arrays.asList(tk("t1", 0)));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t2", 0)), Arrays.asList(tk("t2", 0)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t4", 0), tk("t4", 1), tk("t4", 2)),
                Arrays.asList(tk("t4", 0), tk("t4", 1), tk("t4", 2)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t5", 0), tk("t5", 1), tk("t5", 2)),
                Arrays.asList(tk("t5", 0), tk("t5", 1), tk("t5", 2)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t6", 0), tk("t6", 1), tk("t6", 2)),
                Arrays.asList(tk("t6", 0), tk("t6", 1), tk("t6", 2)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t7", 0), tk("t7", 1), tk("t7", 2)),
                Arrays.asList(tk("t7", 0), tk("t7", 1), tk("t7", 2)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t8", 0), tk("t9", 0)), Arrays.asList(tk("t8", 0)));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t9", 0)), Arrays.asList(tk("t9", 0)));

        final TcsCtrlMessage msg = producer.getMessage();
        Assert.assertNotNull(msg);
        final JobCompleteMessage jobCompleteMsg = (JobCompleteMessage) msg;
        Assert.assertEquals(jobCompleteMsg.getJobName(), jobName0);
        Assert.assertEquals(jobCompleteMsg.getJobId(), jobId);

        getAndAssertJobState(jobId, JobState.COMPLETE);
    }

    @Test
    public void testJobExecutionSuccessWithNonZeroParallelCount2() throws IOException, InterruptedException {

        final String jobId = UUID.randomUUID().toString();
        final JobSubmitRequest req = new JobSubmitRequest(jobName0, jobId, "rmq://1.2.3.4/def/rkey", null);

        final JobSubmitRequestMessage jobSubmitMessage = new JobSubmitRequestMessage();
        jobSubmitMessage.setRequest(req);
        jobHandler.handleSubmitJob(jobSubmitMessage);

        final List<JobInstanceDAO> runningJobs = jobInstanceAdapter.getAllInprogressJobsForShard(shardId);
        Assert.assertEquals(runningJobs.size(), 1);
        Assert.assertEquals(runningJobs.get(0).getInstanceId(), jobId);

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t0", 0)), Arrays.asList(tk("t0", 0)));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t1", 0), tk("t2", 0), tk("t3", 0)),
                Arrays.asList(tk("t3", 0)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t1", 0), tk("t2", 0)), new ArrayList<TaskKey>());

        completeTaskAndInjectStepCount(tk("t1", 0), "teststep", 3);

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t2", 0)), Arrays.asList(tk("t2", 0)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t4", 0), tk("t4", 1), tk("t4", 2)),
                Arrays.asList(tk("t4", 0), tk("t4", 1), tk("t4", 2)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t5", 0), tk("t5", 1), tk("t5", 2)),
                Arrays.asList(tk("t5", 0), tk("t5", 1), tk("t5", 2)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t6", 0), tk("t6", 1), tk("t6", 2)),
                Arrays.asList(tk("t6", 0), tk("t6", 1), tk("t6", 2)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t7", 0), tk("t7", 1), tk("t7", 2)),
                Arrays.asList(tk("t7", 0), tk("t7", 2)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t7", 1)), Arrays.asList(tk("t7", 1)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t8", 0), tk("t9", 0)), Arrays.asList(tk("t8", 0)));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t9", 0)), Arrays.asList(tk("t9", 0)));

        final TcsCtrlMessage msg = producer.getMessage();
        Assert.assertNotNull(msg);
        final JobCompleteMessage jobCompleteMsg = (JobCompleteMessage) msg;
        Assert.assertEquals(jobCompleteMsg.getJobName(), jobName0);
        Assert.assertEquals(jobCompleteMsg.getJobId(), jobId);

        getAndAssertJobState(jobId, JobState.COMPLETE);
    }

    @Test
    public void testJobExecutionSuccessWithNonZeroParallelCount3() throws IOException, InterruptedException {

        final String jobId = UUID.randomUUID().toString();
        final JobSubmitRequest req = new JobSubmitRequest(jobName1, jobId, "rmq://1.2.3.4/def/rkey", null);

        final JobSubmitRequestMessage jobSubmitMessage = new JobSubmitRequestMessage();
        jobSubmitMessage.setRequest(req);
        jobHandler.handleSubmitJob(jobSubmitMessage);

        final List<JobInstanceDAO> runningJobs = jobInstanceAdapter.getAllInprogressJobsForShard(shardId);
        Assert.assertEquals(runningJobs.size(), 1);
        Assert.assertEquals(runningJobs.get(0).getInstanceId(), jobId);

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t0", 0)), Arrays.asList(tk("t0", 0)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t1", 0), tk("t2", 0), tk("t3", 0)),
                Arrays.asList(tk("t2", 0)));

        completeTaskAndInjectStepCount(tk("t1", 0), "teststep", 3);

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t3", 0)), Arrays.asList(tk("t3", 0)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t10", 0), tk("t4", 0), tk("t4", 1), tk("t4", 2)),
                Arrays.asList(tk("t10", 0)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t4", 0), tk("t4", 1), tk("t4", 2)),
                Arrays.asList(tk("t4", 0), tk("t4", 1), tk("t4", 2)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t5", 0), tk("t5", 1), tk("t5", 2)),
                Arrays.asList(tk("t5", 0), tk("t5", 1), tk("t5", 2)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t6", 0), tk("t6", 1), tk("t6", 2)),
                Arrays.asList(tk("t6", 0), tk("t6", 1), tk("t6", 2)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t7", 0), tk("t7", 1), tk("t7", 2)),
                Arrays.asList(tk("t7", 0), tk("t7", 2)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t7", 1)), Arrays.asList(tk("t7", 1)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t8", 0), tk("t9", 0)), Arrays.asList(tk("t8", 0)));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t9", 0)), Arrays.asList(tk("t9", 0)));

        final TcsCtrlMessage msg = producer.getMessage();
        Assert.assertNotNull(msg);
        final JobCompleteMessage jobCompleteMsg = (JobCompleteMessage) msg;
        Assert.assertEquals(jobCompleteMsg.getJobName(), jobName1);
        Assert.assertEquals(jobCompleteMsg.getJobId(), jobId);

        getAndAssertJobState(jobId, JobState.COMPLETE);
    }

    @Test
    public void testJobExecutionSuccessWithNonZeroParallelCountAndOneTaskInStep()
            throws IOException, InterruptedException {

        final String jobId = UUID.randomUUID().toString();
        final JobSubmitRequest req = new JobSubmitRequest(jobName2, jobId, "rmq://1.2.3.4/def/rkey", null);
        final Map<String, String> jobContext = new HashMap<>();
        final String key = String.format(TCSConstants.STEP_PARALLEL_COUNT_KEY_FORMAT, "teststep");
        jobContext.put(key, "4");
        req.setJobContext(jobContext);

        final JobSubmitRequestMessage jobSubmitMessage = new JobSubmitRequestMessage();
        jobSubmitMessage.setRequest(req);
        jobHandler.handleSubmitJob(jobSubmitMessage);

        final List<JobInstanceDAO> runningJobs = jobInstanceAdapter.getAllInprogressJobsForShard(shardId);
        Assert.assertEquals(runningJobs.size(), 1);
        Assert.assertEquals(runningJobs.get(0).getInstanceId(), jobId);

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t0", 0)), Arrays.asList(tk("t0", 0)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t1", 0), tk("t1", 1), tk("t1", 2), tk("t1", 3)),
                Arrays.asList(tk("t1", 0), tk("t1", 1), tk("t1", 2), tk("t1", 3)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t2", 0)), Arrays.asList(tk("t2", 0)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t3", 0)), Arrays.asList(tk("t3", 0)));

        final TcsCtrlMessage msg = producer.getMessage();
        Assert.assertNotNull(msg);
        final JobCompleteMessage jobCompleteMsg = (JobCompleteMessage) msg;
        Assert.assertEquals(jobCompleteMsg.getJobName(), jobName2);
        Assert.assertEquals(jobCompleteMsg.getJobId(), jobId);

        getAndAssertJobState(jobId, JobState.COMPLETE);
    }

    @Test
    public void testJobExecutionSuccessWithNonZeroParallelCountAndTwoTasksInStep()
            throws IOException, InterruptedException {

        final String jobId = UUID.randomUUID().toString();
        final JobSubmitRequest req = new JobSubmitRequest(jobName3, jobId, "rmq://1.2.3.4/def/rkey", null);
        final Map<String, String> jobContext = new HashMap<>();
        final String key = String.format(TCSConstants.STEP_PARALLEL_COUNT_KEY_FORMAT, "teststep");
        jobContext.put(key, "4");
        req.setJobContext(jobContext);

        final JobSubmitRequestMessage jobSubmitMessage = new JobSubmitRequestMessage();
        jobSubmitMessage.setRequest(req);
        jobHandler.handleSubmitJob(jobSubmitMessage);

        final List<JobInstanceDAO> runningJobs = jobInstanceAdapter.getAllInprogressJobsForShard(shardId);
        Assert.assertEquals(runningJobs.size(), 1);
        Assert.assertEquals(runningJobs.get(0).getInstanceId(), jobId);

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t0", 0)), Arrays.asList(tk("t0", 0)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t1", 0), tk("t1", 1), tk("t1", 2), tk("t1", 3)),
                Arrays.asList(tk("t1", 0), tk("t1", 1), tk("t1", 2), tk("t1", 3)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t2", 0), tk("t2", 1), tk("t2", 2), tk("t2", 3)),
                Arrays.asList(tk("t2", 0), tk("t2", 1), tk("t2", 2), tk("t2", 3)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t3", 0)), Arrays.asList(tk("t3", 0)));

        final TcsCtrlMessage msg = producer.getMessage();
        Assert.assertNotNull(msg);
        final JobCompleteMessage jobCompleteMsg = (JobCompleteMessage) msg;
        Assert.assertEquals(jobCompleteMsg.getJobName(), jobName3);
        Assert.assertEquals(jobCompleteMsg.getJobId(), jobId);

        getAndAssertJobState(jobId, JobState.COMPLETE);
    }

    @Test
    public void testRecoveredJobExecutionSuccessWithZeroParallelCount() throws IOException, InterruptedException {

        final String jobId = UUID.randomUUID().toString();
        final JobSubmitRequest req = new JobSubmitRequest(jobName0, jobId, "rmq://1.2.3.4/def/rkey", null);

        final JobSubmitRequestMessage jobSubmitMessage = new JobSubmitRequestMessage();
        jobSubmitMessage.setRequest(req);
        jobHandler.handleSubmitJob(jobSubmitMessage);

        final List<JobInstanceDAO> runningJobs = jobInstanceAdapter.getAllInprogressJobsForShard(shardId);
        Assert.assertEquals(runningJobs.size(), 1);
        Assert.assertEquals(runningJobs.get(0).getInstanceId(), jobId);

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t0", 0)), Arrays.asList(tk("t0", 0)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t1", 0), tk("t2", 0), tk("t3", 0)),
                Arrays.asList(tk("t3", 0)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t1", 0), tk("t2", 0)), Arrays.asList(tk("t1", 0)));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t2", 0)), Arrays.asList(tk("t2", 0)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t4", 0)), new ArrayList<TaskKey>());

        simulateShutdown();

        simulateRecovery();

        startRecoveryManagerThread();

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t4", 0)), Arrays.asList(tk("t4", 0)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t5", 0)), Arrays.asList(tk("t5", 0)));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t6", 0)), Arrays.asList(tk("t6", 0)));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t7", 0)), Arrays.asList(tk("t7", 0)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t8", 0), tk("t9", 0)), Arrays.asList(tk("t8", 0)));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t9", 0)), Arrays.asList(tk("t9", 0)));

        final TcsCtrlMessage msg = producer.getMessage();
        Assert.assertNotNull(msg);
        final JobCompleteMessage jobCompleteMsg = (JobCompleteMessage) msg;
        Assert.assertEquals(jobCompleteMsg.getJobName(), jobName0);
        Assert.assertEquals(jobCompleteMsg.getJobId(), jobId);

        getAndAssertJobState(jobId, JobState.COMPLETE);
    }

    @Test
    public void testRecoveredJobExecutionSuccessWithNonZeroParallelCount() throws IOException, InterruptedException {

        final String jobId = UUID.randomUUID().toString();
        final JobSubmitRequest req = new JobSubmitRequest(jobName0, jobId, "rmq://1.2.3.4/def/rkey", null);
        final Map<String, String> jobContext = new HashMap<>();
        final String key = String.format(TCSConstants.STEP_PARALLEL_COUNT_KEY_FORMAT, "teststep");
        jobContext.put(key, "3");
        req.setJobContext(jobContext);

        final JobSubmitRequestMessage jobSubmitMessage = new JobSubmitRequestMessage();
        jobSubmitMessage.setRequest(req);

        simulateShutdown();

        jobHandler.handleSubmitJob(jobSubmitMessage);

        final List<JobInstanceDAO> runningJobs = jobInstanceAdapter.getAllInprogressJobsForShard(shardId);
        Assert.assertEquals(runningJobs.size(), 1);
        Assert.assertEquals(runningJobs.get(0).getInstanceId(), jobId);

        simulateRecovery();

        startRecoveryManagerThread();

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t0", 0)), Arrays.asList(tk("t0", 0)));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t1", 0), tk("t2", 0), tk("t3", 0)),
                Arrays.asList(tk("t3", 0)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t1", 0), tk("t2", 0)), Arrays.asList(tk("t1", 0)));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t2", 0)), Arrays.asList(tk("t2", 0)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t4", 0), tk("t4", 1), tk("t4", 2)),
                Arrays.asList(tk("t4", 0), tk("t4", 1), tk("t4", 2)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t5", 0), tk("t5", 1), tk("t5", 2)),
                Arrays.asList(tk("t5", 0), tk("t5", 1), tk("t5", 2)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t6", 0), tk("t6", 1), tk("t6", 2)),
                Arrays.asList(tk("t6", 0), tk("t6", 1), tk("t6", 2)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t7", 0), tk("t7", 1), tk("t7", 2)),
                Arrays.asList(tk("t7", 0), tk("t7", 1), tk("t7", 2)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t8", 0), tk("t9", 0)), Arrays.asList(tk("t8", 0)));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t9", 0)), Arrays.asList(tk("t9", 0)));

        final TcsCtrlMessage msg = producer.getMessage();
        Assert.assertNotNull(msg);
        final JobCompleteMessage jobCompleteMsg = (JobCompleteMessage) msg;
        Assert.assertEquals(jobCompleteMsg.getJobName(), jobName0);
        Assert.assertEquals(jobCompleteMsg.getJobId(), jobId);

        getAndAssertJobState(jobId, JobState.COMPLETE);
    }

    @Test
    public void testRecoveredJobExecutionSuccessWithNonZeroParallelCount2() throws IOException, InterruptedException {

        final String jobId = UUID.randomUUID().toString();
        final JobSubmitRequest req = new JobSubmitRequest(jobName0, jobId, "rmq://1.2.3.4/def/rkey", null);

        final JobSubmitRequestMessage jobSubmitMessage = new JobSubmitRequestMessage();
        jobSubmitMessage.setRequest(req);
        jobHandler.handleSubmitJob(jobSubmitMessage);

        final List<JobInstanceDAO> runningJobs = jobInstanceAdapter.getAllInprogressJobsForShard(shardId);
        Assert.assertEquals(runningJobs.size(), 1);
        Assert.assertEquals(runningJobs.get(0).getInstanceId(), jobId);

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t0", 0)), Arrays.asList(tk("t0", 0)));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t1", 0), tk("t2", 0), tk("t3", 0)),
                Arrays.asList(tk("t3", 0)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t1", 0), tk("t2", 0)), new ArrayList<TaskKey>());

        completeTaskAndInjectStepCount(tk("t1", 0), "teststep", 3);

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t2", 0)), Arrays.asList(tk("t2", 0)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t4", 0), tk("t4", 1), tk("t4", 2)),
                Arrays.asList(tk("t4", 0), tk("t4", 1), tk("t4", 2)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t5", 0), tk("t5", 1), tk("t5", 2)),
                new ArrayList<TaskKey>());

        simulateShutdown();

        simulateRecovery();

        startRecoveryManagerThread();

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t5", 0), tk("t5", 1), tk("t5", 2)),
                Arrays.asList(tk("t5", 0), tk("t5", 1), tk("t5", 2)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t6", 0), tk("t6", 1), tk("t6", 2)),
                Arrays.asList(tk("t6", 0), tk("t6", 1), tk("t6", 2)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t7", 0), tk("t7", 1), tk("t7", 2)),
                Arrays.asList(tk("t7", 0), tk("t7", 2)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t7", 1)), Arrays.asList(tk("t7", 1)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t8", 0), tk("t9", 0)), Arrays.asList(tk("t8", 0)));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t9", 0)), Arrays.asList(tk("t9", 0)));

        final TcsCtrlMessage msg = producer.getMessage();
        Assert.assertNotNull(msg);
        final JobCompleteMessage jobCompleteMsg = (JobCompleteMessage) msg;
        Assert.assertEquals(jobCompleteMsg.getJobName(), jobName0);
        Assert.assertEquals(jobCompleteMsg.getJobId(), jobId);

        getAndAssertJobState(jobId, JobState.COMPLETE);
    }

    @Test
    public void testRecoveredJobExecutionSuccessWithNonZeroParallelCount3() throws IOException, InterruptedException {

        final String jobId = UUID.randomUUID().toString();
        final JobSubmitRequest req = new JobSubmitRequest(jobName0, jobId, "rmq://1.2.3.4/def/rkey", null);

        final JobSubmitRequestMessage jobSubmitMessage = new JobSubmitRequestMessage();
        jobSubmitMessage.setRequest(req);
        jobHandler.handleSubmitJob(jobSubmitMessage);

        final List<JobInstanceDAO> runningJobs = jobInstanceAdapter.getAllInprogressJobsForShard(shardId);
        Assert.assertEquals(runningJobs.size(), 1);
        Assert.assertEquals(runningJobs.get(0).getInstanceId(), jobId);

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t0", 0)), Arrays.asList(tk("t0", 0)));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t1", 0), tk("t2", 0), tk("t3", 0)),
                Arrays.asList(tk("t3", 0)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t1", 0), tk("t2", 0)), new ArrayList<TaskKey>());

        completeTaskAndInjectStepCount(tk("t1", 0), "teststep", 3);

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t2", 0)), Arrays.asList(tk("t2", 0)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t4", 0), tk("t4", 1), tk("t4", 2)),
                new ArrayList<TaskKey>());

        simulateShutdown();

        simulateRecovery();

        startRecoveryManagerThread();

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t4", 0), tk("t4", 1), tk("t4", 2)),
                Arrays.asList(tk("t4", 0), tk("t4", 2)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t4", 1), tk("t5", 0), tk("t5", 2)),
                Arrays.asList(tk("t4", 1), tk("t5", 0)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t6", 0), tk("t5", 1), tk("t5", 2)),
                Arrays.asList(tk("t5", 1), tk("t5", 2)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t6", 0), tk("t6", 1), tk("t6", 2)),
                Arrays.asList(tk("t6", 0), tk("t6", 1), tk("t6", 2)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t7", 0), tk("t7", 1), tk("t7", 2)),
                Arrays.asList(tk("t7", 0), tk("t7", 2)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t7", 1)), Arrays.asList(tk("t7", 1)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t8", 0), tk("t9", 0)), Arrays.asList(tk("t8", 0)));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t9", 0)), Arrays.asList(tk("t9", 0)));

        final TcsCtrlMessage msg = producer.getMessage();
        Assert.assertNotNull(msg);
        final JobCompleteMessage jobCompleteMsg = (JobCompleteMessage) msg;
        Assert.assertEquals(jobCompleteMsg.getJobName(), jobName0);
        Assert.assertEquals(jobCompleteMsg.getJobId(), jobId);

        getAndAssertJobState(jobId, JobState.COMPLETE);
    }

    @Test
    public void testRecoveredJobExecutionSuccessWithNonZeroParallelCount4() throws IOException, InterruptedException {

        final String jobId = UUID.randomUUID().toString();
        final JobSubmitRequest req = new JobSubmitRequest(jobName0, jobId, "rmq://1.2.3.4/def/rkey", null);

        final JobSubmitRequestMessage jobSubmitMessage = new JobSubmitRequestMessage();
        jobSubmitMessage.setRequest(req);
        jobHandler.handleSubmitJob(jobSubmitMessage);

        final List<JobInstanceDAO> runningJobs = jobInstanceAdapter.getAllInprogressJobsForShard(shardId);
        Assert.assertEquals(runningJobs.size(), 1);
        Assert.assertEquals(runningJobs.get(0).getInstanceId(), jobId);

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t0", 0)), Arrays.asList(tk("t0", 0)));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t1", 0), tk("t2", 0), tk("t3", 0)),
                Arrays.asList(tk("t3", 0)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t1", 0), tk("t2", 0)), new ArrayList<TaskKey>());

        completeTaskAndInjectStepCount(tk("t1", 0), "teststep", 3);

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t2", 0)), Arrays.asList(tk("t2", 0)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t4", 0), tk("t4", 1), tk("t4", 2)),
                new ArrayList<TaskKey>());

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t4", 0), tk("t4", 1), tk("t4", 2)),
                Arrays.asList(tk("t4", 0), tk("t4", 2)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t4", 1), tk("t5", 0), tk("t5", 2)),
                Arrays.asList(tk("t4", 1), tk("t5", 0)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t6", 0), tk("t5", 1), tk("t5", 2)),
                new ArrayList<TaskKey>());

        simulateShutdown();

        simulateRecovery();

        startRecoveryManagerThread();

        completeTasks(Arrays.asList(tk("t6", 0)));
        completeTasks(Arrays.asList(tk("t5", 1), tk("t5", 2)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t7", 0), tk("t6", 1), tk("t6", 2)),
                Arrays.asList(tk("t6", 1), tk("t6", 2)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t7", 0), tk("t7", 1), tk("t7", 2)),
                Arrays.asList(tk("t7", 0), tk("t7", 2)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t7", 1)), Arrays.asList(tk("t7", 1)));

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t8", 0), tk("t9", 0)), Arrays.asList(tk("t8", 0)));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList(tk("t9", 0)), Arrays.asList(tk("t9", 0)));

        final TcsCtrlMessage msg = producer.getMessage();
        Assert.assertNotNull(msg);
        final JobCompleteMessage jobCompleteMsg = (JobCompleteMessage) msg;
        Assert.assertEquals(jobCompleteMsg.getJobName(), jobName0);
        Assert.assertEquals(jobCompleteMsg.getJobId(), jobId);

        getAndAssertJobState(jobId, JobState.COMPLETE);
    }

    private Map<TaskKey, BeginTaskMessage> getAllStartedTasks(int expectedTaskCount) {
        final Map<TaskKey, BeginTaskMessage> tasks = new HashMap<>();
        int receivedTaskCount = 0;
        while (receivedTaskCount < expectedTaskCount) {
            final TcsCtrlMessage msg = producer.getMessage();
            if (msg == null) {
                break;
            }
            final BeginTaskMessage taskMsg = (BeginTaskMessage) msg;
            tasks.put(tk(taskMsg.getTaskName(), taskMsg.getTaskParallelIndex()), taskMsg);
            receivedTaskCount++;
        }
        return tasks;
    }

    private void assertTaskDetails(Collection<BeginTaskMessage> tasks, List<TaskInstanceDAO> taskDAOs) {
        final Set<TaskKey> taskNames = new HashSet<>();
        final Set<String> taskIds = new HashSet<>();
        for (final BeginTaskMessage task : tasks) {
            taskNames.add(tk(task.getTaskName(), task.getTaskParallelIndex()));
            taskIds.add(task.getTaskId());
        }
        Assert.assertTrue(tasks.size() == taskDAOs.size());
        for (final TaskInstanceDAO taskDAO : taskDAOs) {
            Assert.assertTrue(taskNames.contains(tk(taskDAO.getName(), taskDAO.getParallelExecutionIndex())));
            Assert.assertTrue(taskIds.contains(taskDAO.getInstanceId()));
        }
    }

    private void completeTasks(List<TaskKey> taskKeys) {
        for (final TaskKey taskKey : taskKeys) {
            final BeginTaskMessage task = inProgressTasks.get(taskKey);
            final TaskCompleteMessage taskCompleteMsg = new TaskCompleteMessage(task.getTaskId(), task.getJobId(),
                    RandomStringUtils.randomAlphanumeric(128));
            taskHandler.handleTaskComplete(taskCompleteMsg);
            inProgressTasks.remove(taskKey);
        }
    }

    private void completeTaskAndInjectStepCount(TaskKey taskKey, String stepName, int parallelCount) {
        final BeginTaskMessage task = inProgressTasks.get(taskKey);
        final TaskCompleteMessage taskCompleteMsg = new TaskCompleteMessage(task.getTaskId(), task.getJobId(),
                RandomStringUtils.randomAlphanumeric(128));

        final Map<String, String> taskContextOutput = new HashMap<>();
        final String key = String.format(TCSConstants.STEP_PARALLEL_COUNT_KEY_FORMAT, stepName);
        taskContextOutput.put(key, String.valueOf(parallelCount));
        taskCompleteMsg.setTaskContextOutput(taskContextOutput);

        taskHandler.handleTaskComplete(taskCompleteMsg);
        inProgressTasks.remove(taskKey);
    }

    private void getTasksAssertAndCompleteTasks(String jobId, List<TaskKey> expectedInProgressTasks,
            List<TaskKey> tasksToComplete) {

        final Map<TaskKey, BeginTaskMessage> runningTasks = getAllStartedTasks(expectedInProgressTasks.size());
        inProgressTasks.putAll(runningTasks);
        Assert.assertTrue(inProgressTasks.keySet().containsAll(expectedInProgressTasks));

        final List<TaskInstanceDAO> inProgressTaskDAOs = taskInstanceAdapter.getAllInProgressTasksForJobId(jobId);
        assertTaskDetails(inProgressTasks.values(), inProgressTaskDAOs);

        completeTasks(tasksToComplete);
    }

    private void getAndAssertJobState(String jobId, JobState expectedState) throws InterruptedException {
        String jobState = null;
        for (int i = 0; i < 5; i++) {
            Thread.sleep(200);
            final JobInstanceDAO jobDAO = jobInstanceAdapter.getJobInstanceFromDB(jobId);
            jobState = jobDAO.getState();
            if (StringUtils.equalsIgnoreCase(jobDAO.getState(), expectedState.value())) {
                break;
            }
        }
        Assert.assertEquals(jobState, expectedState.value());
    }

    private void registerJob(JobDefinition jobDef) {
        final MessageConverter messageConverter = Mockito.mock(MessageConverter.class);
        final TCSTestProducer producer = Mockito.mock(TCSTestProducer.class);

        final TcsJobRegisterListener jobRegistrationHandler = new TcsJobRegisterListener(messageConverter, producer);

        final JobSpecRegistrationMessage message = new JobSpecRegistrationMessage();
        message.setJobSpec(jobDef);

        final TcsCtrlMessageResult<?> resultObj = jobRegistrationHandler.processRegisterJob(message);
        Assert.assertTrue(resultObj instanceof TcsCtrlMessageResult);
    }

    private void simulateShutdown() {
        final StopNotifier stopNotifier = new StopNotifier(1);
        dispatcher.stop(stopNotifier);
        stopNotifier.waitUntilAllDispatchersStopped();
    }

    private void simulateRecovery() {
        producer = new TCSTestProducer();

        final MessageConverter messageConverter = Mockito.mock(MessageConverter.class);
        final TCSRollbackDispatcher rollbackDispatcher = Mockito.mock(TCSRollbackDispatcher.class);

        taskBoard = new TaskBoard();

        dispatcher = new TCSDispatcher(taskBoard, producer);
        executor.submit(dispatcher);

        beginJobHandler = new TcsJobExecBeginListener(shardId, messageConverter, producer, taskBoard,
                rollbackDispatcher);

        final TcsJobExecBeginListener handler = beginJobHandler;
        jobHandler = new TcsJobExecSubmitListener(messageConverter, producer) {

            @Override
            String chooseARandomShard() {
                return shardId;
            }

            @Override
            void routeBeginJobMessage(BeginJobMessage beginJobMessage, String shardId) {
                handler.handleBeginJob(beginJobMessage);
            }
        };

        taskHandler = new TcsTaskExecEventListener(shardId, messageConverter, producer, taskBoard, rollbackDispatcher);
    }

    private void startRecoveryManagerThread() {
        final Runnable recoverThread = new Runnable() {
            @Override
            public void run() {
                final TCSShardRecoveryManager recoveryManager = new TCSShardRecoveryManager(shardId, taskBoard);
                recoveryManager.recoverShard();
                System.out.println("Shard recovered");
            }
        };

        executor.submit(recoverThread);
    }

    private static TaskKey tk(String name, int parallelIndex) {
        return new TaskKey(name, parallelIndex);
    }
}
