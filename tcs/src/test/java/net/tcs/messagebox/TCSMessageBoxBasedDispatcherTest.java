package net.tcs.messagebox;

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
import com.task.coordinator.request.message.BeginTaskMessage;
import com.task.coordinator.request.message.JobCompleteMessage;
import com.task.coordinator.request.message.JobFailedMessage;
import com.task.coordinator.request.message.JobSpecRegistrationMessage;
import com.task.coordinator.request.message.JobSubmitRequestMessage;
import com.task.coordinator.request.message.TaskCompleteMessage;
import com.task.coordinator.request.message.TaskFailedMessage;

import net.tcs.core.TCSDispatcher;
import net.tcs.core.TCSRollbackDispatcher;
import net.tcs.core.TaskBoard;
import net.tcs.core.TestJobDefCreateUtils;
import net.tcs.db.JobInstanceDAO;
import net.tcs.db.TaskInstanceDAO;
import net.tcs.messages.JobSubmitRequest;
import net.tcs.shard.TCSShardRecoveryManager;
import net.tcs.shard.TCSShardRunner.StopNotifier;
import net.tcs.state.JobState;
import net.tcs.task.JobDefinition;

public class TCSMessageBoxBasedDispatcherTest extends DBAdapterTestBase {
    private TcsJobExecSubmitMessageBox jobHandler;

    private TcsTaskExecEventMessageBox taskHandler;

    private final Map<String, BeginTaskMessage> inProgressTasks = new HashMap<>();

    private final String shardId = "shard0";

    private final String jobName = "testjob";

    private TCSTestProducer producer;

    private TCSDispatcher dispatcher;

    private TaskBoard taskBoard;

    private ExecutorService executor;

    @Override
    @BeforeClass
    public void setup() throws ClassNotFoundException, SQLException, IOException {
        MockitoAnnotations.initMocks(this);
        super.setup();

        registerJob(jobName);
    }

    @Override
    @AfterClass
    public void cleanup() {
        System.out.println("jjacobj message box");
        super.cleanup();
    }

    @BeforeMethod
    public void before() {
        executor = Executors.newCachedThreadPool();
        simulateRecovery();
    }

    @AfterMethod
    public void after() {
        simulateShutdown();
        inProgressTasks.clear();

        executor.shutdown();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    public void testJobExecutionSuccess() throws IOException, InterruptedException {

        final String jobId = UUID.randomUUID().toString();
        final JobSubmitRequest req = new JobSubmitRequest(jobName, jobId, "rmq://1.2.3.4/def/rkey", null);

        final JobSubmitRequestMessage jobSubmitMessage = new JobSubmitRequestMessage();
        jobSubmitMessage.setRequest(req);

        jobHandler.handleSubmitJob(jobSubmitMessage);

        final List<JobInstanceDAO> runningJobs = jobInstanceAdapter.getAllInprogressJobsForShard(shardId);
        Assert.assertEquals(runningJobs.size(), 1);
        Assert.assertEquals(runningJobs.get(0).getInstanceId(), jobId);

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t0", "t10"), Arrays.asList("t0", "t10"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t1", "t3"), Arrays.asList("t3"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t1", "t2", "t5"), Arrays.asList("t1", "t5"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t2", "t7", "t8"), Arrays.asList("t2"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t4", "t7", "t8"), Arrays.asList("t7", "t8"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t4"), Arrays.asList("t4"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t6"), Arrays.asList("t6"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t9"), Arrays.asList("t9"));

        final TcsCtrlMessage msg = producer.getMessage();
        Assert.assertNotNull(msg);
        final JobCompleteMessage jobCompleteMsg = (JobCompleteMessage) msg;
        Assert.assertEquals(jobCompleteMsg.getJobName(), jobName);
        Assert.assertEquals(jobCompleteMsg.getJobId(), jobId);

        getAndAssertJobState(jobId, JobState.COMPLETE);
    }

    @Test
    public void testJobExecutionFailedJob() throws IOException, InterruptedException {

        final String jobId = UUID.randomUUID().toString();
        final JobSubmitRequest req = new JobSubmitRequest(jobName, jobId, "rmq://1.2.3.4/def/rkey", null);

        final JobSubmitRequestMessage jobSubmitMessage = new JobSubmitRequestMessage();
        jobSubmitMessage.setRequest(req);

        jobHandler.handleSubmitJob(jobSubmitMessage);

        final List<JobInstanceDAO> runningJobs = jobInstanceAdapter.getAllInprogressJobsForShard(shardId);
        Assert.assertEquals(runningJobs.size(), 1);
        Assert.assertEquals(runningJobs.get(0).getInstanceId(), jobId);

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t0", "t10"), Arrays.asList("t0", "t10"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t1", "t3"), Arrays.asList("t3"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t1", "t2", "t5"), Arrays.asList("t1", "t5"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t2", "t7", "t8"), new ArrayList<String>());

        failTasks(Arrays.asList("t2"));

        final TcsCtrlMessage msg = producer.getMessage();
        Assert.assertNotNull(msg);
        final JobFailedMessage jobCompleteMsg = (JobFailedMessage) msg;
        Assert.assertEquals(jobCompleteMsg.getJobName(), jobName);
        Assert.assertEquals(jobCompleteMsg.getJobId(), jobId);

        getAndAssertJobState(jobId, JobState.FAILED);
    }

    @Test
    public void testJobExecutionRecoverAfterJobSubmissionAndNoTasksExecutedYet()
            throws IOException, InterruptedException {

        final String jobId = UUID.randomUUID().toString();
        final JobSubmitRequest req = new JobSubmitRequest(jobName, jobId, "rmq://1.2.3.4/def/rkey", null);

        final JobSubmitRequestMessage jobSubmitMessage = new JobSubmitRequestMessage();
        jobSubmitMessage.setRequest(req);

        simulateShutdown();

        jobHandler.handleSubmitJob(jobSubmitMessage);

        final List<JobInstanceDAO> runningJobs = jobInstanceAdapter.getAllInprogressJobsForShard(shardId);
        Assert.assertEquals(runningJobs.size(), 1);
        Assert.assertEquals(runningJobs.get(0).getInstanceId(), jobId);

        simulateRecovery();

        startRecoveryManagerThread();

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t0", "t10"), Arrays.asList("t0", "t10"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t1", "t3"), Arrays.asList("t3"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t1", "t2", "t5"), Arrays.asList("t1", "t5"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t2", "t7", "t8"), Arrays.asList("t2"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t4", "t7", "t8"), Arrays.asList("t7", "t8"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t4"), Arrays.asList("t4"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t6"), Arrays.asList("t6"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t9"), Arrays.asList("t9"));

        final TcsCtrlMessage msg = producer.getMessage();
        Assert.assertNotNull(msg);
        final JobCompleteMessage jobCompleteMsg = (JobCompleteMessage) msg;
        Assert.assertEquals(jobCompleteMsg.getJobName(), jobName);
        Assert.assertEquals(jobCompleteMsg.getJobId(), jobId);

        getAndAssertJobState(jobId, JobState.COMPLETE);
    }

    @Test
    public void testJobExecutionRecoverAfterJobSubmissionAndAllTasksExecuted()
            throws IOException, InterruptedException {

        final String jobId = UUID.randomUUID().toString();
        final JobSubmitRequest req = new JobSubmitRequest(jobName, jobId, "rmq://1.2.3.4/def/rkey", null);

        final JobSubmitRequestMessage jobSubmitMessage = new JobSubmitRequestMessage();
        jobSubmitMessage.setRequest(req);

        jobHandler.handleSubmitJob(jobSubmitMessage);

        final List<JobInstanceDAO> runningJobs = jobInstanceAdapter.getAllInprogressJobsForShard(shardId);
        Assert.assertEquals(runningJobs.size(), 1);
        Assert.assertEquals(runningJobs.get(0).getInstanceId(), jobId);

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t0", "t10"), Arrays.asList("t0", "t10"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t1", "t3"), Arrays.asList("t3"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t1", "t2", "t5"), Arrays.asList("t1", "t5"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t2", "t7", "t8"), Arrays.asList("t2"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t4", "t7", "t8"), Arrays.asList("t7", "t8"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t4"), Arrays.asList("t4"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t6"), Arrays.asList("t6"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t9"), new ArrayList<String>());

        simulateShutdown();

        completeTasks(Arrays.asList("t9"));

        simulateRecovery();

        startRecoveryManagerThread();

        final TcsCtrlMessage msg = producer.getMessage();
        Assert.assertNotNull(msg);
        final JobCompleteMessage jobCompleteMsg = (JobCompleteMessage) msg;
        Assert.assertEquals(jobCompleteMsg.getJobName(), jobName);
        Assert.assertEquals(jobCompleteMsg.getJobId(), jobId);

        getAndAssertJobState(jobId, JobState.COMPLETE);
    }

    @Test
    public void testJobExecutionRecoverSomeTasksCompleteAndNoTasksInProgress()
            throws IOException, InterruptedException {

        final String jobId = UUID.randomUUID().toString();
        final JobSubmitRequest req = new JobSubmitRequest(jobName, jobId, "rmq://1.2.3.4/def/rkey", null);

        final JobSubmitRequestMessage jobSubmitMessage = new JobSubmitRequestMessage();
        jobSubmitMessage.setRequest(req);

        jobHandler.handleSubmitJob(jobSubmitMessage);

        final List<JobInstanceDAO> runningJobs = jobInstanceAdapter.getAllInprogressJobsForShard(shardId);
        Assert.assertEquals(runningJobs.size(), 1);
        Assert.assertEquals(runningJobs.get(0).getInstanceId(), jobId);

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t0", "t10"), Arrays.asList("t0", "t10"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t1", "t3"), Arrays.asList("t3"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t1", "t2", "t5"), new ArrayList<String>());

        simulateShutdown();

        completeTasks(Arrays.asList("t1", "t5"));

        simulateRecovery();

        startRecoveryManagerThread();

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t2", "t7", "t8"), Arrays.asList("t2"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t4", "t7", "t8"), Arrays.asList("t7", "t8"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t4"), Arrays.asList("t4"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t6"), Arrays.asList("t6"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t9"), Arrays.asList("t9"));

        final TcsCtrlMessage msg = producer.getMessage();
        Assert.assertNotNull(msg);
        final JobCompleteMessage jobCompleteMsg = (JobCompleteMessage) msg;
        Assert.assertEquals(jobCompleteMsg.getJobName(), jobName);
        Assert.assertEquals(jobCompleteMsg.getJobId(), jobId);

        getAndAssertJobState(jobId, JobState.COMPLETE);
    }

    @Test
    public void testJobExecutionRecoverSomeTasksCompleteAndSomeTasksInProgress()
            throws IOException, InterruptedException {

        final String jobId = UUID.randomUUID().toString();
        final JobSubmitRequest req = new JobSubmitRequest(jobName, jobId, "rmq://1.2.3.4/def/rkey", null);

        final JobSubmitRequestMessage jobSubmitMessage = new JobSubmitRequestMessage();
        jobSubmitMessage.setRequest(req);

        jobHandler.handleSubmitJob(jobSubmitMessage);

        final List<JobInstanceDAO> runningJobs = jobInstanceAdapter.getAllInprogressJobsForShard(shardId);
        Assert.assertEquals(runningJobs.size(), 1);
        Assert.assertEquals(runningJobs.get(0).getInstanceId(), jobId);

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t0", "t10"), Arrays.asList("t0", "t10"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t1", "t3"), Arrays.asList("t3"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t1", "t2", "t5"), new ArrayList<String>());

        simulateShutdown();

        simulateRecovery();

        startRecoveryManagerThread();

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t1", "t2", "t5"), Arrays.asList("t1", "t5"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t2", "t7", "t8"), Arrays.asList("t2"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t4", "t7", "t8"), Arrays.asList("t7", "t8"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t4"), Arrays.asList("t4"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t6"), Arrays.asList("t6"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t9"), Arrays.asList("t9"));

        final TcsCtrlMessage msg = producer.getMessage();
        Assert.assertNotNull(msg);
        final JobCompleteMessage jobCompleteMsg = (JobCompleteMessage) msg;
        Assert.assertEquals(jobCompleteMsg.getJobName(), jobName);
        Assert.assertEquals(jobCompleteMsg.getJobId(), jobId);

        getAndAssertJobState(jobId, JobState.COMPLETE);
    }

    @Test
    public void testJobExecutionRecoverAfterFailedTask() throws IOException, InterruptedException {

        final String jobId = UUID.randomUUID().toString();
        final JobSubmitRequest req = new JobSubmitRequest(jobName, jobId, "rmq://1.2.3.4/def/rkey", null);

        final JobSubmitRequestMessage jobSubmitMessage = new JobSubmitRequestMessage();
        jobSubmitMessage.setRequest(req);

        jobHandler.handleSubmitJob(jobSubmitMessage);

        final List<JobInstanceDAO> runningJobs = jobInstanceAdapter.getAllInprogressJobsForShard(shardId);
        Assert.assertEquals(runningJobs.size(), 1);
        Assert.assertEquals(runningJobs.get(0).getInstanceId(), jobId);

        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t0", "t10"), Arrays.asList("t0", "t10"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t1", "t3"), Arrays.asList("t3"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t1", "t2", "t5"), Arrays.asList("t1", "t5"));
        getTasksAssertAndCompleteTasks(jobId, Arrays.asList("t2", "t7", "t8"), new ArrayList<String>());

        simulateShutdown();

        failTasks(Arrays.asList("t2", "t7"));

        simulateRecovery();

        startRecoveryManagerThread();

        final TcsCtrlMessage msg = producer.getMessage();
        Assert.assertNotNull(msg);
        final JobFailedMessage jobCompleteMsg = (JobFailedMessage) msg;
        Assert.assertEquals(jobCompleteMsg.getJobName(), jobName);
        Assert.assertEquals(jobCompleteMsg.getJobId(), jobId);

        getAndAssertJobState(jobId, JobState.FAILED);
    }

    private Map<String, BeginTaskMessage> getAllStartedTasks(int expectedTaskCount) {
        final Map<String, BeginTaskMessage> tasks = new HashMap<>();
        int receivedTaskCount = 0;
        while (receivedTaskCount < expectedTaskCount) {
            final TcsCtrlMessage msg = producer.getMessage();
            if (msg == null) {
                break;
            }
            final BeginTaskMessage taskMsg = (BeginTaskMessage) msg;
            tasks.put(taskMsg.getTaskName(), taskMsg);
            receivedTaskCount++;
        }
        return tasks;
    }

    private void assertTaskDetails(Collection<BeginTaskMessage> tasks, List<TaskInstanceDAO> taskDAOs) {
        final Set<String> taskNames = new HashSet<>();
        final Set<String> taskIds = new HashSet<>();
        for (final BeginTaskMessage task : tasks) {
            taskNames.add(task.getTaskName());
            taskIds.add(task.getTaskId());
        }
        Assert.assertTrue(tasks.size() == taskDAOs.size());
        for (final TaskInstanceDAO taskDAO : taskDAOs) {
            Assert.assertTrue(taskNames.contains(taskDAO.getName()));
            Assert.assertTrue(taskIds.contains(taskDAO.getInstanceId()));
        }
    }

    private void completeTasks(List<String> taskNames) {
        for (final String taskName : taskNames) {
            final BeginTaskMessage task = inProgressTasks.get(taskName);
            final TaskCompleteMessage taskCompleteMsg = new TaskCompleteMessage(task.getTaskId(), task.getJobId(),
                    RandomStringUtils.randomAlphanumeric(128));
            taskHandler.handleTaskComplete(taskCompleteMsg);
            inProgressTasks.remove(taskName);
        }
    }

    private void failTasks(List<String> taskNames) {
        for (final String taskName : taskNames) {
            final TaskFailedMessage failMsg = new TaskFailedMessage();
            failMsg.setError("error");
            failMsg.setTaskId(inProgressTasks.get(taskName).getTaskId());
            taskHandler.handleTaskFailed(failMsg);
        }
    }

    private void getTasksAssertAndCompleteTasks(String jobId, List<String> expectedInProgressTasks,
            List<String> tasksToComplete) {

        final Map<String, BeginTaskMessage> runningTasks = getAllStartedTasks(expectedInProgressTasks.size());
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

    private void registerJob(String jobName) {

        final TcsJobRegisterMessageBox jobRegistrationHandler = new TcsJobRegisterMessageBox();

        final JobDefinition jobDef = TestJobDefCreateUtils.createJobDef(jobName);

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

        jobHandler = new TcsJobExecSubmitMessageBox(shardId, taskBoard, rollbackDispatcher);
        taskHandler = new TcsTaskExecEventMessageBox(shardId, taskBoard, rollbackDispatcher);
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
}
