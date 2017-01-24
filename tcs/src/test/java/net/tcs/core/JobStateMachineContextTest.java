package net.tcs.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import net.tcs.core.JobStateMachineContext;
import net.tcs.core.TaskStateMachineContextBase;
import net.tcs.db.TaskInstanceDAO;
import net.tcs.state.TaskState;
import net.tcs.task.JobDefinition;

public class JobStateMachineContextTest {
    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testJobStateMachine() {

        final String jobName = "testjob";
        final JobDefinition job = TestJobDefCreateUtils.createJobDef(jobName);

        final String instanceId = UUID.randomUUID().toString();
        final String shardId = "shard0";
        final String jobNotificationURI = "rmq://1.2.3.4/foo/bar";
        final JobStateMachineContext stateMachine = new JobStateMachineContext(jobName, instanceId, shardId,
                jobNotificationURI);
        stateMachine.initializeFromJobDefinition(job);

        Assert.assertTrue(stateMachine.getAllCompletedTasks().isEmpty());

        final List<TaskStateMachineContextBase> rootTasks = stateMachine.getRootTasks();
        Assert.assertEquals(2, rootTasks.size());
        Assert.assertTrue(rootTasks.get(0).isRootTask());
        Assert.assertTrue(rootTasks.get(1).isRootTask());
        Assert.assertTrue(stateMachine.getTaskStateMachine("t0", 0).isRootTask());
        Assert.assertTrue(stateMachine.getTaskStateMachine("t10", 0).isRootTask());

        taskCompleteAndAssertTransistions(stateMachine, "t0", Arrays.asList("t1", "t3"));
        taskCompleteAndAssertTransistions(stateMachine, "t3", Arrays.asList("t2", "t5"));
        taskCompleteAndAssertTransistions(stateMachine, "t1", new ArrayList<String>());
        taskCompleteAndAssertTransistions(stateMachine, "t5", new ArrayList<String>());
        taskCompleteAndAssertTransistions(stateMachine, "t10", Arrays.asList("t7", "t8"));
        taskCompleteAndAssertTransistions(stateMachine, "t2", Arrays.asList("t4"));
        taskCompleteAndAssertTransistions(stateMachine, "t4", Arrays.asList("t6"));
        taskCompleteAndAssertTransistions(stateMachine, "t6", new ArrayList<String>());
        taskCompleteAndAssertTransistions(stateMachine, "t8", new ArrayList<String>());
        taskCompleteAndAssertTransistions(stateMachine, "t7", Arrays.asList("t9"));
        taskCompleteAndAssertTransistions(stateMachine, "t9", new ArrayList<String>());

        Assert.assertTrue(stateMachine.areAllTasksComplete());
    }

    @Test
    public void testRecoveredJobStateMachine() {

        final String jobName = "testjob";
        final JobDefinition job = TestJobDefCreateUtils.createJobDef(jobName);

        final String instanceId = UUID.randomUUID().toString();
        final String shardId = "shard0";
        final String jobNotificationURI = "rmq://1.2.3.4/foo/bar";

        final List<TaskInstanceDAO> taskDAOs = createTaskDAOs(Arrays.asList("t0", "t10"), new ArrayList<String>());
        final JobStateMachineContext stateMachine = new JobStateMachineContext(jobName, instanceId, shardId,
                jobNotificationURI);
        stateMachine.initializeFromJobDefinition(job);
        stateMachine.updateGraph(taskDAOs);

        Assert.assertEquals(2, stateMachine.getAllCompletedTasks().size());
        assertCompletedTasks(stateMachine, stateMachine.getAllCompletedTasks(), Arrays.asList("t0", "t10"));

        final List<TaskStateMachineContextBase> tasksReadyToExec = stateMachine.getTasksReadyToExecute();
        assertReadyTasks(stateMachine, tasksReadyToExec, Arrays.asList("t1", "t3"));


        taskCompleteAndAssertTransistions(stateMachine, "t3", Arrays.asList("t2", "t5"));
        taskCompleteAndAssertTransistions(stateMachine, "t1", new ArrayList<String>());
        taskCompleteAndAssertTransistions(stateMachine, "t5", Arrays.asList("t7", "t8"));
        taskCompleteAndAssertTransistions(stateMachine, "t2", Arrays.asList("t4"));
        taskCompleteAndAssertTransistions(stateMachine, "t4", Arrays.asList("t6"));
        taskCompleteAndAssertTransistions(stateMachine, "t6", new ArrayList<String>());
        taskCompleteAndAssertTransistions(stateMachine, "t8", new ArrayList<String>());
        taskCompleteAndAssertTransistions(stateMachine, "t7", Arrays.asList("t9"));
        taskCompleteAndAssertTransistions(stateMachine, "t9", new ArrayList<String>());

        Assert.assertTrue(stateMachine.areAllTasksComplete());
    }

    @Test
    public void testRecoveredJobStateMachineWithSomeTasksInProgress() {

        final String jobName = "testjob";
        final JobDefinition job = TestJobDefCreateUtils.createJobDef(jobName);

        final String instanceId = UUID.randomUUID().toString();
        final String shardId = "shard0";
        final String jobNotificationURI = "rmq://1.2.3.4/foo/bar";

        final List<String> inProgressTasks = new ArrayList<>(Arrays.asList("t1", "t3"));
        final List<TaskInstanceDAO> taskDAOs = createTaskDAOs(Arrays.asList("t0", "t10"), inProgressTasks);
        final JobStateMachineContext stateMachine = new JobStateMachineContext(jobName, instanceId, shardId,
                jobNotificationURI);
        stateMachine.initializeFromJobDefinition(job);
        stateMachine.updateGraph(taskDAOs);

        Assert.assertEquals(2, stateMachine.getAllCompletedTasks().size());
        assertCompletedTasks(stateMachine, stateMachine.getAllCompletedTasks(), Arrays.asList("t0", "t10"));

        final List<TaskStateMachineContextBase> tasksReadyToExec = stateMachine.getTasksReadyToExecute();
        for (final TaskStateMachineContextBase t : tasksReadyToExec) {
            Assert.assertTrue(t.isInProgress());
            Assert.assertTrue(inProgressTasks.contains(t.getName()));
        }

        taskCompleteAndAssertTransistions(stateMachine, "t3", Arrays.asList("t2", "t5"));
        taskCompleteAndAssertTransistions(stateMachine, "t1", new ArrayList<String>());
        taskCompleteAndAssertTransistions(stateMachine, "t5", Arrays.asList("t7", "t8"));
        taskCompleteAndAssertTransistions(stateMachine, "t2", Arrays.asList("t4"));
        taskCompleteAndAssertTransistions(stateMachine, "t4", Arrays.asList("t6"));
        taskCompleteAndAssertTransistions(stateMachine, "t6", new ArrayList<String>());
        taskCompleteAndAssertTransistions(stateMachine, "t8", new ArrayList<String>());
        taskCompleteAndAssertTransistions(stateMachine, "t7", Arrays.asList("t9"));
        taskCompleteAndAssertTransistions(stateMachine, "t9", new ArrayList<String>());

        Assert.assertTrue(stateMachine.areAllTasksComplete());
    }

    private static void taskCompleteAndAssertTransistions(JobStateMachineContext stateMachine, String taskName,
            List<String> tasksReadyToExecute) {

        final TaskStateMachineContextBase task = stateMachine.getTaskExecutionContext(taskName, 0);
        task.markInProgress();

        final List<TaskStateMachineContextBase> tasksList = stateMachine.taskComplete(taskName, 0);
        Assert.assertTrue(task.isComplete());
        Assert.assertEquals(tasksReadyToExecute.size(), tasksList.size());
        final Set<String> output = new HashSet<>();
        for (final TaskStateMachineContextBase taskSM : tasksList) {
            output.add(taskSM.getName());
            Assert.assertTrue(taskSM.isReadyToExecute());
        }

        Assert.assertTrue(output.containsAll(tasksReadyToExecute));
    }

    private static void assertCompletedTasks(JobStateMachineContext stateMachine,
            List<TaskStateMachineContextBase> completedTasks, List<String> expectedCompletedTaskNames) {

        for (final TaskStateMachineContextBase task : completedTasks) {
            Assert.assertTrue(task.isComplete());
            Assert.assertTrue(expectedCompletedTaskNames.contains(task.getName()));
        }
    }

    private static void assertReadyTasks(JobStateMachineContext stateMachine,
            List<TaskStateMachineContextBase> readyTasks, List<String> expectedReadyTaskNames) {

        for (final TaskStateMachineContextBase task : readyTasks) {
            Assert.assertTrue(task.isReadyToExecute());
            Assert.assertTrue(expectedReadyTaskNames.contains(task.getName()));
        }
    }

    private static List<TaskInstanceDAO> createTaskDAOs(List<String> completedTasks, List<String> inProgressTasks) {

        final List<TaskInstanceDAO> taskDAOs = new ArrayList<>();
        for (final String task : completedTasks) {
            final TaskInstanceDAO taskDAO = Mockito.mock(TaskInstanceDAO.class);
            Mockito.when(taskDAO.getName()).thenReturn(task);
            Mockito.when(taskDAO.getParallelExecutionIndex()).thenReturn(0);
            Mockito.when(taskDAO.getState()).thenReturn(TaskState.COMPLETE.name());
            taskDAOs.add(taskDAO);
        }

        for (final String task : inProgressTasks) {
            final TaskInstanceDAO taskDAO = Mockito.mock(TaskInstanceDAO.class);
            Mockito.when(taskDAO.getName()).thenReturn(task);
            Mockito.when(taskDAO.getParallelExecutionIndex()).thenReturn(0);
            Mockito.when(taskDAO.getState()).thenReturn(TaskState.INPROGRESS.name());
            taskDAOs.add(taskDAO);
        }
        return taskDAOs;
    }
}
