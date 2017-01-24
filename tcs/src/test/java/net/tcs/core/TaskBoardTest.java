package net.tcs.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import junit.framework.Assert;
import net.tcs.core.TCSDispatcher;
import net.tcs.core.TaskBoard;

public class TaskBoardTest {

    private TaskBoard taskBoard;
    final Map<String, String> dispatcherIdMap = new HashMap<>();
    final Map<String, Set<String>> dispatcherToJobIdsMap = new HashMap<>();

    @BeforeMethod
    public void setUp() {
        taskBoard = new TaskBoard();
        dispatcherToJobIdsMap.clear();
        dispatcherIdMap.clear();
    }

    @Test
    public void testNewJobRoutingToispatcher() {


        final TCSDispatcher td1 = new TCSDispatcher(taskBoard, null);
        dispatcherIdMap.put("td1", td1.getTaskDispatcherId());

        final TCSDispatcher td2 = new TCSDispatcher(taskBoard, null);
        dispatcherIdMap.put("td2", td2.getTaskDispatcherId());

        final TCSDispatcher td3 = new TCSDispatcher(taskBoard, null);
        dispatcherIdMap.put("td3", td3.getTaskDispatcherId());

        final TCSDispatcher td4 = new TCSDispatcher(taskBoard, null);
        dispatcherIdMap.put("td4", td4.getTaskDispatcherId());



        for (final String s : dispatcherIdMap.values()) {
            dispatcherToJobIdsMap.put(s, new HashSet<String>());
        }

        for (int i = 0; i < 20; i++) {
            final String jobId = UUID.randomUUID().toString();
            taskBoard.registerJobForExecution(jobId);
            final TCSDispatcher td = taskBoard.getDispatcherForJob(jobId);
            dispatcherToJobIdsMap.get(td.getTaskDispatcherId()).add(jobId);
        }

        for (final Set<String> set : dispatcherToJobIdsMap.values()) {
            Assert.assertEquals(5, set.size());
        }

        completeJobs("td4", 2);

        completeJobs("td2", 4);

        addJobAndAssertDispatcher("td2");
        addJobAndAssertDispatcher("td2");

        completeJobs("td4", 1);

        addJobAndAssertDispatcher("td4");
    }

    private void addJobAndAssertDispatcher(String dispatcherName) {
        final String jobId = UUID.randomUUID().toString();
        taskBoard.registerJobForExecution(jobId);
        final TCSDispatcher td = taskBoard.getDispatcherForJob(jobId);
        dispatcherToJobIdsMap.get(td.getTaskDispatcherId()).add(jobId);
        Assert.assertEquals(td.getTaskDispatcherId(), dispatcherIdMap.get(dispatcherName));
    }

    private void completeJobs(String dispatcherName, int jobCount) {
        final String td4DispatcherId = dispatcherIdMap.get(dispatcherName);

        final Set<String> runningJobs = dispatcherToJobIdsMap.get(td4DispatcherId);

        final List<String> runningJobsList = new ArrayList<>(runningJobs);

        for (int index = 0; index < jobCount; index++) {
            final String jobId = runningJobsList.get(index);
            taskBoard.jobComplete(jobId);
            runningJobs.remove(jobId);
        }
    }
}
