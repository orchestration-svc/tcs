package net.tcs.core;

import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import net.tcs.core.JobDefinitionCycleDetector;
import net.tcs.task.JobDefinition;
import net.tcs.task.TaskDefinition;

public class JobDefinitionCycleTest {
    private JobDefinition job;
    private JobDefinitionCycleDetector cycleDetector;

    @BeforeMethod
    public void before() {
        job = TestJobDefCreateUtils.createJobDef("testjob");
        cycleDetector = new JobDefinitionCycleDetector(job);
        Assert.assertFalse(cycleDetector.detectCycle());
    }

    @AfterMethod
    public void after() {
        Assert.assertTrue(cycleDetector.detectCycle());
    }

    @Test
    public void testCycle1() {
        makeNewParent("t5", "t9");
    }

    @Test
    public void testCycle2() {
        makeNewParent("t0", "t6");
    }

    @Test
    public void testCycle3() {
        makeNewParent("t3", "t6");
    }

    @Test
    public void testCycle4() {
        makeNewParent("t2", "t4");
    }

    @Test
    public void testCycle5() {
        makeNewParent("t0", "t10");
        makeNewParent("t10", "t0");
    }

    @Test
    public void testCycle6() {
        makeNewParent("t0", "t1");
        makeNewParent("t1", "t4");
    }

    /**
     * Introduce loop, by making task2 as new parent of task1
     *
     * @param task1
     * @param task2
     */
    private void makeNewParent(String task1, String task2) {
        final TaskDefinition taskDef = (TaskDefinition) job.getTaskSpec(task1);
        final Set<String> taskParents = taskDef.getParents();
        taskParents.add(task2);
        taskDef.setParents(taskParents);
    }
}
