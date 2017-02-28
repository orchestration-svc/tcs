package net.tcs.task;

import java.util.List;
import java.util.Set;

/**
 * Job specification
 */
public interface JobSpec {
    /**
     * Job Name
     *
     * @return
     */
    public String getJobName();

    /**
     * Set of task names
     *
     * @return
     */
    public Set<String> getTasks();

    /**
     * Adds a Task specification to the JobSpec. A task will be scheduled for
     * execution only after all its parent tasks are complete.
     *
     * @param taskName
     * @param taskExecutionTarget
     *            URI of the form rmq://BrokerAddress/exchange/routingkey
     * @param parentTasks
     *            set of parent task names
     */
    public JobSpec addTask(String taskName, String taskExecutionTarget, Set<String> parentTasks);

    /**
     * Get Task Specification
     *
     * @param taskName
     * @return
     */
    public TaskSpec getTaskSpec(String taskName);

    /**
     * Mark a list of Tasks under a Step. A Task cannot be part of more than a
     * step. For a list of Tasks to be under a step, all the tasks (except the
     * first task) must only have one predecessor.
     *
     * For example, if a step consists of T1, T2, T3, T4, then:
     *
     * T2 must have only one predecessor: T1.
     *
     * T3 must have only one predecessor: T2.
     *
     * T4 must have only one predecessor: T3.
     *
     * @param name
     * @param tasks
     */
    public void markStep(String name, List<String> tasks);
}
