package net.tcs.api;

import net.tcs.core.TCSClientRuntime;
import net.tcs.task.JobDefinition;
import net.tcs.task.JobSpec;

public class TCSClientFactory {
    /**
     * Create a Job Spec
     *
     * @param jobName
     * @return
     */
    public static JobSpec createJobSpec(String jobName) {
        return new JobDefinition(jobName);
    }

    /**
     * Create a TCSClientRuntime
     *
     * @return
     */
    public static TCSClient createTCSClient(int numPartitions, String rmqBroker) {
        return new TCSClientRuntime(numPartitions, rmqBroker);
    }
}
