package net.tcs.db.adapter;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import net.tcs.task.JobDefinition;

public class JobSpecCache {

    private static final JobSpecCache CACHE = new JobSpecCache();

    public static JobSpecCache getJobSpecCache() {
        return CACHE;
    }

    private final ConcurrentMap<String, JobDefinition> jobSpecs = new ConcurrentHashMap<>();

    public JobDefinition registerJobSpec(String jobName, JobDefinition jobSpec) {
        jobSpec.validateSteps();
        final JobDefinition existingSpec = jobSpecs.putIfAbsent(jobName, jobSpec);
        if (existingSpec != null) {
            return existingSpec;
        } else {
            return jobSpec;
        }
    }

    public JobDefinition getJobSpec(String jobName) {
        return jobSpecs.get(jobName);
    }
}
