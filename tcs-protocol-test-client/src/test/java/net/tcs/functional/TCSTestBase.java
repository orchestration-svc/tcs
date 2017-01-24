package net.tcs.functional;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.tcs.api.TCSClient;
import net.tcs.api.TCSClientFactory;
import net.tcs.task.JobDefinition;
import net.tcs.task.JobSpec;

public class TCSTestBase {

    protected TCSClient tcsClientRuntime;
    protected TCSTestRuntime testRuntime;

    protected final Map<String, JobSpec> jobspecMap = new HashMap<>();

    public void setup(String rmqIP, int partitionCount) throws IOException {
        tcsClientRuntime = TCSClientFactory.createTCSClient(partitionCount, rmqIP);
        tcsClientRuntime.initialize();

        testRuntime = new TCSTestRuntime();
        testRuntime.initialize(tcsClientRuntime);

        final Set<String> jobs = new HashSet<>();

        try (BufferedReader fileReader = new BufferedReader(new FileReader("jobs.txt"))) {
            String line = null;
            while ((line = fileReader.readLine()) != null) {
                jobs.add(line);
            }
        }

        for (final String jobName : jobs) {
            final JobSpec jobSpec = tcsClientRuntime.queryRegisteredJob(jobName);
            if (jobSpec != null) {
                final JobDefinition jobDef = (JobDefinition) jobSpec;
                jobDef.validateSteps();
                jobspecMap.put(jobName, jobDef);
            }
        }
    }

    public void cleanup() {
        if (testRuntime != null) {
            testRuntime.cleanup();
        }

        if (tcsClientRuntime != null) {
            tcsClientRuntime.cleanup();
        }

        TCSTestRuntime.registerTaskNotifCallback(null);
    }
}
