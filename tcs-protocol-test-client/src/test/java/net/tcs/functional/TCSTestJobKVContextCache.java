package net.tcs.functional;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.testng.Assert;

public class TCSTestJobKVContextCache {

    private static class JobContext {
        private final ConcurrentMap<String, String> kvContextMap = new ConcurrentHashMap<>();

        public void addKVContext(String key, String val) {
            kvContextMap.put(key, val);
        }

        public void checkKVContext(String key, String val) {
            Assert.assertTrue(kvContextMap.containsKey(key));
            Assert.assertEquals(kvContextMap.get(key), val);
        }
    }

    private final ConcurrentMap<String, JobContext> jobContextMap = new ConcurrentHashMap<>();

    public void registerAndSaveKVOutput(String jobId, Map<String, String> kvMap) {
        final JobContext context = new JobContext();
        jobContextMap.put(jobId, context);
        saveKVOutput(jobId, kvMap);
    }

    public void saveKVOutput(String jobId, Map<String, String> kvMap) {
        final JobContext jobContext = jobContextMap.get(jobId);
        Assert.assertNotNull(jobContext);
        for (final Entry<String, String> entry : kvMap.entrySet()) {
            jobContext.addKVContext(entry.getKey(), entry.getValue());
        }
    }

    public void checkKVInput(String jobId, Map<String, String> kvMap) {
        JobContext jobContext = null;
        for (int i = 0; i < 5; i++) {
            jobContext = jobContextMap.get(jobId);
            if (jobContext != null) {
                break;
            } else {
                try {
                    Thread.sleep(200);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        Assert.assertNotNull(jobContextMap.get(jobId));
        for (final Entry<String, String> entry : kvMap.entrySet()) {
            jobContext.checkKVContext(entry.getKey(), entry.getValue());
        }

    }

    public void cleanup() {
        jobContextMap.clear();
    }
}

