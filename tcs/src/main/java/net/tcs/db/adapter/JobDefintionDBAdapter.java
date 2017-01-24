package net.tcs.db.adapter;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.StringUtils;
import org.javalite.activejdbc.DBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import net.tcs.db.JobDefinitionDAO;
import net.tcs.drivers.TCSDriver;
import net.tcs.exceptions.JobAlreadyExistsException;
import net.tcs.task.JobDefinition;

public class JobDefintionDBAdapter {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobDefintionDBAdapter.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ConcurrentMap<String, JobDefinitionDAO> jobSpecs = new ConcurrentHashMap<>();

    public JobDefinitionDAO getJobSpec(String jobName) {
        JobDefinitionDAO jobSpec = jobSpecs.get(jobName);
        if (jobSpec != null) {
            return jobSpec;
        }

        TCSDriver.getDbAdapter().openConnection();
        try {

            jobSpec = JobDefinitionDAO.findById(jobName);
            if (jobSpec != null) {
                final JobDefinitionDAO oldval = jobSpecs.putIfAbsent(jobName, jobSpec);
                if (oldval != null) {
                    return oldval;
                } else {
                    return jobSpec;
                }
            }
        } finally {
            TCSDriver.getDbAdapter().closeConnection();
        }
        return jobSpec;
    }

    public void saveJobSpec(String jobName, byte[] jobBody) {
        TCSDriver.getDbAdapter().openConnection();
        final JobDefinitionDAO jobSpec = new JobDefinitionDAO();
        jobSpec.setId(jobName);
        jobSpec.setBody(new String(jobBody));
        try {
            jobSpec.insert();
            jobSpecs.putIfAbsent(jobName, jobSpec);
        } catch (final DBException ex) {
            if (StringUtils.containsIgnoreCase(ex.getMessage(), "Duplicate entry")
                    || StringUtils.containsIgnoreCase(ex.getMessage(), "primary key violation")) {
                throw new JobAlreadyExistsException();
            }
        } finally {
            TCSDriver.getDbAdapter().closeConnection();
        }
    }

    /**
     * Get Job Definition from in-memory JobSpec cache. If not found in cache,
     * read from DB, parse JobSpec and save it in cache.
     *
     * @param jobName
     * @return
     */
    public JobDefinition getJobDefinition(String jobName) {

        JobDefinition jobSpec = JobSpecCache.getJobSpecCache().getJobSpec(jobName);
        if (jobSpec != null) {
            return jobSpec;
        }

        final JobDefinitionDAO jobDefDAO = getJobSpec(jobName);
        if (jobDefDAO == null) {
            LOGGER.warn("No Job definition found in DB, for JobName: {}", jobName);
            return null;
        }

        try {
            jobSpec = objectMapper.readValue(jobDefDAO.getBody().getBytes(), JobDefinition.class);
            return JobSpecCache.getJobSpecCache().registerJobSpec(jobName, jobSpec);
        } catch (final IOException e) {
            LOGGER.error("Failed to parse job definition details for JobName: {}", jobName);
            return null;
        }
    }
}
