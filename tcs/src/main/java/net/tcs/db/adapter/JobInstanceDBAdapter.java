package net.tcs.db.adapter;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import net.tcs.core.TaskKey;
import net.tcs.db.JobInstanceDAO;
import net.tcs.db.TaskInstanceDAO;
import net.tcs.db.TaskInstanceDataDAO;
import net.tcs.drivers.TCSDriver;
import net.tcs.exceptions.JobInstanceNotFoundException;
import net.tcs.exceptions.JobStateException;
import net.tcs.state.JobState;
import net.tcs.state.TaskRollbackState;
import net.tcs.state.TaskState;
import net.tcs.task.JobDefinition;
import net.tcs.task.TaskDefinition;

public class JobInstanceDBAdapter {

    /**
     * Save a newly submitted JobInstance, along with all its Tasks in READY
     * state, and with the initial Task input.
     *
     * @param jobDAO
     * @param jobDefinition
     * @param taskInputs
     */
    public void saveSubmittedJob(JobInstanceDAO jobDAO, JobDefinition jobDefinition, Map<String, String> taskInputs) {
        TCSDriver.getDbAdapter().openConnection();

        try {
            TCSDriver.getDbAdapter().beginTX();
            jobDAO.insert();

            final Map<String, TaskDefinition> taskMap = jobDefinition.getTaskMap();
            for (final TaskDefinition task : taskMap.values()) {
                final TaskInstanceDAO taskDAO = createTaskDAO(task.getTaskName(), jobDAO, 0);

                final TaskInstanceDataDAO taskDataDAO = new TaskInstanceDataDAO();
                taskDataDAO.setInstanceId(taskDAO.getInstanceId());
                if (taskInputs != null) {
                    final String taskInput = taskInputs.get(task.getTaskName());
                    if (taskInput != null) {
                        taskDataDAO.setTaskInput(taskInput);
                    }
                }
                taskDAO.insert();
                taskDataDAO.insert();
            }

            TCSDriver.getDbAdapter().commitTX();
        } catch (final RuntimeException re) {
            TCSDriver.getDbAdapter().rollbackTX();
            throw re;
        } finally {
            TCSDriver.getDbAdapter().closeConnection();
        }
    }

    /**
     * Save parallel tasks during Step parallelization
     *
     * @param jobDAO
     * @param parallelCount
     * @param taskNameToInputMap
     * @return Map of {TaskKey => TaskInstanceId}
     */
    public Map<TaskKey, String> saveParallelTasks(JobInstanceDAO jobDAO, int parallelCount,
            LinkedHashMap<String, String> taskNameToInputMap) {

        final Map<TaskKey, String> map = new HashMap<>();

        TCSDriver.getDbAdapter().openConnection();

        try {
            TCSDriver.getDbAdapter().beginTX();

            for (final String taskName : taskNameToInputMap.keySet()) {

                for (int parallelIndex = 1; parallelIndex < parallelCount; parallelIndex++) {
                    final TaskInstanceDAO taskDAO = createTaskDAO(taskName, jobDAO, parallelIndex);

                    final TaskInstanceDataDAO taskDataDAO = new TaskInstanceDataDAO();
                    taskDataDAO.setInstanceId(taskDAO.getInstanceId());

                    final String taskInput = taskNameToInputMap.get(taskName);
                    if (taskInput != null) {
                        taskDataDAO.setTaskInput(taskInput);
                    }

                    taskDAO.insert();
                    taskDataDAO.insert();

                    final TaskKey key = new TaskKey(taskName, parallelIndex);
                    map.put(key, taskDAO.getInstanceId());
                }
            }

            TCSDriver.getDbAdapter().commitTX();
        } catch (final RuntimeException re) {
            TCSDriver.getDbAdapter().rollbackTX();
            throw re;
        } finally {
            TCSDriver.getDbAdapter().closeConnection();
        }
        return map;
    }

    private TaskInstanceDAO createTaskDAO(String taskName, JobInstanceDAO jobDAO, int parallelIndex) {
        final TaskInstanceDAO taskDAO = new TaskInstanceDAO();
        taskDAO.setName(taskName);
        taskDAO.setInstanceId(UUID.randomUUID().toString());
        taskDAO.setJobInstanceId(jobDAO.getInstanceId());
        taskDAO.setShardId(jobDAO.getShardId());
        taskDAO.setParallelExecutionIndex(parallelIndex);
        taskDAO.setState(TaskState.INIT.value());
        taskDAO.setRollbackState(TaskRollbackState.ROLLBACK_NOT_READY.value());
        return taskDAO;
    }

    public void saveCompletedJob(final String jobId) {

        new RetryOnStaleDecorator<JobInstanceDAO>() {

            @Override
            public JobInstanceDAO executeDB() {
                final JobInstanceDAO jobDAO = JobInstanceDAO.findById(jobId);
                if (jobDAO == null) {
                    throw new JobInstanceNotFoundException(jobId);
                }

                if (JobState.INPROGRESS != JobState.get(jobDAO.getState())) {
                    throw new JobStateException(JobState.INPROGRESS, JobState.get(jobDAO.getState()));
                }

                jobDAO.setState(JobState.COMPLETE.value());
                jobDAO.setCompletionTime(new Date());
                jobDAO.saveIt();
                return jobDAO;
            }
        }.executeNoTransaction("saving completed job");
    }

    public void saveRolledbackJob(final String jobId) {

        new RetryOnStaleDecorator<JobInstanceDAO>() {

            @Override
            public JobInstanceDAO executeDB() {
                final JobInstanceDAO jobDAO = JobInstanceDAO.findById(jobId);
                if (jobDAO == null) {
                    throw new JobInstanceNotFoundException(jobId);
                }

                if (JobState.ROLLBACK_INPROGRESS != JobState.get(jobDAO.getState())) {
                    throw new JobStateException(JobState.ROLLBACK_INPROGRESS, JobState.get(jobDAO.getState()));
                }

                jobDAO.setState(JobState.ROLLBACK_COMPLETE.value());
                jobDAO.saveIt();
                return jobDAO;
            }
        }.executeNoTransaction("saving rolled-back job");
    }

    public JobInstanceDAO beginRollbackJob(final String jobId, final String jobNotificationUri) {

        return new RetryOnStaleDecorator<JobInstanceDAO>() {

            @Override
            public JobInstanceDAO executeDB() {
                final JobInstanceDAO jobDAO = JobInstanceDAO.findById(jobId);
                if (jobDAO == null) {
                    throw new JobInstanceNotFoundException(jobId);
                }

                if (JobState.FAILED != JobState.get(jobDAO.getState())) {
                    throw new JobStateException(JobState.FAILED, JobState.get(jobDAO.getState()));
                }

                jobDAO.setState(JobState.ROLLBACK_INPROGRESS.value());
                jobDAO.setJobNotificationUri(jobNotificationUri);
                jobDAO.saveIt();
                return jobDAO;
            }
        }.executeNoTransaction("beginning rolled-back job");
    }

    public void saveFailedJob(final String jobId) {

        new RetryOnStaleDecorator<JobInstanceDAO>() {

            @Override
            public JobInstanceDAO executeDB() {
                final JobInstanceDAO jobDAO = JobInstanceDAO.findById(jobId);

                if (JobState.INPROGRESS != JobState.get(jobDAO.getState())) {
                    throw new JobStateException(JobState.INPROGRESS, JobState.get(jobDAO.getState()));
                }

                jobDAO.setState(JobState.FAILED.value());
                jobDAO.setCompletionTime(new Date());
                jobDAO.saveIt();
                return jobDAO;
            }
        }.executeNoTransaction("saving failed job");
    }

    /**
     * Get all In-Progress JobInstances for the ShardId.
     *
     * @param shardId
     * @return
     */
    public List<JobInstanceDAO> getAllInprogressJobsForShard(String shardId) {
        final List<JobInstanceDAO> jobs = new ArrayList<>();

        TCSDriver.getDbAdapter().openConnection();

        try {
            final String query = "shardId = ? and state = ?";
            final List<JobInstanceDAO> output = JobInstanceDAO.where(query, shardId, JobState.INPROGRESS.value());
            if (output != null) {
                for (final JobInstanceDAO job : output) {
                    jobs.add(job);
                }
            }
            return jobs;
        } finally {
            TCSDriver.getDbAdapter().closeConnection();
        }
    }

    public JobInstanceDAO getJobInstanceFromDB(String jobId) {
        TCSDriver.getDbAdapter().openConnection();
        try {
            return JobInstanceDAO.findById(jobId);
        } finally {
            TCSDriver.getDbAdapter().closeConnection();
        }
    }
}
