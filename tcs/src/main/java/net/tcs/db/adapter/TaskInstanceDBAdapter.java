package net.tcs.db.adapter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.task.coordinator.message.utils.TCSConstants;

import net.tcs.core.TaskKey;
import net.tcs.db.JobInstanceDAO;
import net.tcs.db.TaskInstanceDAO;
import net.tcs.db.TaskInstanceDataDAO;
import net.tcs.drivers.TCSDriver;
import net.tcs.exceptions.TaskRetryException;
import net.tcs.exceptions.TaskRollbackStateException;
import net.tcs.exceptions.TaskStateException;
import net.tcs.state.TaskRollbackState;
import net.tcs.state.TaskState;
import net.tcs.task.ParentTaskInfo;
import net.tcs.task.TCSTaskInput;

public class TaskInstanceDBAdapter {

    /**
     * Get the Task input for the task. It is a combination of input for the
     * task, and output from its parent tasks.
     *
     * @param taskId
     * @param parentIdToNameMap
     * @return
     */
    public TCSTaskInput getInputForTask(String taskId, Map<String, TaskKey> parentIdToNameMap) {

        TCSDriver.getDbAdapter().openConnection();

        try {
            final TaskInstanceDataDAO taskDAO = TaskInstanceDataDAO.findById(taskId);
            if (taskDAO == null) {
                return null;
            }

            final String taskInput = taskDAO.getTaskInput();

            final Map<String, ParentTaskInfo> parentTaskOutput = new HashMap<>();
            for (final Entry<String, TaskKey> entry : parentIdToNameMap.entrySet()) {
                final TaskInstanceDataDAO parentTaskDAO = TaskInstanceDataDAO.findById(entry.getKey());
                if (parentTaskDAO != null) {
                    parentTaskOutput.put(entry.getValue().taskName,
                            new ParentTaskInfo(entry.getKey(), parentTaskDAO.getTaskOutput()));
                }
            }

            return new TCSTaskInput(taskInput, parentTaskOutput);
        } finally {
            TCSDriver.getDbAdapter().closeConnection();
        }
    }

    public String getOutputForTask(String taskId) {

        TCSDriver.getDbAdapter().openConnection();

        try {
            final TaskInstanceDataDAO taskDAO = TaskInstanceDataDAO.findById(taskId);
            if (taskDAO == null) {
                return null;
            }

            return taskDAO.getTaskOutput();
        } finally {
            TCSDriver.getDbAdapter().closeConnection();
        }
    }

    /**
     * Get TaskInput(s) for task(s)
     *
     * @param taskIds
     * @return
     */
    public LinkedHashMap<String, String> getInputForTask(Collection<String> taskIds) {

        final LinkedHashMap<String, String> taskIdToInputMap = new LinkedHashMap<>();

        TCSDriver.getDbAdapter().openConnection();

        try {
            for (final String taskId : taskIds) {
                final TaskInstanceDataDAO taskDAO = TaskInstanceDataDAO.findById(taskId);
                if (taskDAO != null) {
                    taskIdToInputMap.put(taskId, taskDAO.getTaskInput());
                }
            }
        } finally {
            TCSDriver.getDbAdapter().closeConnection();
        }
        return taskIdToInputMap;
    }

    /**
     * Saves a completed task and its output, plus context map. Also updates the
     * state from INPROGRESS to COMPLETE.
     *
     * @param taskId
     * @param jobId
     * @param taskOutput
     * @param taskOutputContext
     * @param mapper
     * @return
     */
    public TaskInstanceDAO saveCompletedTaskWithContext(final String taskId, final String jobId,
            final String taskOutput, final Map<String, String> taskOutputContext, final ObjectMapper mapper) {

        return new RetryOnStaleDecorator<TaskInstanceDAO>() {

            @Override
            public TaskInstanceDAO executeDB() {
                final JobInstanceDAO jobDAO = JobInstanceDAO.findById(jobId);
                final TaskInstanceDAO taskDAO = TaskInstanceDAO.findById(taskId);
                final TaskInstanceDataDAO taskDataDAO = TaskInstanceDataDAO.findById(taskId);

                if (jobDAO != null && taskDAO != null && taskDataDAO != null) {
                    if (TaskState.INPROGRESS != TaskState.get(taskDAO.getState())) {
                        throw new TaskStateException(TaskState.INPROGRESS, TaskState.get(taskDAO.getState()));
                    }
                    taskDAO.setState(TaskState.COMPLETE.value());
                    taskDAO.setRollbackState(TaskRollbackState.ROLLBACK_READY.value());
                    taskDataDAO.setTaskOutput(taskOutput);

                    final Map<String, String> jobContextMap = getJobContext(mapper, jobDAO);
                    jobContextMap.putAll(taskOutputContext);

                    try {
                        final String context = mapper.writeValueAsString(jobContextMap);
                        jobDAO.setJobContext(context);
                    } catch (final JsonProcessingException e) {
                    }

                    final Date currentTime = new Date();
                    taskDAO.setUpdateTime(currentTime);
                    taskDAO.setCompletionTime(currentTime);

                    jobDAO.saveIt();
                    taskDAO.saveIt();
                    taskDataDAO.saveIt();
                }
                return taskDAO;
            }

        }.execute("saving completed Task");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> getJobContext(ObjectMapper mapper, JobInstanceDAO jobDAO) {
        try {
            return StringUtils.isEmpty(jobDAO.getJobContext()) ? new HashMap<>()
                    : mapper.readValue(jobDAO.getJobContext(), Map.class);
        } catch (final IOException e) {
            return new HashMap<>();
        }

    }

    /**
     * Saves a completed task and its output. Also updates the state from
     * INPROGRESS to COMPLETE.
     *
     * @param taskId
     * @param taskOutput
     * @return
     */
    public TaskInstanceDAO saveCompletedTask(final String taskId, final String taskOutput) {

        return new RetryOnStaleDecorator<TaskInstanceDAO>() {

            @Override
            public TaskInstanceDAO executeDB() {
                final TaskInstanceDAO taskDAO = TaskInstanceDAO.findById(taskId);
                final TaskInstanceDataDAO taskDataDAO = TaskInstanceDataDAO.findById(taskId);

                if (taskDAO != null && taskDataDAO != null) {
                    if (TaskState.INPROGRESS != TaskState.get(taskDAO.getState())) {
                        throw new TaskStateException(TaskState.INPROGRESS, TaskState.get(taskDAO.getState()));
                    }
                    taskDAO.setState(TaskState.COMPLETE.value());
                    taskDAO.setRollbackState(TaskRollbackState.ROLLBACK_READY.value());
                    taskDataDAO.setTaskOutput(taskOutput);
                    final Date currentTime = new Date();
                    taskDAO.setUpdateTime(currentTime);
                    taskDAO.setCompletionTime(currentTime);
                    taskDAO.saveIt();
                    taskDataDAO.saveIt();
                }
                return taskDAO;
            }

        }.execute("saving completed Task");
    }

    /**
     * Updates the state from INPROGRESS to FAILED.
     *
     * @param taskId
     * @param taskOutput
     * @return
     */
    public TaskInstanceDAO saveFailedTask(final String taskId, final String error) {
        return new RetryOnStaleDecorator<TaskInstanceDAO>() {

            @Override
            public TaskInstanceDAO executeDB() {
                final TaskInstanceDAO taskDAO = TaskInstanceDAO.findById(taskId);
                final TaskInstanceDataDAO taskDataDAO = TaskInstanceDataDAO.findById(taskId);

                if (taskDAO != null && taskDataDAO != null) {
                    if (TaskState.INPROGRESS != TaskState.get(taskDAO.getState())) {
                        throw new TaskStateException(TaskState.INPROGRESS, TaskState.get(taskDAO.getState()));
                    }
                    taskDAO.setState(TaskState.FAILED.value());
                    taskDAO.setRollbackState(TaskRollbackState.ROLLBACK_DISALLOWED.value());
                    taskDataDAO.setTaskOutput(error);
                    final Date currentTime = new Date();
                    taskDAO.setUpdateTime(currentTime);
                    taskDAO.setCompletionTime(currentTime);
                    taskDAO.saveIt();
                    taskDataDAO.saveIt();
                }
                return taskDAO;
            }
        }.execute("saving failed Task");
    }

    /**
     * For a long-running tasks, TCS clients have the option to periodically
     * ping TCS and notify that the task is still in progress. In such cases,
     * update the task's updateTime, so that the task will not be picked up for
     * retry.
     *
     * @param taskId
     * @return
     */
    public TaskInstanceDAO saveUpdatedTask(final String taskId) {
        return new RetryOnStaleDecorator<TaskInstanceDAO>() {

            @Override
            public TaskInstanceDAO executeDB() {
                final TaskInstanceDAO taskDAO = TaskInstanceDAO.findById(taskId);

                if (taskDAO != null) {
                    if (TaskState.INPROGRESS != TaskState.get(taskDAO.getState())) {
                        throw new TaskStateException(TaskState.INPROGRESS, TaskState.get(taskDAO.getState()));
                    }
                    taskDAO.setUpdateTime(new Date());
                    taskDAO.saveIt();
                }
                return taskDAO;
            }
        }.executeNoTransaction("updating Task");
    }

    public TaskInstanceDAO saveRollbackCompletedTask(final String taskId) {
        return new RetryOnStaleDecorator<TaskInstanceDAO>() {

            @Override
            public TaskInstanceDAO executeDB() {
                final TaskInstanceDAO taskDAO = TaskInstanceDAO.findById(taskId);

                if (taskDAO != null) {
                    if (TaskRollbackState.ROLLBACK_INPROGRESS != TaskRollbackState.get(taskDAO.getRollbackState())) {
                        throw new TaskRollbackStateException(TaskRollbackState.ROLLBACK_INPROGRESS,
                                TaskRollbackState.get(taskDAO.getRollbackState()));
                    }
                    taskDAO.setRollbackState(TaskRollbackState.ROLLBACK_COMPLETE.value());
                    taskDAO.setRollbackCompletionTime(new Date());
                    taskDAO.saveIt();
                }
                return taskDAO;
            }
        }.executeNoTransaction("saving rolled-back Task");
    }

    public TaskInstanceDataDAO getTaskFromDB(String taskId) {
        TCSDriver.getDbAdapter().openConnection();
        try {
            return TaskInstanceDataDAO.findById(taskId);
        } finally {
            TCSDriver.getDbAdapter().closeConnection();
        }
    }

    /**
     * Updates the state from READY to INPROGRESS.
     *
     * @param taskId
     * @return
     */
    public TaskInstanceDAO updateTaskStateToInProgress(final String taskId) {
        return new RetryOnStaleDecorator<TaskInstanceDAO>() {

            @Override
            public TaskInstanceDAO executeDB() {
                final TaskInstanceDAO taskDAO = TaskInstanceDAO.findById(taskId);

                if (taskDAO != null) {
                    if (TaskState.INIT != TaskState.get(taskDAO.getState())) {
                        throw new TaskStateException(TaskState.INIT, TaskState.get(taskDAO.getState()));
                    }
                    final Date time = new Date();
                    taskDAO.setStartTime(time);
                    taskDAO.setUpdateTime(time);
                    taskDAO.setState(TaskState.INPROGRESS.value());
                    taskDAO.saveIt();
                }
                return taskDAO;
            }
        }.executeNoTransaction("saving In-Progress Task");
    }

    /**
     * Updates the state from ROLLBACK_READY to ROLLBACK_INPROGRESS.
     *
     * @param taskId
     * @return
     */
    public TaskInstanceDAO updateTaskRollbackStateToInProgress(final String taskId) {

        return new RetryOnStaleDecorator<TaskInstanceDAO>() {

            @Override
            public TaskInstanceDAO executeDB() {
                final TaskInstanceDAO taskDAO = TaskInstanceDAO.findById(taskId);

                if (taskDAO != null) {
                    if (TaskRollbackState.ROLLBACK_READY != TaskRollbackState.get(taskDAO.getRollbackState())) {
                        throw new TaskRollbackStateException(TaskRollbackState.ROLLBACK_READY,
                                TaskRollbackState.get(taskDAO.getRollbackState()));
                    }
                    taskDAO.setRollbackStartTime(new Date());
                    taskDAO.setRollbackState(TaskRollbackState.ROLLBACK_INPROGRESS.value());
                    taskDAO.saveIt();
                }
                return taskDAO;
            }
        }.executeNoTransaction("initializing Task for rollback");
    }

    /**
     * Get all In-Progress / Expired Task Instances for the ShardId.
     *
     * @param shardId
     * @param timeout
     *            timeout in seconds
     * @return
     */
    public List<TaskInstanceDAO> getAllInprogressTasksForShard(String shardId, int timeout) {
        final List<TaskInstanceDAO> tasks = new ArrayList<>();

        TCSDriver.getDbAdapter().openConnection();

        try {
            final String query = "shardId = ? and state = ? and updateTime < ?";

            final Date date = new Date(System.currentTimeMillis() - timeout * 1000);
            final List<TaskInstanceDAO> output = TaskInstanceDAO.where(query, shardId, TaskState.INPROGRESS.value(),
                    date);
            if (output != null) {
                for (final TaskInstanceDAO task : output) {
                    tasks.add(task);
                }
            }
            return tasks;
        } finally {
            TCSDriver.getDbAdapter().closeConnection();
        }
    }

    /**
     * Update Task retry count.
     *
     * @param taskId
     * @return
     * @throws TaskRetryException,
     *             if it reaches {@link TCSConstants#TASK_MAX_RETRY_COUNT}
     */
    public TaskInstanceDAO updateTaskRetry(final String taskId) {
        return new RetryOnStaleDecorator<TaskInstanceDAO>() {

            @Override
            public TaskInstanceDAO executeDB() {
                final TaskInstanceDAO taskDAO = TaskInstanceDAO.findById(taskId);

                if (taskDAO != null) {

                    if (TaskState.INPROGRESS != TaskState.get(taskDAO.getState())) {
                        throw new TaskStateException(TaskState.INPROGRESS, TaskState.get(taskDAO.getState()));
                    }

                    if (taskDAO.getRetryCount() >= TCSConstants.TASK_MAX_RETRY_COUNT) {
                        taskDAO.setState(TaskState.FAILED.value());
                        taskDAO.saveIt();
                        throw new TaskRetryException("Reached max retry count: " + TCSConstants.TASK_MAX_RETRY_COUNT);
                    } else {
                        taskDAO.setUpdateTime(new Date());
                        taskDAO.setRetryCount(taskDAO.getRetryCount() + 1);
                        taskDAO.saveIt();
                    }
                }
                return taskDAO;
            }
        }.executeNoTransaction("updating Task retry count");
    }

    /**
     * Gets all the Task instances for a given Job instance. This API is used
     * during ShardRecovery.
     *
     * @param jobInstanceId
     * @return
     */
    public List<TaskInstanceDAO> getAllTasksForJobId(String jobInstanceId) {

        final List<TaskInstanceDAO> tasks = new ArrayList<>();
        TCSDriver.getDbAdapter().openConnection();

        try {
            final String query = "jobInstanceId = ?";
            final List<TaskInstanceDAO> output = TaskInstanceDAO.where(query, jobInstanceId);
            if (output != null) {
                for (final TaskInstanceDAO task : output) {
                    tasks.add(task);
                }
            }
            return tasks;
        } finally {
            TCSDriver.getDbAdapter().closeConnection();
        }
    }

    public List<TaskInstanceDAO> getAllCompletedTasksForJobId(String jobInstanceId) {
        return getAllTasksForJobIdInState(jobInstanceId, TaskState.COMPLETE);
    }

    public List<TaskInstanceDAO> getAllInProgressTasksForJobId(String jobInstanceId) {
        return getAllTasksForJobIdInState(jobInstanceId, TaskState.INPROGRESS);
    }

    private List<TaskInstanceDAO> getAllTasksForJobIdInState(String jobInstanceId, TaskState taskState) {
        final List<TaskInstanceDAO> tasks = new ArrayList<>();

        TCSDriver.getDbAdapter().openConnection();

        try {
            final String query = "jobInstanceId = ? and state = ?";

            final List<TaskInstanceDAO> output = TaskInstanceDAO.where(query, jobInstanceId,
                    taskState.value());
            if (output != null) {
                for (final TaskInstanceDAO task : output) {
                    tasks.add(task);
                }
            }
            return tasks;
        } finally {
            TCSDriver.getDbAdapter().closeConnection();
        }
    }
}
