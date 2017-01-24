package net.tcs.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.MessageConverter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.task.coordinator.amqp.framework.TcsListenerContainerFactory;
import com.task.coordinator.base.message.ErrorResponse;
import com.task.coordinator.base.message.SuccessResultMessage;
import com.task.coordinator.base.message.TcsCtrlMessageResult;
import com.task.coordinator.endpoint.TcsTaskExecutionEndpoint;
import com.task.coordinator.message.utils.TCSConstants;
import com.task.coordinator.message.utils.TCSMessageUtils;
import com.task.coordinator.producer.TcsProducer;
import com.task.coordinator.request.message.JobRollbackRequestMessage;
import com.task.coordinator.request.message.JobSpecRegistrationMessage;
import com.task.coordinator.request.message.JobSubmitRequestMessage;
import com.task.coordinator.request.message.QueryJobInstanceRequest;
import com.task.coordinator.request.message.QueryJobInstanceResponse;
import com.task.coordinator.request.message.QueryJobSpecMessage;
import com.task.coordinator.request.message.TaskCompleteMessage;
import com.task.coordinator.request.message.TaskFailedMessage;
import com.task.coordinator.request.message.TaskInProgressMessage;
import com.task.coordinator.request.message.TaskRollbackCompleteMessage;
import com.tcs.amqp.producer.TcsProducerImpl;

import net.tcs.api.TCSCallback;
import net.tcs.api.TCSClient;
import net.tcs.api.TCSJobHandler;
import net.tcs.api.TCSTaskContext;
import net.tcs.exceptions.UnregisteredTaskSpecException;
import net.tcs.messages.JobRollbackRequest;
import net.tcs.messages.JobRollbackResponse;
import net.tcs.messages.JobSubmitRequest;
import net.tcs.messages.JobSubmitResponse;
import net.tcs.messaging.AddressParser;
import net.tcs.messaging.SpringRmqConnectionFactory;
import net.tcs.task.JobDefinition;
import net.tcs.task.JobSpec;
import net.tcs.task.TaskDefinition;

public class TCSClientRuntime implements TCSClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(TCSClientRuntime.class);

    private final int numPartitions;
    private final String rmqBrokerAddress;

    private SpringRmqConnectionFactory rmqFactory;
    private AmqpTemplate template;
    private volatile boolean initialized = false;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Random r = new Random();
    private final ConcurrentMap<String, JobDefinition> jobSpecs = new ConcurrentHashMap<>();
    private final TcsListenerContainerFactory factory = new TcsListenerContainerFactory();
    private TcsProducer producer;
    private final TCSRMQCommandExecutor rmqCommandExecutor = new TCSRMQCommandExecutor();

    public TCSClientRuntime(int numPartitions, String rmqBrokerAddress) {
        this.numPartitions = numPartitions;
        this.rmqBrokerAddress = rmqBrokerAddress;
    }

    private static final class TaskHandlerKey {
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((jobName == null) ? 0 : jobName.hashCode());
            result = prime * result + ((taskName == null) ? 0 : taskName.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            final TaskHandlerKey other = (TaskHandlerKey) obj;
            if (jobName == null) {
                if (other.jobName != null)
                    return false;
            } else if (!jobName.equals(other.jobName))
                return false;
            if (taskName == null) {
                if (other.taskName != null)
                    return false;
            } else if (!taskName.equals(other.taskName))
                return false;
            return true;
        }

        public TaskHandlerKey(String jobName, String taskName) {
            this.jobName = jobName;
            this.taskName = taskName;
        }

        private final String jobName;
        private final String taskName;
    }

    private final ConcurrentMap<String, TCSClientJobCallbackListener> jobListeners = new ConcurrentHashMap<>();
    private final ConcurrentMap<TaskHandlerKey, TCSClientTaskCallbackListener> taskCallbackListeners = new ConcurrentHashMap<>();

    public SpringRmqConnectionFactory getRmqFactory() {
        return rmqFactory;
    }

    @Override
    public void initialize() {
        if (!initialized) {
            synchronized (this) {
                if (!initialized) {
                    rmqFactory = SpringRmqConnectionFactory.createConnectionFactory(rmqBrokerAddress);
                    template = rmqFactory.createRabbitTemplate();

                    producer = new TcsProducerImpl(template);
                    factory.setConnectionFactory(rmqFactory.getRmqConnectionFactory());

                    rmqCommandExecutor.initialize(rmqBrokerAddress);
                    initialized = true;
                }
            }
        }
    }

    @Override
    public void cleanup() {
        if (initialized) {

            rmqCommandExecutor.close();
            for (final TCSClientTaskCallbackListener listener : taskCallbackListeners.values()) {
                listener.close();
            }
            for (final TCSClientJobCallbackListener listener : jobListeners.values()) {
                listener.close();
            }
            rmqFactory.cleanup();
        }
    }

    @Override
    public void registerJob(JobSpec jobSpec) {
        final JobDefinition job = (JobDefinition) jobSpec;

        final JobSpecRegistrationMessage jobSpecRegistrationMessage = new JobSpecRegistrationMessage();
        jobSpecRegistrationMessage.setJobSpec(job);

        final Object result = template.convertSendAndReceive(TCSConstants.TCS_EXCHANGE,
                TCSConstants.TCS_REGISTER_TASK_RKEY, jobSpecRegistrationMessage);

        if (result == null) {
            LOGGER.error("RegisterJob failed for Job: {}", jobSpec.getJobName());
            return;
        }

        try {
            final TcsCtrlMessageResult<?> resultMessage = mapper.readValue((byte[]) result, TcsCtrlMessageResult.class);

            if (resultMessage instanceof TcsCtrlMessageResult) {
                final TcsCtrlMessageResult<?> ctrlMsg = resultMessage;
                LOGGER.info("RegisterJob result: {}", ctrlMsg.getResponse().toString());
            }
        } catch (final Exception e) {
            LOGGER.error("RegisterJob failed for Job: {}", jobSpec.getJobName(), e);
        }
    }

    private String chooseARandomShard() {
        return String.format("%s_%d", TCSConstants.TCS_SHARD_GROUP_NAME, r.nextInt(numPartitions));
    }

    @Override
    public String startJob(String jobName, Map<String, byte[]> input) {

        final Map<String, String> taskInputMap = new HashMap<>();
        if (input != null) {
            for (final Entry<String, byte[]> entry : input.entrySet()) {
                taskInputMap.put(entry.getKey(), new String(entry.getValue()));
            }
        }

        return submitJob(jobName, taskInputMap, new HashMap<String, String>());
    }

    @Override
    public String startJob(String jobName, byte[] jobInputForParentTasks) {
        return startJob(jobName, jobInputForParentTasks, new HashMap<String, String>());
    }

    @Override
    public String startJob(String jobName, byte[] jobInputForParentTasks, Map<String, String> jobContext) {
        final String jobInputAsStr = new String(jobInputForParentTasks);

        final JobDefinition job = getJobDefinition(jobName);

        final Map<String, String> taskInputMap = new HashMap<>();
        final Map<String, TaskDefinition> taskMap = job.getTaskMap();
        for (final TaskDefinition task : taskMap.values()) {
            if (task.getParents().isEmpty()) {
                taskInputMap.put(task.getTaskName(), jobInputAsStr);
            }
        }

        return submitJob(jobName, taskInputMap, jobContext);
    }

    @Override
    public String startJob(String jobName, Map<String, byte[]> input, Map<String, String> jobContext) {

        final Map<String, String> taskInputMap = new HashMap<>();
        if (input != null) {
            for (final Entry<String, byte[]> entry : input.entrySet()) {
                taskInputMap.put(entry.getKey(), new String(entry.getValue()));
            }
        }

        return submitJob(jobName, taskInputMap, jobContext);
    }

    private String submitJob(String jobName, final Map<String, String> taskInputMap, Map<String, String> jobContext) {
        final TCSClientJobCallbackListener jobListener = jobListeners.get(jobName);

        if (jobListener == null) {
            System.out.println("Not prepared to execute job: " + jobName);
            System.out.println("use Prepare command to prepare for Job and task(s)");
            return null;
        }

        final String jobId = UUID.randomUUID().toString();

        final String shardId = chooseARandomShard();
        System.out.println("startJob: " + jobId + "   chose shard: " + shardId);

        final JobSubmitRequest req = new JobSubmitRequest(jobName, jobId, jobListener.getEndpoint().toEndpointURI(),
                taskInputMap);
        req.setJobContext(jobContext);

        final JobSubmitRequestMessage jobSubmitMessage = new JobSubmitRequestMessage();
        jobSubmitMessage.setRequest(req);

        final TcsTaskExecutionEndpoint address = TCSMessageUtils
                .getEndpointAddressForPublishingJobNotificationsOnShard(shardId);

        final Object result = template
                .convertSendAndReceive(address.getExchangeName(), address.getRoutingKey(), jobSubmitMessage);

        if (result == null) {
            LOGGER.error("StartJob failed for Job: {}", jobName);
            return null;
        }

        TcsCtrlMessageResult<?> resultMessage = null;
        try {
            resultMessage = mapper.readValue((byte[]) result, TcsCtrlMessageResult.class);
        } catch (final Exception e) {
            e.printStackTrace();
        }

        if (resultMessage instanceof TcsCtrlMessageResult) {
            final JobSubmitResponse response = (JobSubmitResponse) resultMessage.getResponse();
            if (response != null) {
                LOGGER.debug("Started Job; Name: {}, JobId: {}, ShardId: {}", jobName, response.getJobId(),
                        response.getShardId());
            }
        }
        return jobId;
    }

    @Override
    public void taskComplete(TCSTaskContext taskExecutionContext, byte[] taskOutput) {
        taskComplete(taskExecutionContext, taskOutput, new HashMap<String, String>());
    }

    @Override
    public void taskComplete(TCSTaskContext taskExecutionContext, byte[] taskOutput,
            Map<String, String> taskContextOutput) {
        final TaskCompleteMessage req = new TaskCompleteMessage(taskExecutionContext.getTaskId(),
                taskExecutionContext.getJobId(),
                new String(taskOutput));
        req.setTaskContextOutput(taskContextOutput);

        final TcsTaskExecutionEndpoint address = TCSMessageUtils
                .getEndpointAddressForPublishingTaskNotificationsOnShard(taskExecutionContext.getShardId());
        template.convertAndSend(address.getExchangeName(), address.getRoutingKey(), req);
    }

    private JobDefinition getJobDefinition(String jobName) {
        if (jobSpecs.containsKey(jobName)) {
            return jobSpecs.get(jobName);
        }

        final JobSpec jobSpec = queryRegisteredJob(jobName);
        if (jobSpec != null) {
            final JobDefinition job = (JobDefinition) jobSpec;
            final JobDefinition existing = jobSpecs.putIfAbsent(jobName, job);
            if (existing != null) {
                return existing;
            } else {
                return job;
            }
        }
        return null;
    }

    @Override
    public void prepareToExecute(String jobName, TCSJobHandler jobHandler, Map<String, TCSCallback> taskHandlers) {
        final JobDefinition job = getJobDefinition(jobName);
        if (job != null) {
            final Map<String, TaskDefinition> taskMap = job.getTaskMap();
            final Set<String> registeredTasks = taskMap.keySet();

            final Set<String> tasks = taskHandlers.keySet();
            if (!registeredTasks.containsAll(tasks)) {
                LOGGER.error("Some of the tasks are not registered for Job: {}", jobName);

                for (final String task : tasks) {
                    if (!registeredTasks.contains(task)) {
                        throw new UnregisteredTaskSpecException(jobName, task);
                    }
                }
                return;
            }

            final RabbitTemplate rmqTemplate = (RabbitTemplate) template;
            final MessageConverter converter = rmqTemplate.getMessageConverter();

            final String jobHandlerQueue = String.format("%s-%s", jobName, UUID.randomUUID().toString());
            final String rkey = jobHandlerQueue;
            final TcsTaskExecutionEndpoint jobHandlerEndpoint = new TcsTaskExecutionEndpoint(rmqBrokerAddress,
                    TCSConstants.TCS_EXECUTOR_EXCHANGE, rkey);
            rmqCommandExecutor.bindWithPrivateQueue(jobHandlerQueue, jobHandlerEndpoint);

            final TCSClientJobCallbackListener jobListener = new TCSClientJobCallbackListener(jobHandlerQueue, producer,
                    converter);

            if (null == jobListeners.putIfAbsent(jobName, jobListener)) {
                jobListener.registerJobCallback(jobHandler);
                jobListener.initialize(factory, jobHandlerEndpoint);
            }

            for (final String task : tasks) {

                final TaskHandlerKey key = new TaskHandlerKey(jobName, task);
                if (taskCallbackListeners.containsKey(key)) {
                    LOGGER.info("Already registered to execute: {}/{}", jobName, task);
                    continue;
                }

                final TaskDefinition taskDef = taskMap.get(task);

                final TcsTaskExecutionEndpoint endpoint = AddressParser.parseAddress(taskDef.getTaskExecutionTarget());
                final String queueName = rmqCommandExecutor.bindWithPrivateQueue(endpoint);

                final TCSClientTaskCallbackListener taskListener = new TCSClientTaskCallbackListener(queueName,
                        producer, converter);

                if (null == taskCallbackListeners.putIfAbsent(key, taskListener)) {
                    taskListener.registerTCSCallback(taskHandlers.get(task));
                    taskListener.initialize(factory);
                }
            }
        }
    }

    @Override
    public void taskFailed(TCSTaskContext taskContext, byte[] error) {
        final TaskFailedMessage req = new TaskFailedMessage(taskContext.getTaskId(), new String(error));

        final TcsTaskExecutionEndpoint address = TCSMessageUtils
                .getEndpointAddressForPublishingTaskNotificationsOnShard(taskContext.getShardId());
        template.convertAndSend(address.getExchangeName(), address.getRoutingKey(), req);
    }

    @Override
    public JobSpec queryRegisteredJob(String jobName) {

        final QueryJobSpecMessage message = new QueryJobSpecMessage();
        message.setJobName(jobName);

        final Object result = template.convertSendAndReceive(TCSConstants.TCS_EXCHANGE,
                TCSConstants.TCS_QUERY_TASK_RKEY, message);

        if (result == null) {
            LOGGER.warn("Job: {} not registered with TCS", jobName);
            return null;
        }

        try {
            final TcsCtrlMessageResult<?> ctrlMsg = mapper.readValue((byte[]) result, TcsCtrlMessageResult.class);

            if (ctrlMsg instanceof TcsCtrlMessageResult) {
                final SuccessResultMessage<String> resultMsg = new SuccessResultMessage(ctrlMsg.getResponse());

                if (resultMsg != null) {
                    try {
                        return mapper.readValue(resultMsg.getResponse(), JobDefinition.class);
                    } catch (final Exception e) {
                        LOGGER.error(resultMsg.getResponse(), e);
                        return null;
                    }
                }
            }
        } catch (final Exception e) {
            LOGGER.error(e.getMessage());
            return null;
        }

        LOGGER.warn("Job: {} not registered with TCS", jobName);
        return null;
    }

    @Override
    public void taskInProgress(TCSTaskContext taskContext) {
        final TaskInProgressMessage req = new TaskInProgressMessage(taskContext.getTaskId());

        final TcsTaskExecutionEndpoint address = TCSMessageUtils
                .getEndpointAddressForPublishingTaskNotificationsOnShard(taskContext.getShardId());
        template.convertAndSend(address.getExchangeName(), address.getRoutingKey(), req);
    }

    @Override
    public void rollbackJob(String jobName, String jobInstanceId) {

        final QueryJobInstanceRequest requestQuery = new QueryJobInstanceRequest();
        requestQuery.setJobInstanceId(jobInstanceId);

        final Object resultQuery = template.convertSendAndReceive(TCSConstants.TCS_EXCHANGE,
                TCSConstants.TCS_QUERY_TASK_RKEY, requestQuery);

        if (resultQuery == null) {
            LOGGER.warn("Job InstanceId: {} not found with TCS", jobInstanceId);
            return;
        }

        final QueryJobInstanceResponse responseQuery;
        try {
            responseQuery = mapper.readValue((byte[]) resultQuery, QueryJobInstanceResponse.class);
        } catch (final Exception e) {
            LOGGER.error(e.getMessage());
            return;
        }

        if (!StringUtils.equalsIgnoreCase("FAILED", responseQuery.getStatus())) {
            final String errMessage = String.format(
                    "Job: %s, instanceId: %s is not FAILED; therefore cannot rollback. Current state: %s",
                    jobName, jobInstanceId, responseQuery.getStatus());
            LOGGER.error(errMessage);
            System.out.println(errMessage);
            return;
        }

        final TCSClientJobCallbackListener jobListener = jobListeners.get(jobName);
        final JobRollbackRequest req = new JobRollbackRequest(jobName, jobInstanceId,
                jobListener.getEndpoint().toEndpointURI());

        final JobRollbackRequestMessage jobRollbackMessage = new JobRollbackRequestMessage();
        jobRollbackMessage.setRequest(req);

        final TcsTaskExecutionEndpoint address = TCSMessageUtils
                .getEndpointAddressForPublishingJobNotificationsOnShard(responseQuery.getShardId());

        final Object result = template.convertSendAndReceive(address.getExchangeName(), address.getRoutingKey(),
                jobRollbackMessage);

        if (result == null) {
            LOGGER.error("Rollback failed for Job: {}", jobName);
            return;
        }

        TcsCtrlMessageResult<?> resultMessage = null;
        try {
            resultMessage = mapper.readValue((byte[]) result, TcsCtrlMessageResult.class);
        } catch (final Exception e) {
            e.printStackTrace();
        }

        if (resultMessage instanceof TcsCtrlMessageResult) {
            if (resultMessage.isError()) {
                final ErrorResponse errorMessage = (ErrorResponse) resultMessage.getResponse();
                System.out.println(errorMessage.getErrorCode());
                System.out.println(errorMessage.getErrorMessage());
                System.out.println(errorMessage.getDetailedMessage());
            } else {
                final JobRollbackResponse response = (JobRollbackResponse) resultMessage.getResponse();
                if (response != null) {
                    LOGGER.debug("Started Job rollback; Name: {}, JobId: {}", jobName, response.getJobId());
                }
            }
        }
    }

    @Override
    public void taskRollbackComplete(TCSTaskContext taskContext) {
        final TaskRollbackCompleteMessage req = new TaskRollbackCompleteMessage(taskContext.getTaskId());

        final TcsTaskExecutionEndpoint address = TCSMessageUtils
                .getEndpointAddressForPublishingTaskNotificationsOnShard(taskContext.getShardId());
        template.convertAndSend(address.getExchangeName(), address.getRoutingKey(), req);
    }

    @Override
    public void taskRollbackNotSupported() {
        // TODO Auto-generated method stub

    }

    @Override
    public void taskRollbackFailed() {
        // TODO Auto-generated method stub
    }
}
