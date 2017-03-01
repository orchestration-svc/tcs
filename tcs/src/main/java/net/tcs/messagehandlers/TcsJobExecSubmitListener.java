package net.tcs.messagehandlers;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.MessageConverter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.task.coordinator.amqp.framework.TcsListenerContainerFactory;
import com.task.coordinator.base.message.listener.TcsMessageListener;
import com.task.coordinator.base.message.listener.TcsMessageListenerContainer;
import com.task.coordinator.endpoint.TcsTaskExecutionEndpoint;
import com.task.coordinator.message.utils.TCSConstants;
import com.task.coordinator.message.utils.TCSMessageUtils;
import com.task.coordinator.producer.TcsProducer;

import net.tcs.db.JobDefinitionDAO;
import net.tcs.db.JobInstanceDAO;
import net.tcs.db.TaskInstanceDAO;
import net.tcs.db.adapter.JobDefintionDBAdapter;
import net.tcs.db.adapter.JobInstanceDBAdapter;
import net.tcs.db.adapter.TaskInstanceDBAdapter;
import net.tcs.drivers.TCSDriver;
import net.tcs.exceptions.JobInstanceNotFoundException;
import net.tcs.exceptions.JobRollbackIllegalStateException;
import net.tcs.exceptions.JobStateException;
import net.tcs.messages.BeginJobMessage;
import net.tcs.messages.JobRollbackRequest;
import net.tcs.messages.JobRollbackResponse;
import net.tcs.messages.JobSubmitRequest;
import net.tcs.messages.JobSubmitResponse;
import net.tcs.messages.RollbackJobMessage;
import net.tcs.state.JobState;
import net.tcs.task.JobDefinition;

public class TcsJobExecSubmitListener extends TcsMessageListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(TcsJobExecSubmitListener.class);

    private final JobDefintionDBAdapter jobDBAdapter = new JobDefintionDBAdapter();
    private final JobInstanceDBAdapter jobInstanceDBAdapter = new JobInstanceDBAdapter();
    protected final TaskInstanceDBAdapter taskDBAdapter = new TaskInstanceDBAdapter();
    private final ObjectMapper mapper = new ObjectMapper().enableDefaultTyping();
    private TcsMessageListenerContainer listenerContainer;
    private final TcsProducer producer;
    private final Random r = new Random();

    public static TcsJobExecSubmitListener createTCSListenerForSubmitJob() {

        final MessageConverter messageConverter = (MessageConverter) TCSDriver.getContext()
                .getBean("defaultMessageConverter");
        final TcsProducer producer = TCSDriver.getContext().getBean(TcsProducer.class);
        return new TcsJobExecSubmitListener(messageConverter, producer);
    }

    public TcsJobExecSubmitListener(MessageConverter messageConverter, TcsProducer producer) {
        super(messageConverter, producer);
        this.producer = producer;
    }

    public void initialize() {
        final TcsListenerContainerFactory factory = TCSDriver.getContext().getBean(TcsListenerContainerFactory.class);

        listenerContainer = factory.createListenerContainer(this, Arrays.asList(TCSConstants.TCS_SUBMIT_JOB_QUEUE));
        listenerContainer.start(1);
    }

    public void cleanup() {
        if (listenerContainer != null) {
            listenerContainer.destroy();
        }
    }

    @Override
    public void onMessage(Message message, Channel channel) {

        try {
            final Object request = messageConverter.fromMessage(message);

            Object response = null;
            if (request instanceof JobSubmitRequest) {
                response = handleSubmitJob((JobSubmitRequest) request);
            } else if (request instanceof JobRollbackRequest) {
                response = handleRollbackJob(((JobRollbackRequest) request));
            } else {
                LOGGER.error("Unsupported message type while processing message: {}", new String(message.getBody()));
                return;
            }

            if (response != null) {
                BasicProperties property = new AMQP.BasicProperties.Builder()
                        .contentType(MessageProperties.CONTENT_TYPE_JSON).build();

                String jsonResponse = new ObjectMapper().writeValueAsString(response);
                channel.basicPublish("", message.getMessageProperties().getReplyTo(), property, jsonResponse.getBytes("UTF-8"));
            }
        } catch (Exception ex) {
            LOGGER.error("Exception while processing message: {}", new String(message.getBody()));
        }
    }

    String chooseARandomShard() {
        int numPartitions = TCSDriver.getNumPartitions();
        return String.format("%s_%d", TCSConstants.TCS_SHARD_GROUP_NAME, r.nextInt(numPartitions));
    }

    private JobInstanceDAO createJobDAO(JobSubmitRequest request, String shardId) {
        final JobInstanceDAO jobDAO = new JobInstanceDAO();
        jobDAO.setInstanceId(request.getJobId());
        jobDAO.setName(request.getJobName());
        jobDAO.setShardId(shardId);
        jobDAO.setJobNotificationUri(request.getJobNotificationUri());
        jobDAO.setState(JobState.INIT.name());
        jobDAO.setStartTime(new Date());

        try {
            final String context = mapper.writeValueAsString(request.getJobContext());
            jobDAO.setJobContext(context);
        } catch (final JsonProcessingException e) {
            LOGGER.error("Exception while serializing JobContext", e);
        }
        return jobDAO;
    }

    /**
     * On receipt of a SubmitJob event, (1) choose a shard for the Job, (2) save
     * the Job in DB, (3) ack the message and (4) route the message to the
     * shard-specific handler for execution.
     *
     * @param channel
     * @param objectMapper
     * @param properties
     * @param body
     */
    JobSubmitResponse handleSubmitJob(JobSubmitRequest jobRequest) {

        /*
         * Check if Job is registered
         */
        final JobDefinitionDAO jobDefDAO = jobDBAdapter.getJobSpec(jobRequest.getJobName());
        if (jobDefDAO == null) {
            LOGGER.warn("No Job definition found in DB, for JobName: {}", jobRequest.getJobName());
            JobSubmitResponse response = new JobSubmitResponse(jobRequest.getJobName(), "FAILED");
            response.setErrorDetails(jobRequest.getJobName() + " is not registered");
            return response;
        }

        String shardId = chooseARandomShard();
        /*
         * Create JobInstance and save in DB
         */
        final JobInstanceDAO jobDAO = createJobDAO(jobRequest, shardId);
        final JobDefinition jobDef = jobDBAdapter.getJobDefinition(jobRequest.getJobName());
        jobInstanceDBAdapter.saveSubmittedJob(jobDAO, jobDef, jobRequest.getInput());
        BeginJobMessage beginJobMessage = new BeginJobMessage(jobDAO.getInstanceId());

        routeBeginJobMessage(beginJobMessage, shardId);

        /*
         * Send JobSubmitResponse
         */
        return new JobSubmitResponse(jobDAO.getName(), jobDAO.getInstanceId(), jobDAO.getShardId());
    }

    void routeBeginJobMessage(BeginJobMessage beginJobMessage, String shardId) {
        /*
         * Route to the shard
         */
        final TcsTaskExecutionEndpoint address = TCSMessageUtils
                .getEndpointAddressForPublishingJobNotificationsOnShard(shardId);
        producer.sendMessage(address.getExchangeName(), address.getRoutingKey(), beginJobMessage);
    }

    private JobRollbackResponse handleRollbackJob(JobRollbackRequest jobRequest) {

        /*
         * Check if Job is registered
         */
        final JobDefinitionDAO jobDefDAO = jobDBAdapter.getJobSpec(jobRequest.getJobName());
        if (jobDefDAO == null) {
            LOGGER.warn("No Job definition found in DB, for JobName: {}", jobRequest.getJobName());
            return new JobRollbackResponse(jobRequest.getJobName(), "FAILED",
                    jobRequest.getJobName() + " is not registered");
        }

        final JobInstanceDAO jobDAO;

        try {
            checkIfJobReadyForRollback(jobRequest.getJobName(), jobRequest.getJobId());
            jobDAO = jobInstanceDBAdapter.beginRollbackJob(jobRequest.getJobId(), jobRequest.getJobNotificationUri());
        } catch (final RuntimeException ex) {
            return new JobRollbackResponse(jobRequest.getJobName(), "FAILED", ex.getMessage());
        }

        if (jobDAO == null) {
            return new JobRollbackResponse(jobRequest.getJobName(), "NOT_FOUND",
                    "Job not found: " + jobRequest.getJobName());
        }

        /*
         * Route to the shard
         */

        final TcsTaskExecutionEndpoint address = TCSMessageUtils
                .getEndpointAddressForPublishingJobNotificationsOnShard(jobDAO.getShardId());
        RollbackJobMessage rollbackJobMessage = new RollbackJobMessage(jobDAO.getInstanceId());
        producer.sendMessage(address.getExchangeName(), address.getRoutingKey(), rollbackJobMessage);

        /*
         * Send JobRollbackResponse
         */
        return new JobRollbackResponse(jobDAO.getName(), jobDAO.getInstanceId());
    }

    private void checkIfJobReadyForRollback(String jobName, String jobInstanceId) {
        final JobInstanceDAO jobDAO = jobInstanceDBAdapter.getJobInstanceFromDB(jobInstanceId);
        if (jobDAO == null) {
            throw new JobInstanceNotFoundException(jobInstanceId);
        }

        if (JobState.FAILED != JobState.get(jobDAO.getState())) {
            throw new JobStateException(JobState.FAILED, JobState.get(jobDAO.getState()));
        }

        final List<TaskInstanceDAO> inProgressTasks = taskDBAdapter.getAllInProgressTasksForJobId(jobInstanceId);
        if (!inProgressTasks.isEmpty()) {
            final String errMessage = String.format(
                    "Job cannot be rolled back, as one or more tasks are in progress. JobName: %s, JobId: %s", jobName,
                    jobInstanceId);
            throw new JobRollbackIllegalStateException(errMessage);
        }
    }
}
