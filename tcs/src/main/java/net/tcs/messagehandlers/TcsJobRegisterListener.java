package net.tcs.messagehandlers;

import java.io.IOException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.support.converter.MessageConverter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.task.coordinator.amqp.framework.TcsListenerContainerFactory;
import com.task.coordinator.base.message.SuccessResultMessage;
import com.task.coordinator.base.message.TcsCtrlMessageResult;
import com.task.coordinator.base.message.listener.TcsMessageListener;
import com.task.coordinator.base.message.listener.TcsMessageListenerContainer;
import com.task.coordinator.message.utils.TCSConstants;
import com.task.coordinator.producer.TcsProducer;
import com.task.coordinator.request.message.JobSpecRegistrationMessage;
import com.task.coordinator.request.message.QueryJobInstanceRequest;
import com.task.coordinator.request.message.QueryJobInstanceResponse;
import com.task.coordinator.request.message.QueryJobSpecMessage;

import net.tcs.core.JobDefinitionCycleDetector;
import net.tcs.db.JobDefinitionDAO;
import net.tcs.db.JobInstanceDAO;
import net.tcs.db.adapter.JobDefintionDBAdapter;
import net.tcs.db.adapter.JobInstanceDBAdapter;
import net.tcs.drivers.TCSDriver;
import net.tcs.exceptions.JobAlreadyExistsException;
import net.tcs.task.JobDefinition;

/**
 * RMQ Listener for RegisterJobSpec and QueryJobSpec events.
 *
 */
public class TcsJobRegisterListener extends TcsMessageListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(TcsJobRegisterListener.class);

    final ObjectMapper objectMapper = new ObjectMapper();
    private final JobDefintionDBAdapter jobDefAdapter;
    private final JobInstanceDBAdapter jobInstanceAdapter;
    private TcsMessageListenerContainer listenerContainer;

    public static TcsJobRegisterListener createTCSListenerForRegisterJob() {

        final MessageConverter messageConverter = (MessageConverter) TCSDriver.getContext()
                .getBean("defaultMessageConverter");
        final TcsProducer producer = TCSDriver.getContext().getBean(TcsProducer.class);
        return new TcsJobRegisterListener(messageConverter, producer);
    }

    protected TcsJobRegisterListener(MessageConverter messageConverter, TcsProducer producer) {
        super(messageConverter, producer);
        jobDefAdapter = new JobDefintionDBAdapter();
        jobInstanceAdapter = new JobInstanceDBAdapter();

    }

    public void initialize() {
        final TcsListenerContainerFactory factory = TCSDriver.getContext().getBean(TcsListenerContainerFactory.class);

        listenerContainer = factory.createListenerContainer(this,
                Arrays.asList(TCSConstants.TCS_REGISTER_TASK_QUEUE, TCSConstants.TCS_QUERY_TASK_QUEUE));
        listenerContainer.start(1);
    }

    public void cleanup() {
        if (listenerContainer != null) {
            listenerContainer.destroy();
        }
    }

    @Override
    public void onMessage(Message message, Channel channel) throws Exception {

        TcsCtrlMessageResult<?> result = null;
        try {
            final Object object = messageConverter.fromMessage(message);
            if ( object instanceof JobSpecRegistrationMessage ) {
                result = processRegisterJob((JobSpecRegistrationMessage) object);
            } else if ( object instanceof QueryJobSpecMessage) {
                result = processQueryJob((QueryJobSpecMessage) object);
            } else if (object instanceof QueryJobInstanceRequest) {
                processQueryJobInstanceRequest(message, channel, object);
                return;
            } else {
                LOGGER.error("Error unsupported message type");
            }
            if (result != null ) {
                channel.basicPublish("", message.getMessageProperties().getReplyTo(), null, new ObjectMapper().writeValueAsBytes(result));
            }
        } catch (final Exception e) {
            LOGGER.error("Exception in TcsJobRegisterListener.onMessage()", e);
        }
    }

    private void processQueryJobInstanceRequest(Message message, Channel channel, final Object object)
            throws IOException, JsonProcessingException {

        final QueryJobInstanceRequest request = (QueryJobInstanceRequest) object;
        final String jobId = request.getJobInstanceId();
        final JobInstanceDAO jobDAO = jobInstanceAdapter.getJobInstanceFromDB(jobId);
        if (jobDAO != null) {
            final QueryJobInstanceResponse result = new QueryJobInstanceResponse(jobDAO.getInstanceId(),
                    jobDAO.getShardId(), jobDAO.getState());
            channel.basicPublish("", message.getMessageProperties().getReplyTo(), null,
                    new ObjectMapper().writeValueAsBytes(result));
        }
    }

    TcsCtrlMessageResult<?> processQueryJob(QueryJobSpecMessage message) {
        final String jobName = message.getJobName();
        final JobDefinitionDAO jobDAO = jobDefAdapter.getJobSpec(jobName);
        if (jobDAO != null) {
            return new SuccessResultMessage<String>(jobDAO.getBody());
        } else {
            return new SuccessResultMessage<String>("JOB_NOT_FOUND");
        }
    }

    /*
     * TODO REVISIT error codes
     */
    TcsCtrlMessageResult<?> processRegisterJob(JobSpecRegistrationMessage message) {
        final JobDefinition job = message.getJobSpec();

        final JobDefinitionCycleDetector cycleDetector = new JobDefinitionCycleDetector(job);
        if (cycleDetector.detectCycle()) {
            LOGGER.error("Cycle detected in Job definition for job: {}", job.getJobName());
            return new SuccessResultMessage<String>("JOB_DEFINITION_DETECTED_CYCLE");
        }

        try {
            job.validateSteps();
        } catch (final IllegalArgumentException ex) {
            LOGGER.error("Exception in JobDefinition.validateSteps() for Job: " + job.getJobName(), ex);
            return new SuccessResultMessage<String>("JOB_DEFINITION_INVALID_STEPS");
        }

        byte[] bytes;
        try {
            bytes = objectMapper.writeValueAsBytes(job);
        } catch ( final JsonProcessingException e) {
            LOGGER.error("Exception in TcsJobExecSubmitListener.processRegisterJob()", e);
            return null;
        }

        try {
            jobDefAdapter.saveJobSpec(job.getJobName(), bytes);
            return new SuccessResultMessage<String>("ACK : " + job.getJobName());
        } catch (final JobAlreadyExistsException ex) {
            return new SuccessResultMessage<String>("JOB_EXISTS");
        }
    }
}
