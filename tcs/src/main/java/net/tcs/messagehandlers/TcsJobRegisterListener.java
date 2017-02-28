package net.tcs.messagehandlers;

import java.util.Arrays;

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
import com.task.coordinator.message.utils.TCSConstants;
import com.task.coordinator.producer.TcsProducer;
import com.task.coordinator.request.message.QueryJobSpecRequest;

import net.tcs.core.JobDefinitionCycleDetector;
import net.tcs.db.JobDefinitionDAO;
import net.tcs.db.adapter.JobDefintionDBAdapter;
import net.tcs.drivers.TCSDriver;
import net.tcs.exceptions.JobAlreadyExistsException;
import net.tcs.messages.QueryJobSpecResponse;
import net.tcs.messages.JobRegistrationResponse;
import net.tcs.task.JobDefinition;

/**
 * RMQ Listener for RegisterJobSpec and QueryJobSpec events.
 *
 */
public class TcsJobRegisterListener extends TcsMessageListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(TcsJobRegisterListener.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final JobDefintionDBAdapter jobDefAdapter;
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

        Object result = null;
        try {
            final Object object = messageConverter.fromMessage(message);
            if (object instanceof JobDefinition) {
                result = processRegisterJob((JobDefinition) object);
            } else if ( object instanceof QueryJobSpecRequest) {
                result = processQueryJob((QueryJobSpecRequest) object);
            } else {
                LOGGER.error("Error unsupported message type");
            }

            if (result != null ) {
                BasicProperties property = new AMQP.BasicProperties.Builder().contentType(MessageProperties.CONTENT_TYPE_JSON).build();

                String response = new ObjectMapper().writeValueAsString(result);
                channel.basicPublish("", message.getMessageProperties().getReplyTo(), property,
                        response.getBytes("UTF-8"));

            }
        } catch (final Exception e) {
            LOGGER.error("Exception in TcsJobRegisterListener.onMessage()", e);
        }
    }

    QueryJobSpecResponse processQueryJob(QueryJobSpecRequest message) {
        final String jobName = message.getJobName();
        final JobDefinitionDAO jobDAO = jobDefAdapter.getJobSpec(jobName);
        if (jobDAO != null) {
            return new QueryJobSpecResponse(jobDAO.getBody(), "OK");
        } else {
            return new QueryJobSpecResponse(null, "JOB_NOT_FOUND", null);
        }
    }

    /*
     * TODO REVISIT error codes
     */
    JobRegistrationResponse processRegisterJob(JobDefinition job) {
        final JobDefinitionCycleDetector cycleDetector = new JobDefinitionCycleDetector(job);
        if (cycleDetector.detectCycle()) {
            LOGGER.error("Cycle detected in Job definition for job: {}", job.getJobName());
            return new JobRegistrationResponse(job.getJobName(), "JOB_DEFINITION_DETECTED_CYCLE");
        }

        try {
            job.validateSteps();
        } catch (final IllegalArgumentException ex) {
            LOGGER.error("Exception in JobDefinition.validateSteps() for Job: " + job.getJobName(), ex);
            return new JobRegistrationResponse(job.getJobName(), "JOB_DEFINITION_INVALID_STEPS");
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
            return new JobRegistrationResponse(job.getJobName(), "ACK");
        } catch (final JobAlreadyExistsException ex) {
            return new JobRegistrationResponse(job.getJobName(), "JOB_EXISTS");
        }
    }
}
