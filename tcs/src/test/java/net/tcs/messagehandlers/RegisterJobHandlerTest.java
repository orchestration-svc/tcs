package net.tcs.messagehandlers;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.lang3.StringUtils;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.amqp.support.converter.MessageConverter;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.task.coordinator.base.message.SuccessResultMessage;
import com.task.coordinator.base.message.TcsCtrlMessageResult;
import com.task.coordinator.producer.TcsProducer;
import com.task.coordinator.request.message.JobSpecRegistrationMessage;
import com.task.coordinator.request.message.QueryJobSpecMessage;

import junit.framework.Assert;
import net.tcs.core.TestJobDefCreateUtils;
import net.tcs.task.JobDefinition;

public class RegisterJobHandlerTest extends DBAdapterTestBase {
    @Override
    @BeforeClass
    public void setup() throws ClassNotFoundException, SQLException, IOException {
        MockitoAnnotations.initMocks(this);
        super.setup();
    }

    @Override
    @AfterClass
    public void cleanup() {
        super.cleanup();
    }

    @Test
    public void testRegisterAndQueryJobSpec() throws IOException {

        final MessageConverter messageConverter = Mockito.mock(MessageConverter.class);
        final TcsProducer producer = Mockito.mock(TcsProducer.class);

        final TcsJobRegisterListener jobHandler = new TcsJobRegisterListener(messageConverter, producer);

        final String jobName = "testjob";

        final QueryJobSpecMessage queryMessage = new QueryJobSpecMessage();
        queryMessage.setJobName(jobName);

        TcsCtrlMessageResult<?> resultObjQuery = jobHandler.processQueryJob(queryMessage);
        if (resultObjQuery instanceof TcsCtrlMessageResult) {
            final TcsCtrlMessageResult<?> ctrlMsg = resultObjQuery;
            Assert.assertTrue(StringUtils.containsIgnoreCase(ctrlMsg.getResponse().toString(), "JOB_NOT_FOUND"));
        }

        final JobDefinition jobDef = TestJobDefCreateUtils.createJobDef(jobName);

        final JobSpecRegistrationMessage message = new JobSpecRegistrationMessage();
        message.setJobSpec(jobDef);

        TcsCtrlMessageResult<?> resultObj = jobHandler.processRegisterJob(message);
        Assert.assertTrue(resultObj instanceof TcsCtrlMessageResult);

        {
            final TcsCtrlMessageResult<?> ctrlMsg = resultObj;
            Assert.assertTrue(StringUtils.containsIgnoreCase(ctrlMsg.getResponse().toString(), "ACK"));
            Assert.assertTrue(StringUtils.containsIgnoreCase(ctrlMsg.getResponse().toString(), jobName));
        }

        resultObjQuery = jobHandler.processQueryJob(queryMessage);
        Assert.assertTrue(resultObjQuery instanceof TcsCtrlMessageResult);
        {
            final TcsCtrlMessageResult<?> ctrlMsg = resultObjQuery;
            final SuccessResultMessage<String> resultMsg = new SuccessResultMessage(ctrlMsg.getResponse());

            final JobDefinition jobDefRead = mapper.readValue(resultMsg.getResponse(), JobDefinition.class);
            Assert.assertNotNull(jobDefRead);
            Assert.assertEquals(jobName, jobDefRead.getJobName());

        }

        resultObj = jobHandler.processRegisterJob(message);
        Assert.assertTrue(resultObj instanceof TcsCtrlMessageResult);
        {
            final TcsCtrlMessageResult<?> ctrlMsg = resultObj;
            Assert.assertTrue(StringUtils.containsIgnoreCase(ctrlMsg.getResponse().toString(), "JOB_EXISTS"));
        }
    }
}
