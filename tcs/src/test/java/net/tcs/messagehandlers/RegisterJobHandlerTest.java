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

import com.task.coordinator.producer.TcsProducer;

import junit.framework.Assert;
import net.tcs.core.TestJobDefCreateUtils;
import net.tcs.messages.QueryJobSpecResponse;
import net.tcs.messages.JobRegistrationResponse;
import net.tcs.messages.QueryJobSpecRequest;
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

        final QueryJobSpecRequest queryMessage = new QueryJobSpecRequest();
        queryMessage.setJobName(jobName);

        QueryJobSpecResponse resultObjQuery = jobHandler.processQueryJob(queryMessage);
        Assert.assertEquals("JOB_NOT_FOUND", resultObjQuery.getStatus());

        final JobDefinition jobDef = TestJobDefCreateUtils.createJobDef(jobName);

        JobRegistrationResponse resultObj = jobHandler.processRegisterJob(jobDef);

        {
            Assert.assertTrue(StringUtils.equalsIgnoreCase(resultObj.getStatus(), "ACK"));
            Assert.assertTrue(StringUtils.equalsIgnoreCase(resultObj.getJobName(), jobName));
        }

        resultObjQuery = jobHandler.processQueryJob(queryMessage);
        Assert.assertEquals("OK", resultObjQuery.getStatus());
        {
            final JobDefinition jobDefRead = mapper.readValue(resultObjQuery.getJobSpec(), JobDefinition.class);
            Assert.assertNotNull(jobDefRead);
            Assert.assertEquals(jobName, jobDefRead.getJobName());

        }

        resultObj = jobHandler.processRegisterJob(jobDef);
        {
            Assert.assertTrue(StringUtils.equalsIgnoreCase(resultObj.getStatus(), "JOB_EXISTS"));
        }
    }
}
