package net.messaging.clusterbox.main;

import org.apache.log4j.BasicConfigurator;

import net.messaging.clusterbox.Address;
import net.messaging.clusterbox.ClusterBox;
import net.messaging.clusterbox.message.RequestMessage;
import net.messaging.clusterbox.rabbitmq.RabbitmqBrokerConfig;
import net.messaging.clusterbox.rabbitmq.RabbitmqClusterBox;
import net.messaging.clusterbox.rabbitmq.RabbitmqClusterBoxConfig;

public class RabbitmqClusterBoxMain {

    public static void main(String[] args) {
        BasicConfigurator.configure();
        RabbitmqBrokerConfig brokerConfig = new RabbitmqBrokerConfig("localhost", null, null, null, true);
        RabbitmqClusterBoxConfig clusterBoxConfig = new RabbitmqClusterBoxConfig(brokerConfig, "Ardenwood",
                "Ardenwood-1");
        ClusterBox clusterBox = RabbitmqClusterBox.newClusterBox(clusterBoxConfig);
        clusterBox.start();
        RabbitmqClusterBoxConfig clusterBoxConfig2 = new RabbitmqClusterBoxConfig(brokerConfig, "misson", "misson-2");
        ClusterBox clusterBox2 = RabbitmqClusterBox.newClusterBox(clusterBoxConfig2);
        clusterBox2.start();
        DefaultClusterBoxMessageBox messageBox = new DefaultClusterBoxMessageBox("1234-1", "1234");
        clusterBox2.registerMessageBox(messageBox);
        try {
            Thread.currentThread().sleep(5000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        MyPayload payload = new MyPayload("firstName", 10, "LastName");
        RequestMessage<MyPayload> message = new RequestMessage<MyPayload>();
        message.setPayload(payload);
        message.setRequestKey(MyPayload.class.getName());
        Address from = new Address();
        from.setClusterBoxName("Ardenwood");
        from.setClusterBoxMailBoxName("34173");
        Address to = new Address();
        to.setClusterBoxName("misson");
        to.setClusterBoxMailBoxName("1234");
        message.setTo(to);
        message.pushFrom(from);
        clusterBox.getDropBox().drop(message);
    }
}
