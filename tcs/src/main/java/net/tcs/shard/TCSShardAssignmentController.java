package net.tcs.shard;

import java.util.Date;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.IdealStateChangeListener;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;

import net.tcs.drivers.TCSDriver;
import net.tcs.utils.TCSConfig;

public class TCSShardAssignmentController implements IdealStateChangeListener {
    private HelixManager manager;
    private HelixManager controllerManager;
    private final String instanceName;

    public TCSShardAssignmentController(String instanceName) {
        this.instanceName = instanceName;
    }

    public void start() throws Exception {
        System.out.println("TCS ShardAssignment Controller starting: " + new Date().toString());
        final TCSConfig config = TCSDriver.getConfig();
        final String clusterName = config.getClusterConfig().getClusterName();

        final ZKHelixAdmin helixAdmin = new ZKHelixAdmin(config.getZookeeperConnectString());

        final List<String> instancesInCluster = helixAdmin
                .getInstancesInCluster(config.getClusterConfig().getClusterName());

        if ((instancesInCluster == null) || !instancesInCluster.contains(instanceName)) {
            final InstanceConfig instanceConfig = new InstanceConfig(instanceName);
            instanceConfig.setHostName("localhost");
            instanceConfig.setPort(instanceName);
            helixAdmin.addInstance(clusterName, instanceConfig);
        }

        manager = HelixManagerFactory.getZKHelixManager(clusterName, instanceName, InstanceType.CONTROLLER_PARTICIPANT,
                config.getZookeeperConnectString());

        manager.getStateMachineEngine().registerStateModelFactory(StateModelDefId.from("OnlineOffline"),
                new TCSShardLockFactory());
        manager.addIdealStateChangeListener(this);
        manager.connect();

        controllerManager = HelixControllerMain.startHelixController(config.getZookeeperConnectString(), clusterName,
                "controller", HelixControllerMain.STANDALONE);

        System.out.println("TCS ShardAssignment Controller started: " + new Date().toString());
    }

    public void stop() {
        if (controllerManager != null) {
            controllerManager.disconnect();
        }

        if (manager != null) {
            manager.disconnect();
        }

        System.out.println("TCS ShardAssignment Controller shut down");
    }

    @Override
    public void onIdealStateChange(List<IdealState> idealState, NotificationContext changeContext) {
        System.out.println("Ideal state changed for: " + instanceName + "  " + new Date().toString());

        for (IdealState is : idealState) {
            if (StringUtils.equalsIgnoreCase(TCSDriver.getConfig().getClusterConfig().getShardGroupName(),
                    is.getResourceName())) {
                Set<String> partitions = is.getPartitionSet();
                if (partitions != null) {
                    System.out.println("New Parition count: " + partitions.size());
                    TCSDriver.setNumPartitions(partitions.size());
                }
            }
        }
    }
}
