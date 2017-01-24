package net.tcs.shard;

import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;

import net.tcs.drivers.TCSDriver;
import net.tcs.utils.TCSConfig;

public class TCSShardAssignmentController implements LiveInstanceChangeListener {
    private HelixManager manager;
    private HelixManager controllerManager;
    private final String instanceName;

    private final Set<String> activeInstances = new HashSet<>();

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
        manager.addLiveInstanceChangeListener(this);
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
    public void onLiveInstanceChange(List<LiveInstance> liveInstances, NotificationContext changeContext) {
        final Set<String> instances = new HashSet<>();

        for (final LiveInstance instance : liveInstances) {
            instances.add(instance.getInstanceName());
        }

        synchronized (activeInstances) {
            activeInstances.clear();
            activeInstances.addAll(instances);
        }
    }

    public Collection<String> getActiveInstances() {
        final Set<String> instances = new HashSet<>();
        synchronized (activeInstances) {
            instances.addAll(activeInstances);
        }
        return instances;
    }
}

