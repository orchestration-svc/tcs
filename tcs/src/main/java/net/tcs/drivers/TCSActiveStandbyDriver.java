package net.tcs.drivers;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import net.tcs.shard.TCSActiveInstanceController;
import net.tcs.shard.TCSShardLockRegistry;

public class TCSActiveStandbyDriver extends TCSDriverBase {

    private CuratorFramework zkClient = null;
    private TCSActiveInstanceController controller;

    @Override
    public void initialize(String instanceName) {
        initializeZK();
        initializeActiveInstanceController();

        System.out.println("Waiting to process tasks");

        /*
         * REVISIT TODO Investigate why the TCSDriver Shutdown Hook does not
         * work with TCSActiveStandbyDriver
         */
        while (true) {
            try {
                Thread.sleep(300000);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void cleanup() {
        TCSShardLockRegistry.getLockRegistry().cleanup();

        if (controller != null) {
            controller.close();
        }

        if (zkClient != null) {
            zkClient.close();
        }

        super.cleanup();
    }

    private void initializeZK() {
        zkClient = CuratorFrameworkFactory.newClient(TCSDriver.getConfig().getZookeeperConnectString(),
                new ExponentialBackoffRetry(1000, 3));
        zkClient.start();
        System.out.println("Connected to ZooKeeper: " + TCSDriver.getConfig().getZookeeperConnectString());
    }

    public void becomeLeader() {
        initializeDB();

        initializeRMQ();

        initializePartitions();
    }

    private void initializeActiveInstanceController() {
        final String clusterName = TCSDriver.getConfig().getClusterConfig().getClusterName();
        controller = new TCSActiveInstanceController(clusterName, zkClient, this);
        controller.start();
    }
}

