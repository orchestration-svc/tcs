package net.tcs.drivers;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;

import net.tcs.shard.TCSBarrierController;
import net.tcs.shard.TCSShardAssignmentController;
import net.tcs.shard.TCSShardLockRegistry;

public class TCSMultiInstanceDriver extends TCSDriverBase {

    private CuratorFramework zkClient = null;

    private TCSShardAssignmentController shardController = null;

    @Override
    public void initialize(String instanceName) throws Exception {
        initializeZK();
        waitForBarrier();

        initializeDB();

        initializeRMQ();

        initializeShardController(instanceName);
    }

    @Override
    public void cleanup() {
        TCSShardLockRegistry.getLockRegistry().cleanup();

        if (zkClient != null) {
            zkClient.close();
        }

        if (shardController != null) {
            shardController.stop();
        }

        super.cleanup();
    }

    private void initializeZK() {
        zkClient = CuratorFrameworkFactory.newClient(TCSDriver.getConfig().getZookeeperConnectString(),
                new ExponentialBackoffRetry(1000, 3));
        zkClient.start();
        System.out.println("Connected to ZooKeeper: " + TCSDriver.getConfig().getZookeeperConnectString());
    }

    private void waitForBarrier() throws Exception {
        final DistributedBarrier controlBarrier = new DistributedBarrier(zkClient, TCSBarrierController.BARRIER_PATH);
        System.out.println("Waiting for Barrier");

        if (controlBarrier.waitOnBarrier(30, TimeUnit.MINUTES)) {
            System.out.println("Barrier open/unset");
        } else {
            System.err.println("Timedout waiting for Barrier");
        }
    }

    private void initializeShardController(final String instanceName) throws Exception {
        shardController = new TCSShardAssignmentController(instanceName);
        try {
            shardController.start();
        } catch (final Exception e) {
            System.err.println("Error while initializing ShardControlller.");
            throw e;
        }
    }
}
