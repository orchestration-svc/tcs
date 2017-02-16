package net.tcs.utils;

import java.io.IOException;

import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.IdealState;

/**
 * Performs TCS cluster configuration in ZooKeeper
 */
public class TCSClusterPartitionGrower {
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Required parameters: <config file name> <desired Partition count>");
            return;
        }

        System.out.println("Config file name: " + args[0]);

        final TCSConfig config = TCSConfigReader.readConfig(args[0]);

        final int desiredPartitionCount = Integer.parseInt(args[1]);
        System.out.println("Desired Partition count: " + desiredPartitionCount);

        growPartitionCount(config, desiredPartitionCount);
    }

    private static void growPartitionCount(TCSConfig config, int desiredPartitionCount) {
        final String clusterName = config.getClusterConfig().getClusterName();
        final String shardGroupName = config.getClusterConfig().getShardGroupName();
        final int numPartitions = config.getClusterConfig().getNumPartitions();

        if (desiredPartitionCount < numPartitions) {
            final String error = String.format(
                    "Partition count can only be increased: current partition count: %d, desired partition count: %d",
                    numPartitions, desiredPartitionCount);
            System.err.println(error);
            throw new RuntimeException(error);
        } else if (desiredPartitionCount == numPartitions) {
            final String error = String.format(
                    "Desired partition count equals to current partitions, nothing needs to be done : current partition count: %d, desired partition count: %d",
                    numPartitions, desiredPartitionCount);
            System.err.println(error);
            return;
        }

        final HelixAdmin admin = new ZKHelixAdmin(config.getZookeeperConnectString());
        final IdealState is = admin.getResourceIdealState(clusterName, shardGroupName);

        is.setNumPartitions(desiredPartitionCount);
        admin.setResourceIdealState(clusterName, shardGroupName, is);
        admin.rebalance(clusterName, shardGroupName, 1);
        System.out.println("Partition grow configuration done");
    }
}
