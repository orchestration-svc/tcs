package net.tcs.utils;

import java.io.IOException;

import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.StateModelConfigGenerator;

/**
 * Performs TCS cluster configuration in ZooKeeper
 */
public class TCSClusterConfigDriver {
    public static final String SHARD_TYPE = "OnlineOffline";

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.out.println("Required parameters: <config file name>");
            return;
        }
        System.out.println("Config file name: " + args[0]);
        final TCSConfig config = TCSConfigReader.readConfig(args[0]);
        configureCluster(config);
    }

    private static void configureCluster(TCSConfig config) {
        final String clusterName = config.getClusterConfig().getClusterName();
        final String shardGroupName = config.getClusterConfig().getShardGroupName();
        final int numPartitions = config.getClusterConfig().getNumPartitions();

        final HelixAdmin admin = new ZKHelixAdmin(config.getZookeeperConnectString());
        admin.addCluster(config.getClusterConfig().getClusterName(), true);
        admin.addStateModelDef(clusterName, SHARD_TYPE,
                new StateModelDefinition(StateModelConfigGenerator.generateConfigForOnlineOffline()));
        admin.addResource(clusterName, shardGroupName, numPartitions, SHARD_TYPE,
                RebalanceMode.FULL_AUTO.toString());
        admin.rebalance(clusterName, shardGroupName, 1);

        System.out.println("Helix config Done for TCS cluster. ClusterName: " + clusterName + " ShardGroupName: "
                + shardGroupName + " Number of partitions: " + numPartitions);
    }
}
