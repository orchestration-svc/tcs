package net.tcs.utils;

import java.util.Arrays;

import net.tcs.drivers.TCSDeploymentMode;

public class TCSConfig {
    public static class ClusterConfig {
        @Override
        public String toString() {
            return "ClusterConfig [clusterName=" + clusterName + ", shardGroupName=" + shardGroupName
                    + ", numPartitions=" + numPartitions + "]";
        }

        public String getClusterName() {
            return clusterName;
        }

        public void setClusterName(String clusterName) {
            this.clusterName = clusterName;
        }

        public String getShardGroupName() {
            return shardGroupName;
        }

        public void setShardGroupName(String shardGroupName) {
            this.shardGroupName = shardGroupName;
        }

        public int getNumPartitions() {
            return numPartitions;
        }

        public void setNumPartitions(int numPartitions) {
            this.numPartitions = numPartitions;
        }

        private String clusterName;
        private String shardGroupName;
        private int numPartitions;
    }

    public static class RMQConfig {

        @Override
        public String toString() {
            return "RMQConfig [brokerAddress=" + brokerAddress + "]";
        }

        public RMQConfig() {
        }

        public String getBrokerAddress() {
            return brokerAddress;
        }

        public void setBrokerAddress(String brokerAddress) {
            this.brokerAddress = brokerAddress;
        }

        private String brokerAddress;
    }

    public static class DBConfig {
        public String getDbConnectString() {
            return dbConnectString;
        }

        public void setDbConnectString(String dbConnectString) {
            this.dbConnectString = dbConnectString;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        @Override
        public String toString() {
            return "DBConfig [dbConnectString=" + dbConnectString + ", userName=" + userName + ", password="
                    + password + "]";
        }

        private String dbConnectString;
        private String userName;
        private String password;
    }

    public TCSConfig() {
    }

    public String getZookeeperConnectString() {
        return zookeeperConnectString;
    }

    public void setZookeeperConnectString(String zookeeperConnectString) {
        this.zookeeperConnectString = zookeeperConnectString;
    }

    public ClusterConfig getClusterConfig() {
        return clusterConfig;
    }

    public void setClusterConfig(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
    }

    public RMQConfig getRabbitConfig() {
        return rabbitConfig;
    }

    public void setRabbitConfig(RMQConfig rabbitConfig) {
        this.rabbitConfig = rabbitConfig;
    }

    public DBConfig getDbConfig() {
        return dbConfig;
    }

    public void setDbConfig(DBConfig dbConfig) {
        this.dbConfig = dbConfig;
    }

    public TCSDeploymentMode getTcsDeploymentMode() {
        return tcsDeploymentMode;
    }

    public void setTcsDeploymentMode(String tcsMode) {
        try {
            this.tcsDeploymentMode = TCSDeploymentMode.valueOf(TCSDeploymentMode.class, tcsMode);
        } catch (final IllegalArgumentException ex) {
            System.out.println(
                    "Permissible Values for TCS DeploymentMode are: " + Arrays.asList(TCSDeploymentMode.values()));
            throw ex;
        }
    }

    private TCSDeploymentMode tcsDeploymentMode = TCSDeploymentMode.MULTI_INSTANCE;
    private String zookeeperConnectString;
    private ClusterConfig clusterConfig;
    private RMQConfig rabbitConfig;
    private DBConfig dbConfig;
}

