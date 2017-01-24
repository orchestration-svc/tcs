package net.messaging.clusterbox.rabbitmq;

import net.messaging.clusterbox.ClusterBoxConfig;

public class RabbitmqClusterBoxConfig implements ClusterBoxConfig {

    private final RabbitmqBrokerConfig brokerConfig;
    /*
     * Exchange Name
     */
    private final String clusterBoxName;
    /*
     * Exchange Id
     */
    private final String clusterBoxId;
    private boolean autoStart = false;
    private boolean autoDeclare = false;

    public RabbitmqClusterBoxConfig(RabbitmqBrokerConfig brokerConfig, String clusterBoxName, String clusterBoxId) {
        super();
        this.brokerConfig = brokerConfig;
        this.clusterBoxName = clusterBoxName;
        this.clusterBoxId = clusterBoxId;
    }

    @Override
    public RabbitmqBrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    @Override
    public String getClusterBoxName() {
        return clusterBoxName;
    }

    @Override
    public String getClusterBoxId() {
        return clusterBoxId;
    }

    public boolean isAutoStart() {
        return autoStart;
    }

    public void setAutoStart(boolean autoStart) {
        this.autoStart = autoStart;
    }

    public boolean isAutoDeclare() {
        return autoDeclare;
    }

    public void setAutoDeclare(boolean autoDeclare) {
        this.autoDeclare = autoDeclare;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (autoStart ? 1231 : 1237);
        result = prime * result + ((brokerConfig == null) ? 0 : brokerConfig.hashCode());
        result = prime * result + ((clusterBoxId == null) ? 0 : clusterBoxId.hashCode());
        result = prime * result + ((clusterBoxName == null) ? 0 : clusterBoxName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RabbitmqClusterBoxConfig other = (RabbitmqClusterBoxConfig) obj;
        if (autoStart != other.autoStart)
            return false;
        if (brokerConfig == null) {
            if (other.brokerConfig != null)
                return false;
        } else if (!brokerConfig.equals(other.brokerConfig))
            return false;
        if (clusterBoxId == null) {
            if (other.clusterBoxId != null)
                return false;
        } else if (!clusterBoxId.equals(other.clusterBoxId))
            return false;
        if (clusterBoxName == null) {
            if (other.clusterBoxName != null)
                return false;
        } else if (!clusterBoxName.equals(other.clusterBoxName))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "RabbitmqClusterBoxConfig [brokerConfig=" + brokerConfig + ", clusterBoxName=" + clusterBoxName
                + ", clusterBoxId=" + clusterBoxId + ", autoStart=" + autoStart + "]";
    }

}
