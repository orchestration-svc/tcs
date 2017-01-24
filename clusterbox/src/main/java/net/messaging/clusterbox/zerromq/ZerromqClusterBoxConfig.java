package net.messaging.clusterbox.zerromq;

import net.messaging.clusterbox.ClusterBoxConfig;
import net.messaging.clusterbox.litemq.broker.zerromq.ZerromqBrokerConfig;

public class ZerromqClusterBoxConfig implements ClusterBoxConfig {

    private final ZerromqBrokerConfig brokerConfig;
    private final String clusterBoxName;
    private final String clusterBoxId;
    private final String backendProtocol = "ipc";
    private String backendAddress;
    private boolean autoStart = false;
    private Long pingInterval;

    public ZerromqClusterBoxConfig(ZerromqBrokerConfig brokerConfig, String clusterBoxName, String clusterBoxId) {
        super();
        this.brokerConfig = brokerConfig;
        this.clusterBoxName = clusterBoxName;
        this.clusterBoxId = clusterBoxId;
    }

    @Override
    public ZerromqBrokerConfig getBrokerConfig() {
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

    public String getBackendProtocol() {
        return backendProtocol;
    }

    public String getBackendAddress() {
        return new String(clusterBoxId + ".ipc");
    }

    public boolean isAutoStart() {
        return autoStart;
    }

    public void setAutoStart(boolean autoStart) {
        this.autoStart = autoStart;
    }

    public Long getPingInterval() {
        return pingInterval;
    }

    public void setPingInterval(Long pingInterval) {
        this.pingInterval = pingInterval;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((backendAddress == null) ? 0 : backendAddress.hashCode());
        result = prime * result + ((backendProtocol == null) ? 0 : backendProtocol.hashCode());
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
        ZerromqClusterBoxConfig other = (ZerromqClusterBoxConfig) obj;
        if (backendAddress == null) {
            if (other.backendAddress != null)
                return false;
        } else if (!backendAddress.equals(other.backendAddress))
            return false;
        if (backendProtocol == null) {
            if (other.backendProtocol != null)
                return false;
        } else if (!backendProtocol.equals(other.backendProtocol))
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
        return "ZerromqClusterBoxConfig [brokerConfig=" + brokerConfig + ", clusterBoxName=" + clusterBoxName
                + ", clusterBoxId=" + clusterBoxId + ", backendProtocol=" + backendProtocol + ", backendAddress="
                + backendAddress + "]";
    }

}
