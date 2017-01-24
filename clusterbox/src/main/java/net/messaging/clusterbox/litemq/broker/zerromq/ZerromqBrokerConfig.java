package net.messaging.clusterbox.litemq.broker.zerromq;

import net.messaging.clusterbox.litemq.broker.BrokerConfig;

public class ZerromqBrokerConfig implements BrokerConfig {

    private final String ipAddress;
    private final String frontendPort;
    private final String backendPort;
    private final String frontendProtocol;
    private final String backendProtocol;
    private boolean autoStart;
    public static final Long HEART_BEAT = 30000L; // Unit ms;
    public static final Integer LIVELINESS_COUNT = 3;

    public ZerromqBrokerConfig(String ipAddress, String frontendPort, String backendPort, String frontendProtocol,
            String backendProtocol) {
        super();
        this.ipAddress = ipAddress;
        this.frontendPort = frontendPort;
        this.backendPort = backendPort;
        this.frontendProtocol = frontendProtocol;
        this.backendProtocol = backendProtocol;
    }

    @Override
    public String getIpAddress() {
        return ipAddress;
    }

    @Override
    public String getFrontendPort() {
        return frontendPort;
    }

    public String getBackendPort() {
        return backendPort;
    }

    @Override
    public String getFrontendProtocol() {
        return frontendProtocol;
    }

    public String getBackendProtocol() {
        return backendProtocol;
    }

    public boolean isAutoStart() {
        return autoStart;
    }

    public void setAutoStart(boolean autoStart) {
        this.autoStart = autoStart;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((backendPort == null) ? 0 : backendPort.hashCode());
        result = prime * result + ((backendProtocol == null) ? 0 : backendProtocol.hashCode());
        result = prime * result + ((frontendPort == null) ? 0 : frontendPort.hashCode());
        result = prime * result + ((frontendProtocol == null) ? 0 : frontendProtocol.hashCode());
        result = prime * result + ((ipAddress == null) ? 0 : ipAddress.hashCode());
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
        ZerromqBrokerConfig other = (ZerromqBrokerConfig) obj;
        if (backendPort == null) {
            if (other.backendPort != null)
                return false;
        } else if (!backendPort.equals(other.backendPort))
            return false;
        if (backendProtocol == null) {
            if (other.backendProtocol != null)
                return false;
        } else if (!backendProtocol.equals(other.backendProtocol))
            return false;
        if (frontendPort == null) {
            if (other.frontendPort != null)
                return false;
        } else if (!frontendPort.equals(other.frontendPort))
            return false;
        if (frontendProtocol == null) {
            if (other.frontendProtocol != null)
                return false;
        } else if (!frontendProtocol.equals(other.frontendProtocol))
            return false;
        if (ipAddress == null) {
            if (other.ipAddress != null)
                return false;
        } else if (!ipAddress.equals(other.ipAddress))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "ZerromqBrokerConfig [ipAddress=" + ipAddress + ", frontendPort=" + frontendPort + ", backendPort="
                + backendPort + ", frontendProtocol=" + frontendProtocol + ", backendProtocol=" + backendProtocol + "]";
    }

}
