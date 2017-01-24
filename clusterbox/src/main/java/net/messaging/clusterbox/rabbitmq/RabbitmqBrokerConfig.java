package net.messaging.clusterbox.rabbitmq;

import net.messaging.clusterbox.litemq.broker.BrokerConfig;

public class RabbitmqBrokerConfig implements BrokerConfig {

    private final String ipAddress;
    private final String frontendPort;
    private final String username;
    private final String password;
    private boolean autoStart;

    public RabbitmqBrokerConfig(String ipAddress, String frontendPort, String username, String password,
            boolean autoStart) {
        super();
        this.ipAddress = ipAddress;
        this.frontendPort = frontendPort;
        this.username = username;
        this.password = password;
        this.autoStart = autoStart;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public String getFrontendPort() {
        return frontendPort;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public boolean isAutoStart() {
        return autoStart;
    }

    // TODO : Clean it up
    public String getFrontendProtocol() {
        return "tcp";
    }

}
