package com.task.coordinator.endpoint;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TcsEndPoint {

    public String getExchangeName() {
        return exchangeName;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    /**
     *
     * @param brokerAddress
     * @param exchangeName
     * @param routingKey
     */
    @JsonCreator
    public TcsEndPoint(@JsonProperty("exchangeName")String exchangeName, @JsonProperty("routingKey") String routingKey) {
        super();
        this.exchangeName = exchangeName;
        this.routingKey = routingKey;
    }

    private final String exchangeName;

    private final String routingKey;

}
