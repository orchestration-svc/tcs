package com.task.coordinator.endpoint;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TcsTaskExecutionEndpoint extends TcsEndPoint {

    private final String brokerAddress;

    @JsonCreator
    public TcsTaskExecutionEndpoint (@JsonProperty("brokerAddress") String brokerAddress, @JsonProperty("exchangeName") String exchangeName, @JsonProperty("routingKey") String routingKey) {
        super( exchangeName,  routingKey);
        this.brokerAddress = brokerAddress;
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }

    @JsonIgnore
    public String toEndpointURI() {
        return String.format("rmq://%s/%s/%s", brokerAddress, getExchangeName(), getRoutingKey());
    }
}
