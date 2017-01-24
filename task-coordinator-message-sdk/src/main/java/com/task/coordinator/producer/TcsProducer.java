package com.task.coordinator.producer;

import com.task.coordinator.base.message.TcsCtrlMessage;

public interface TcsProducer {
    <R extends TcsCtrlMessage > void sendMessage(String exchange, String routingKey, R message);
}
