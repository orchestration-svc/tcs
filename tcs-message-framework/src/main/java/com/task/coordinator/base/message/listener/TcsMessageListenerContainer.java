package com.task.coordinator.base.message.listener;

public interface TcsMessageListenerContainer {

    /*
     * Stars a message listener container will provider concurrent consumers count
     * */
    public void start(int concurrentConsumers);

    public void stop();

    public void destroy();

    public TcsMessageListener getMessageListener();

}
