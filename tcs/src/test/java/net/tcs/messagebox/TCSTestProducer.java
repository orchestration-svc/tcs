package net.tcs.messagebox;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.task.coordinator.base.message.TcsCtrlMessage;
import com.task.coordinator.producer.TcsProducer;

public class TCSTestProducer implements TcsProducer {

    private final BlockingQueue<TcsCtrlMessage> queue = new LinkedBlockingQueue<>();

    @Override
    public <R extends TcsCtrlMessage> void sendMessage(String exchange, String routingKey, R message) {
        queue.offer(message);
    }

    public TcsCtrlMessage getMessage() {
        try {
            return queue.poll(500, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }
}
