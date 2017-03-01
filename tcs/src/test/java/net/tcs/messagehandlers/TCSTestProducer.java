package net.tcs.messagehandlers;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.task.coordinator.producer.TcsProducer;

public class TCSTestProducer implements TcsProducer {

    private final BlockingQueue<Object> queue = new LinkedBlockingQueue<>();

    @Override
    public void sendMessage(String exchange, String routingKey, Object message) {
        queue.offer(message);
    }

    public Object getMessage() {
        try {
            return queue.poll(500, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }
}
