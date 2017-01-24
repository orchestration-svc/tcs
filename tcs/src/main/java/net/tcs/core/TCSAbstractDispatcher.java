package net.tcs.core;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.task.coordinator.producer.TcsProducer;

import net.tcs.db.adapter.JobDefintionDBAdapter;
import net.tcs.db.adapter.JobInstanceDBAdapter;
import net.tcs.db.adapter.TaskInstanceDBAdapter;
import net.tcs.shard.TCSShardRunner.StopNotifier;

public abstract class TCSAbstractDispatcher implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TCSAbstractDispatcher.class);

    private static final int COMMAND_QUEUE_POLL_TIME = 2; // seconds

    protected final TcsProducer producer;

    protected final TaskInstanceDBAdapter taskDBAdapter = new TaskInstanceDBAdapter();

    protected final JobInstanceDBAdapter jobInstanceDBAdapter = new JobInstanceDBAdapter();

    protected final JobDefintionDBAdapter jobDefDBAdapter = new JobDefintionDBAdapter();

    /**
     * Used by RMQ Listeners and TCSRetryDriver to enqueue event commands, to be
     * processed by TCSDispatcher.
     */
    protected final BlockingQueue<TCSCommand> commandQueue = new LinkedBlockingQueue<>();

    private final AtomicReference<StopNotifier> stopNotifier = new AtomicReference<>();

    /**
     * Map of in-progress jobs; JobInstanceId => JobStateMachineContextBase
     */
    protected final ConcurrentMap<String, JobStateMachineContextBase> inProgressJobsMap = new ConcurrentHashMap<>();

    public void stop(StopNotifier stopNotifier) {
        this.stopNotifier.set(stopNotifier);
    }

    public TCSAbstractDispatcher(TcsProducer producer) {
        this.producer = producer;
    }

    public void enqueueCommand(TCSCommandType commandName, Object body) {
        commandQueue.add(new TCSCommand(commandName, body));
    }

    public abstract void processCommand(TCSCommand command);

    @Override
    public void run() {
        while (stopNotifier.get() == null) {
            try {
                final TCSCommand command = commandQueue.poll(COMMAND_QUEUE_POLL_TIME, TimeUnit.SECONDS);
                if (command != null) {
                    try {
                        processCommand(command);
                    } catch (final Exception ex) {
                        final String errMsg = String.format("Exception while processing command: {%s}",
                                command.commandType);
                        LOGGER.error(errMsg, ex);
                    }
                }

            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        if (stopNotifier.get() != null) {
            stopNotifier.get().notifyStopped();
        }
    }
}
