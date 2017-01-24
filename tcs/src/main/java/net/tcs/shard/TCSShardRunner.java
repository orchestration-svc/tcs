package net.tcs.shard;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.ConfigurableApplicationContext;

import com.task.coordinator.amqp.framework.TcsListenerContainerFactory;
import com.task.coordinator.producer.TcsProducer;

import net.tcs.core.TCSDispatcher;
import net.tcs.core.TCSRollbackDispatcher;
import net.tcs.core.TaskBoard;
import net.tcs.core.TaskRetryDriver;
import net.tcs.drivers.TCSDriver;
import net.tcs.messagehandlers.TcsJobExecSubmitListener;
import net.tcs.messagehandlers.TcsTaskExecEventListener;

/**
 * Entry-point driver for a Shard.
 *
 */
public class TCSShardRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(TCSShardRunner.class);

    private static TcsProducer producer = null;
    private final ConfigurableApplicationContext context;

    public static final class StopNotifier {
        private final CountDownLatch latch;

        public StopNotifier(int notifierCount) {
            latch = new CountDownLatch(notifierCount);
        }

        public void waitUntilAllDispatchersStopped() {
            try {
                latch.await(30, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        public void notifyStopped() {
            latch.countDown();
        }
    }

    private static final int TASK_DISPATCHER_THREAD_COUNT = 4;

    private final AtomicBoolean closing = new AtomicBoolean(false);

    private StopNotifier stopNotifier;

    private final String shardId;

    private final TaskBoard taskBoard = new TaskBoard();

    private ExecutorService executor;

    private final List<TCSDispatcher> dispatchers = new ArrayList<>();

    private TCSRollbackDispatcher rollbackDispatcher;

    private TcsJobExecSubmitListener tcsJobExecSubmitListener;

    private TcsTaskExecEventListener tcsTaskExecEventListener;

    private TaskRetryDriver taskRetryDriver;

    private ScheduledExecutorService scheduledExecutor;

    public TCSShardRunner(String shardId) {
        this.shardId = shardId;
        this.context = TCSDriver.getContext();
    }

    /**
     * Perform Shard initialization.
     */
    public void initialize() {

        /*
         * Get the producer required to produce message from context
         */
        producer = context.getBean(TcsProducer.class);
        /*
         * Create TCSDispatchers
         */
        executor = Executors.newCachedThreadPool(new ThreadFactory() {
            private final AtomicInteger index = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                final String threadName = String.format("TCSDispatcher_%s_%d", shardId, index.getAndIncrement());
                return new Thread(r, threadName);
            }
        });

        for (int i = 0; i < TASK_DISPATCHER_THREAD_COUNT; i++) {
            final TCSDispatcher dispatcher = new TCSDispatcher(taskBoard, producer);
            dispatchers.add(dispatcher);
            executor.submit(dispatcher);
        }

        rollbackDispatcher = new TCSRollbackDispatcher(producer);
        executor.submit(rollbackDispatcher);

        /*
         * Recover the shard and build the TaskBoard with InProgress Job
         * instances.
         */
        final TCSShardRecoveryManager shardRecoveryManager = new TCSShardRecoveryManager(shardId, taskBoard);
        shardRecoveryManager.recoverShard();

        /*
         * Process the In-Progress tasks once, before starting the RMQ
         * listeners.
         */
        taskRetryDriver = new TaskRetryDriver(shardId, taskBoard);
        taskRetryDriver.run();

        /*
         * Schedule RetryDriver for periodic execution.
         */
        scheduledExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "TCSRetryThread_" + shardId);
            }
        });

        final int retryInterval = taskRetryDriver.getRetryInterval();
        scheduledExecutor.scheduleWithFixedDelay(taskRetryDriver, retryInterval, retryInterval, TimeUnit.SECONDS);

        /*
         * Now, open up RMQ listeners for processing incoming TCS events.
         */
        startProcessing();
    }

    private void startProcessing() {
        final MessageConverter messageConverter = (MessageConverter) context.getBean("defaultMessageConverter");
        final TcsListenerContainerFactory factory = context.getBean(TcsListenerContainerFactory.class);

        tcsJobExecSubmitListener = new TcsJobExecSubmitListener(shardId, messageConverter, producer, taskBoard,
                rollbackDispatcher);
        tcsJobExecSubmitListener.initialize(factory);

        tcsTaskExecEventListener = new TcsTaskExecEventListener(shardId, messageConverter, producer, taskBoard,
                rollbackDispatcher);
        tcsTaskExecEventListener.initialize(factory);
    }

    public void close() {

        try {
            if (tcsJobExecSubmitListener != null) {
                tcsJobExecSubmitListener.cleanup();
            }

            if (tcsTaskExecEventListener != null) {
                tcsTaskExecEventListener.cleanup();
            }

            if (scheduledExecutor != null) {
                scheduledExecutor.shutdown();
            }

            if (executor != null) {
                stopNotifier = new StopNotifier(dispatchers.size() + 1);
                rollbackDispatcher.stop(stopNotifier);
                for (final TCSDispatcher dispatcher : dispatchers) {
                    dispatcher.stop(stopNotifier);
                }
            }
        } finally {
            closing.set(true);
        }
    }

    public void waitForClosed() {
        if (!closing.get()) {
            LOGGER.error("close() must be called before calling wairtForClosed(), for shard: {}", shardId);
            return;
        }

        if (scheduledExecutor != null) {
            try {
                scheduledExecutor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        if (executor != null) {
            if (stopNotifier != null) {
                stopNotifier.waitUntilAllDispatchersStopped();
            }

            executor.shutdown();
            try {
                executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public TaskBoard getTaskBoard() {
        return taskBoard;
    }

    public TCSRollbackDispatcher getRollbackDispatcher() {
        return rollbackDispatcher;
    }
}
