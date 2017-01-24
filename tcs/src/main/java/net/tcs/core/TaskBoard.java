package net.tcs.core;

import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class acts as a router for all InProgress Jobs for a Shard.
 *
 * It keeps track of which JobInstance => TCSDispatcher mapping, so that it can
 * route a new Job-specific event (such as JobStarted, TaskComplete, TaskFailed,
 * TaskRetry) to the right TCSDispatcher.
 *
 * With this mechanism, we can ensure that all the events pertaining to a
 * JobInstance are routed to the same TCSDispatcher, and are handled in a serial
 * manner by a TCSDispatcher.
 */
public class TaskBoard {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskBoard.class);

    private static final class TCSDispatcherContext implements Comparable<TCSDispatcherContext> {
        private final AtomicInteger jobCount = new AtomicInteger(0);

        private final TCSDispatcher taskDispatcher;

        public TCSDispatcherContext(TCSDispatcher taskDispatcher) {
            this.taskDispatcher = taskDispatcher;
        }

        public TCSDispatcher getTaskDispatcher() {
            return taskDispatcher;
        }

        @Override
        public int compareTo(TCSDispatcherContext that) {
            if (this.jobCount.get() == that.jobCount.get()) {
                return 0;
            } else if (this.jobCount.get() < that.jobCount.get()) {
                return -1;
            } else {
                return 1;
            }
        }

        public void jobStarted() {
            jobCount.incrementAndGet();
        }

        public void jobDone() {
            jobCount.decrementAndGet();
        }
    }

    /**
     * Map of TaskDispatcherId => TCSDispatcher
     */
    private final ConcurrentMap<String, TCSDispatcherContext> taskDispatchers = new ConcurrentHashMap<>();

    /**
     * Map of TaskDispatcherId => JobInstanceId
     */
    private final ConcurrentMap<String, String> jobOwnerMap = new ConcurrentHashMap<>();

    public void registerTaskDispatcher(String dispatcherName, TCSDispatcher taskDispatcher) {
        taskDispatchers.putIfAbsent(dispatcherName, new TCSDispatcherContext(taskDispatcher));
    }

    public TCSDispatcher registerJobForExecution(String jobInstanceId) {

        /*
         * Pick up the TCSDispatcher that has the fewest number of JobInstances
         * in-progress.
         */
        final PriorityQueue<TCSDispatcherContext> pq = new PriorityQueue<>();
        pq.addAll(taskDispatchers.values());
        final TCSDispatcherContext context = pq.remove();
        LOGGER.trace("TCSDispatcher {} chosen for JobId: {}", context.getTaskDispatcher().getTaskDispatcherId(),
                jobInstanceId);
        context.jobStarted();
        jobOwnerMap.put(jobInstanceId, context.getTaskDispatcher().getTaskDispatcherId());

        return context.getTaskDispatcher();
    }

    public TCSDispatcher getDispatcherForJob(String jobInstanceId) {
        final String taskDispatcherId = jobOwnerMap.get(jobInstanceId);
        if (taskDispatcherId != null) {
            final TCSDispatcherContext taskDispatcherContext = taskDispatchers.get(taskDispatcherId);
            if (taskDispatcherContext != null) {
                return taskDispatcherContext.getTaskDispatcher();
            }
        }
        return null;
    }

    public void jobComplete(String jobInstanceId) {
        final String taskDispatcherId = jobOwnerMap.remove(jobInstanceId);
        if (taskDispatcherId != null) {
            final TCSDispatcherContext taskDispatcherContext = taskDispatchers.get(taskDispatcherId);
            if (taskDispatcherContext != null) {
                taskDispatcherContext.jobDone();
            }
        }
    }
}
