package net.tcs.shard;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tcs.core.TCSCommandType;
import net.tcs.core.TCSDispatcher;
import net.tcs.core.TaskBoard;
import net.tcs.db.JobInstanceDAO;
import net.tcs.db.adapter.JobInstanceDBAdapter;

/**
 * This class performs the recovery of a shard, if needed. It is run only once
 * (per-shard), when a TCS instance assumes ownership of a shard.
 */
public class TCSShardRecoveryManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(TCSShardRecoveryManager.class);

    public static final class JobRecoverContext {
        private final CountDownLatch latch;
        private final JobInstanceDAO jobDAO;

        public JobInstanceDAO getJobDAO() {
            return jobDAO;
        }

        public JobRecoverContext(final CountDownLatch latch, final JobInstanceDAO jobDAO) {
            this.latch = latch;
            this.jobDAO = jobDAO;
        }

        public void notifyJobRecovered() {
            latch.countDown();
        }
    }

    private final String shardId;
    private final JobInstanceDBAdapter jobDBAdapter = new JobInstanceDBAdapter();
    private final TaskBoard taskBoard;
    private CountDownLatch latch;

    public TCSShardRecoveryManager(String shardId, TaskBoard taskBoard) {
        this.shardId = shardId;
        this.taskBoard = taskBoard;
    }

    /**
     * Gets all the in-progress JobInstances for a Shard, from DB. Sends the
     * JobInstances to TaskBoard, and wait until all the Jobs have been picked
     * up by TCSDispathcers.
     */
    public void recoverShard() {
        LOGGER.info("TCSShardRecoveryManager.recoverShard() for shardId: {}", shardId);
        final List<JobInstanceDAO> inprogressJobs = jobDBAdapter.getAllInprogressJobsForShard(shardId);

        if (inprogressJobs == null || inprogressJobs.isEmpty()) {
            LOGGER.info("There are no InProgress jobs to recover, for shardId: {}", shardId);
            return;
        }

        LOGGER.info("For shardId: {}, InProgress Jobs to recover: {}", shardId, inprogressJobs.size());
        latch = new CountDownLatch(inprogressJobs.size());

        for (final JobInstanceDAO jobDAO : inprogressJobs) {
            final TCSDispatcher taskDispatcher = taskBoard
                    .registerJobForExecution(jobDAO.getInstanceId());
            taskDispatcher.enqueueCommand(TCSCommandType.COMMAND_RECOVER_JOB,
                    new JobRecoverContext(latch, jobDAO));
        }

        try {
            latch.await(300, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            LOGGER.error("Timed out waiting for InProgress Jobs to recover for shardId: {}", shardId);
            Thread.currentThread().interrupt();
        }

        LOGGER.info("Completed recovery of InProgress Job for shardId: {}", shardId);
    }
}
