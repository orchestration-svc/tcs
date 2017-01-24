package net.tcs.shard;

import java.util.Date;

import org.apache.helix.NotificationContext;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helix Lock implementation, that is acquired/released by a TCS instance. When
 * a TCS instance acquires a lock, it then becomes the shard-owner. So, this
 * also acts as an entry-point for {@link #tcsShardRunner} initialization.
 */
@StateModelInfo(initialState = "OFFLINE", states = { "OFFLINE", "ONLINE" })
public class TCSShardLock extends TransitionHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(TCSShardLock.class);

    private final String shardName;
    private TCSShardRunner tcsShardRunner;

    public TCSShardLock(String lockName) {
        this.shardName = lockName;
    }

    /**
     * Acquire the shard. Perform TCSShardRunner initialization.
     *
     * @param m
     * @param context
     */
    @Transition(from = "OFFLINE", to = "ONLINE")
    public void acquireShard(Message m, NotificationContext context) {
        initializeShard(true);
    }

    public void initializeShard(boolean shardRebalancing) {
        if (TCSShardLockRegistry.getLockRegistry().isRegistered(shardName)) {
            LOGGER.error("acquireShard notification received for a shard that was already owned: {}", shardName);
            return;
        }

        if (shardRebalancing) {
            LOGGER.info("Acquired shard-ownership for shard: {}", shardName);
            System.out.println("Acquired shard-ownership for shard: " + shardName + " Time: " + new Date().toString());
        } else {
            LOGGER.info("Processing shard: {}", shardName);
            System.out.println("Processing shard: " + shardName + " Time: " + new Date().toString());
        }

        TCSShardLockRegistry.getLockRegistry().register(shardName, this);

        tcsShardRunner = new TCSShardRunner(shardName);
        tcsShardRunner.initialize();
    }

    /**
     * Release the shard. Perform TCSShardRunner cleanup.
     *
     * @param m
     * @param context
     */
    @Transition(from = "ONLINE", to = "OFFLINE")
    public void releaseShard(Message m, NotificationContext context) {

        if (!TCSShardLockRegistry.getLockRegistry().isRegistered(shardName)) {
            LOGGER.error("releaseShard notification received for a shard that was never owned: {}", shardName);
            return;
        }

        cleanup();
        waitForCleanup();

        TCSShardLockRegistry.getLockRegistry().unregister(shardName);

        LOGGER.info("Released shard-ownership for shard: {}", shardName);
        System.out.println("Released shard-ownership for shard: " + shardName + " Time: " + new Date().toString());
    }

    @Override
    public void reset() {
    }

    public void cleanup() {
        if (tcsShardRunner != null) {
            tcsShardRunner.close();
        }
    }

    public void waitForCleanup() {
        if (tcsShardRunner != null) {
            tcsShardRunner.waitForClosed();
        }
    }

    @Transition(from = "ONLINE", to = "DROPPED")
    public void handleErrorOnlineToDropped(Message m, NotificationContext context) {
    }

    @Transition(from = "OFFLINE", to = "DROPPED")
    public void handleErrorOfflineToDropped(Message m, NotificationContext context) {
    }

    @Transition(from = "ONLINE", to = "ERROR")
    public void handleErrorOnlineToError(Message m, NotificationContext context) {
    }

    @Transition(from = "OFFLINE", to = "ERROR")
    public void handleErrorOfflineToError(Message m, NotificationContext context) {
    }
}
