package net.tcs.shard;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A registry to keep-track of shards owned by the TCS instance.
 */
public class TCSShardLockRegistry {
    private static final TCSShardLockRegistry lockRegistry = new TCSShardLockRegistry();

    private final ConcurrentMap<String, TCSShardLock> shards = new ConcurrentHashMap<>();

    public static TCSShardLockRegistry getLockRegistry() {
        return lockRegistry;
    }

    public void register(String shardId, TCSShardLock shardLock) {
        shards.put(shardId, shardLock);
    }

    public void unregister(String shardId) {
        shards.remove(shardId);
    }

    public boolean isRegistered(String shardId) {
        return shards.containsKey(shardId);
    }

    public List<String> list() {
        final List<String> locks = new ArrayList<>();
        locks.addAll(shards.keySet());
        return locks;
    }

    public void cleanup() {
        final List<TCSShardLock> owningShards = new ArrayList<>();
        owningShards.addAll(shards.values());

        for (final TCSShardLock owningShard : owningShards) {
            owningShard.cleanup();
        }

        for (final TCSShardLock owningShard : owningShards) {
            owningShard.waitForCleanup();
        }
    }
}
