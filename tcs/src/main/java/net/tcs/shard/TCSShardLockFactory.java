package net.tcs.shard;

import org.apache.helix.api.StateTransitionHandlerFactory;
import org.apache.helix.api.id.PartitionId;

public class TCSShardLockFactory extends StateTransitionHandlerFactory<TCSShardLock> {

    @Override
    public TCSShardLock createStateTransitionHandler(PartitionId lockName) {
        return new TCSShardLock(lockName.toString());
    }
}
