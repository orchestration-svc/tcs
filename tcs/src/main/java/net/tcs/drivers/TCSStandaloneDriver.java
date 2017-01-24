package net.tcs.drivers;

import net.tcs.shard.TCSShardLockRegistry;

public class TCSStandaloneDriver extends TCSDriverBase {

    @Override
    public void initialize(final String instanceName) throws Exception {
        initializeDB();

        initializeRMQ();

        initializePartitions();
    }

    @Override
    public void cleanup() {
        TCSShardLockRegistry.getLockRegistry().cleanup();

        super.cleanup();
    }
}
