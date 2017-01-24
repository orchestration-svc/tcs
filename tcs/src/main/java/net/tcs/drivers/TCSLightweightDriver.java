package net.tcs.drivers;

import net.tcs.db.ActiveJDBCAdapter;
import net.tcs.db.ActiveJDBCEmbeddedDBAdapter;
import net.tcs.shard.TCSShardLockRegistry;

public class TCSLightweightDriver extends TCSDriverBase {

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

    @Override
    protected void initializeDB() {
        final ActiveJDBCAdapter dbAdapter = new ActiveJDBCEmbeddedDBAdapter();

        dbAdapter.initialize(ActiveJDBCEmbeddedDBAdapter.H2_EMBEDDED_DB_URL,
                TCSDriver.getConfig().getDbConfig().getUserName(), TCSDriver.getConfig().getDbConfig().getPassword());

        TCSDriver.setDbAdapter(dbAdapter);

        System.out.println("Connected to Embedded Database: " + ActiveJDBCEmbeddedDBAdapter.H2_EMBEDDED_DB_URL);
    }
}
