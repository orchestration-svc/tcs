package net.tcs.drivers;

import net.tcs.db.ActiveJDBCAdapter;
import net.tcs.messagehandlers.TcsJobExecSubmitListener;
import net.tcs.messagehandlers.TcsJobRegisterListener;
import net.tcs.shard.TCSShardLock;

public abstract class TCSDriverBase {
    private TcsJobRegisterListener listener = null;

    private TcsJobExecSubmitListener submitJobListener = null;

    public abstract void initialize(String instanceName) throws Exception;

    public void cleanup() {
        if (submitJobListener != null) {
            submitJobListener.cleanup();
        }

        if (listener != null) {
            listener.cleanup();
        }

        if (TCSDriver.getDbAdapter() != null) {
            TCSDriver.getDbAdapter().cleanup();
        }
    }

    protected void initializeRMQ() {
        listener = TcsJobRegisterListener.createTCSListenerForRegisterJob();
        listener.initialize();

        submitJobListener = TcsJobExecSubmitListener.createTCSListenerForSubmitJob();
        submitJobListener.initialize();

        System.out
        .println("Connected to RabbitMQ Broker: " + TCSDriver.getConfig().getRabbitConfig().getBrokerAddress());
    }

    protected void initializeDB() {
        final ActiveJDBCAdapter dbAdapter = new ActiveJDBCAdapter();

        dbAdapter.initialize(TCSDriver.getConfig().getDbConfig().getDbConnectString(),
                TCSDriver.getConfig().getDbConfig().getUserName(), TCSDriver.getConfig().getDbConfig().getPassword());
        TCSDriver.setDbAdapter(dbAdapter);

        System.out.println("Connected to Database: " + TCSDriver.getConfig().getDbConfig().getDbConnectString());
    }

    protected void initializePartitions() {
        for (int i = 0; i < TCSDriver.getConfig().getClusterConfig().getNumPartitions(); i++) {
            final String shardName = String.format("%s_%d",
                    TCSDriver.getConfig().getClusterConfig().getShardGroupName(), i);
            final TCSShardLock shardLock = new TCSShardLock(shardName);
            shardLock.initializeShard(false);
        }
    }
}
