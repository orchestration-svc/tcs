package net.tcs.shard;

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tcs.drivers.TCSActiveStandbyDriver;

public class TCSActiveInstanceController extends LeaderSelectorListenerAdapter {
    private static final Logger LOGGER = LoggerFactory.getLogger(TCSActiveInstanceController.class);

    private final String leaderPath;

    private final AtomicBoolean stop = new AtomicBoolean(false);

    private final CountDownLatch latch = new CountDownLatch(1);

    private final TCSActiveStandbyDriver driver;

    private LeaderSelector leaderSelector = null;

    private CuratorFramework zkClient = null;

    public TCSActiveInstanceController(String clusterName, CuratorFramework client, TCSActiveStandbyDriver driver) {
        this.leaderPath = String.format("/tcs/%s/leader", clusterName);
        this.zkClient = client;
        this.driver = driver;
    }

    public void start() {
        leaderSelector = new LeaderSelector(zkClient, leaderPath, this);
        leaderSelector.autoRequeue();
        leaderSelector.start();
        System.out.println("Initialized TCSActiveInstanceController: " + leaderPath);
        LOGGER.info("Initialized TCSActiveInstanceController: {}", leaderPath);
    }

    public void close() {
        stop.set(true);
        try {
            latch.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        leaderSelector.close();
    }

    @Override
    public void takeLeadership(CuratorFramework client) {
        System.out.println("TCSActiveInstanceController became leader: " + new Date().toString());
        LOGGER.info("TCSActiveInstanceController {} became leader" + leaderPath);

        driver.becomeLeader();

        while (!stop.get()) {
            try {
                Thread.sleep(5000);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("TCSActiveInstanceController Leader interrupted: " + new Date().toString());
                LOGGER.info("TCSActiveInstanceController Leader interrupted");
            }
        }
        latch.countDown();
    }
}
