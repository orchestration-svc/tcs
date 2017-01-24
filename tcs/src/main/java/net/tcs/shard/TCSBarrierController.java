package net.tcs.shard;

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;

import net.tcs.utils.TCSConfig;
import net.tcs.utils.TCSConfigReader;

/**
 * This Driver is used as a BarrierController to synchronize the beginning of
 * multiple instances of TCS.
 */
public class TCSBarrierController {
    public static final String BARRIER_PATH = "/apic/tcs/barrier";

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.out.println("Required parameters: <config file name> <create/remove>");
            return;
        }

        System.out.println("Config file name: " + args[0]);
        final TCSConfig config = TCSConfigReader.readConfig(args[0]);
        final String command = args[1];

        final CuratorFramework client = CuratorFrameworkFactory.newClient(config.getZookeeperConnectString(),
                new ExponentialBackoffRetry(1000, 3));
        client.start();

        final DistributedBarrier controlBarrier = new DistributedBarrier(client, BARRIER_PATH);
        try {
            if (command.equalsIgnoreCase("create")) {
                controlBarrier.setBarrier();
                System.out.println("Created Barrier");
            } else if (command.equalsIgnoreCase("remove")) {
                controlBarrier.removeBarrier();
                System.out.println("Removed Barrier");
            } else {
                System.out.println("unknown command. Must be either create or remove");
            }
        } catch (final Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            client.close();
        }
    }
}
