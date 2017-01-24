package net.tcs.drivers;

import java.io.IOException;

import net.tcs.api.TCSClient;
import net.tcs.api.TCSClientFactory;

public class TCSClientDriver {

    private static TCSClient tcsClientRuntime;

    public static TCSClient getTcsClientRuntime() {
        return tcsClientRuntime;
    }

    public static void main(String[] args) throws IOException {
        if (args == null || args.length != 2) {
            System.err.println("Please specify the number of partitions, and RMQBroker address");
            return;
        }

        final int numPartitions = Integer.valueOf(args[0]);
        final String rmqBroker = args[1];
        tcsClientRuntime = TCSClientFactory.createTCSClient(numPartitions, rmqBroker);

        tcsClientRuntime.initialize();

        CLIProcessor.processConsoleInput();
        tcsClientRuntime.cleanup();
    }
}
