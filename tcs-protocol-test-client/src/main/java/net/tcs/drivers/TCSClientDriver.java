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
        if (args == null || args.length != 1) {
            System.err.println("Please specify RMQBroker address");
            return;
        }

        final String rmqBroker = args[0];
        tcsClientRuntime = TCSClientFactory.createTCSClient(rmqBroker);

        tcsClientRuntime.initialize();

        CLIProcessor.processConsoleInput();
        tcsClientRuntime.cleanup();
    }
}
