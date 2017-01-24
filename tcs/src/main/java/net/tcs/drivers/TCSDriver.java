package net.tcs.drivers;

import java.io.IOException;

import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import net.tcs.db.ActiveJDBCAdapter;
import net.tcs.utils.TCSConfig;
import net.tcs.utils.TCSConfigReader;

/**
 * Entry-point for Task Coordination Service (TCS)
 */
public class TCSDriver {
    private static volatile TCSConfig config;

    private static ActiveJDBCAdapter dbAdapter;

    private static ConfigurableApplicationContext context;

    public static ActiveJDBCAdapter getDbAdapter() {
        return dbAdapter;
    }

    /*
     * For TestNG tests only
     */
    public static void setDbAdapter(ActiveJDBCAdapter dbAdapter) {
        TCSDriver.dbAdapter = dbAdapter;
    }

    public static ConfigurableApplicationContext getContext() {
        return context;
    }

    public static TCSConfig getConfig() {
        return config;
    }

    private static TCSDriverBase tcsDriver;

    public static void main(String[] args) {

        if (args.length < 1) {
            System.out.println("Required parameters: <InstanceID>");
            return;
        }

        final String configFile = System.getProperty("tcs.config", "conf/config.json");
        System.out.println("Config file name: " + configFile);
        try {
            config = TCSConfigReader.readConfig(configFile);
        } catch (final IOException e) {
            System.err.println("Error parsing Config file: " + configFile);
            e.printStackTrace();
            return;
        }

        final String instanceName = String.format("TCS_%s", args[0]);
        System.out.println("TCS Service instance name: " + instanceName);
        System.out.println("TCS Deployment mode: " + config.getTcsDeploymentMode());

        context = new AnnotationConfigApplicationContext(TCSRmqConfiguration.class);

        switch (config.getTcsDeploymentMode()) {
        case MULTI_INSTANCE:
            tcsDriver = new TCSMultiInstanceDriver();
            break;

        case ACTIVE_STANDBY:
            tcsDriver = new TCSActiveStandbyDriver();
            break;

        case STANDALONE:
            tcsDriver = new TCSStandaloneDriver();
            break;

        case LIGHTWEIGHT:
            tcsDriver = new TCSLightweightDriver();
            break;

        default:
            System.err.println("Unsupported deployment type: " + config.getTcsDeploymentMode());
            return;
        }

        try {
            tcsDriver.initialize(instanceName);
        } catch (final Exception ex) {
            System.err.println("Error while initializing TCS");
            ex.printStackTrace();
            cleanup();
            return;
        }

        System.out.println("Waiting to process tasks");
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                cleanup();
            }
        });
    }

    static void cleanup() {
        System.out.println("TCS Shutting down");

        if (tcsDriver != null) {
            tcsDriver.cleanup();
        }

        if (context != null) {
            context.close();
        }
        System.out.println("TCS Shutdown complete");
    }
}
