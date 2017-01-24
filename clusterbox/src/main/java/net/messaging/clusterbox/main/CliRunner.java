package net.messaging.clusterbox.main;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
@interface CliRunner {
    public String command();
}
