package com.task.coordinator.controller;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.spectator.RoutingTableProvider;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
public class TCSClusterObserver {

    private static final String TCS_CLUSTER_NAME = "APIC-TCS";

    private static final String TCS_RESOURCE_NAME = "tcs-shard";

    private HelixAdmin admin;
    private HelixManager manager;
    final RoutingTableProvider routingTableProvider = new RoutingTableProvider();

    private final AtomicBoolean initialized = new AtomicBoolean(false);

    @PostConstruct
    public void init() {
        final String zkIpAddress = System.getProperty("tcs.zookeeperIP");
        if (StringUtils.isEmpty(zkIpAddress)) {
            return;
        }

        try {
            admin = new ZKHelixAdmin(String.format("%s:2181", zkIpAddress));

            manager = HelixManagerFactory.getZKHelixManager(TCS_CLUSTER_NAME, "foobar", InstanceType.SPECTATOR,
                    zkIpAddress);
            manager.connect();

            manager.addExternalViewChangeListener(routingTableProvider);
            initialized.set(true);
        } catch (final Exception ex) {
            ex.printStackTrace();
        }
    }

    public Map<String, List<String>> getShardInfo() {

        if (!initialized.get()) {
            return Collections.emptyMap();
        }

        final IdealState idealState = admin.getResourceIdealState(TCS_CLUSTER_NAME, TCS_RESOURCE_NAME);
        if (idealState == null) {
            return Collections.emptyMap();
        }
        final Set<String> partitionSet = idealState.getPartitionSet();
        if (partitionSet == null) {
            return Collections.emptyMap();
        }

        final Map<String, List<String>> shardMap = new HashMap<>();

        for (final String partition : partitionSet) {
            final List<InstanceConfig> instances = routingTableProvider.getInstances(TCS_RESOURCE_NAME, partition,
                    "ONLINE");

            if (instances != null) {
                for (final InstanceConfig inst : instances) {
                    List<String> partitions = shardMap.get(inst.getInstanceName());
                    if (partitions == null) {
                        partitions = new ArrayList<String>();
                        shardMap.put(inst.getInstanceName(), partitions);
                    }
                    partitions.add(partition);
                }
            }
        }
        return shardMap;
    }

    @PreDestroy
    public void shutdown() {
        if (initialized.get()) {
            if (manager != null) {
                manager.disconnect();
            }

            if (admin != null) {
                admin.close();
            }
        }
    }
}
