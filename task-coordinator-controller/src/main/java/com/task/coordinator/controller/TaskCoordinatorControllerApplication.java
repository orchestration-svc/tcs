package com.task.coordinator.controller;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.util.StringUtils;

@SpringBootApplication
public class TaskCoordinatorControllerApplication {

    public static void main(String[] args) {
        final List<Object> objects = new ArrayList<>();
        objects.add(JobInstanceController.class);
        objects.add(JobDefinitionController.class);
        objects.add(TaskInstanceController.class);
        objects.add(TaskCoordinatorControllerApplication.class);

        final String zkIpAddress = System.getProperty("tcs.zookeeperIP");
        if (!StringUtils.isEmpty(zkIpAddress)) {
            objects.add(TCSClusterInfoController.class);
        }

        SpringApplication.run(objects.toArray(new Object[objects.size()]), args);
    }
}
