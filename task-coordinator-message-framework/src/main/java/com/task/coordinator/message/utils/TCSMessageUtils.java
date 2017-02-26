package com.task.coordinator.message.utils;

import com.task.coordinator.endpoint.TcsTaskExecutionEndpoint;

public class TCSMessageUtils {
    public static TcsTaskExecutionEndpoint getEndpointAddressForProcessingJobsOnShard(String brokerAddress,
            String shardId) {

        final String jobRoutingKey = String.format("%s.%s", TCSConstants.TCS_JOB_EXEC_RKEY, shardId);
        return new TcsTaskExecutionEndpoint(brokerAddress, TCSConstants.TCS_JOB_EXEC_EXCHANGE, jobRoutingKey);
    }

    public static TcsTaskExecutionEndpoint getEndpointAddressForSubmitJob() {

        final String jobRoutingKey = TCSConstants.TCS_SUBMIT_JOB_ROUTE;
        return new TcsTaskExecutionEndpoint(null, TCSConstants.TCS_JOB_EXEC_EXCHANGE, jobRoutingKey);
    }

    public static TcsTaskExecutionEndpoint getEndpointAddressForPublishingJobNotificationsOnShard(String shardId) {

        final String jobRoutingKey = String.format("%s.%s", TCSConstants.TCS_JOB_EXEC_RKEY, shardId);
        return new TcsTaskExecutionEndpoint(null, TCSConstants.TCS_JOB_EXEC_EXCHANGE, jobRoutingKey);
    }

    public static String getQueueNameForProcessingJobsOnShard(String shardId) {
        return String.format("%s.%s", TCSConstants.TCS_JOB_EXEC_QUEUE, shardId);
    }

    public static TcsTaskExecutionEndpoint getEndpointAddressForProcessingTasksOnShard(String brokerAddress,
            String shardId) {

        final String taskRoutingKey = String.format("%s.%s", TCSConstants.TCS_TASK_NOTIF_RKEY, shardId);
        return new TcsTaskExecutionEndpoint(brokerAddress, TCSConstants.TCS_JOB_EXEC_EXCHANGE, taskRoutingKey);
    }

    public static TcsTaskExecutionEndpoint getEndpointAddressForPublishingTaskNotificationsOnShard(String shardId) {

        final String taskRoutingKey = String.format("%s.%s", TCSConstants.TCS_TASK_NOTIF_RKEY, shardId);
        return new TcsTaskExecutionEndpoint(null, TCSConstants.TCS_JOB_EXEC_EXCHANGE, taskRoutingKey);
    }

    public static String getQueueNameForProcessingTasksOnShard(String shardId) {
        return String.format("%s.%s", TCSConstants.TCS_TASK_NOTIF_QUEUE, shardId);
    }
}
