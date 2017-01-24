package com.task.coordinator.message.utils;

public class TCSConstants {

    /*
     * Shard constants
     */
    public static final String TCS_SHARD_GROUP_NAME = "tcs-shard";

    public static final int TASK_TIMEOUT_RETRY_INTERVAL_SECS = 30;

    public static final int TASK_MAX_RETRY_COUNT = 2;

    /*
     * RMQ constants for Job registration/query
     */
    public static final String TCS_EXCHANGE = "tcs.exchange.registerjob";

    public static final String TCS_REGISTER_TASK_QUEUE = "tcs.registerjob";

    public static final String TCS_REGISTER_TASK_RKEY = "tcs.registerjob.route";

    public static final String TCS_QUERY_TASK_QUEUE = "tcs.queryjob";

    public static final String TCS_QUERY_TASK_RKEY = "tcs.queryjob.route";

    /*
     * RMQ constants for Job submission
     */
    public static final String TCS_JOB_EXEC_EXCHANGE = "tcs.exchange.execjob";

    public static final String TCS_JOB_EXEC_QUEUE = "tcs.job.exec";

    public static final String TCS_JOB_EXEC_RKEY = "tcs.job.exec.route";

    /*
     * RMQ constants for Task completion
     */
    public static final String TCS_TASK_NOTIF_QUEUE = "tcs.task.notif";

    public static final String TCS_TASK_NOTIF_RKEY = "tcs.task.notif.route";

    /*
     * RMQ exchange used server to submit task to executor clients
     */
    public static final String TCS_EXECUTOR_EXCHANGE = "tcs.exchange.test";

    /*
     * Example: tcs.step.teststep
     */
    public static final String STEP_PARALLEL_COUNT_KEY_FORMAT = "tcs.step.%s";

}
