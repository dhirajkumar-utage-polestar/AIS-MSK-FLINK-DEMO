package com.polestar.ais.config;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Job configuration loaded from environment variables.
 */
public class JobConfig implements Serializable {

    // MSK Source
    public final String mskBrokers;
    public final List<String> sourceTopics;
    public final String consumerGroup;

    // MSK Sink
    public final String ingestTopic;

    // S3 Sink
    public final String s3Bucket;
    public final String s3Prefix;
    public final long targetFileSizeBytes;

    // AWS
    public final String awsRegion;

    // Stateful join TTLs
    public final long type24JoinTtlMs;
    public final long fragmentTtlMs;

    // MSK DLQ
    public final String dlqTopic;

    // Flink
    public final int parallelism;
    public final long checkpointIntervalMs;
    public final long minPauseBetweenCheckpointsMs;
    public final String checkpointDir;

    // Restart strategy
    public final int restartAttempts;
    public final long restartDelayMs;

    // Sink toggles
    public final boolean disableMskSink;

    public JobConfig() {
        this.mskBrokers = env("MSK_BROKERS",
                "b-1.aismskcluster.fvmx2c.c1.kafka.us-east-1.amazonaws.com:9098," +
                "b-2.aismskcluster.fvmx2c.c1.kafka.us-east-1.amazonaws.com:9098," +
                "b-3.aismskcluster.fvmx2c.c1.kafka.us-east-1.amazonaws.com:9098");

        this.sourceTopics = Arrays.asList(
                env("SOURCE_TOPICS", "ais-terrestrial,ais-satellite,ais-roaming").split(","));

        this.consumerGroup  = env("CONSUMER_GROUP",   "ais-flink-parser-job1");
        this.ingestTopic    = env("INGEST_TOPIC",     "ais.ingest");
        this.dlqTopic       = env("DLQ_TOPIC",        "ais.parse.dlq");
        this.s3Bucket       = env("S3_BUCKET",        "ps-dev-data-platform-ingest-1svf35");
        this.s3Prefix       = env("S3_PREFIX",        "ais-bronze-ingest");
        this.awsRegion      = env("AWS_REGION",       "us-east-1");

        long fileMb = Long.parseLong(env("S3_TARGET_FILE_SIZE_MB", "64"));
        this.targetFileSizeBytes = fileMb * 1024 * 1024;

        this.type24JoinTtlMs   = Long.parseLong(env("TYPE24_JOIN_TTL_SECONDS", "300")) * 1000L;
        this.fragmentTtlMs     = Long.parseLong(env("FRAGMENT_TTL_SECONDS",   "30"))  * 1000L;
        this.parallelism       = Integer.parseInt(env("FLINK_PARALLELISM",     "4"));
        this.checkpointIntervalMs = Long.parseLong(env("CHECKPOINT_INTERVAL_MS", "60000"));
        this.minPauseBetweenCheckpointsMs = Long.parseLong(env("MIN_PAUSE_BETWEEN_CHECKPOINTS_MS", "30000"));
        this.checkpointDir     = env("CHECKPOINT_DIR",
                "s3://" + s3Bucket + "/flink-checkpoints/job1");
        this.restartAttempts   = Integer.parseInt(env("RESTART_ATTEMPTS",  "3"));
        this.restartDelayMs    = Long.parseLong(env("RESTART_DELAY_MS",    "10000"));
        this.disableMskSink    = "true".equalsIgnoreCase(env("DISABLE_MSK_SINK", "false"));
    }

    private static String env(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isBlank()) ? val : defaultValue;
    }
}
