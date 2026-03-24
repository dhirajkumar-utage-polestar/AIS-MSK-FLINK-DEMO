package com.polestar.ais.sink;

import com.polestar.ais.config.JobConfig;
import com.polestar.ais.model.AISRecord;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import software.amazon.msk.auth.iam.IAMClientCallbackHandler;
import software.amazon.msk.auth.iam.IAMLoginModule;

import java.util.Properties;

/**
 * Builds a Flink KafkaSink targeting MSK with IAM authentication (SASL_SSL).
 *
 * Uses the AWS MSK IAM auth provider so no credentials are embedded in config.
 * Mirrors the MSK producer config in the Go AIS receiver (kafka_writer.go).
 */
public class MSKSinkBuilder {

    private MSKSinkBuilder() {}

    public static KafkaSink<AISRecord> build(JobConfig config) {
        Properties props = producerProps();
        return KafkaSink.<AISRecord>builder()
                .setBootstrapServers(config.mskBrokers)
                .setKafkaProducerConfig(props)
                .setRecordSerializer(new AISRecordSerializer(config.ingestTopic))
                .build();
    }

    /**
     * Builds a KafkaSink for the parse DLQ topic.
     * Records are JSON strings emitted by NMEAParser on parse failure.
     */
    public static KafkaSink<String> buildDlq(JobConfig config) {
        Properties props = producerProps();
        return KafkaSink.<String>builder()
                .setBootstrapServers(config.mskBrokers)
                .setKafkaProducerConfig(props)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic(config.dlqTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .build();
    }

    private static Properties producerProps() {
        Properties props = new Properties();
        String auth = System.getenv("KAFKA_AUTH");
        if (!"NONE".equalsIgnoreCase(auth)) {
            props.setProperty("security.protocol", "SASL_SSL");
            props.setProperty("sasl.mechanism", "AWS_MSK_IAM");
            props.setProperty("sasl.jaas.config",
                    IAMLoginModule.class.getName() + " required;");
            props.setProperty("sasl.client.callback.handler.class",
                    IAMClientCallbackHandler.class.getName());
        }
        props.setProperty("acks",                    "all");
        props.setProperty("enable.idempotence",      "true");
        props.setProperty("max.in.flight.requests.per.connection", "5");
        props.setProperty("compression.type",        "lz4");
        props.setProperty("linger.ms",               "20");
        props.setProperty("batch.size",              String.valueOf(256 * 1024));
        props.setProperty("buffer.memory",           String.valueOf(64 * 1024 * 1024));
        props.setProperty("retries",                 "3");
        props.setProperty("retry.backoff.ms",        "500");
        return props;
    }
}
