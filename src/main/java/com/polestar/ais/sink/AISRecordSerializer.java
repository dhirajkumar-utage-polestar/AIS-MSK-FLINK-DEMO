package com.polestar.ais.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.polestar.ais.model.AISRecord;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * Serializes AISRecord to a Kafka ProducerRecord as JSON.
 *
 * Key:   MMSI as UTF-8 bytes (enables Kafka partition affinity per vessel)
 * Value: JSON-encoded AISRecord (Jackson; nulls omitted via @JsonInclude)
 */
public class AISRecordSerializer
        implements KafkaRecordSerializationSchema<AISRecord> {

    private final String topic;
    private transient ObjectMapper mapper;

    public AISRecordSerializer(String topic) {
        this.topic = topic;
    }

    @Override
    public void open(
            SerializationSchema.InitializationContext context,
            KafkaRecordSerializationSchema.KafkaSinkContext sinkContext) {
        mapper = new ObjectMapper();
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            AISRecord record,
            KafkaRecordSerializationSchema.KafkaSinkContext context,
            Long timestamp) {
        try {
            byte[] key   = record.mmsi != null
                    ? Long.toString(record.mmsi).getBytes(java.nio.charset.StandardCharsets.UTF_8)
                    : null;
            byte[] value = mapper.writeValueAsBytes(record);
            return new ProducerRecord<>(topic, null, timestamp, key, value);
        } catch (Exception e) {
            return null;
        }
    }
}
