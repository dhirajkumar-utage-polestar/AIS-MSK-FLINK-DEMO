package com.polestar.ais;

import com.polestar.ais.config.JobConfig;
import com.polestar.ais.model.AISRecord;
import com.polestar.ais.operators.*;
import com.polestar.ais.sink.MSKSinkBuilder;
import com.polestar.ais.sink.S3ParquetSinkBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.util.Collector;
import software.amazon.msk.auth.iam.IAMClientCallbackHandler;
import software.amazon.msk.auth.iam.IAMLoginModule;

import java.util.List;
import java.util.Properties;

/**
 * Flink Job 1 — AIS Parser + Validator (Bronze Layer)
 *
 * Pipeline topology:
 *
 *   [MSK source topics]
 *       |
 *   NMEANormalizer          (MapFunction: Kpler line normalization)
 *       |
 *   NMEAFragmentAssembler   (KeyedProcessFunction: buffer multi-part NMEA, keyed by topic:channel:seqId)
 *       |
 *   NMEAParser              (FlatMapFunction: AisLib decode + tag block extraction)
 *       |
 *   ASMDecoder              (MapFunction: decode Type 6/8 binary ASM payloads)
 *       |
 *   Type24Joiner            (KeyedProcessFunction: join Part A + Part B by MMSI, 5-min TTL)
 *       |
 *   ValidationEngine        (MapFunction: rules.yml-driven field validation)
 *       |
 *   ┌──────────────────┐
 *   │  MSK Sink         │  → ais.ingest topic (JSON per record)
 *   │  S3 Parquet Sink  │  → s3://bucket/ais-bronze-ingest/year=.../month=.../
 *   └──────────────────┘
 *
 * Source topics: ais-terrestrial, ais-satellite, ais-roaming (from JobConfig)
 * Auth: MSK IAM (SASL_SSL port 9098)
 */
public class Job1ParserValidator {

    public static void main(String[] args) throws Exception {

        JobConfig config = new JobConfig();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.parallelism);

        // ── Restart strategy ─────────────────────────────────────────────────
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                config.restartAttempts,
                Time.milliseconds(config.restartDelayMs)));

        // ── Checkpointing ─────────────────────────────────────────────────────
        env.enableCheckpointing(config.checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig cp = env.getCheckpointConfig();
        cp.setCheckpointStorage(config.checkpointDir);
        cp.setMinPauseBetweenCheckpoints(config.minPauseBetweenCheckpointsMs);
        cp.setMaxConcurrentCheckpoints(1);
        cp.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // ── Source ────────────────────────────────────────────────────────────
        Properties consumerProps = mskConsumerProps(config);

        KafkaSource<Tuple2<String, String>> source = KafkaSource
                .<Tuple2<String, String>>builder()
                .setBootstrapServers(config.mskBrokers)
                .setTopics(config.sourceTopics)
                .setGroupId(config.consumerGroup)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(
                        org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST))
                .setDeserializer(new TopicAwareStringDeserializer())
                .setProperties(consumerProps)
                .build();

        DataStream<Tuple2<String, String>> rawLines = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "MSK-AIS-Source");

        // ── NMEANormalizer ────────────────────────────────────────────────────
        DataStream<Tuple2<String, String>> normalized = rawLines
                .filter(t -> t.f1 != null && !t.f1.isBlank())
                .map(t -> Tuple2.of(t.f0, new NMEANormalizer().map(t.f1)))
                .returns(Types.TUPLE(Types.STRING, Types.STRING))
                .filter(t -> t.f1 != null)
                .name("NMEANormalizer");

        // ── NMEAFragmentAssembler ─────────────────────────────────────────────
        // Key: "{sourceTopic}:{channel}:{seqId}" — built inside the assembler
        KeyedStream<Tuple2<String, String>, String> keyedFragments = normalized
                .keyBy(t -> {
                    NMEAFragmentAssembler.FragmentHeader h =
                            NMEAFragmentAssembler.parseHeader(t.f1);
                    if (h == null) return t.f0 + ":single:" + System.nanoTime();
                    return t.f0 + ":" + h.channel + ":" + h.seqId;
                });

        DataStream<Tuple2<String, List<String>>> assembled = keyedFragments
                .process(new NMEAFragmentAssembler(config))
                .name("NMEAFragmentAssembler");

        // ── NMEAParser ────────────────────────────────────────────────────────
        // ProcessFunction gives us a side output for unparseable lines (DLQ).
        SingleOutputStreamOperator<AISRecord> parsed = assembled
                .process(new NMEAParser())
                .name("NMEAParser");

        // ── ASMDecoder ────────────────────────────────────────────────────────
        DataStream<AISRecord> asmDecoded = parsed
                .map(new ASMDecoder())
                .name("ASMDecoder");

        // ── Type24Joiner ──────────────────────────────────────────────────────
        DataStream<AISRecord> joined = asmDecoded
                .keyBy(r -> r.mmsi != null ? r.mmsi : 0L)
                .process(new Type24Joiner(config))
                .name("Type24Joiner");

        // ── ValidationEngine ──────────────────────────────────────────────────
        DataStream<AISRecord> validated = joined
                .map(new ValidationEngine())
                .name("ValidationEngine");

        // ── Sinks ─────────────────────────────────────────────────────────────
        if (!config.disableMskSink) {
            validated.sinkTo(MSKSinkBuilder.build(config))
                     .name("MSK-Ingest-Sink");

            parsed.getSideOutput(NMEAParser.DLQ_TAG)
                  .sinkTo(MSKSinkBuilder.buildDlq(config))
                  .name("DLQ-Sink");
        } else {
            // DISABLE_MSK_SINK=true: discard Kafka output, useful when
            // ais.ingest / ais.parse.dlq don't exist on the target cluster.
            validated.addSink(new org.apache.flink.streaming.api.functions.sink.DiscardingSink<>())
                     .name("MSK-Ingest-Sink-DISABLED");

            parsed.getSideOutput(NMEAParser.DLQ_TAG)
                  .addSink(new org.apache.flink.streaming.api.functions.sink.DiscardingSink<>())
                  .name("DLQ-Sink-DISABLED");
        }

        validated.sinkTo(S3ParquetSinkBuilder.build(config))
                 .name("S3-Parquet-Sink");

        env.execute("AIS-Job1-ParserValidator");
    }

    // -----------------------------------------------------------------------
    // MSK Consumer properties — IAM + SASL_SSL
    // -----------------------------------------------------------------------

    private static Properties mskConsumerProps(JobConfig config) {
        Properties props = new Properties();
        props.setProperty("session.timeout.ms", "30000");
        props.setProperty("max.poll.records",   "500");

        // Use IAM auth only when explicitly enabled (MSK on AWS).
        // When KAFKA_AUTH=NONE (local / Redpanda), connect plaintext.
        String auth = System.getenv("KAFKA_AUTH");
        if (!"NONE".equalsIgnoreCase(auth)) {
            props.setProperty("security.protocol", "SASL_SSL");
            props.setProperty("sasl.mechanism",    "AWS_MSK_IAM");
            props.setProperty("sasl.jaas.config",
                    IAMLoginModule.class.getName() + " required;");
            props.setProperty("sasl.client.callback.handler.class",
                    IAMClientCallbackHandler.class.getName());
        }
        return props;
    }

    // -----------------------------------------------------------------------
    // Deserializer: produces Tuple2<sourceTopic, rawLineValue>
    // -----------------------------------------------------------------------

    private static class TopicAwareStringDeserializer
            implements KafkaRecordDeserializationSchema<Tuple2<String, String>> {

        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> record,
                                Collector<Tuple2<String, String>> out) {
            if (record.value() == null) return;
            String value = new String(record.value(), java.nio.charset.StandardCharsets.UTF_8);
            out.collect(Tuple2.of(record.topic(), value));
        }

        @Override
        public org.apache.flink.api.common.typeinfo.TypeInformation<Tuple2<String, String>>
        getProducedType() {
            return org.apache.flink.api.java.typeutils.TypeExtractor
                    .getForObject(Tuple2.of("", ""));
        }
    }
}
