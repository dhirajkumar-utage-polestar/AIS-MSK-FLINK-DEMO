package com.polestar.ais.sink;

import com.polestar.ais.config.JobConfig;
import com.polestar.ais.model.AISRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

/**
 * Builds a Flink FileSink writing Parquet files to S3.
 *
 * Partitioned by: year/month/day/hour (Hive-style path partitioning)
 * Rolling policy: on Flink checkpoint (every CHECKPOINT_INTERVAL_MS)
 * Target file size: S3_TARGET_FILE_SIZE_MB (default 64MB)
 *
 * The sink uses Avro-based Parquet encoding for compatibility with AWS Glue
 * and Athena.
 *
 * Note: Full Avro schema for AISRecord is generated from the POJO. For
 * production use, replace AvroParquetWriters.forReflectRecord with a
 * hand-crafted schema to ensure consistent column ordering and nullability.
 */
public class S3ParquetSinkBuilder {

    private S3ParquetSinkBuilder() {}

    public static FileSink<AISRecord> build(JobConfig config) {
        String basePath = "s3a://" + config.s3Bucket + "/" + config.s3Prefix;

        // Build writer factory using ReflectData.AllowNull for both schema and data model.
        // The default AvroParquetWriters.forReflectRecord uses ReflectData.get() for the
        // schema (all fields required) but AllowNull for writing — that mismatch causes
        // Parquet to reject null values on nullable Integer/Boolean fields.
        ParquetWriterFactory<AISRecord> writerFactory = new ParquetWriterFactory<>(out -> {
            org.apache.avro.Schema schema =
                    ReflectData.AllowNull.get().getSchema(AISRecord.class);
            return AvroParquetWriter.<AISRecord>builder(out)
                    .withSchema(schema)
                    .withDataModel(ReflectData.AllowNull.get())
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE)
                    .build();
        });

        return FileSink
                .<AISRecord>forBulkFormat(new Path(basePath), writerFactory)
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withBucketAssigner(new HiveStyleDateBucketAssigner())
                .build();
    }
}
