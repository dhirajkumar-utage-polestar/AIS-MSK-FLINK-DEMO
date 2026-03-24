package com.polestar.ais.sink;

import com.polestar.ais.model.AISRecord;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * Assigns S3 bucket paths in Hive-style date partitioning:
 *   year=YYYY/month=MM/day=DD/hour=HH
 *
 * Uses the receivedTimestamp from the NMEA tag block (seconds since epoch).
 * Falls back to processing time if receivedTimestamp is null.
 */
public class HiveStyleDateBucketAssigner implements BucketAssigner<AISRecord, String> {

    @Override
    public String getBucketId(AISRecord record, Context context) {
        long epochSeconds = record.receivedTimestamp != null
                ? record.receivedTimestamp
                : context.currentProcessingTime() / 1000L;

        ZonedDateTime dt = Instant.ofEpochSecond(epochSeconds).atZone(ZoneOffset.UTC);

        return String.format("year=%04d/month=%02d/day=%02d/hour=%02d",
                dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(), dt.getHour());
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
