package com.polestar.ais.operators;

import com.polestar.ais.config.JobConfig;
import com.polestar.ais.model.AISRecord;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Stateful joiner for AIS Type 24 Part A + Part B messages.
 *
 * Type 24 Part A carries vessel name only.
 * Type 24 Part B carries ship type, callsign, vendor ID, and dimensions.
 * Both share the same MMSI.
 *
 * This operator:
 *   - Buffers the first part received (keyed by MMSI)
 *   - Emits a merged AISRecord when both parts have arrived
 *   - Registers a TTL timer to clear stale state (default 5 minutes)
 *   - Also emits each part individually so downstream consumers see partial data
 *
 * Key:   Long  — MMSI
 * Input:  AISRecord (messageId == 24, partNumber == 0 or 1)
 * Output: AISRecord (partNumber == null means joined; otherwise partial)
 */
public class Type24Joiner
        extends KeyedProcessFunction<Long, AISRecord, AISRecord> {

    private final long ttlMs;

    private transient ValueState<AISRecord> partAState;
    private transient ValueState<AISRecord> partBState;
    private transient ValueState<Long>      timerState;

    public Type24Joiner(JobConfig config) {
        this.ttlMs = config.type24JoinTtlMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        TypeInformation<AISRecord> recType = TypeInformation.of(new TypeHint<AISRecord>() {});

        partAState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("type24_partA", recType));
        partBState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("type24_partB", recType));
        timerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("type24_timer", Long.class));
    }

    @Override
    public void processElement(AISRecord record,
                               Context ctx,
                               Collector<AISRecord> out) throws Exception {

        // Always emit the individual part so downstream sees real-time updates
        out.collect(record);

        if (record.partNumber == null) return;

        if (record.partNumber == 0) {
            partAState.update(record);
        } else {
            partBState.update(record);
        }

        // Attempt join
        AISRecord partA = partAState.value();
        AISRecord partB = partBState.value();

        if (partA != null && partB != null) {
            AISRecord joined = merge(partA, partB);
            joined.type24Joined = true;
            out.collect(joined);
            clearState(ctx);
            return;
        }

        // Refresh TTL timer waiting for the other part
        Long oldTimer = timerState.value();
        if (oldTimer != null) {
            ctx.timerService().deleteProcessingTimeTimer(oldTimer);
        }
        long newTimer = ctx.timerService().currentProcessingTime() + ttlMs;
        ctx.timerService().registerProcessingTimeTimer(newTimer);
        timerState.update(newTimer);
    }

    @Override
    public void onTimer(long timestamp,
                        OnTimerContext ctx,
                        Collector<AISRecord> out) throws Exception {
        // TTL expired — drop incomplete join silently
        clearState(ctx);
    }

    private AISRecord merge(AISRecord a, AISRecord b) {
        AISRecord merged = new AISRecord();

        // Identity (both parts have same MMSI)
        merged.messageId       = 24;
        merged.mmsi            = a.mmsi;
        merged.repeatIndicator = a.repeatIndicator;
        merged.sourceTopic     = a.sourceTopic != null ? a.sourceTopic : b.sourceTopic;
        merged.sourceStation   = a.sourceStation != null ? a.sourceStation : b.sourceStation;
        merged.receivedTimestamp = a.receivedTimestamp != null ? a.receivedTimestamp : b.receivedTimestamp;

        // Part A fields
        merged.vesselName = a.vesselName;

        // Part B fields
        merged.shipType             = b.shipType;
        merged.callSign             = b.callSign;
        merged.vendorId             = b.vendorId;
        merged.dimensionToBow       = b.dimensionToBow;
        merged.dimensionToStern     = b.dimensionToStern;
        merged.dimensionToPort      = b.dimensionToPort;
        merged.dimensionToStarboard = b.dimensionToStarboard;

        return merged;
    }

    private void clearState(KeyedProcessFunction<?, ?, ?>.Context ctx) throws Exception {
        partAState.clear();
        partBState.clear();
        Long ts = timerState.value();
        if (ts != null) {
            ctx.timerService().deleteProcessingTimeTimer(ts);
        }
        timerState.clear();
    }
}
