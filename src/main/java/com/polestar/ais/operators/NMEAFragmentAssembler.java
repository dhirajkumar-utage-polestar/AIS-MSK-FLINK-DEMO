package com.polestar.ais.operators;

import com.polestar.ais.config.JobConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Stateful multi-part NMEA fragment assembler.
 *
 * AIS messages such as Type 5 span 2 NMEA sentences.  Each Kafka record
 * is a single raw line.  This operator:
 *   - Buffers fragments keyed by "{sourceTopic}:{channel}:{seqId}"
 *   - Emits List<String> (ordered lines) once all parts have arrived
 *   - Registers a processing-time timer to purge stale sets after TTL
 *
 * Key:    String  — "{sourceTopic}:{channel}:{seqId}"
 * Input:  Tuple2<String sourceTopic, String normalizedLine>
 * Output: Tuple2<String sourceTopic, List<String> completeLines>
 */
public class NMEAFragmentAssembler
        extends KeyedProcessFunction<String,
                                     Tuple2<String, String>,
                                     Tuple2<String, List<String>>> {

    private final long fragmentTtlMs;

    private transient ListState<String>  fragmentsState;
    private transient ValueState<Integer> totalState;
    private transient ValueState<Long>    timerState;

    public NMEAFragmentAssembler(JobConfig config) {
        this.fragmentTtlMs = config.fragmentTtlMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        fragmentsState = getRuntimeContext().getListState(
                new ListStateDescriptor<>("nmea_fragments", Types.STRING));
        totalState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("fragment_total", Types.INT));
        timerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("fragment_timer", Types.LONG));
    }

    @Override
    public void processElement(Tuple2<String, String> value,
                               Context ctx,
                               Collector<Tuple2<String, List<String>>> out) throws Exception {

        String sourceTopic = value.f0;
        String line        = value.f1;

        FragmentHeader header = parseHeader(line);

        // Single-part or unparseable — emit immediately
        if (header == null || header.total == 1) {
            List<String> single = new ArrayList<>(1);
            single.add(line);
            out.collect(Tuple2.of(sourceTopic, single));
            return;
        }

        // Buffer fragment
        fragmentsState.add(line);
        if (totalState.value() == null) {
            totalState.update(header.total);
        }

        List<String> accumulated = new ArrayList<>();
        fragmentsState.get().forEach(accumulated::add);

        if (accumulated.size() >= header.total) {
            // All parts arrived — sort by fragment number and emit
            accumulated.sort(Comparator.comparingInt(l -> {
                FragmentHeader h = parseHeader(l);
                return h != null ? h.num : 0;
            }));
            out.collect(Tuple2.of(sourceTopic, accumulated));
            clearState(ctx);
        } else {
            // Refresh TTL timer
            Long oldTimer = timerState.value();
            if (oldTimer != null) {
                ctx.timerService().deleteProcessingTimeTimer(oldTimer);
            }
            long newTimer = ctx.timerService().currentProcessingTime() + fragmentTtlMs;
            ctx.timerService().registerProcessingTimeTimer(newTimer);
            timerState.update(newTimer);
        }
    }

    @Override
    public void onTimer(long timestamp,
                        OnTimerContext ctx,
                        Collector<Tuple2<String, List<String>>> out) throws Exception {
        // TTL expired — drop stale fragments silently
        clearState(ctx);
    }

    private void clearState(KeyedProcessFunction<?, ?, ?>.Context ctx) throws Exception {
        fragmentsState.clear();
        totalState.clear();
        Long ts = timerState.value();
        if (ts != null) {
            ctx.timerService().deleteProcessingTimeTimer(ts);
        }
        timerState.clear();
    }

    // -------------------------------------------------------------------
    // NMEA fragment header parsing
    // AIVDM format: !AIVDM,<total>,<num>,<seqId>,<channel>,<payload>,<pad>*<ck>
    // -------------------------------------------------------------------

    public static FragmentHeader parseHeader(String line) {
        try {
            // Strip tag block prefix  \...\
            String raw = line.strip();
            if (raw.startsWith("\\")) {
                int end = raw.indexOf('\\', 1);
                if (end != -1) {
                    raw = raw.substring(end + 1).strip();
                }
            }
            String[] parts = raw.split(",", -1);
            if (parts.length < 5) return null;
            int total  = Integer.parseInt(parts[1].trim());
            int num    = Integer.parseInt(parts[2].trim());
            String seq = parts[3].trim();
            String ch  = parts[4].trim();
            return new FragmentHeader(total, num, seq, ch);
        } catch (Exception e) {
            return null;
        }
    }

    public static class FragmentHeader {
        public final int    total;
        public final int    num;
        public final String seqId;
        public final String channel;

        public FragmentHeader(int total, int num, String seqId, String channel) {
            this.total   = total;
            this.num     = num;
            this.seqId   = seqId;
            this.channel = channel;
        }
    }
}
