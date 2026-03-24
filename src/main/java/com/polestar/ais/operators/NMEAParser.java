package com.polestar.ais.operators;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.polestar.ais.model.AISRecord;
import dk.dma.ais.message.*;
import dk.dma.ais.packet.AisPacket;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Flink FlatMapFunction: parse complete NMEA lines into AISRecord.
 *
 * Uses DMA AisLib 2.8.6 for decoding all standard AIS message types.
 *
 * Extracts NMEA tag block fields:
 *   c: → receivedTimestamp (unix seconds from vendor antenna)
 *   s: → sourceStation
 *
 * Input : Tuple2<String sourceTopic, List<String> completeLines>
 * Output: AISRecord (main stream) | String JSON (DLQ side output on parse failure)
 *
 * DLQ format: {"ts":...,"topic":"...","error":"...","lines":["..."]}
 */
public class NMEAParser
        extends ProcessFunction<Tuple2<String, List<String>>, AISRecord> {

    /** Side output tag for records that could not be parsed. */
    public static final OutputTag<String> DLQ_TAG =
            new OutputTag<String>("parse-dlq") {};

    private static final Logger LOG = LoggerFactory.getLogger(NMEAParser.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Pattern TAG_BLOCK_C = Pattern.compile("\\bc:(-?\\d+)");
    private static final Pattern TAG_BLOCK_S = Pattern.compile("\\bs:([^*,\\\\]+)");

    @Override
    public void processElement(Tuple2<String, List<String>> value,
                               Context ctx,
                               Collector<AISRecord> out) throws Exception {

        String       sourceTopic = value.f0;
        List<String> lines       = value.f1;

        if (lines == null || lines.isEmpty()) return;

        try {
            AisPacket packet = buildPacket(lines);
            if (packet == null) return;

            AisMessage msg = packet.tryGetAisMessage();
            if (msg == null) return;

            AISRecord record = new AISRecord();
            record.sourceTopic    = sourceTopic;
            record.messageId      = msg.getMsgId();
            record.mmsi           = (long) msg.getUserId();
            record.repeatIndicator = msg.getRepeat();

            extractTagBlock(lines.get(0), record);
            populateByType(msg, record);

            out.collect(record);

        } catch (Exception e) {
            LOG.debug("Failed to parse NMEA topic={}: {}", sourceTopic, e.getMessage());
            emitDlq(ctx, sourceTopic, lines, e);
        }
    }

    private void emitDlq(Context ctx, String sourceTopic, List<String> lines, Exception e) {
        try {
            Map<String, Object> dlq = new LinkedHashMap<>();
            dlq.put("ts",    System.currentTimeMillis());
            dlq.put("topic", sourceTopic);
            dlq.put("error", e.getClass().getSimpleName() + ": " + e.getMessage());
            dlq.put("lines", lines);
            ctx.output(DLQ_TAG, MAPPER.writeValueAsString(dlq));
        } catch (Exception ignored) {}
    }

    // -----------------------------------------------------------------------

    private AisPacket buildPacket(List<String> lines) {
        try {
            StringBuilder sb = new StringBuilder();
            for (String l : lines) sb.append(l);
            return AisPacket.from(sb.toString().trim());
        } catch (Exception e) {
            return null;
        }
    }

    private void extractTagBlock(String line, AISRecord record) {
        if (!line.startsWith("\\")) return;

        Matcher mc = TAG_BLOCK_C.matcher(line);
        if (mc.find()) {
            try {
                record.receivedTimestamp = Long.parseLong(mc.group(1));
            } catch (NumberFormatException ignored) {}
        }

        Matcher ms = TAG_BLOCK_S.matcher(line);
        if (ms.find()) {
            record.sourceStation = ms.group(1).trim();
        }

        if (record.receivedTimestamp == null) {
            record.receivedTimestamp = System.currentTimeMillis() / 1000L;
        }
    }

    private void populateByType(AisMessage msg, AISRecord record) {
        int type = msg.getMsgId();

        switch (type) {
            case 1:
            case 2:
            case 3: {
                // AisMessage1/2/3 all extend AisPositionMessage
                AisPositionMessage pos = (AisPositionMessage) msg;
                record.navStatus        = pos.getNavStatus();
                record.rateOfTurnRaw    = pos.getRot();
                record.sog              = pos.getSog() / 10.0;
                record.positionAccuracy = pos.getPosAcc();
                record.longitude        = pos.getPos() != null ? pos.getPos().getLongitudeDouble() : null;
                record.latitude         = pos.getPos() != null ? pos.getPos().getLatitudeDouble()  : null;
                record.cog              = pos.getCog() / 10.0;
                record.trueHeading      = pos.getTrueHeading();
                record.timestamp        = pos.getUtcSec();
                record.maneuverIndicator = pos.getSpecialManIndicator();
                record.raimFlag         = pos.getRaim() == 1;
                break;
            }

            case 4: {
                AisMessage4 msg4 = (AisMessage4) msg;
                record.longitude = msg4.getPos() != null ? msg4.getPos().getLongitudeDouble() : null;
                record.latitude  = msg4.getPos() != null ? msg4.getPos().getLatitudeDouble()  : null;
                break;
            }

            case 5: {
                AisMessage5 msg5 = (AisMessage5) msg;
                record.imoNumber    = msg5.getImo();
                record.callSign     = msg5.getCallsign() != null ? msg5.getCallsign().trim() : null;
                record.vesselName   = msg5.getName()     != null ? msg5.getName().trim()     : null;
                record.shipType     = msg5.getShipType();
                record.dimensionToBow       = msg5.getDimBow();
                record.dimensionToStern     = msg5.getDimStern();
                record.dimensionToPort      = msg5.getDimPort();
                record.dimensionToStarboard = msg5.getDimStarboard();
                record.positionFixType      = msg5.getPosType();
                // ETA is packed as a 20-bit value; extract sub-fields manually
                long eta = msg5.getEta();
                record.etaMinute = (int) (eta & 0x3F);         // bits 0-5
                record.etaHour   = (int) ((eta >> 6) & 0x1F);  // bits 6-10
                record.etaDay    = (int) ((eta >> 11) & 0x1F); // bits 11-15
                record.etaMonth  = (int) ((eta >> 16) & 0x0F); // bits 16-19
                record.draught   = msg5.getDraught() / 10.0;
                record.destination = msg5.getDest() != null ? msg5.getDest().trim() : null;
                record.dteFlag   = msg5.getDte() == 0;
                break;
            }

            case 6:
            case 12: {
                AisBinaryMessage bin = (AisBinaryMessage) msg;
                record.putExtra("dac",  bin.getDac());
                record.putExtra("fid",  bin.getFi());
                record.putExtra("data", bin.getData());
                break;
            }

            case 8: {
                AisBinaryMessage bin = (AisBinaryMessage) msg;
                record.putExtra("dac",  bin.getDac());
                record.putExtra("fid",  bin.getFi());
                record.putExtra("data", bin.getData());
                break;
            }

            case 9: {
                AisMessage9 msg9 = (AisMessage9) msg;
                record.sog       = (double) msg9.getSog();
                record.longitude = msg9.getValidPosition() != null ? msg9.getValidPosition().getLongitude() : null;
                record.latitude  = msg9.getValidPosition() != null ? msg9.getValidPosition().getLatitude()  : null;
                record.cog       = msg9.getCog() / 10.0;
                break;
            }

            case 14: {
                AisMessage14 msg14 = (AisMessage14) msg;
                record.putExtra("safety_text", msg14.getMessage());
                break;
            }

            case 17: {
                AisMessage17 msg17 = (AisMessage17) msg;
                // AisMessage17 uses raw int lon/lat (1/10000 min resolution)
                record.longitude = msg17.getLon() / 600000.0;
                record.latitude  = msg17.getLat() / 600000.0;
                break;
            }

            case 18: {
                AisMessage18 msg18 = (AisMessage18) msg;
                record.sog              = msg18.getSog() / 10.0;
                record.positionAccuracy = msg18.getPosAcc();
                record.longitude        = msg18.getPos() != null ? msg18.getPos().getLongitudeDouble() : null;
                record.latitude         = msg18.getPos() != null ? msg18.getPos().getLatitudeDouble()  : null;
                record.cog              = msg18.getCog() / 10.0;
                record.trueHeading      = msg18.getTrueHeading();
                record.timestamp        = msg18.getUtcSec();
                record.csUnit           = msg18.getClassBUnitFlag() == 1;
                record.displayFlag      = msg18.getClassBDisplayFlag() == 1;
                record.dscFlag          = msg18.getClassBDscFlag() == 1;
                record.bandFlag         = msg18.getClassBBandFlag() == 1;
                record.message22Flag    = msg18.getClassBMsg22Flag() == 1;
                record.assignedMode     = msg18.getModeFlag() == 1;
                record.raimFlag         = msg18.getRaim() == 1;
                break;
            }

            case 19: {
                AisMessage19 msg19 = (AisMessage19) msg;
                record.sog              = msg19.getSog() / 10.0;
                record.positionAccuracy = msg19.getPosAcc();
                record.longitude        = msg19.getPos() != null ? msg19.getPos().getLongitudeDouble() : null;
                record.latitude         = msg19.getPos() != null ? msg19.getPos().getLatitudeDouble()  : null;
                record.cog              = msg19.getCog() / 10.0;
                record.trueHeading      = msg19.getTrueHeading();
                record.vesselName       = msg19.getName() != null ? msg19.getName().trim() : null;
                record.shipType         = msg19.getShipType();
                break;
            }

            case 21: {
                AisMessage21 msg21 = (AisMessage21) msg;
                record.vesselName       = msg21.getName() != null ? msg21.getName().trim() : null;
                record.longitude        = msg21.getPos() != null ? msg21.getPos().getLongitudeDouble() : null;
                record.latitude         = msg21.getPos() != null ? msg21.getPos().getLatitudeDouble()  : null;
                record.positionAccuracy = msg21.getPosAcc();
                record.putExtra("type_of_aids_to_nav", msg21.getAtonType());
                record.putExtra("off_position",        msg21.getOffPosition() == 1);
                break;
            }

            case 24: {
                AisMessage24 msg24 = (AisMessage24) msg;
                record.partNumber = msg24.getPartNumber();
                if (record.partNumber == 0) {
                    record.vesselName = msg24.getName() != null ? msg24.getName().trim() : null;
                } else {
                    record.shipType             = msg24.getShipType();
                    record.vendorId             = msg24.getVendorId();
                    record.callSign             = msg24.getCallsign() != null ? msg24.getCallsign().trim() : null;
                    record.dimensionToBow       = msg24.getDimBow();
                    record.dimensionToStern     = msg24.getDimStern();
                    record.dimensionToPort      = msg24.getDimPort();
                    record.dimensionToStarboard = msg24.getDimStarboard();
                }
                break;
            }

            case 27: {
                AisMessage27 msg27 = (AisMessage27) msg;
                record.navStatus          = msg27.getNavStatus();
                record.sog                = (double) msg27.getSog();
                record.cog                = (double) msg27.getCog();
                record.positionAccuracy   = msg27.getPosAcc();
                record.raimFlag           = msg27.getRaim() == 1;
                record.gnssPositionStatus = msg27.getGnssPosStatus() == 1;
                record.longitude          = msg27.getPos() != null ? msg27.getPos().getLongitudeDouble() : null;
                record.latitude           = msg27.getPos() != null ? msg27.getPos().getLatitudeDouble()  : null;
                break;
            }

            default:
                break;
        }
    }
}
