package com.polestar.ais.operators;

import com.polestar.ais.model.AISRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Validates parsed AISRecord fields against rules.yml.
 *
 * Mirrors the Python RecordValidator hierarchy from validators.py:
 *   - MMSI format + range check (9 digits, 100_000_000-999_999_999)
 *   - Lat/Lon range
 *   - SOG/COG/Heading with reserved values
 *   - Enum validation for navStatus, maneuverIndicator
 *   - IMO checksum (Type 5)
 *   - Draught range (Type 5)
 *
 * Quality classification:
 *   GOOD  — no violations
 *   WARN  — violations but not fatal
 *   ERROR — MMSI_FORMAT, MMSI_RANGE, LAT_RANGE, LON_RANGE, or MSG_ID_CONST
 *
 * Input:  AISRecord (parsed, not yet validated)
 * Output: AISRecord (with recordQuality + violations populated)
 */
public class ValidationEngine implements MapFunction<AISRecord, AISRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(ValidationEngine.class);

    private static final Set<String> ERROR_VIOLATIONS = new HashSet<>(Arrays.asList(
            "MSG_ID_CONST", "MMSI_FORMAT", "MMSI_RANGE", "LAT_RANGE", "LON_RANGE"
    ));

    // Loaded once at construction; safe to serialize since it's immutable after init
    private final Map<String, Map<Integer, String>> enums;

    public ValidationEngine() {
        this.enums = loadEnums();
    }

    @Override
    public AISRecord map(AISRecord record) {
        if (record == null) return null;
        List<String> violations = new ArrayList<>();

        validateMmsi(record, violations);
        validateLatLon(record, violations);
        validatePositionFields(record, violations);

        int type = record.messageId != null ? record.messageId : -1;
        switch (type) {
            case 1: case 2: case 3:
                validateType123(record, violations);
                break;
            case 5:
                validateType5(record, violations);
                break;
            case 18:
                validateType18(record, violations);
                break;
            case 19:
                validateType19(record, violations);
                break;
            case 24:
                validateType24(record, violations);
                break;
            case 27:
                validateType27(record, violations);
                break;
            default:
                break;
        }

        record.violations = violations;
        record.recordQuality = determineQuality(violations);
        return record;
    }

    // -----------------------------------------------------------------------
    // Shared validators
    // -----------------------------------------------------------------------

    private void validateMmsi(AISRecord r, List<String> v) {
        if (r.mmsi == null) {
            v.add("MMSI_FORMAT");
            return;
        }
        String s = Long.toString(r.mmsi);
        if (s.length() != 9 || !s.chars().allMatch(Character::isDigit)) {
            v.add("MMSI_FORMAT");
        } else {
            long m = r.mmsi;
            if (m < 100_000_000L || m > 999_999_999L) {
                v.add("MMSI_RANGE");
            }
        }
    }

    private void validateLatLon(AISRecord r, List<String> v) {
        if (r.latitude != null && (r.latitude < -90.0 || r.latitude > 90.0)) {
            v.add("LAT_RANGE");
        }
        if (r.longitude != null && (r.longitude < -180.0 || r.longitude > 180.0)) {
            v.add("LON_RANGE");
        }
    }

    private void validatePositionFields(AISRecord r, List<String> v) {
        // SOG: 0-102.2, reserved 102.3
        if (r.sog != null && !inRangeWithReserved(r.sog, 0.0, 102.2, 102.3)) {
            v.add("SOG_RANGE");
        }
        // COG: 0-359.9, reserved 360.0
        if (r.cog != null && !inRangeWithReserved(r.cog, 0.0, 359.9, 360.0)) {
            v.add("COG_RANGE");
        }
        // True heading: 0-359, reserved 511
        if (r.trueHeading != null && r.trueHeading != 511 &&
                (r.trueHeading < 0 || r.trueHeading > 359)) {
            v.add("HDG_RANGE");
        }
    }

    // -----------------------------------------------------------------------
    // Type-specific validators
    // -----------------------------------------------------------------------

    private void validateType123(AISRecord r, List<String> v) {
        if (r.rateOfTurnRaw != null &&
                (r.rateOfTurnRaw < -128 || r.rateOfTurnRaw > 127)) {
            v.add("ROT_RAW_RANGE");
        }
        if (!inEnum(r.navStatus, "nav_status")) {
            v.add("NAV_STATUS_ENUM");
        }
        if (!inEnum(r.maneuverIndicator, "maneuver_indicator")) {
            v.add("MANEUVER_ENUM");
        }
        if (r.timestamp != null && (r.timestamp < 0 || r.timestamp > 63)) {
            v.add("TIMESTAMP_RANGE");
        }
    }

    private void validateType5(AISRecord r, List<String> v) {
        // IMO checksum (7-digit, last digit is modulo-10 check digit)
        if (r.imoNumber != null && r.imoNumber > 0) {
            String s = String.format("%07d", r.imoNumber);
            if (s.length() == 7) {
                int[] weights = {7, 6, 5, 4, 3, 2};
                int total = 0;
                for (int i = 0; i < 6; i++) {
                    total += Character.getNumericValue(s.charAt(i)) * weights[i];
                }
                int checkDigit = total % 10;
                if (checkDigit != Character.getNumericValue(s.charAt(6))) {
                    v.add("IMO_CHECKSUM");
                }
            } else {
                v.add("IMO_CHECKSUM");
            }
        }
        if (r.draught != null && (r.draught < 0.0 || r.draught > 25.5)) {
            v.add("DRAUGHT_RANGE");
        }
        if (!inEnum(r.shipType, "ship_type")) {
            v.add("SHIP_TYPE_ENUM");
        }
    }

    private void validateType18(AISRecord r, List<String> v) {
        if (r.sog != null && !inRangeWithReserved(r.sog, 0.0, 102.2, 102.3)) {
            v.add("SOG_RANGE");
        }
        if (r.timestamp != null && (r.timestamp < 0 || r.timestamp > 63)) {
            v.add("TIMESTAMP_RANGE");
        }
    }

    private void validateType19(AISRecord r, List<String> v) {
        if (!inEnum(r.shipType, "ship_type")) {
            v.add("SHIP_TYPE_ENUM");
        }
    }

    private void validateType24(AISRecord r, List<String> v) {
        if (r.partNumber != null && r.partNumber == 1) {
            if (!inEnum(r.shipType, "ship_type")) {
                v.add("SHIP_TYPE_ENUM");
            }
        }
    }

    private void validateType27(AISRecord r, List<String> v) {
        if (!inEnum(r.navStatus, "nav_status")) {
            v.add("NAV_STATUS_ENUM");
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private boolean inRangeWithReserved(double val, double min, double max, double reserved) {
        return (val >= min && val <= max) || val == reserved;
    }

    private boolean inEnum(Integer val, String enumKey) {
        if (val == null) return true;  // null is allowed
        Map<Integer, String> enumMap = enums.getOrDefault(enumKey, Collections.emptyMap());
        return enumMap.containsKey(val);
    }

    private String determineQuality(List<String> violations) {
        for (String v : violations) {
            if (ERROR_VIOLATIONS.contains(v)) return "ERROR";
        }
        return violations.isEmpty() ? "GOOD" : "WARN";
    }

    // -----------------------------------------------------------------------
    // YAML enum loader
    // -----------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private Map<String, Map<Integer, String>> loadEnums() {
        Map<String, Map<Integer, String>> result = new HashMap<>();
        try (InputStream is = getClass().getClassLoader()
                .getResourceAsStream("rules.yml")) {
            if (is == null) {
                LOG.warn("rules.yml not found on classpath — validation will use empty enums");
                return result;
            }
            Yaml yaml = new Yaml();
            Map<String, Object> root = yaml.load(is);
            Map<String, Object> enumsRaw = (Map<String, Object>) root.getOrDefault("enums", new HashMap<>());

            for (Map.Entry<String, Object> entry : enumsRaw.entrySet()) {
                String enumKey = entry.getKey();
                Map<Object, Object> rawMap = (Map<Object, Object>) entry.getValue();
                Map<Integer, String> expanded = expandRanges(rawMap);
                result.put(enumKey, expanded);
            }
        } catch (Exception e) {
            LOG.error("Failed to load rules.yml: {}", e.getMessage());
        }
        return result;
    }

    private static final Pattern RANGE_PATTERN = Pattern.compile("^(\\d+)-(\\d+)$");

    private Map<Integer, String> expandRanges(Map<Object, Object> rawMap) {
        Map<Integer, String> out = new HashMap<>();
        for (Map.Entry<Object, Object> entry : rawMap.entrySet()) {
            String key = String.valueOf(entry.getKey());
            String val = String.valueOf(entry.getValue());
            Matcher m = RANGE_PATTERN.matcher(key);
            if (m.matches()) {
                int lo = Integer.parseInt(m.group(1));
                int hi = Integer.parseInt(m.group(2));
                for (int i = lo; i <= hi; i++) {
                    out.put(i, val);
                }
            } else {
                try {
                    out.put(Integer.parseInt(key), val);
                } catch (NumberFormatException ignored) {}
            }
        }
        return out;
    }
}
