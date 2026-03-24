package com.polestar.ais.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.avro.reflect.AvroIgnore;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parsed + validated AIS record.
 *
 * Core fields are typed. All extra fields (tag block metadata, ASM data,
 * vendor-specific extensions) are stored in the extras map and serialized
 * as flat JSON fields.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AISRecord implements Serializable {

    // ---- Identity -------------------------------------------------------
    public Integer messageId;
    public Long    mmsi;
    public Integer repeatIndicator;

    // ---- Position (Types 1,2,3,18,19,27) --------------------------------
    public Double  latitude;
    public Double  longitude;
    public Double  sog;           // speed over ground (knots)
    public Double  cog;           // course over ground (degrees)
    public Integer trueHeading;
    public Integer navStatus;
    public Integer rateOfTurnRaw;
    public Integer positionAccuracy;
    public Integer timestamp;
    public Integer maneuverIndicator;
    public Boolean raimFlag;
    public Integer communicationState;

    // ---- Vessel static (Type 5) -----------------------------------------
    public Long    imoNumber;
    public String  callSign;
    public String  vesselName;
    public Integer shipType;
    public Integer dimensionToBow;
    public Integer dimensionToStern;
    public Integer dimensionToPort;
    public Integer dimensionToStarboard;
    public Integer positionFixType;
    public Integer etaMonth;
    public Integer etaDay;
    public Integer etaHour;
    public Integer etaMinute;
    public Double  draught;
    public String  destination;
    public Boolean dteFlag;

    // ---- Class B (Types 18,24) ------------------------------------------
    public Boolean csUnit;
    public Boolean displayFlag;
    public Boolean dscFlag;
    public Boolean bandFlag;
    public Boolean message22Flag;
    public Boolean assignedMode;

    // ---- Type 24 specific -----------------------------------------------
    public Integer partNumber;
    public String  vendorId;
    public Integer unitModelCode;
    public Integer serialNumber;
    public Boolean type24Joined;

    // ---- Type 27 --------------------------------------------------------
    public Boolean gnssPositionStatus;

    // ---- Tag block fields (from NMEA tag block) -------------------------
    public Long    receivedTimestamp;  // c: tag — unix ts from vendor
    public String  sourceStation;      // s: tag — source station ID
    public String  sourceTopic;        // which MSK topic this came from

    // ---- Validation output ----------------------------------------------
    public String       recordQuality;  // GOOD | WARN | ERROR
    public List<String> violations = new ArrayList<>();

    // ---- ASM decoded fields (Type 6/8) ----------------------------------
    @AvroIgnore  // Map<String,Object> not serializable by Avro reflect; use asmDataJson for Parquet
    public Map<String, Object> asmData;
    public String  asmDataJson;   // JSON-serialized asmData (Parquet-safe)
    public Integer asmDac;
    public Integer asmFid;

    // ---- Catch-all for any extra fields ---------------------------------
    @AvroIgnore  // private + Object values not Avro-reflectable
    private final Map<String, Object> extras = new HashMap<>();

    @JsonAnyGetter
    public Map<String, Object> getExtras() {
        return extras;
    }

    @JsonAnySetter
    public void setExtra(String key, Object value) {
        extras.put(key, value);
    }

    public void putExtra(String key, Object value) {
        extras.put(key, value);
    }

    public Object getExtra(String key) {
        return extras.get(key);
    }
}
