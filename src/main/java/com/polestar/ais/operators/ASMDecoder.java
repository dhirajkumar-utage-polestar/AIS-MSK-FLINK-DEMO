package com.polestar.ais.operators;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.polestar.ais.model.AISRecord;
import dk.dma.ais.binary.BinArray;
import dk.dma.ais.binary.SixbitException;
import dk.dma.ais.message.binary.MetHyd11;
import dk.dma.ais.message.binary.RouteSuggestion;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Decodes Application-Specific Messages (ASM) stored in AISRecord extras.
 *
 * Supported:
 *   DAC=1, FID=11 — Meteorological and Hydrographic (MetHyd11)
 *   DAC=1, FID=28 — Route Suggestion
 */
public class ASMDecoder implements MapFunction<AISRecord, AISRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(ASMDecoder.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public AISRecord map(AISRecord record) {
        Object dacObj  = record.getExtra("dac");
        Object fidObj  = record.getExtra("fid");
        Object dataObj = record.getExtra("data");

        if (dacObj == null || fidObj == null || dataObj == null) return record;

        int dac, fid;
        try {
            dac = ((Number) dacObj).intValue();
            fid = ((Number) fidObj).intValue();
        } catch (ClassCastException e) {
            return record;
        }

        record.asmDac = dac;
        record.asmFid = fid;

        if (!(dataObj instanceof BinArray)) return record;
        BinArray binArray = (BinArray) dataObj;

        try {
            if (dac == 1 && fid == 11) {
                decodeMeteorological(record, binArray);
            } else if (dac == 1 && fid == 28) {
                decodeRouteSuggestion(record, binArray);
            }
        } catch (Exception e) {
            LOG.debug("ASM decode failed DAC={} FID={}: {}", dac, fid, e.getMessage());
        }

        return record;
    }

    private void decodeMeteorological(AISRecord record, BinArray binArray) throws SixbitException {
        MetHyd11 met = new MetHyd11(binArray);

        Map<String, Object> asm = new LinkedHashMap<>();
        asm.put("asm_type",              "meteo");
        asm.put("wind_speed",            met.getWind()          != 127  ? met.getWind()              : null);
        asm.put("wind_gust",             met.getGust()          != 127  ? met.getGust()              : null);
        asm.put("wind_direction",        met.getWindDirection()  != 360  ? met.getWindDirection()     : null);
        asm.put("wind_gust_direction",   met.getGustDirection()  != 360  ? met.getGustDirection()     : null);
        asm.put("air_temp",              met.getAirTemp()        != -1024 ? met.getAirTemp() / 10.0  : null);
        asm.put("humidity",              met.getHumidity()       != 101  ? met.getHumidity()         : null);
        asm.put("dew_point",             met.getDewPoint()       != -1024 ? met.getDewPoint() / 10.0 : null);
        asm.put("air_pressure",          met.getAirPressure()    != 511  ? met.getAirPressure() + 799 : null);
        asm.put("air_pressure_tendency", met.getAirPressureTend() != 3  ? met.getAirPressureTend()  : null);
        asm.put("wave_height",           met.getWaveHeight()     != 255  ? met.getWaveHeight()       : null);
        asm.put("wave_period",           met.getWavePeriod()     != 63   ? met.getWavePeriod()       : null);
        asm.put("wave_direction",        met.getWaveDirection()  != 360  ? met.getWaveDirection()    : null);
        asm.put("swell_height",          met.getSwellHeight()    != 255  ? met.getSwellHeight()      : null);
        asm.put("swell_period",          met.getSwellPeriod()    != 63   ? met.getSwellPeriod()      : null);
        asm.put("swell_direction",       met.getSwellDirection() != 360  ? met.getSwellDirection()   : null);
        asm.put("sea_state",             met.getSeaState()       != 13   ? met.getSeaState()         : null);
        asm.put("water_temp",            met.getWaterTemp()      != 601  ? met.getWaterTemp() / 10.0 : null);
        asm.put("precipitation",         met.getPrecipitation()  != 7    ? met.getPrecipitation()    : null);
        asm.put("salinity",              met.getSalinity()       != 511  ? met.getSalinity() / 10.0  : null);
        asm.put("ice",                   met.getIce()            != 3    ? met.getIce()              : null);
        record.asmData = asm;
        try { record.asmDataJson = MAPPER.writeValueAsString(asm); } catch (Exception ignored) {}
    }

    private void decodeRouteSuggestion(AISRecord record, BinArray binArray) throws SixbitException {
        RouteSuggestion route = new RouteSuggestion(binArray);

        Map<String, Object> asm = new LinkedHashMap<>();
        asm.put("asm_type",   "route_suggestion");
        asm.put("route_type", route.getMsgLinkId());
        record.asmData = asm;
        try { record.asmDataJson = MAPPER.writeValueAsString(asm); } catch (Exception ignored) {}
    }
}
