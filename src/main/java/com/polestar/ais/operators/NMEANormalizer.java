package com.polestar.ais.operators;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * Flink MapFunction: normalize Kpler-specific NMEA line format.
 *
 * Mirrors normalize_kpler_line() from lambda_function.py exactly so that
 * parsing behavior is identical between the Lambda and Flink job.
 *
 * Input : raw NMEA line as String (from Kafka record value)
 * Output: normalized line as String, or null if line is empty/invalid
 */
public class NMEANormalizer implements MapFunction<String, String> {

    @Override
    public String map(String rawLine) {
        if (rawLine == null || rawLine.isBlank()) {
            return null;
        }

        // Strip null bytes and whitespace
        String line = rawLine.replace("\u0000", "").strip();
        if (line.isEmpty()) {
            return null;
        }

        // Unescape double-backslashes
        line = line.replace("\\\\", "\\");

        // Drop leading \r:... prefix, keep from \c: onward
        if (line.startsWith("\\r:")) {
            int idx = line.indexOf("\\c:");
            if (idx != -1) {
                line = line.substring(idx);
            }
        }

        // Ensure "\!AIV" separator after '*X' checksum characters
        for (char c : "0123456789ABCDEF".toCharArray()) {
            String wrong = "*" + c + "!AIV";
            String right = "*" + c + "\\!AIV";
            line = line.replace(wrong, right);
        }

        // Normalize line endings to CRLF (pyais / AisLib expectation)
        if (line.endsWith("\r\n")) {
            // already correct
        } else if (line.endsWith("\n")) {
            line = line.substring(0, line.length() - 1) + "\r\n";
        } else {
            line = line + "\r\n";
        }

        return line;
    }
}
