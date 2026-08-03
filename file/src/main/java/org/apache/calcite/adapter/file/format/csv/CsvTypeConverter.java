/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.file.format.csv;

import org.apache.calcite.adapter.file.util.NullEquivalents;
import org.apache.calcite.sql.type.SqlTypeName;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Set;

/**
 * Utility class for converting CSV string values to typed objects using
 * the same parsing logic and formatters that were successful during type inference.
 *
 * <p>This ensures consistency between type inference and runtime conversion,
 * avoiding duplicate parsing logic and potential inconsistencies.
 */
public final class CsvTypeConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(CsvTypeConverter.class);

  // Common timestamp formats to try during parsing
  private static final DateTimeFormatter[] TIMESTAMP_FORMATTERS = {
      DateTimeFormatter.ISO_LOCAL_DATE_TIME,
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss'Z'"),
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"),
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX"),
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX"),
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z"),  // Timezone abbreviation like EST, PST
      DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss Z"),  // RFC 2822 format
      DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss"),
      DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")
  };

  private static final DateTimeFormatter[] DATE_FORMATTERS = {
      DateTimeFormatter.ISO_LOCAL_DATE,
      DateTimeFormatter.ofPattern("yyyy-MM-dd"),
      DateTimeFormatter.ofPattern("yyyy/MM/dd"),
      DateTimeFormatter.ofPattern("yyyy.MM.dd"),
      DateTimeFormatter.ofPattern("MM/dd/yyyy"),
      DateTimeFormatter.ofPattern("dd/MM/yyyy")
  };

  private final Set<String> nullEquivalents;
  @SuppressWarnings("UnusedVariable")
  private final boolean blankStringsAsNull;

  public CsvTypeConverter(Set<String> nullEquivalents, boolean blankStringsAsNull) {
    this.nullEquivalents = nullEquivalents;
    this.blankStringsAsNull = blankStringsAsNull;
  }

  /**
   * Converts a string value to the target SQL type using the built-in fallback formatters.
   *
   * @param value the string value to convert
   * @param targetType the target SQL type
   * @return the converted value, or null if the value represents null
   */
  public @Nullable Object convert(String value, SqlTypeName targetType) {
    return convert(value, targetType, null);
  }

  /**
   * Converts a string value to the target SQL type, preferring the formatter that was
   * proven to match this column during type inference before falling back to the
   * built-in formatter lists.
   *
   * @param value the string value to convert
   * @param targetType the target SQL type
   * @param inferredFormatter the formatter selected for this column during type inference,
   *                          or null if none was recorded
   * @return the converted value, or null if the value represents null
   */
  public @Nullable Object convert(String value, SqlTypeName targetType,
      @Nullable DateTimeFormatter inferredFormatter) {
    LOGGER.debug("CsvTypeConverter.convert() called with value='{}' targetType={}", value, targetType);
    Object finalResult;
    switch (targetType) {
    case BOOLEAN:
      if (isNullRepresentation(value)) {
        finalResult = null;
      } else {
        finalResult = parseBoolean(value);
      }
      break;
    case TINYINT:
      if (isNullRepresentation(value)) {
        finalResult = null;
      } else {
        finalResult = parseByte(value);
      }
      break;
    case SMALLINT:
      if (isNullRepresentation(value)) {
        finalResult = null;
      } else {
        finalResult = parseShort(value);
      }
      break;
    case INTEGER:
      if (isNullRepresentation(value)) {
        finalResult = null;
      } else {
        finalResult = parseInt(value);
      }
      break;
    case BIGINT:
      if (isNullRepresentation(value)) {
        finalResult = null;
      } else {
        finalResult = parseLong(value);
      }
      break;
    case REAL:
      if (isNullRepresentation(value)) {
        finalResult = null;
      } else {
        finalResult = parseFloat(value);
      }
      break;
    case FLOAT:
    case DOUBLE:
      if (isNullRepresentation(value)) {
        finalResult = null;
      } else {
        finalResult = parseDouble(value);
      }
      break;
    case DECIMAL:
      if (isNullRepresentation(value)) {
        finalResult = null;
      } else {
        finalResult = parseDecimal(value);
      }
      break;
    case DATE:
      if (isNullRepresentation(value)) {
        LOGGER.debug("DATE conversion: value '{}' is null representation, returning null", value);
        finalResult = null;
      } else {
        Object result = parseDate(value, inferredFormatter);
        LOGGER.debug("DATE conversion: value '{}' converted to: {} (type: {})", value, result, result.getClass().getSimpleName());
        finalResult = result;
      }
      break;
    case TIME:
      if (isNullRepresentation(value)) {
        finalResult = null;
      } else {
        finalResult = parseTime(value, inferredFormatter);
      }
      break;
    case TIMESTAMP:
      LOGGER.debug("Processing {} field with value='{}'", targetType, value);
      if (isNullRepresentation(value)) {
        finalResult = null;
        LOGGER.debug("=== TIMESTAMPTZ DEBUG: Value is null representation for {} ===", targetType);
      } else {
        // TIMESTAMP = wall clock time, no timezone conversion
        finalResult = parseTimestamp(value, inferredFormatter);
        LOGGER.debug("=== TIMESTAMPTZ DEBUG: parseTimestamp returned {} for {} field ===", finalResult, targetType);
      }
      break;
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      LOGGER.debug("Processing {} field with value='{}'", targetType, value);
      if (isNullRepresentation(value)) {
        finalResult = null;
        LOGGER.debug("=== TIMESTAMPTZ DEBUG: Value is null representation for {} ===", targetType);
      } else {
        // TIMESTAMP_WITH_LOCAL_TIME_ZONE = timezone-aware, must convert to UTC
        finalResult = parseTimestampWithTimezone(value, inferredFormatter);
        LOGGER.debug("=== TIMESTAMPTZ DEBUG: parseTimestampWithTimezone returned {} for {} field ===", finalResult, targetType);
      }
      break;
    case VARCHAR:
    case CHAR:
      // For string types, always preserve empty strings
      // blankStringsAsNull should only apply to non-string types
      finalResult = value;  // Always preserve the value as-is for strings
      break;
    default:
      LOGGER.warn("Unknown target type: {}, returning string value", targetType);
      finalResult = value;
      break;
    }
    LOGGER.debug("CsvTypeConverter.convert() final result: {} (type: {}) for value='{}' targetType={}",
                finalResult, (finalResult != null ? finalResult.getClass().getSimpleName() : "null"), value, targetType);
    return finalResult;
  }

  private boolean isNullRepresentation(String value) {
    return NullEquivalents.isNullRepresentation(value, nullEquivalents);
  }

  private Boolean parseBoolean(String value) {
    String lower = value.toLowerCase();
    if ("true".equals(lower) || "1".equals(value) || "yes".equals(lower) || "y".equals(lower)
        || "t".equals(lower)) {
      return Boolean.TRUE;
    }
    if ("false".equals(lower) || "0".equals(value) || "no".equals(lower) || "n".equals(lower)
        || "f".equals(lower)) {
      return Boolean.FALSE;
    }
    throw new NumberFormatException("Cannot parse boolean: " + value);
  }

  private Byte parseByte(String value) {
    return Byte.valueOf(NumericFormats.stripFormatting(value));
  }

  private Short parseShort(String value) {
    return Short.valueOf(NumericFormats.stripFormatting(value));
  }

  private Integer parseInt(String value) {
    return Integer.valueOf(NumericFormats.stripFormatting(value));
  }

  private Long parseLong(String value) {
    return Long.valueOf(NumericFormats.stripFormatting(value));
  }

  private Float parseFloat(String value) {
    return Float.valueOf(NumericFormats.stripFormatting(value));
  }

  private Double parseDouble(String value) {
    return Double.valueOf(NumericFormats.stripFormatting(value));
  }

  private BigDecimal parseDecimal(String value) {
    return new BigDecimal(NumericFormats.stripFormatting(value));
  }

  private Integer parseDate(String value, @Nullable DateTimeFormatter inferredFormatter) {
    if (inferredFormatter != null) {
      try {
        LocalDate localDate = LocalDate.parse(value, inferredFormatter);
        LOGGER.debug("Successfully parsed date '{}' using inferred formatter, returning epoch day: {}", value, (int) localDate.toEpochDay());
        return Integer.valueOf((int) localDate.toEpochDay());
      } catch (DateTimeParseException e) {
        LOGGER.debug("Failed to parse date '{}' with inferred formatter: {}", value, e.getMessage());
      }
    }

    // Try common date formats
    for (DateTimeFormatter dateFormatter : DATE_FORMATTERS) {
      try {
        LocalDate localDate = LocalDate.parse(value, dateFormatter);
        LOGGER.debug("Successfully parsed date '{}' using built-in formatter, returning epoch day: {}", value, (int) localDate.toEpochDay());
        return Integer.valueOf((int) localDate.toEpochDay());
      } catch (DateTimeParseException e) {
        LOGGER.debug("Failed to parse date '{}' with formatter {}: {}", value, dateFormatter, e.getMessage());
      }
    }

    LOGGER.warn("Failed to parse date: '{}' - returning null", value);
    return null;
  }

  private Integer parseTime(String value, @Nullable DateTimeFormatter inferredFormatter) {
    if (inferredFormatter != null) {
      try {
        LocalTime localTime = LocalTime.parse(value, inferredFormatter);
        // Convert to milliseconds since midnight correctly
        // toNanoOfDay() gives nanoseconds, divide by 1_000_000 to get milliseconds
        int millisSinceMidnight = (int) (localTime.toNanoOfDay() / 1_000_000L);
        LOGGER.debug("Successfully parsed time '{}' using inferred formatter, returning millis: {}", value, millisSinceMidnight);
        return Integer.valueOf(millisSinceMidnight);
      } catch (DateTimeParseException e) {
        LOGGER.debug("Failed to parse time '{}' with inferred formatter: {}", value, e.getMessage());
      }
    }


    // Fallback: try the most common format
    try {
      LocalTime localTime = LocalTime.parse(value);
      // Convert to milliseconds since midnight correctly
      int millisSinceMidnight = (int) (localTime.toNanoOfDay() / 1_000_000L);
      LOGGER.debug("Successfully parsed time '{}' using default formatter, returning millis: {}", value, millisSinceMidnight);
      return Integer.valueOf(millisSinceMidnight);
    // fallback-guard: allow parseTime's final fallback null mirrors the project's TRY_CAST-style lenient-cast convention, logged at WARN
    } catch (DateTimeParseException e) {
      LOGGER.warn("Failed to parse time: '{}' - returning null", value);
      return null;
    }
  }

  private Long parseTimestamp(String value, @Nullable DateTimeFormatter inferredFormatter) {
    LOGGER.debug("=== TIMESTAMP DEBUG: parseTimestamp called with value='{}' ===", value);

    if (inferredFormatter != null) {
      try {
        LocalDateTime ldt = LocalDateTime.parse(value, inferredFormatter);
        long millis = ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
        LOGGER.debug("=== TIMESTAMP DEBUG: Parsed timestamp '{}' using inferred formatter, storing millis: {} ===",
            value, millis);
        return Long.valueOf(millis);
      } catch (DateTimeParseException e) {
        LOGGER.debug("Failed to parse timestamp '{}' with inferred formatter: {}", value, e.getMessage());
      }
    }

    // Try common timestamp formats
    for (DateTimeFormatter formatter : TIMESTAMP_FORMATTERS) {
      try {
        LocalDateTime ldt = LocalDateTime.parse(value, formatter);
        // For TIMESTAMP WITHOUT TIME ZONE (wall clock time):
        // Store the wall clock time as if it were UTC
        // This preserves the wall clock value regardless of the JVM's timezone
        long millis = ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
        LOGGER.debug("=== TIMESTAMP DEBUG: Parsed timestamp '{}' as UTC wall clock (formatter={}), storing millis: {} ===",
            value, formatter, millis);
        return Long.valueOf(millis);
      } catch (DateTimeParseException e) {
        LOGGER.debug("Failed to parse timestamp '{}' with formatter {}: {}", value, formatter, e.getMessage());
      }
    }

    // Try parsing as date-only (assume midnight)
    for (DateTimeFormatter dateFormatter : DATE_FORMATTERS) {
      try {
        LocalDate localDate = LocalDate.parse(value, dateFormatter);
        LocalDateTime ldt = localDate.atStartOfDay();
        LOGGER.warn("Parsed date-only value '{}' as timestamp by assuming midnight: {}", value, ldt);
        // For TIMESTAMP WITHOUT TIME ZONE: store as UTC wall clock
        long millis = ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
        return Long.valueOf(millis);
      } catch (DateTimeParseException e) {
        // Continue trying other formats
      }
    }

    LOGGER.warn("Failed to parse timestamp: '{}' - returning null", value);
    return null;
  }

  private Long parseTimestampWithTimezone(String value, @Nullable DateTimeFormatter inferredFormatter) {
    LOGGER.debug("parseTimestampWithTimezone called with value='{}'", value);

    if (inferredFormatter != null) {
      try {
        OffsetDateTime odt = OffsetDateTime.parse(value, inferredFormatter);
        long utcMillis = odt.toInstant().toEpochMilli();
        LOGGER.debug("=== TIMESTAMPTZ DEBUG: Parsed '{}' as OffsetDateTime using inferred formatter, UTC millis: {} ===",
            value, utcMillis);
        return Long.valueOf(utcMillis);
      } catch (DateTimeParseException e) {
        LOGGER.debug("Failed to parse '{}' as OffsetDateTime with inferred formatter: {}", value, e.getMessage());
        try {
          ZonedDateTime zdt = ZonedDateTime.parse(value, inferredFormatter);
          long utcMillis = zdt.toInstant().toEpochMilli();
          LOGGER.debug("=== TIMESTAMPTZ DEBUG: Parsed '{}' as ZonedDateTime using inferred formatter, UTC millis: {} ===",
              value, utcMillis);
          return Long.valueOf(utcMillis);
        } catch (DateTimeParseException e2) {
          LOGGER.debug("Failed to parse '{}' as ZonedDateTime with inferred formatter: {}", value, e2.getMessage());
        }
      }
    }

    // Timezone-aware formatters for TIMESTAMP_WITH_LOCAL_TIME_ZONE
    DateTimeFormatter[] TIMEZONE_AWARE_FORMATTERS = {
      DateTimeFormatter.ISO_OFFSET_DATE_TIME,  // 2024-03-15T10:30:45Z, 2024-03-15T10:30:45+05:30
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssX"),     // 2024-03-15 10:30:45Z, +05:30
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX"),   // 2024-03-15T10:30:45Z, +05:30
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX"),   // 2024-03-15 10:30:45+05:30
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX"), // 2024-03-15T10:30:45+05:30
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss'Z'"),   // 2024-03-15 10:30:45Z (literal Z = UTC)
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"), // 2024-03-15T10:30:45Z (literal Z = UTC)
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z"),    // 2024-03-15 10:30:45 EST
      DateTimeFormatter.RFC_1123_DATE_TIME,  // Fri, 15 Mar 2024 10:30:45 +0000
      DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss Z")  // RFC 2822 format
    };

    // First try parsing with timezone information
    for (int i = 0; i < TIMEZONE_AWARE_FORMATTERS.length; i++) {
      DateTimeFormatter formatter = TIMEZONE_AWARE_FORMATTERS[i];
      try {
        // Handle literal 'Z' patterns specially
        if ((i == 5 || i == 6) && value.endsWith("Z")) { // These are the literal 'Z' formatters
          // Parse the timestamp part without the 'Z'
          String timestampPart = value.substring(0, value.length() - 1);
          String basePattern = i == 5 ? "yyyy-MM-dd HH:mm:ss" : "yyyy-MM-dd'T'HH:mm:ss";

          // Try with milliseconds first, then without
          String[] patterns = {
            basePattern + ".SSS",  // With milliseconds
            basePattern           // Without milliseconds
          };

          for (String pattern : patterns) {
            try {
              LocalDateTime ldt = LocalDateTime.parse(timestampPart, DateTimeFormatter.ofPattern(pattern));
              OffsetDateTime odt = ldt.atOffset(ZoneOffset.UTC);
              long utcMillis = odt.toInstant().toEpochMilli();
              LOGGER.debug("=== TIMESTAMPTZ DEBUG: SUCCESS! Parsed literal 'Z' as UTC: '{}' -> {} ===", value, utcMillis);
              return Long.valueOf(utcMillis);
            } catch (DateTimeParseException e) {
              // Try next pattern
            }
          }
        }

        // Try parsing as OffsetDateTime first
        OffsetDateTime odt = OffsetDateTime.parse(value, formatter);
        long utcMillis = odt.toInstant().toEpochMilli();
        LOGGER.debug("=== TIMESTAMPTZ DEBUG: SUCCESS! Parsed '{}' as OffsetDateTime, UTC millis: {} ===",
            value, utcMillis);
        return Long.valueOf(utcMillis);
      // fallback-guard: allow the shown return only fires on a nested ZonedDateTime parse's genuine success; a further failure falls through to the next formatter
      } catch (DateTimeParseException e) {
        LOGGER.debug("Failed to parse as OffsetDateTime '{}' with formatter: {}", value, e.getMessage());
        try {
          // Try parsing as ZonedDateTime for timezone abbreviations
          ZonedDateTime zdt = ZonedDateTime.parse(value, formatter);
          long utcMillis = zdt.toInstant().toEpochMilli();
          LOGGER.debug("=== TIMESTAMPTZ DEBUG: SUCCESS! Parsed '{}' as ZonedDateTime, UTC millis: {} ===",
              value, utcMillis);
          return Long.valueOf(utcMillis);
        } catch (DateTimeParseException e2) {
          LOGGER.debug("Failed to parse as ZonedDateTime '{}' with formatter: {}", value, e2.getMessage());
        }
      }
    }

    // If no timezone parsing worked, log warning and fall back to wall clock parsing
    LOGGER.warn("=== TIMESTAMPTZ DEBUG: Failed to parse timezone from '{}', falling back to wall clock time ===", value);
    return parseTimestamp(value, inferredFormatter);
  }
}
