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
package org.apache.calcite.adapter.askamerica;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A chart's {@code value_format}, used for bar value labels, value-axis ticks and default
 * tooltips.
 *
 * <p>Accepts either a Java {@link DecimalFormat} pattern ({@code $#,##0}, {@code 0.0'%'}) or the
 * d3-style subset a chart caller more often reaches for: an optional {@code +} sign, an optional
 * {@code $}, an optional {@code ,} digit grouping, an optional {@code .N} precision and a type of
 * {@code f} (fixed) or {@code %} (percent, value times 100) — {@code $,.0f}, {@code +.1%}. The
 * d3 form is translated to the equivalent DecimalFormat pattern, so one formatter serves both.
 *
 * <p>An unrecognised spec is rejected rather than approximated, so a caller that wrote a format
 * this class does not implement learns that instead of getting unformatted numbers.
 */
final class ValueFormat {

    private static final Pattern D3 =
        Pattern.compile("^(\\+)?(\\$)?(,)?(?:\\.(\\d{1,2}))?([f%])?$");

    private final String pattern;

    private ValueFormat(String pattern) {
        this.pattern = pattern;
    }

    /** Parses a spec; {@code null} or empty means the caller asked for no format. */
    static ValueFormat parse(String spec) {
        if (spec == null || spec.isEmpty()) {
            return null;
        }
        Matcher m = D3.matcher(spec);
        String pattern = m.matches() ? fromD3(m) : spec;
        try {
            decimalFormat(pattern);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("value_format '" + spec
                + "' is not a valid number format — use a Java DecimalFormat pattern such as "
                + "\"$#,##0\" or \"0.0'%'\", or a d3-style one such as \"$,.0f\" or \"+.1%\": "
                + e.getMessage(), e);
        }
        return new ValueFormat(pattern);
    }

    private static String fromD3(Matcher m) {
        boolean percent = "%".equals(m.group(5));
        int precision = m.group(4) != null ? Integer.parseInt(m.group(4)) : (percent ? 0 : 2);
        StringBuilder body = new StringBuilder();
        if (m.group(2) != null) {
            body.append('$');
        }
        body.append(m.group(3) != null ? "#,##0" : "0");
        if (precision > 0) {
            body.append('.');
            for (int i = 0; i < precision; i++) {
                body.append('0');
            }
        }
        if (percent) {
            body.append('%');
        }
        return m.group(1) != null ? "+" + body + ";-" + body : body.toString();
    }

    private static DecimalFormat decimalFormat(String pattern) {
        DecimalFormat d = new DecimalFormat(pattern, DecimalFormatSymbols.getInstance(Locale.US));
        d.setRoundingMode(RoundingMode.HALF_UP);
        return d;
    }

    String format(double v) {
        String s = decimalFormat(pattern).format(v);
        // A value that rounds to zero is written without the sign of the tiny number it was.
        return v < 0 && isZero(s) ? decimalFormat(pattern).format(0.0) : s;
    }

    private static boolean isZero(String formatted) {
        for (int i = 0; i < formatted.length(); i++) {
            char c = formatted.charAt(i);
            if (c >= '1' && c <= '9') {
                return false;
            }
        }
        return true;
    }

    /** A value when the caller gave no {@code value_format}. */
    static String plain(double v) {
        return new DecimalFormat(Math.abs(v) < 1 && v != 0 ? "0.###" : "#,##0.##",
            DecimalFormatSymbols.getInstance(Locale.US)).format(v);
    }

    static String render(ValueFormat fmt, double v) {
        return fmt == null ? plain(v) : fmt.format(v);
    }
}
