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
package org.apache.calcite.adapter.servicenow;

import org.apache.calcite.adapter.servicenow.ServiceNowColumn.Kind;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Maps a ServiceNow field type (the {@code internal_type} of a {@code sys_dictionary} row) to a
 * {@link Kind}.
 *
 * <p>The table below covers the types ServiceNow documents in its field types reference. A type
 * that is not in it is resolved through {@code sys_glide_object}, which names the scalar type a
 * custom or unfamiliar type is stored as. A type that resolves neither way is an error naming the
 * type; it is never mapped to text silently.
 */
final class GlideTypes {
  private GlideTypes() {}

  private static final Map<String, Kind> KNOWN = new HashMap<>();

  /** Scalar type names as {@code sys_glide_object.scalar_type} spells them, where they differ. */
  private static final Map<String, Kind> SCALAR_ALIASES = new HashMap<>();

  static {
    KNOWN.put("guid", Kind.GUID);
    for (String text : new String[] {"string", "url", "email", "html", "script", "translated_text",
        "conditions", "sys_class_name", "document_id", "domain_id", "glide_list", "journal",
        "journal_input", "journal_list", "password", "password2", "user_image", "currency2",
        "glide_duration"}) {
      KNOWN.put(text, Kind.TEXT);
    }
    KNOWN.put("integer", Kind.INTEGER);
    KNOWN.put("longint", Kind.LONG);
    KNOWN.put("decimal", Kind.DECIMAL);
    KNOWN.put("float", Kind.DOUBLE);
    KNOWN.put("boolean", Kind.BOOLEAN);
    KNOWN.put("glide_date_time", Kind.TIMESTAMP);
    KNOWN.put("due_date", Kind.TIMESTAMP);
    KNOWN.put("glide_date", Kind.DATE);
    KNOWN.put("glide_time", Kind.TIME);
    KNOWN.put("reference", Kind.REFERENCE);
    KNOWN.put("currency", Kind.CURRENCY);
    KNOWN.put("price", Kind.CURRENCY);

    SCALAR_ALIASES.put("datetime", Kind.TIMESTAMP);
    SCALAR_ALIASES.put("date", Kind.DATE);
    SCALAR_ALIASES.put("time", Kind.TIME);
  }

  /** Returns the kind of a documented field type, or null if the type is not in the table. */
  static Kind known(String glideType) {
    return KNOWN.get(glideType.toLowerCase(Locale.ROOT));
  }

  /**
   * Returns the kind for a scalar type name from {@code sys_glide_object}, or null if it is not
   * one the adapter maps.
   */
  static Kind scalar(String scalarType) {
    final String key = scalarType.toLowerCase(Locale.ROOT);
    final Kind alias = SCALAR_ALIASES.get(key);
    return alias != null ? alias : KNOWN.get(key);
  }
}
