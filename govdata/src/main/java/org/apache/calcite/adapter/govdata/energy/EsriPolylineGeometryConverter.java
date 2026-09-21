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
package org.apache.calcite.adapter.govdata.energy;

import com.fasterxml.jackson.databind.JsonNode;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts Esri ArcGIS JSON polyline geometry ({@code {"paths": [[[x,y],...],...]}}) into a
 * WGS84 WKT string. Same JTS-based approach as
 * {@code org.apache.calcite.adapter.govdata.disasters.EsriGeometryConverter} (rings/polygons) and
 * {@code org.apache.calcite.adapter.govdata.lands.PadusGeometryConverter}, duplicated here rather
 * than shared across schema packages per the project's per-schema geometry-helper convention.
 *
 * <p>Each {@code paths} entry is one continuous line; a single path produces a
 * {@code LINESTRING}, multiple paths a {@code MULTILINESTRING}. A path with fewer than two
 * coordinates (a degenerate point) is skipped. A feature can legitimately carry no geometry at
 * all (confirmed live against
 * {@code Natural_Gas_Interstate_and_Intrastate_Pipelines_1}: a small fraction of rows, ~0.2% in a
 * sample, have no {@code geometry} node) — those convert to {@code null} rather than throwing.
 */
final class EsriPolylineGeometryConverter {

  /** Shared factory; SRID 4326 (WGS84) — the source is queried with {@code outSR=4326}. */
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory(
      new org.locationtech.jts.geom.PrecisionModel(), 4326);

  private EsriPolylineGeometryConverter() {
  }

  /**
   * Converts an Esri geometry node into WKT.
   *
   * @param geometry the {@code feature.geometry} node (expects a {@code paths} array)
   * @return the WKT string, or {@code null} if the node carries no usable path
   */
  static String convert(JsonNode geometry) {
    if (geometry == null || geometry.isMissingNode() || geometry.isNull()) {
      return null;
    }
    JsonNode paths = geometry.path("paths");
    if (!paths.isArray() || paths.size() == 0) {
      return null;
    }

    List<LineString> lines = new ArrayList<LineString>();
    for (JsonNode path : paths) {
      Coordinate[] coords = toCoordinates(path);
      if (coords != null) {
        lines.add(GEOMETRY_FACTORY.createLineString(coords));
      }
    }
    if (lines.isEmpty()) {
      return null;
    }

    Geometry geom = lines.size() == 1
        ? lines.get(0)
        : GEOMETRY_FACTORY.createMultiLineString(lines.toArray(new LineString[0]));
    return geom.toText();
  }

  /** Parses one Esri path ({@code [[x,y],...]}) into a JTS coordinate array. */
  private static Coordinate[] toCoordinates(JsonNode path) {
    if (!path.isArray() || path.size() < 2) {
      return null;
    }
    List<Coordinate> coords = new ArrayList<Coordinate>(path.size());
    for (JsonNode point : path) {
      if (!point.isArray() || point.size() < 2) {
        continue;
      }
      coords.add(new Coordinate(point.get(0).asDouble(), point.get(1).asDouble()));
    }
    if (coords.size() < 2) {
      return null;
    }
    return coords.toArray(new Coordinate[0]);
  }
}
