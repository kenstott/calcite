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
package org.apache.calcite.adapter.govdata.disasters;

import com.fasterxml.jackson.databind.JsonNode;

import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts Esri ArcGIS JSON polygon geometry ({@code {"rings": [[[x,y],...],...]}}) into a
 * simplified WGS84 WKT string, reusing the project's JTS stack (the same
 * {@link org.locationtech.jts.geom.GeometryFactory} /
 * {@link TopologyPreservingSimplifier} approach as
 * {@code org.apache.calcite.adapter.govdata.geo.TigerShapefileParser}).
 *
 * <p>Esri ring orientation convention: exterior rings are clockwise, interior rings (holes)
 * are counter-clockwise. Each hole is attached to the exterior shell that contains its first
 * vertex; multiple exterior shells produce a {@code MULTIPOLYGON}. Rings with fewer than four
 * coordinates (an unclosed or degenerate ring) are skipped.
 *
 * <p>Perimeters over {@value #SIMPLIFY_POINT_THRESHOLD} vertices are simplified with a
 * topology-preserving tolerance of {@value #SIMPLIFY_TOLERANCE_DEG} degrees (~100 m at the
 * equator) so the stored WKT stays compact for display and coarse spatial joins.
 */
final class EsriGeometryConverter {

  /** Shared factory; SRID 4326 (WGS84) — WFIGS is queried with {@code outSR=4326}. */
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory(
      new org.locationtech.jts.geom.PrecisionModel(), 4326);

  private static final double SIMPLIFY_TOLERANCE_DEG = 0.001;
  private static final int SIMPLIFY_POINT_THRESHOLD = 1000;

  private EsriGeometryConverter() {
  }

  /** Immutable result of a conversion: WKT plus the polygon centroid. */
  static final class Result {
    final String wkt;
    final Double centroidLat;
    final Double centroidLon;

    Result(String wkt, Double centroidLat, Double centroidLon) {
      this.wkt = wkt;
      this.centroidLat = centroidLat;
      this.centroidLon = centroidLon;
    }
  }

  /**
   * Converts an Esri geometry node into simplified WKT and its centroid.
   *
   * @param geometry the {@code feature.geometry} node (expects a {@code rings} array)
   * @return the conversion result, or {@code null} if the node carries no usable rings
   */
  static Result convert(JsonNode geometry) {
    if (geometry == null || geometry.isMissingNode() || geometry.isNull()) {
      return null;
    }
    JsonNode rings = geometry.path("rings");
    if (!rings.isArray() || rings.size() == 0) {
      return null;
    }

    List<LinearRing> shells = new ArrayList<LinearRing>();
    List<LinearRing> holes = new ArrayList<LinearRing>();
    for (JsonNode ring : rings) {
      Coordinate[] coords = toCoordinates(ring);
      if (coords == null) {
        continue;
      }
      LinearRing linearRing = GEOMETRY_FACTORY.createLinearRing(coords);
      // Esri: clockwise exterior (not CCW), counter-clockwise interior (CCW).
      if (Orientation.isCCW(coords)) {
        holes.add(linearRing);
      } else {
        shells.add(linearRing);
      }
    }
    if (shells.isEmpty()) {
      // No exterior ring recognized (e.g. all rings CCW); treat every ring as its own shell.
      for (JsonNode ring : rings) {
        Coordinate[] coords = toCoordinates(ring);
        if (coords != null) {
          shells.add(GEOMETRY_FACTORY.createLinearRing(coords));
        }
      }
    }
    if (shells.isEmpty()) {
      return null;
    }

    List<Polygon> polygons = new ArrayList<Polygon>();
    for (LinearRing shell : shells) {
      Polygon shellPolygon = GEOMETRY_FACTORY.createPolygon(shell);
      List<LinearRing> owned = new ArrayList<LinearRing>();
      for (LinearRing hole : holes) {
        if (shellPolygon.contains(GEOMETRY_FACTORY.createPoint(hole.getCoordinateN(0)))) {
          owned.add(hole);
        }
      }
      polygons.add(GEOMETRY_FACTORY.createPolygon(shell,
          owned.toArray(new LinearRing[0])));
    }

    Geometry geom = polygons.size() == 1
        ? polygons.get(0)
        : GEOMETRY_FACTORY.createMultiPolygon(polygons.toArray(new Polygon[0]));

    if (geom.getNumPoints() > SIMPLIFY_POINT_THRESHOLD) {
      Geometry simplified = TopologyPreservingSimplifier.simplify(geom, SIMPLIFY_TOLERANCE_DEG);
      if (simplified != null && !simplified.isEmpty()) {
        geom = simplified;
      }
    }

    Point centroid = geom.getCentroid();
    Double lat = null;
    Double lon = null;
    if (centroid != null && !centroid.isEmpty()) {
      lon = centroid.getX();
      lat = centroid.getY();
    }
    return new Result(geom.toText(), lat, lon);
  }

  /** Parses one Esri ring ({@code [[x,y],...]}) into a closed JTS coordinate array. */
  private static Coordinate[] toCoordinates(JsonNode ring) {
    if (!ring.isArray() || ring.size() < 3) {
      return null;
    }
    List<Coordinate> coords = new ArrayList<Coordinate>(ring.size() + 1);
    for (JsonNode point : ring) {
      if (!point.isArray() || point.size() < 2) {
        continue;
      }
      coords.add(new Coordinate(point.get(0).asDouble(), point.get(1).asDouble()));
    }
    if (coords.size() < 3) {
      return null;
    }
    // Close the ring if the source did not repeat the first vertex.
    Coordinate first = coords.get(0);
    Coordinate last = coords.get(coords.size() - 1);
    if (!first.equals2D(last)) {
      coords.add(new Coordinate(first.x, first.y));
    }
    if (coords.size() < 4) {
      return null;
    }
    return coords.toArray(new Coordinate[0]);
  }
}
