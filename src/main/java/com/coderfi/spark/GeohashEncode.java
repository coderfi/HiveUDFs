package com.coderfi.spark;

import com.github.davidmoten.geo.GeoHash;

import org.apache.spark.sql.api.java.UDF3;

/**
 * Spark UDF wrapper around com.coderfi.GeohashEncode
 *
 * Usage:
 * <pre>
 *      SELECT geohashEncode(37.789063, -122.404713, 12);
 *      -- 9q8yyx795ryt
 * </pre>
 *
 * @see https://github.com/davidmoten/geo GeoHash.encodeHash(lat, lon, length)
 */
public class GeohashEncode extends com.coderfi.hive.GeohashEncode implements UDF3<Double, Double, Integer, String> {
        @Override
        public String call(Double latitude, Double longitude, Integer precision) {
                return this.makeGeoHash(latitude, longitude, precision);
        }
}
