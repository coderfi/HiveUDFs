package com.coderfi.spark;

import static java.util.Arrays.asList;
import java.util.ArrayList;
import java.util.List;

import com.github.davidmoten.geo.GeoHash;
import com.github.davidmoten.geo.LatLong;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


/**
 * Spark UDF wrapper around com.coderfi.GeohashDecode
 *
 * Usage:
 * <pre>
 *      SELECT geohashDecode("9q8yyx795ryt");
 *      -- glatitude,glongitude
 *      -- 37.789063,-122.404713
 * </pre>
 *
 * Naturally, don't expect your 'original' latitude and longitude.
 * The output of this UDF can only be as good as the encoded geohash precision.
 *
 * @see https://github.com/davidmoten/geo GeoHash.decodeHash(geohash)
 */
public class GeohashDecode extends com.coderfi.hive.GeohashDecode implements UDF1<String, List<Double>> {

        private static StructType LatLonSchema;

        static {
            List<StructField> inputFields = new ArrayList<>();
            inputFields.add(DataTypes.createStructField("glatitude", DataTypes.DoubleType, false));
            inputFields.add(DataTypes.createStructField("glongitude", DataTypes.DoubleType, false));
            GeohashDecode.LatLonSchema = DataTypes.createStructType(inputFields);
        }

        @Override
        public List<Double> call(String geohash) {
                final LatLong ll = GeoHash.decodeHash(geohash);
                return asList(ll.getLat(), ll.getLon());
        }
}
