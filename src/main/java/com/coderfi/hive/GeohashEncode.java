package com.coderfi.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import com.github.davidmoten.geo.GeoHash;

/**
 * Wrapper around https://github.com/davidmoten/geo GeoHash.encodeHash(lat, lon, length)
 *
 * Usage:
 * <pre>
 *      SELECT geohashEncode(37.789063, -122.404713, 12);
 *      -- 9q8yyx795ryt
 * </pre>
 *
 * @see https://github.com/davidmoten/geo GeoHash.encodeHash(lat, lon, length)
 */
@UDFType(deterministic = true)
@Description(
        name = "geohashEncode",
value = "_FUNC_(lat, lon, length) - returns the geohash with the specified precision",
extended = "Example:\n"
+ " > SELECT _FUNC_(37.789063, -122.404713, 12) FROM table"
+ " > 9q8yyx795ryt")
public class GeohashEncode extends GenericUDF {

        private transient PrimitiveObjectInspector inputLat;
        private transient PrimitiveObjectInspector inputLon;
        private int precision;
        private PrimitiveObjectInspector outputGeohash;

        @Override
        public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
                if (arguments.length != 3) {
                        throw new UDFArgumentLengthException("_FUNC_ expects exactly 3 arguments.");
                }

                ObjectInspector argument;

                argument = arguments[0];
                if (argument.getCategory() != Category.PRIMITIVE || 
                    ((PrimitiveObjectInspector) argument).getPrimitiveCategory() != PrimitiveCategory.DOUBLE) { 
                        throw new UDFArgumentTypeException(0, "Expected Double, got " + argument.getTypeName());
                } else {
                    inputLat = (PrimitiveObjectInspector)argument;
                }

                argument = arguments[1];
                if (argument.getCategory() != Category.PRIMITIVE ||  
                    ((PrimitiveObjectInspector) argument).getPrimitiveCategory() != PrimitiveCategory.DOUBLE) { 
                        throw new UDFArgumentTypeException(1, "Expected Double, got " + argument.getTypeName());
                } else {
                    inputLon = (PrimitiveObjectInspector)argument;
                }  

                precision = Integer.valueOf(((ConstantObjectInspector)arguments[2]).getWritableConstantValue().toString());

                outputGeohash = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.STRING);
                return outputGeohash;
        }

        @Override
        public Object evaluate(DeferredObject[] arguments) throws HiveException {
                Object olat = arguments[0].get();
                Object olon = arguments[1].get();

                if (olat == null || olon == null) {
                    return null;
                }

                final double lat = PrimitiveObjectInspectorUtils.getDouble(olat, inputLat);
                final double lon = PrimitiveObjectInspectorUtils.getDouble(olon, inputLon);

                final String s = this.makeGeoHash(lat, lon, precision);
                if (s == null) {
                    return null;
                } else {
                    return new Text(s);
                }
        }

        @Override
        public String getDisplayString(String[] strings) {
                assert (strings.length == 1);
                return "_FUNC_(" + strings[0] + ")";
        }
        
        protected String makeGeoHash(Double latitude, Double longitude, Integer precision) {
                if (latitude < -90 || latitude > 90) {
                    return null;
                }
                return GeoHash.encodeHash(latitude, longitude, precision);
        }
        
}
