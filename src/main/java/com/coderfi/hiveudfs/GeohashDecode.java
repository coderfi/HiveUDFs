package com.coderfi.hiveudfs;

import static java.util.Arrays.asList;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import com.github.davidmoten.geo.GeoHash;
import com.github.davidmoten.geo.LatLong;


/**
 * Wrapper around https://github.com/davidmoten/geo GeoHash.decodeHash(hash)
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
@UDFType(deterministic = true)
@Description(
        name = "geohashDecode",
value = "_FUNC_(geohash) - returns the latitude and longitude of the geohash as two columns: glatitude, glongitude",
extended = "Example:\n"
+ " > SELECT _FUNC_('9q8yyx795ryt');"
+ " > -- glatitude,glongitude"
+ " > -- 37.789063,-122.404713")
public class GeohashDecode extends GenericUDTF implements UDF1<String, List<Double>> {

        private static StructType LatLonSchema;
        private transient Converter inputGeohash;
        private transient Object[] outputLatLon;

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

        @Override
        public StructObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

                if (arguments.length != 1) {
                        throw new UDFArgumentLengthException("_FUNC_ expects exactly 1 argument.");
                }

                ObjectInspector argument;

                argument = arguments[0];
                if (argument.getCategory() != Category.PRIMITIVE ||
                    ((PrimitiveObjectInspector) argument).getPrimitiveCategory() != PrimitiveCategory.STRING) { 
                        throw new UDFArgumentTypeException(0, "Expected String, got " + argument.getTypeName());
                } else {
                    inputGeohash = ObjectInspectorConverters.getConverter(argument, (PrimitiveObjectInspector) argument);
                }

                final List<ObjectInspector> fields = new ArrayList<ObjectInspector>();
                fields.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.DOUBLE));
                fields.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.DOUBLE));
                return ObjectInspectorFactory.getStandardStructObjectInspector(
                    asList("glatitude", "glongitude"),
                    fields);
        }

        @Override
        public void process(Object[] arguments) throws HiveException {
                outputLatLon = new Object[2];
                final Object obj = inputGeohash.convert(arguments[0]);
                final String geohash;
                if (obj instanceof String) {
                    geohash = (String)obj;  // we seem to get String with geohashDecode(geohashEncode(37.1, -121.2))
                } else if (obj != null) {
                    geohash = ((Text)obj).toString();
                } else {
                    geohash = null;
                }

                if (geohash == null) {
                    outputLatLon[0] = null;
                    outputLatLon[1] = null;
                } else {
                    final LatLong struct = GeoHash.decodeHash(geohash);

                    outputLatLon[0] = struct.getLat();
                    outputLatLon[1] = struct.getLon();
                }
                forward(outputLatLon);
        }

        @Override
        public void close() throws HiveException {
        }
}
