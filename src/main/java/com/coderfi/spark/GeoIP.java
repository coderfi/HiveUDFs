package com.coderfi.spark;

import org.apache.spark.sql.api.java.UDF3;

/**
 * Spark UDF wrapper around net.petrabarus.hiveudfs.GeoIP
 *
 * This is a UDF to look a property of an IP address using MaxMind GeoIP2
 * library.
 *
 * The function will need three arguments. <ol> <li>IP Address in string
 * format.</li> <li>IP attribute (e.g. COUNTRY, CITY, REGION, etc)</li>
 * <li>Database file name.</li> </ol>
 *
 * This is a derived version from https://github.com/petrabarus/HiveUDFs.
 * (Please let me know if I need to modify the license)
 *
 * @author Daniel Muller <daniel@spuul.com>
 * @see https://github.com/petrabarus/HiveUDFs
 */
public class GeoIP extends net.petrabarus.hiveudfs.GeoIP implements UDF3<String, String, String, String> {
        @Override
        public String call(String ip, String attributeName, String databaseName) {
                try {
                        return convertIpAddress(ip, attributeName, databaseName);
                }
                catch(Exception e) {
                        return null;
                }
        }
}
