package com.coderfi.spark;

import net.petrabarus.hiveudfs.helpers.InetAddrHelper;
import org.apache.spark.sql.api.java.UDF1;

/**
 * Spark UDF wrapper around net.petrabarus.hiveudfs.LongToIP.
 * 
 * LongToIP is a basic UDF to translate IP in long format to string format.
 *
 * Usage:
 * <pre>
 *      SELECT LongToIP(cast(iplong AS bigint)) FROM table;
 * </pre>
 *
 * @author Petra Barus <petra.barus@gmail.com>
 * @see http://muhmahmed.blogspot.com/2009/02/java-ip-address-to-long.html
 */
public class LongToIP extends net.petrabarus.hiveudfs.LongToIP implements UDF1<Long, String> {
        @Override
        public String call(Long iplong) {
                if (iplong == null) {
                        return null;
                } else {
                        return InetAddrHelper.longToIP(iplong);
                }
        }
}
