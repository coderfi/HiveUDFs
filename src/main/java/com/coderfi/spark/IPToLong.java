package com.coderfi.spark;

import net.petrabarus.hiveudfs.helpers.InetAddrHelper;
import org.apache.spark.sql.api.java.UDF1;

/**
 * Spark UDF wrapper around net.petrabarus.hiveudfs.IPToLong.
 * 
 * IPToLong is a basic UDF to translate IP in string format to long format.
 *
 * Usage:
 * <pre>
 *      SELECT IPToLong(ipstring) FROM table;
 * </pre>
 *
 * @author Petra Barus <petra.barus@gmail.com>
 * @see http://muhmahmed.blogspot.com/2009/02/java-ip-address-to-long.html
 */
public class IPToLong extends net.petrabarus.hiveudfs.IPToLong implements UDF1<String, Long> {
        @Override
        public Long call(String ip) {
                if (ip == null) {
                        return (long)0;
                } else {
                        return InetAddrHelper.IPToLong(ip);
                }
        }
}
