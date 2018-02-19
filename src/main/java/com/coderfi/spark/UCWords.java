package com.coderfi.spark;

import org.apache.commons.lang.WordUtils;
import org.apache.spark.sql.api.java.UDF1;

/**
 * Spark UDF wrapper around net.petrabarus.hiveudfs.UCWords.
 * 
 * UCWords is equivalent to PHP's ucwords().
 *
 * It's just a wrapper of StringUtils.capitalize();
 *
 * @author Petra Barus <petra.barus@gmail.com>
 */
public class UCWords extends net.petrabarus.hiveudfs.UCWords implements UDF1<String, String> {
        @Override
        public String call(String s) {
                return WordUtils.capitalize(s);
        }
}
