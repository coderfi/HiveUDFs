package com.coderfi.spark;

import net.petrabarus.hiveudfs.helpers.KeywordParser;
import org.apache.spark.sql.api.java.UDF1;

/**
 * Spark UDF wrapper around net.petrabarus.hiveudfs.SearchEngineKeyword.
 * 
 * This is a UDF to get keyword part from a search engine referrer URL.
 *
 * The function will need one argument which is the URL. If the URL is not
 * recognized as a search engine URL it will return null.
 *
 * Currently it only support simple URL from Google, Yahoo, and Bing. More to
 * come.
 *
 * @author Petra Barus <petra.barus@gmail.com>
 */
public class SearchEngineKeyword extends net.petrabarus.hiveudfs.SearchEngineKeyword implements UDF1<String, String> {
        @Override
        public String call(String referer) {
                KeywordParser kp = new KeywordParser(referer);
                if (!kp.hasKeyword) {
                        return null;
                } else {
                        return kp.getKeyword();
                }
        }
}
