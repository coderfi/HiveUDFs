HiveUDFs
========

# Credits

Based on

* [petrabarus/HiveUDFS](https://github.com/petrabarus/HiveUDFs)

* [Spuul/hive-udfs](https://github.com/Spuul/hive-udfs)

# 

My Personal Collection of Hive UDFs

The UDFs extends the [Hive GenericUDF](https://hive.apache.org/javadocs/r2.1.1/api/org/apache/hadoop/hive/ql/udf/generic/GenericUDF.html)
base class and
implements the appropriate [org.apache.spark.sql.api.java](https://spark.apache.org/docs/1.4.0/api/java/org/apache/spark/sql/api/java/package-summary.html).

See [SnappyData CREATE FUNCTION reference](http://snappydatainc.github.io/snappydata/reference/sql_reference/create-function/)

## Compiling

This is a Maven project. To compile it

    mvn install

## Function Lists

Below are function list that is currently in this project. More to come!

### LongToIP
**LongToIP** translates IP in long format to string format.

Usage:

    ADD JAR HiveUDFs.jar;
    CREATE TEMPORARY FUNCTION longtoip as 'net.petrabarus.hiveudfs.LongToIP';
    SELECT longtopip(2130706433) FROM table;

### IPToLong

**IPToLong** translates IP in string format to long format.

Usage:

    ADD JAR HiveUDFs.jar;
    CREATE TEMPORARY FUNCTION iptolong as 'net.petrabarus.hiveudfs.IPToLong';
    SELECT iptolong("127.0.0.1") FROM table;

### GeoIP

**GeoIP** wraps MaxMind GeoIP function for Hive. 
This is a derivation from @edwardcapriolo [hive-geoip](http://github.com/edwardcapriolo/hive-geoip).
Separate GeoIP database will be needed to run the function.
The GeoIP will need three argument.

1. IP address in long
2. IP attribute (e.g. COUNTRY, CITY, REGION, etc. See full list in the javadoc.)
3. Database file name

A lite version of the MaxMind GeoIP can be obtained from [here] (http://dev.maxmind.com/geoip/geolite).

Usage:

    ADD JAR HiveUDFs.jar;
    ADD FILE /usr/share/GeoIP/GeoIPCity.dat;
    CREATE TEMPORARY FUNCTION geoip as 'net.petrabarus.hiveudfs.GeoIP';
    SELECT GeoIP(cast(ip AS bigint), 'CITY', './GeoIPCity.dat') FROM table;

### SearchEngineKeyword

**SearchEngineKeyword** is a simple function to extract keyword from URL referrer
that comes from Google, Bing, and Yahoo. Need to expand this to cover more search
engines.

Usage

    ADD JAR HiveUDFs.jar
    CREATE TEMPORARY FUNCTION searchenginekeyword as 'net.petrabarus.hiveudfs.SearchEngineKeyword';
    SELECT searchenginekeyword(url) FROM table;

### UCWords

**UCWords** is UDF function equivalent to PHP ucwords().

Usage

    ADD JAR HiveUDFs.jar
    CREATE TEMPORARY FUNCTION ucwords as 'net.petrabarus.hiveudfs.UCWords';
    SELECT ucwords(text) FROM table;

##Copyright and License

MaxMind GeoIP is a trademark of MaxMind LLC.
