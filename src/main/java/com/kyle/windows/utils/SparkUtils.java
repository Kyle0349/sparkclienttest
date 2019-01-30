package com.kyle.windows.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkUtils {

    public static JavaSparkContext getJscOnYarn(){
        SparkConf sparkConf = new SparkConf()
                .set("spark.yarn.historyServer.address","http://cdh01:18089")
                .set("spark.eventLog.dir","hdfs://cdh01:8020/user/spark/spark2ApplicationHistory")
                .set("spark.eventLog.enabled","true")
                .set("yarn.resourcemanager.hostname", "192.168.171.101")
                .set("spark.driver.memory", "1g")
                .set("spark.executor.memory", "1g")
                .set("spark.executor.instances", "1")
                .set("spark.default.parallelism", "1")
                .set("spark.driver.host", "192.168.171.1")
                .set("spark.yarn.jars","/opt/cloudera/parcels/SPARK2/lib/spark2/jars/*")
                .set("spark.driver.extraClassPath","/opt/cloudera/parcels/SPARK2/lib/spark2/jars/*:/opt/cloudera/parcels/CDH/jars/*")
                .set("spark.executor.extraClassPath","/opt/cloudera/parcels/SPARK2/lib/spark2/jars/*:/opt/cloudera/parcels/CDH/jars/*")
                .setAppName("spatk230Test")
                .setMaster("yarn");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        jsc.setLogLevel("WARN");
        return jsc;
    }

}
