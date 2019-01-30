package com.kyle.windows.core;

import com.kyle.windows.utils.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCount {

    public static void main(String[] args) {
        String jarFile = "E:\\123\\sparkclienttest\\out\\artifacts\\spark_client_test\\spark-client-test.jar";
        String filePaht = "hdfs://cdh01:8020/tmp/access_2013_05_31.log";
        System.setProperty("HADOOP_USER_NAME","root");
        JavaSparkContext jsc = SparkUtils.getJscOnYarn();
        jsc.addJar(jarFile);
        JavaRDD<String> linesRdd = jsc.textFile(filePaht);
        JavaPairRDD<String, Integer> res = linesRdd
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((v1, v2) -> v1 + v2);
        List<Tuple2<String, Integer>> collect = res.collect();
        System.out.println(collect);

    }

}
