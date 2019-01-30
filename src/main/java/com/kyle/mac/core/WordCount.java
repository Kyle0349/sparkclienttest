package com.kyle.mac.core;

import com.kyle.mac.utils.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCount {

    public static void main(String[] args) {
        String jarFile = "";
        String filePaht = "";
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
