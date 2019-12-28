package com.rode.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ParseNginxLogRdd {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("testRDDJob").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> logFile = sc.textFile("/home/roderick/IdeaProjects/SparkDemo/src/main/resources/input/nginx.log");

        logFile.foreach(x->{
            System.out.println(x);
        });
    }
}
