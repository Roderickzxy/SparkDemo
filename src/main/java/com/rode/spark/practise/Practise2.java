package com.rode.spark.practise;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * 题目2：
 * 给定键值对，("spark",2),("hadoop",6),("hadoop",4),("spark",6)，键值对的key，
 * 表示图书名称，value表示某天图书销量，请计算每个键对应的平均值，也就是计算每种图书的每天平均销量
 * 最后再根据销量做排序
 * <p>
 * 思路：先将键值对转为pairsRDD，然后根据key将结果sum起来再/2, 再做排序
 * @author zhengxinyue
 * @since 2020/10/18
 */
public class Practise2 {
    
    public static void main(String[] args) {
        practise1();
    }
    
    private static void practise1() {
        SparkConf conf = new SparkConf().setAppName("practise1").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Tuple2<String, Integer>> lines = sc.parallelize(
                Arrays.asList(new Tuple2<>("spark", 2), new Tuple2<>("hadoop", 6), new Tuple2<>("hadoop", 2),
                        new Tuple2<>("spark", 6), new Tuple2<>("hive", 200), new Tuple2<>("hbase", 1)));
        JavaPairRDD<String, Integer> pairs = JavaPairRDD.fromJavaRDD(lines);
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((v1, v2) -> (v1 + v2) / 2);
    
        JavaPairRDD<Integer, String> sortPairRdd = counts.mapToPair(v1 -> new Tuple2<>(v1._2, v1._1)).sortByKey();
        sortPairRdd.mapToPair(v1 -> new Tuple2<>(v1._2, v1._1)).collect().forEach(System.out::println);
    }
}
