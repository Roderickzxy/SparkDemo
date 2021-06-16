package com.rode.scala.spark.practise

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext

/**
  *
  * @author zhengxinyue
  * @since 2020/10/19
  */
object Practise2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("practise1").setMaster("local")
    val sc = new JavaSparkContext(conf)
    val lines = sc.parallelize(List(("spark", 2), ("hadoop", 6), ("hadoop", 2), ("spark", 6), ("hive", 200), ("hbase", 1)))
    val counts = lines.reduceByKey((v1, v2) => (v1 + v2) / 2)

    counts.sortBy(v=>v._2).collect.foreach(println)
  }
}
