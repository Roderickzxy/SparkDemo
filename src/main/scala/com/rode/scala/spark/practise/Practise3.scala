package com.rode.scala.spark.practise

import com.rode.spark.practise.PractiseConstants.CLASS_PATH
import org.apache.spark.{SparkConf, SparkContext}

object Practise3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("practise3").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(CLASS_PATH + "practise/practise3/file1-1.txt")
    val pairRdd = lines.map(x => x.replaceAll("\\s+"," ").split(" "))
      .map(x => (x(0),x(1)))

    pairRdd.sortByKey().groupByKey().mapValues(x=>x.toList.sortBy(x=>x)).collect().foreach(println)
  }
}
