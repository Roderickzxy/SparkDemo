package com.rode.scala.spark.practise

import com.rode.spark.practise.PractiseConstants.CLASS_PATH
import org.apache.spark.{SparkConf, SparkContext}

object Practise4 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("practise4").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val line1 = sc.textFile(CLASS_PATH + "practise/practise4/file1-1.txt")
    val line2 = sc.textFile(CLASS_PATH + "practise/practise4/file1-2.txt")
    val lineAll = line1.union(line2)
    lineAll.map(f=>{(Integer.valueOf(f.split(",")(2)), f)}).sortByKey().take(10).map(x=>x._2).foreach(println)
  }
}
