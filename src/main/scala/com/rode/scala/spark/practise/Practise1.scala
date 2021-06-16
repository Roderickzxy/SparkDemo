package com.rode.scala.spark.practise

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author zhengxinyue
  * @since 2020/10/17
  */
object Practise1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("practise").setMaster("local")
    val sc = new SparkContext(conf)

    val lines1 = sc.textFile("C:\\workspace-mine\\SparkDemo\\src\\main\\resources\\practise\\practise1\\file1-1.txt")
    val lines2 = sc.textFile("C:\\workspace-mine\\SparkDemo\\src\\main\\resources\\practise\\practise1\\file2-1.txt")
    val lines3 = sc.textFile("C:\\workspace-mine\\SparkDemo\\src\\main\\resources\\practise\\practise1\\file3-1.txt")
    val linesAll = lines1.union(lines2).union(lines3)

    //假如是想按首字母排序，不需要做Integer::valueOf的类型转换
    val numbers = linesAll.map(Integer.valueOf).sortBy((x: Integer) => x, ascending = true, 1).repartition(1)
    numbers.foreach(item=>println(item))
  }
}
