package com.rode.scala.spark.sql

import org.apache.spark.sql.SparkSession

object ParseNginxLogMain {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ParseNginxLog")
      .master("local")
      .getOrCreate()

    val nginxLogDf = spark.read.text("/home/roderick/IdeaProjects/SparkDemo/src/main/resources/input/nginx.log")

    nginxLogDf.show()
  }
}
