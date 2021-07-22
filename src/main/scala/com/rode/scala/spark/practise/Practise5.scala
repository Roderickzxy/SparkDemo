package com.rode.scala.spark.practise

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 *  列转行练习
 * @author xinyue.zheng
 */
object Practise5 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Practise5").set("spark.debug.maxToStringFields", "100")
    if(!sparkConf.contains("spark.master")){
      // 没有指定master
      sparkConf.setMaster("local")
    }
    val sparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val schema = StructType(List(
      StructField("userid", StringType, nullable = false),
      StructField("pcPlayDuration", IntegerType, nullable = true),
      StructField("pcVideoView", IntegerType, nullable = true),
      StructField("mobilePlayDuration", IntegerType, nullable = true),
      StructField("mobileVideoView", IntegerType, nullable = true)
    ))
    val rdd = sparkSession.sparkContext.parallelize(Seq(
      Row("test1", 100, 200, 300, 400),
      Row("test2", 300, 400, 500, 600)
    ))
    val df = sparkSession.createDataFrame(rdd, schema)
    df.show()

    val unpivot_test =df.selectExpr("`userid`",
      "stack(4, '1', `pcPlayDuration`,'2', `pcVideoView`, '3', `mobilePlayDuration`, '4', `mobileVideoView`) as (`metric`,`value`)")

    unpivot_test.show()
  }
}
