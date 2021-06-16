package com.rode.scala.spark.sql

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, countDistinct, date_format, lit, sum, weekofyear, year}

object ReadMysqlMain {
  def main(args:Array[String]):Unit={
    val conf=new SparkConf().setMaster("local").setAppName("SparkReadMysql")
    val sparkSession=SparkSession
      .builder()
      .config(conf)//设置操作hive的url，相当于jdbc里的url
      .getOrCreate()
    val prop=scala.collection.mutable.Map[String,String]()
    val url = "jdbc:mysql://url/polyv_analytics?characterEncoding=utf8&serverTimezone=UTC"
    val user = "user"
    val password = "password"
    val driver = "com.mysql.jdbc.Driver"
    prop.put("user",user)
    prop.put("password",password)
    prop.put("driver",driver)
    prop.put("dbtable","viewlog_200006")
    prop.put("url",url)
    //从数据库中加载整个表的数据
    val df=sparkSession.read.format("jdbc").options(prop).load()
    //读出来之后注册为临时表
    df.createOrReplaceTempView("viewlog_200006")
    //注册好之后就可以通过sql语句查询了
    val viewlog = sparkSession.sql(
      "select userId,currentDay,currentHour, " +
        "(IF(isMobile='N', playDuration, 0)) AS pcPlayDuration,"+
        " (IF(isMobile='N', flowSize, 0)) AS pcFlowSize,"+
        " (IF(isMobile='N', 1, 0)) AS pcVideoView,"+
        " (IF(isMobile='N', ipAddress, '')) AS pcIpAddress,"+
        " (IF(isMobile='Y', playDuration, 0)) AS mobilePlayDuration,"+
        " (IF(isMobile='Y', flowSize, 0)) AS mobileFlowSize,"+
        " (IF(isMobile='Y', 1, 0)) AS mobileVideoView,"+
        " (IF(isMobile='Y', ipAddress, '')) AS mobileIpAddress"+
        " from viewlog_200006 " +
        " where currentday='2000-06-01'"
    )
    // 聚合求各个指标
    val aggResult = viewlog.groupBy("userId", "currentDay","currentHour")
      .agg(
        sum("pcPlayDuration").alias("pcPlayDuration"),
        sum("pcFlowSize").alias("pcFlowSize"),
        sum("pcVideoView").alias("pcVideoView"),
        countDistinct("pcIpAddress").alias("pcUniqueViewer"),
        sum("mobilePlayDuration").alias("mobilePlayDuration"),
        sum("mobileFlowSize").alias("mobileFlowSize"),
        sum("mobileVideoView").alias("mobileVideoView"),
        countDistinct("mobileIpAddress").alias("mobileUniqueViewer")
      )

    val format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val date_str = format.format(new Date())
    // 拓展多列
    val aggWideResult = aggResult.withColumn("currentMonth", date_format(col("currentDay"), "yyyy-MM"))
      .withColumn("currentWeek", concat_ws("/", year(col("currentDay")), weekofyear(col("currentDay"))))
      .withColumn("createdTime", lit(date_str))
      .withColumn("lastModified", lit(date_str))

    // 根据特殊情况覆盖值

    val properties = new Properties()
    //用户名
    properties.setProperty("user",user)
    properties.setProperty("password",password)
    aggWideResult.write.mode("overwrite").jdbc(url,"temp_user_hourly",properties)
    sparkSession.close()
  }

}
