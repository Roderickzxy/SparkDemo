package com.rode.scala.spark.sql

import com.rode.scala.spark.plugin.mysql.MySQLDataFrameWriter
import com.rode.scala.spark.sql.AggByUserDaily.jdbcReadConfig
import com.rode.scala.spark.sql.AggByViewlog.{jdbcReadConfig, sparkConf}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import scala.collection.mutable.ArrayBuffer

object AggByVideoDaily {
  private final val monthSdf = new SimpleDateFormat("yyyy-MM")
  var sparkConf: SparkConf = _
  var sparkSession: SparkSession = _
  var jdbcReadConfig: scala.collection.mutable.Map[String, String] = _

  def createContext():Unit={
    sparkConf = new SparkConf().setAppName("SparkReadMysql").setMaster("local").set("spark.debug.maxToStringFields", "100")
    val prop = sparkConf
      .getAllWithPrefix(s"spark.mysql.pool.jdbc.")
      .toMap
    jdbcReadConfig = scala.collection.mutable.Map(
      "url" -> prop.getOrElse("vod_analytics.readonly.url", "jdbc:mysql://url/polyv_analytics?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&autoReconnect=true&allowMultiQueries=true&useSSL=false"),
      "user"->prop.getOrElse("vod_analytics.readonly.username", "user"),
      "password" -> prop.getOrElse("vod_analytics.readonly.password", "password"),
      "driver" -> prop.getOrElse("vod_analytics.readonly.driverClass", "com.mysql.jdbc.Driver")
    )

    sparkSession=SparkSession
      .builder()
      .config(sparkConf)//设置操作hive的url，相当于jdbc里的url
      .getOrCreate()
  }

  def main(args:Array[String]):Unit={
    if(args.length<1){
      print("请输入日期")
      return
    }
    val currentDay = args(0)
    val sdf =new SimpleDateFormat("yyyy-MM-dd")
    val date :Date = sdf.parse(currentDay)
    val statsCal = Calendar.getInstance
    statsCal.setTime(date)
    val currentMonth = monthSdf.format(statsCal.getTime)
    statsCal.add(Calendar.DATE, statsCal.get(Calendar.DAY_OF_WEEK) * (-1) + 1)
    val weekStart = sdf.format(statsCal.getTime)
    statsCal.setTime(date)
    statsCal.add(Calendar.DATE, 7 - statsCal.get(Calendar.DAY_OF_WEEK))
    val weekEnd = sdf.format(statsCal.getTime)
    val year = statsCal.get(Calendar.YEAR)
    val weekOfYear = statsCal.get(Calendar.WEEK_OF_YEAR) - 1
    val currentWeek = year+"/"+weekOfYear

    // 初始化上下文
    createContext()

    val userAggCol = getDailyAggCol()

    // 汇总video_daily_a
    val hostIdArray = Array("0","1","2","3","4","5","6","7","8","9","a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z")
    for(hostId <- hostIdArray){

      // 加载user_daily_stat的数据
      createOrReplaceTempView(jdbcReadConfig, "video_daily_"+hostId)
      val videoDailyStats =  sparkSession.sql(getVideoDailySql(currentMonth, hostId))
      // 汇总video_weekly
      val aggVideoWeeklyDf = videoDailyStats.groupBy("videoId","currentWeek", "userId")
        .agg(userAggCol(0), userAggCol.slice(1, userAggCol.length):_*)
        .withColumn("startDay", lit(weekStart))
        .withColumn("endDay", lit(weekEnd))
        .filter(col("currentWeek")===lit(currentWeek))
      outputDB(aggVideoWeeklyDf, "video_weekly_"+hostId)

      // 汇总video_monthly
      val aggVideoMonthlyDf = videoDailyStats.groupBy("videoId","currentMonth", "userId")
        .agg(userAggCol(0), userAggCol.slice(1, userAggCol.length):_*)
      outputDB(aggVideoMonthlyDf, "video_monthly_"+hostId)
    }


    sparkSession.close()
  }

  // 加载表数据并注册为临时表
  def createOrReplaceTempView(prop: scala.collection.mutable.Map[String,String], tableName:String):Unit={
    prop += ("dbtable"->tableName)
    sparkSession.read.format("jdbc").options(prop).load().createOrReplaceTempView(tableName)
  }

  def fillWithCreatedAndLastModifed(df: DataFrame): DataFrame={
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val dateStr = dateFormat.format(new Date())
    df.withColumn("createdTime", lit(dateStr))
      .withColumn("lastModified", lit(dateStr))
  }

  def getVideoDailySql(currentMonth: String, hostId: String):String={
    "select * from video_daily_"+hostId+" where currentMonth='"+currentMonth+"'"
  }


  def getDailyAggCol():Array[Column]={
    Array(sum("pcPlayDuration").alias("pcPlayDuration"),
      sum("pcFlowSize").alias("pcFlowSize"),
      sum("pcVideoView").alias("pcVideoView"),
      sum("pcUniqueViewer").alias("pcUniqueViewer"),
      sum("mobilePlayDuration").alias("mobilePlayDuration"),
      sum("mobileFlowSize").alias("mobileFlowSize"),
      sum("mobileVideoView").alias("mobileVideoView"),
      sum("mobileUniqueViewer").alias("mobileUniqueViewer"),

      sum("percent10").alias("percent10"),
      sum("percent20").alias("percent20"),
      sum("percent30").alias("percent30"),
      sum("percent40").alias("percent40"),
      sum("percent50").alias("percent50"),
      sum("percent60").alias("percent60"),
      sum("percent70").alias("percent70"),
      sum("percent80").alias("percent80"),
      sum("percent90").alias("percent90"),
      sum("percent100").alias("percent100"))
  }

  def outputDB(df: DataFrame, tableName: String): Unit ={
    val writer = new MySQLDataFrameWriter(sparkConf)
    val colList = df.schema.fields.map(f =>f.name).array
    writer.withDF(df)
      .withMode("replace")
      .withTable(tableName)
      .withUpdateColumns(colList)
      .write()
  }

}
