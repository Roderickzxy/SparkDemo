package com.rode.scala.spark.sql

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.rode.scala.spark.plugin.mysql.MySQLDataFrameWriter
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadMysqlMain {
  private final val monthSdf = new SimpleDateFormat("yyyy-MM")
  var sparkConf: SparkConf = _
  var sparkSession: SparkSession = _
  val readJdbcUrl = "jdbc:mysql://url/polyv_analytics?characterEncoding=utf8&serverTimezone=UTC"
  val readUser = "user"
  val readPassword = "password"
  val driver = "com.mysql.jdbc.Driver"

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

    sparkConf=new SparkConf().setAppName("SparkReadMysql").setMaster("local").set("spark.debug.maxToStringFields", "100")
    sparkSession=SparkSession
      .builder()
      .config(sparkConf)//设置操作hive的url，相当于jdbc里的url
      .getOrCreate()
    val prop=scala.collection.mutable.Map[String,String]()
    prop.put("user",readUser)
    prop.put("password",readPassword)
    prop.put("driver",driver)
    prop.put("dbtable","viewlog_202106")
    prop.put("url",readJdbcUrl)
    //从数据库中加载整个表的数据
    val df=sparkSession.read.format("jdbc").options(prop).load()
    //读出来之后注册为临时表
    df.createOrReplaceTempView("viewlog_202106")
    //注册好之后就可以通过sql语句查询了
    val viewlog = sparkSession.sql(
      getViewlogSql(currentDay)
    )

    // 汇总小时粒度的df
    val aggHourlyDf = viewlog.groupBy("userId", "currentDay", "currentHour")
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

    // 汇总天粒度的df
    val aggDailyDf = viewlog.groupBy("userId", "currentDay")
      .agg(
        sum("pcPlayDuration").alias("pcPlayDuration"),
        sum("pcFlowSize").alias("pcFlowSize"),
        sum("pcVideoView").alias("pcVideoView"),
        countDistinct("pcIpAddress").alias("pcUniqueViewer"),
        sum("mobilePlayDuration").alias("mobilePlayDuration"),
        sum("mobileFlowSize").alias("mobileFlowSize"),
        sum("mobileVideoView").alias("mobileVideoView"),
        countDistinct("mobileIpAddress").alias("mobileUniqueViewer"),
        sum("percent10").alias("percent10"),
        sum("percent20").alias("percent20"),
        sum("percent30").alias("percent30"),
        sum("percent40").alias("percent40"),
        sum("percent50").alias("percent50"),
        sum("percent60").alias("percent60"),
        sum("percent70").alias("percent70"),
        sum("percent80").alias("percent80"),
        sum("percent90").alias("percent90"),
        sum("percent100").alias("percent100")
      )


    // 汇总user_hourly_stats
    val userHourlyDf = hourlyAndDailyAggFill(aggHourlyDf, statsCal)
    outputDB(userHourlyDf, "temp_user_hourly")

    // 汇总user_daily_stats
    val userDailyDf = hourlyAndDailyAggFill(aggDailyDf, statsCal)
    outputDB(userDailyDf, "temp_user_daily")


    prop.put("dbtable","user_daily_stats")
    //从数据库中加载整个表的数据
    sparkSession.read.format("jdbc").options(prop).load().createOrReplaceTempView("user_daily_stats")
    val userDailyStats =  sparkSession.sql(getUserDailySql(currentMonth))
    val aggWeeklyDf = userDailyStats.groupBy("userId", "currentMonth", "currentWeek")
        .agg(sum("pcPlayDuration").alias("pcPlayDuration"),
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
          sum("percent100").alias("percent100")
        )

    // 汇总user_weekly_stats
    val userWeeklyDf = weeklyAggFill(aggWeeklyDf, statsCal)
      .drop("currentMonth")
      .withColumn("stardDay", lit(weekStart))
        .withColumn("endDay", lit(weekEnd))
        .filter(col("currentWeek")===lit(currentWeek))


    outputDB(userWeeklyDf, "temp_user_weekly")

    val aggMonthlyDf = aggWeeklyDf.groupBy("userId", "currentMonth")
      .agg(sum("pcPlayDuration").alias("pcPlayDuration"),
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
        sum("percent100").alias("percent100")
      )

    // 汇总user_monthly_stats
    val userMonthlyDf = monthlyAggFill(aggMonthlyDf, statsCal)
    outputDB(userMonthlyDf, "temp_user_monthly")

    userHourlyDf.show()
    userDailyDf.show()
    userWeeklyDf.show()
    userMonthlyDf.show()

    sparkSession.close()
  }

  def hourlyAndDailyAggFill(df: DataFrame, cal: Calendar): DataFrame ={
    var resultDf = fillWithCreatedAndLastModifed(df)
    resultDf = fillWithCurrentWeek(resultDf, cal)
    resultDf = fillWithCurrentMonth(resultDf,cal)
    resultDf = overWriteUniqueViewer(resultDf)
    resultDf
  }

  def weeklyAggFill(df: DataFrame, cal: Calendar): DataFrame ={
    fillWithCreatedAndLastModifed(df)
  }

  def monthlyAggFill(df: DataFrame, cal: Calendar): DataFrame ={
    fillWithCreatedAndLastModifed(df)
  }

  def fillWithCurrentWeek(df: DataFrame, cal: Calendar): DataFrame={
    val year = cal.get(Calendar.YEAR)
    val weekOfYear = cal.get(Calendar.WEEK_OF_YEAR) - 1
    val currentWeek = year+"/"+weekOfYear
    df.withColumn("currentWeek", lit(currentWeek))
  }

  def fillWithCurrentMonth(df: DataFrame, cal: Calendar): DataFrame={
    val currentMonth = monthSdf.format(cal.getTime)
    df.withColumn("currentMonth", lit(currentMonth))
  }

  def fillWithCreatedAndLastModifed(df: DataFrame): DataFrame={
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val dateStr = dateFormat.format(new Date())
    df.withColumn("createdTime", lit(dateStr))
      .withColumn("lastModified", lit(dateStr))
  }

  def overWriteUniqueViewer(df: DataFrame): DataFrame ={
    // 唯一观众数需要做特殊处理
    // 1.duration为0，则唯一观众数置0
    // 2.duration>0，剔除对立端（PC对立移动，移动对立PC）重复算进来的人数
    df.withColumn("pcUniqueViewer",
      when(col("pcPlayDuration").equalTo(0), 0)
        .when(col("mobileVideoView")>0, col("pcUniqueViewer")-lit(1))
        .otherwise(col("pcUniqueViewer"))
    )
      .withColumn("mobileUniqueViewer",
        when(col("mobilePlayDuration").equalTo(0), 0)
          .when(col("pcVideoView")>0, col("mobileUniqueViewer")-lit(1))
          .otherwise(col("mobileUniqueViewer"))
      )
  }

  def getViewlogSql(currentDay: String): String={
    "select userId,currentDay,currentHour, " +
      "(IF(isMobile='N', playDuration, 0)) AS pcPlayDuration,"+
      " (IF(isMobile='N', flowSize, 0)) AS pcFlowSize,"+
      " (IF(isMobile='N', 1, 0)) AS pcVideoView,"+
      " (IF(isMobile='N', ipAddress, '')) AS pcIpAddress,"+
      " (IF(isMobile='Y', playDuration, 0)) AS mobilePlayDuration,"+
      " (IF(isMobile='Y', flowSize, 0)) AS mobileFlowSize,"+
      " (IF(isMobile='Y', 1, 0)) AS mobileVideoView,"+
      " (IF(isMobile='Y', ipAddress, '')) AS mobileIpAddress,"  +
      " (IF((playDuration / IF(playDuration > duration, playDuration, duration) * 100) >= 0"+
        " AND (playDuration / IF(playDuration > duration, playDuration, duration) * 100) < 10"+
        " , 1, 0)) AS percent10,"+
      " (IF((playDuration / IF(playDuration > duration, playDuration, duration) * 100) >= 10"+
        " AND (playDuration / IF(playDuration > duration, playDuration, duration) * 100) < 20"+
        " , 1, 0)) AS percent20,"+
      " (IF((playDuration / IF(playDuration > duration, playDuration, duration) * 100) >= 20"+
        " AND (playDuration / IF(playDuration > duration, playDuration, duration) * 100) < 30"+
        " , 1, 0)) AS percent30,"+
      " (IF((playDuration / IF(playDuration > duration, playDuration, duration) * 100) >= 30"+
        " AND (playDuration / IF(playDuration > duration, playDuration, duration) * 100) < 40"+
        " , 1, 0)) AS percent40,"+
      " (IF((playDuration / IF(playDuration > duration, playDuration, duration) * 100) >= 40"+
        " AND (playDuration / IF(playDuration > duration, playDuration, duration) * 100) < 50"+
        " , 1, 0)) AS percent50,"+
      " (IF((playDuration / IF(playDuration > duration, playDuration, duration) * 100) >= 50"+
        " AND (playDuration / IF(playDuration > duration, playDuration, duration) * 100) < 60"+
        " , 1, 0)) AS percent60,"+
      " (IF((playDuration / IF(playDuration > duration, playDuration, duration) * 100) >= 60"+
        " AND (playDuration / IF(playDuration > duration, playDuration, duration) * 100) < 70"+
        " , 1, 0)) AS percent70,"+
      " (IF((playDuration / IF(playDuration > duration, playDuration, duration) * 100) >= 70"+
        " AND (playDuration / IF(playDuration > duration, playDuration, duration) * 100) < 80"+
        " , 1, 0)) AS percent80,"+
      " (IF((playDuration / IF(playDuration > duration, playDuration, duration) * 100) >= 80"+
        " AND (playDuration / IF(playDuration > duration, playDuration, duration) * 100) < 90"+
        " , 1, 0)) AS percent90,"+
      " (IF((playDuration / IF(playDuration > duration, playDuration, duration) * 100) >= 90"+
        " AND (playDuration / IF(playDuration > duration, playDuration, duration) * 100) <= 100"+
        " , 1, 0)) AS percent100 "+
        " from viewlog_202106 "+
        " where currentday='"+currentDay+"' and userid='ccb547c108'"
  }

  def getUserDailySql(currentMonth: String):String={
    "select * from user_daily_stats where currentMonth='"+currentMonth+"' and userid='ccb547c108'"
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
