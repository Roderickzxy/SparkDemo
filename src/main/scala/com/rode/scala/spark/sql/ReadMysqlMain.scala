package com.rode.scala.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ReadMysqlMain {
  def main(args:Array[String]):Unit={
    //创建一个session，在2.0以后，SparkContext不是必须创建的，而是可以通过如下方式创建
    val conf=new SparkConf().setMaster("local").setAppName("SparkReadMysql")
    val sparkSession=SparkSession
      .builder()
      .config(conf)//设置操作hive的url，相当于jdbc里的url
      .getOrCreate()
    val prop=scala.collection.mutable.Map[String,String]()
    prop.put("user","test")
    prop.put("password","test")
    prop.put("driver","com.mysql.jdbc.Driver")
    prop.put("dbtable","viewlog_200006")
    prop.put("url","jdbc:mysql://test/test?characterEncoding=utf8&serverTimezone=UTC")
    //从数据库中加载整个表的数据
    val df=sparkSession.read.format("jdbc").options(prop).load()
    //读出来之后注册为临时表
    df.createOrReplaceTempView("viewlog_200006")
    //注册好之后就可以通过sql语句查询了
    val viewlog = sparkSession.sql("select videoId, sessionId,playDuration from viewlog_200006 where currentday='2000-06-01' limit 10")
    viewlog.groupBy("videoId").sum("playDuration").show()
    sparkSession.close()
  }

}
