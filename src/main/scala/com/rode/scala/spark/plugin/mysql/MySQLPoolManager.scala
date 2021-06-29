package com.rode.scala.spark.plugin.mysql

import java.sql.{Connection, Statement}

import com.mchange.v2.c3p0.ComboPooledDataSource
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.slf4j.LoggerFactory

import scala.util.Try

class MySQLPoolManager(conf: SparkConf) extends Serializable {

  lazy val logger = LoggerFactory.getLogger(getClass)

  val dataSource = new ComboPooledDataSource(true)

  val paramPrefix = "spark.mysql.pool.jdbc."

  val p: Map[String, String] = conf.getAll.flatMap {
    case (k, v) if k.startsWith(paramPrefix) && Try(v.nonEmpty).getOrElse(false) => Some(k.substring(paramPrefix.length) -> v)
    case _ => None
  } toMap

  try {
    dataSource.setJdbcUrl(p.getOrElse("url", "jdbc:mysql://url/polyv_analytics?characterEncoding=utf8&serverTimezone=UTC"))
    dataSource.setDriverClass(p.getOrElse("driverClass", "com.mysql.jdbc.Driver"))
    dataSource.setUser(p.getOrElse("username", "user"))
    dataSource.setPassword(p.getOrElse("password", "password"))
    dataSource.setMinPoolSize(p.getOrElse("minPoolSize", "3").toInt)
    dataSource.setMaxPoolSize(p.getOrElse("maxPoolSize", "80").toInt)
    dataSource.setAcquireIncrement(p.getOrElse("acquireIncrement", "3").toInt)
    dataSource.setMaxStatements(p.getOrElse("maxStatements", "20").toInt)
    dataSource.setAutoCommitOnClose(p.getOrElse("autoCommitOnClose", "true").toBoolean);
  } catch {
    case e: Exception => logger.error(e.getMessage, e)
  }

  def getDataSource: ComboPooledDataSource = {
    dataSource
  }

  def getConnection: Connection = {
    try {
      dataSource.getConnection
    } catch {
      case e: Exception => logger.error(e.getMessage, e)
        null
    }
  }

  def closeConn(conn: Connection): Unit = {
    if (conn != null) {
      try {
        conn.close()
      } catch {
        case e: Exception => logger.error(e.getMessage, e)
      }
    }
  }

  def closeStmt(stmt: Statement): Unit = {
    if (stmt != null) {
      try {
        stmt.close()
      } catch {
        case e: Exception => logger.error(e.getMessage, e)
      }
    }
  }

  def printInfo(): Unit = {
    try {
      println("------------c3p0连接池链接状态--------------");
      println("c3p0连接池中 【 总共 】 连接数量：" + dataSource.getNumConnectionsAllUsers());
      println("c3p0连接池中 【  忙  】 连接数量：" + dataSource.getNumBusyConnectionsAllUsers());
      println("c3p0连接池中 【 空闲 】 连接数量：" + dataSource.getNumIdleConnectionsAllUsers());
      println("c3p0连接池中 【未关闭】 连接数量：" + dataSource.getNumUnclosedOrphanedConnectionsAllUsers());
    } catch {
      case e: Exception => logger.error(e.getMessage, e)
    }
  }
}


object MySQLPoolManager extends Logging{
  var mySQLPoolManager: MySQLPoolManager = _

  def getPool(conf: SparkConf): MySQLPoolManager = {

    conf.getAll.foreach( x => logInfo(s"${x._1} | ${x._2}"))

    logInfo("start to get a pool ")

    synchronized {
      if (mySQLPoolManager == null) {
        logInfo("start to new a pool ")
        mySQLPoolManager = new MySQLPoolManager(conf: SparkConf)
      }
    }
    logInfo("return a pool")
    mySQLPoolManager
  }
}