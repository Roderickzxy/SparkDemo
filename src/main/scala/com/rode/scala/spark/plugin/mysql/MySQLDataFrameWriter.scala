package com.rode.scala.spark.plugin.mysql

import java.sql.{Date, Timestamp}

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory
import org.sql2o.{Connection, Query, Sql2o}

class MySQLDataFrameWriter(conf: SparkConf) extends Serializable {

  lazy val logger = LoggerFactory.getLogger(getClass)

  private var targetTable: String = _

  private var dataFrame: DataFrame = _

  private var writeMode: String = _

  private var customizedUpdateStatement: String = _

  private var updateColumns: Array[String] = Array.empty

  def customizedUpdateStatement(stmt: String): MySQLDataFrameWriter = {
    customizedUpdateStatement = stmt
    this
  }

  def withTable(table: String): MySQLDataFrameWriter = {
    targetTable = table
    this
  }

  def withDF(df: DataFrame): MySQLDataFrameWriter = {
    dataFrame = df
    this
  }

  def withMode(mode: String): MySQLDataFrameWriter = {
    mode match {
      case "insert" | "replace" | "upsert" =>
      case _ => throw new Exception(s"writeMode: $mode 错误。支持 replace 或 insert 以及 upsert 方式")
    }
    writeMode = mode.toUpperCase
    this
  }

  def withUpdateColumns(columns: Array[String]): MySQLDataFrameWriter = {
    updateColumns = columns
    this
  }

  def getPreparedStatement(): String = {
    val columns = dataFrame.schema.fieldNames
    val columnsHolder = columns.mkString(",")
    val valuesHolder = 0.until(columns.length - 1).map(_ => "?").mkString(",")
    val valuesHolder2 = columns.map(x => s":${x}").mkString(",")

    if (writeMode == "INSERT") {
      s"""
         |$writeMode INTO $targetTable
         |($columnsHolder)
         |VALUES($valuesHolder)
     """.stripMargin
    } else if (writeMode == "REPLACE") {
      s"""
         |$writeMode INTO $targetTable
         |($columnsHolder)
         |VALUES($valuesHolder2)
     """.stripMargin
    } else if (writeMode == "UPSERT") {

      var updateStmtHolder: String = ""
      if (!Option(customizedUpdateStatement).getOrElse("").isEmpty) {
        updateStmtHolder = customizedUpdateStatement
      } else if (!updateColumns.isEmpty) {
        var r = ""
        for (i <- 0 to updateColumns.length - 1) {
          r += s"`${updateColumns(i)}` = :${updateColumns(i)}"
          if (i != updateColumns.length - 1) {
            r += ", "
          }
        }
        updateStmtHolder = r
      } else {
        throw new Exception(s"writeMode: UPSERT 错误。必须设置updateColumn 或 customizedUpdateStatement")
      }
      s"""
         |INSERT INTO $targetTable
         |($columnsHolder)
         |VALUES($valuesHolder2)
         |ON DUPLICATE KEY UPDATE ${updateStmtHolder}
     """.stripMargin
    } else ""
  }


  def write(): Unit = {

    val cols = dataFrame.schema.fieldNames
    val types = dataFrame.schema.fields.map(_.dataType)
    val sql = getPreparedStatement()

    val batchCount = 100

    dataFrame.foreachPartition(partition => {

      val pool = MySQLPoolManager.getPool(conf);
      val sql2o = new Sql2o(pool.getDataSource);

      try {
        val conn:Connection = sql2o.open()
        conn.getJdbcConnection.setAutoCommit(false)

        var count = 0
        partition.foreach(record => {

          count += 1
          val query:Query = conn.createQuery(sql)

          for (i <- 0 to cols.length - 1) {

            val dataType = types(i)

            dataType match {
              case _: ByteType | ShortType | IntegerType => query.addParameter(cols(i), record.getAs[Int](i))
              case _: LongType => query.addParameter(cols(i), record.getAs[Long](i))
              case _: BooleanType => query.addParameter(cols(i), if (record.getAs[Boolean](i)) 1 else 0)
              case _: FloatType => query.addParameter(cols(i), record.getAs[Float](i))
              case _: DoubleType => query.addParameter(cols(i), record.getAs[Double](i))
              case _: StringType => query.addParameter(cols(i), record.getAs[String](i))
              case _: TimestampType => query.addParameter(cols(i), record.getAs[Timestamp](i))
              case _: DateType => query.addParameter(cols(i), record.getAs[Date](i))
              case _: DecimalType => query.addParameter(cols(i), record.getAs[java.math.BigDecimal](i))
              case _ => throw new RuntimeException(s"nonsupport ${dataType} !!!")
            }
          }
          query.executeUpdate()
          if( count % batchCount == 0 )
          {
            conn.commit(false)
            logger.info(s"commit $count record")
          }
        }

        )
        conn.commit(false)
        logger.info(s"commit $count record totally")

        if(conn != null)
        {
          conn.close()
        }
      } catch {
        case e: Exception => {
          logger.error(s"@@ write ${e.getMessage}")
        }
      }
    })
  }

}