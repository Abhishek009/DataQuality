package com.data.analyzer

import scala.collection.immutable.List
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql._
import org.apache.log4j.LogManager
import com.data.job.dqOutputTable
import org.apache.spark.sql.functions._
import com.data.utils.ColumnExt._

object ValidationFunction {

  var output: ListBuffer[dqOutputTable] = ListBuffer();
  val log = LogManager.getLogger(this.getClass.getName)

  /**
   * Fraction of non-null values in a column.
   */
  def completenessCheck(
    sparkSession: SparkSession,
    data: DataFrame,
    columnName: String, completenessCondition: String,out:dqOutputTable): ListBuffer[dqOutputTable] = {

    log.info(s"Completness check on ${columnName}")
    val TotalCount = data.count()
    val notNullCount = data.filter(col(columnName).isNotNullOrBlank).count()
    val completnessPercent = (notNullCount.toDouble / TotalCount.toDouble) * 100

    log.info(s"TotalCount ${TotalCount}")
    log.info(s"notNullCount ${notNullCount}")
    log.info(s"${completnessPercent}")
    log.info(s"${dqOutputTable}")
    if (completnessPercent >= completenessCondition.toDouble) {
      log.info("success")

    } else {
      log.info("Fail")
    }

    output
  }

}