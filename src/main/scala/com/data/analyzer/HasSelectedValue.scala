package com.data.analyzer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import com.data.configuration.ValidationConfiguration
import scala.collection.mutable.ListBuffer
import com.data.job.dqOutputTable
import org.apache.log4j.LogManager
import org.apache.spark.sql.functions._
import com.data.utils.ColumnExt._

class HasSelectedValue(
  sparkSession: SparkSession,
  data: DataFrame,
  val validationConfig: ValidationConfiguration,
  processName: String)  {

  var output: ListBuffer[dqOutputTable] = ListBuffer();
  val log = LogManager.getLogger(this.getClass.getName)

  def run(): ListBuffer[dqOutputTable] = {
    // TODO
    ???
  }

}