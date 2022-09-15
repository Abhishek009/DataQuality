package com.data.analyzer

import com.data.configuration.ValidationConfiguration
import scala.collection.mutable.ListBuffer
import com.data.job.dqOutputTable
import org.apache.log4j.LogManager
import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.sql.functions._
import com.data.utils.ColumnExt._

class Completeness(
                    sparkSession: SparkSession,
                    data: DataFrame,
                    val validationConfig: ValidationConfiguration,
                    processName: String) {
  
   
   var output: ListBuffer[dqOutputTable] = ListBuffer();
   val log = LogManager.getLogger(this.getClass.getName)
   
   
   def run(): ListBuffer[dqOutputTable] = {

    log.info(s"Completness check on ${validationConfig.columnName}")
    val columnName = validationConfig.columnName.get.toString()
    val TotalCount = data.count()
    val notNullCount = data.filter(col(columnName).isNotNullOrBlank).count()
    val completnessPercent = (notNullCount.toDouble / TotalCount.toDouble) * 100
    val completenessCondition = validationConfig.completeness.get
    
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