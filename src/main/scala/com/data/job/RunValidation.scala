package com.data.job

import org.apache.spark.sql.SparkSession
import com.data.utils.CommonUtils
import scala.collection.mutable.ListBuffer
import org.apache.log4j.LogManager
import com.data.analyzer.Completeness
import com.data.configuration.Configuration
import org.apache.spark.annotation.Stable


case class dqOutputTable(
  dp_process_id: String,
  schema_name: String,
  table_name: String,
  rule_granular_level: String,
  column_name: String,
  dq_rule: String,
  dq_criticality_level: String,
  dq_final_value: String,
  dq_result_status: String,
  process_date: String)

object RunValidation {
  
  val log = LogManager.getLogger(this.getClass.getName)
  var output: ListBuffer[dqOutputTable] = ListBuffer();
  
  def validate(sparkSession: SparkSession, yaml: Configuration): Any = {
    val process_name=yaml.processName
   
    yaml.validationRules.foreach(
      validationConfig => (
         if (validationConfig.isRuleActive.getOrElse("").equalsIgnoreCase("True")) {

          var whereCondition = CommonUtils.getWhereCondition(validationConfig.filterCondition)
          log.info(s"Condition being applied ${whereCondition}")
          
          val data = CommonUtils.createDataFrame(sparkSession, validationConfig.schemaName.get, validationConfig.tableName.get, whereCondition)
          log.info(s"Dataframe created ${data}")
          
          CommonUtils.isEmpty(validationConfig.completeness.getOrElse("")) match {
             case true => {
            
                val completenessCheck = new Completeness(sparkSession,data,validationConfig,process_name)
                completenessCheck.run()
                
            }
            case false => {
              log.info("Completeness Dont have Value")
            }
          }
          
          
          /*
           * Check for Total Count Greater
           */
          CommonUtils.isEmpty(validationConfig.totalCountGte.getOrElse("")) match {
            case true => {
              log.info("totalCountGte have Value")
            }
            case false => {
              log.info("totalCountGte Dont have Value")
            }
          }

          CommonUtils.isEmpty(validationConfig.totalCountEquals.getOrElse("")) match {
            case true => { log.info(" totalCountEquals Has Value") }
            case false => { log.info("totalCountEquals Dont have Value") }
          }

          CommonUtils.isEmpty(validationConfig.totalCountBtw.getOrElse("")) match {
            case true => { log.info(" totalCountBtw Has Value") }
            case false => { log.info("totalCountBtw Dont have Value") }
          }

          

          CommonUtils.isEmpty(validationConfig.uniqueness.getOrElse("")) match {
            case true => { log.info(" uniqueness Has Value") }
            case false => { log.info("uniqueness Dont have Value") }
          }

          CommonUtils.isEmpty(validationConfig.hasMin.getOrElse("")) match {
            case true => { log.info(" hasMin Has Value") }
            case false => { log.info("hasMin Dont have Value") }
          }

          CommonUtils.isEmpty(validationConfig.hasMax.getOrElse("")) match {
            case true => { log.info(" hasMax Has Value") }
            case false => { log.info("hasMax Dont have Value") }
          }

          CommonUtils.isEmpty(validationConfig.nonNegative.getOrElse("")) match {
            case true => { log.info(" nonNegative Has Value") }
            case false => { log.info("nonNegative Dont have Value") }
          }

          CommonUtils.isEmpty(validationConfig.withinRange.getOrElse("")) match {
            case true => { log.info(" withinRange Has Value") }
            case false => { log.info("withinRange Dont have Value") }
          }

          CommonUtils.isEmpty(validationConfig.customSql.getOrElse("")) match {
            case true => { log.info(" customSql Has Value") }
            case false => { log.info("customSql Dont have Value") }
          }

        }))
  }

}