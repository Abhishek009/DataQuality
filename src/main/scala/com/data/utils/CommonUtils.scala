package com.data.utils

import java.util.UUID
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row, SparkSession }
import org.apache.log4j.LogManager
import com.data.configuration.ValidationConfiguration
import com.data.job.dqOutputTable
import com.data.configuration.Configuration

object CommonUtils {
  val log = LogManager.getLogger(this.getClass.getName)

  def isEmpty(stringToCheck: String): Boolean = {
    !stringToCheck.isEmpty()

  }

  def createDataFrame(
    sparkSession: SparkSession,
    schemaName: String,
    tableName: String,
    whereCondition: String): DataFrame = {

    val sql = s"Select * from ${schemaName}.${tableName} where ${whereCondition}"
    //sparkSession.sql(sql);
    sparkSession.read.format("csv").option("header", value = true).load("""D:\\SampleData\\IPL\\matches.csv""");
  }

  def jsonPrint(yaml: Configuration): Any = {
    println(yaml.processname)
    yaml.validationRules.foreach(
      f =>

        println(f.schemaName.getOrElse("") + System.lineSeparator() +
          f.tableName.getOrElse("") + System.lineSeparator() +
          f.validationGranualityLevel.getOrElse("") + System.lineSeparator() +
          f.columnName.getOrElse("") + System.lineSeparator() +
          f.isRuleActive.getOrElse("") + System.lineSeparator() +
          f.totalCountGte.getOrElse("") + System.lineSeparator() +
          f.totalCountLte.getOrElse("") + System.lineSeparator() +
          f.totalCountEquals.getOrElse("") + System.lineSeparator() +
          f.totalCountBtw.getOrElse("") + System.lineSeparator() +
          f.completeness.getOrElse("") + System.lineSeparator() +
          f.uniqueness.getOrElse("") + System.lineSeparator() +
          f.hasSelectiveValue.getOrElse("") + System.lineSeparator() +
          f.hasMin.getOrElse("") + System.lineSeparator() +
          f.hasMax.getOrElse("") + System.lineSeparator() +
          f.nonNegative.getOrElse("") + System.lineSeparator() +
          f.withinRange.getOrElse("") + System.lineSeparator() +
          f.customSql.getOrElse("") + System.lineSeparator() +
          f.errorLevel.getOrElse("") + System.lineSeparator()))

  }

  def getWhereCondition(filterInYAML: Option[Any]): String = {
    var whereCondition = "";

    log.info(filterInYAML.get)

    if (filterInYAML.get.isInstanceOf[List[String]]) {
      val listCondition = filterInYAML.get.asInstanceOf[List[String]]
      listCondition.foreach(
        x =>
          whereCondition = whereCondition + x + " and ")
      whereCondition.splitAt(whereCondition.lastIndexOf("and"))._1
    }
    ""
  }

  def convertToList(rowData: ValidationConfiguration, rule: String, processName: String): dqOutputTable = {
    dqOutputTable(processName, rowData.schemaName.get,
      rowData.tableName.get,
      rowData.validationGranualityLevel.get,
      rowData.columnName.get,
      rule,
      rowData.errorLevel.get.toLowerCase(),
      "", "", "")

  }

  //Generate UUID for the jobs
  def generateUUID(): String = {
    val uuid: UUID = UUID.randomUUID();
    uuid.toString;
  }
}