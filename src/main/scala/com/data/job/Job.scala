package com.data.job

import org.apache.spark.sql.SparkSession
import org.apache.log4j.LogManager

object Job {
  
val log = LogManager.getLogger(this.getClass.getName)

  def createSparkSession(appName: String, master: String): SparkSession = {
    
   val sparkSessionBuilder = SparkSession.builder().appName(appName)
   log.info(s"Master: ${master}")
   master match {
     case "local" => sparkSessionBuilder.master(master).getOrCreate()
     case _ => sparkSessionBuilder.enableHiveSupport().getOrCreate()
      }
   
  }
  
  def isSparkSessionAvailable(appName: String, master: String,session: SparkSession) = {
   val sparkSession = session match {
    case ss => ss
    case _ => Job.createSparkSession(appName, master)
  }
  }

}