package com.data.dataquality

import scala.collection.mutable.ListBuffer
import java.io.{ File, FileInputStream }
import scala.collection.immutable.List
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.data.utils.CommonUtils
import com.data.configuration.CLIConfigurationParser
import com.data.configuration.Configuration
import com.data.job.Job
import org.apache.log4j.LogManager
import com.data.job.RunValidation

object DataQuality {

  /*--configFile ./input/DataQualityRule.yaml --dataQuality True --runMode local --jobName DataQuality*/
  

  def main(args: Array[String]): Unit = {

    val log = LogManager.getLogger(this.getClass.getName)

    val conf = new CLIConfigurationParser(args)
    

    val argsMap: scala.collection.mutable.Map[String, String] = conf.getArgMap()

    val fileName = argsMap.getOrElse("configFile", "")
    val isDataQuality = argsMap.getOrElse("dataQuality", "")
    val isDataProfile = argsMap.getOrElse("dataProfile", "")
    val jobName = argsMap.getOrElse("jobName", "").toString()
    val runMode = argsMap.getOrElse("runMode", "")
    
    log.info("CLI Configuration")
    log.info(s"FileName ${fileName}")
    log.info(s"jobName ${jobName}")

    if (isDataQuality.toBoolean || isDataProfile.toBoolean) {

      val mapper = new ObjectMapper(new YAMLFactory())
      mapper.findAndRegisterModules();
      val yaml: Configuration = mapper.readValue(new File(fileName), classOf[Configuration])

      val jsonString: String = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(yaml)
      val jsonObj = mapper.readTree(jsonString)
      log.info("YAML Config File: "+jsonObj.toPrettyString())

      // Creating spark session
      val sparkSession = Job.createSparkSession(jobName,runMode)

      
      // Validate the data validation run based on the yaml file.
      RunValidation.validate(sparkSession,yaml)

    } else {
      log.error("Kindly choose the choose DataQuality or DataProfile")
    }

  }
}

