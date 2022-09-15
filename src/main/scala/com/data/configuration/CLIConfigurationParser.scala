package com.data.configuration

import org.rogach.scallop._
import scala.collection.mutable.Map
import org.rogach.scallop.exceptions.Help
import org.rogach.scallop.exceptions.Exit
import org.rogach.scallop.exceptions.ScallopException
import org.rogach.scallop.exceptions.RequiredOptionNotFound
import com.data.utils.CommonUtils

class CLIConfigurationParser(arguments: Seq[String]) extends ScallopConf(arguments) {

  banner("""Usage: DataQualityFramework [OPTIONS] ...
|DataQualityFramework performs data quality check based on the rules defined by you in the configuration 
|Options:
""")

  val configFile = opt[String](
    name = "configFile",
    required = true,
    descr = "Configuration file is required which contains the Data Validation configuration")

  val dataQuality = opt[String](
    name = "dataQuality",
    default = Some("False"),
    descr = "Give the value as true if you want to run the code for DataQuality")

  val dataProfile = opt[String](
    name = "dataProfile",
    default = Some("False"),
    descr = "Give the value as true if you want to run the code for DataProfile")
    
  val jobName = opt[String](
    name = "jobName",
    default = Some("DataQuality-"+CommonUtils.generateUUID()),
    descr = "Give the jobName")
    
  val runMode = opt[String](
    name = "runMode",
    default = Some("local"),
    descr = "Give the master for your job master/local")
    
  def getArgMap(): Map[String, String] = {
    var argsMap = scala.collection.mutable.Map[String, String]()
    argsMap += ("configFile" -> configFile())
    argsMap += ("dataQuality" -> dataQuality())
    argsMap += ("dataProfile" -> dataProfile())
    argsMap += ("jobName" -> jobName())
    argsMap += ("runMode" -> runMode())
    
    argsMap
  }

  verify()

  override def onError(e: Throwable): Unit = e match {
    case Help("") => printHelp()
    case Exit() => printHelp()
    case ScallopException(message) => {
      println(message)
      printHelp()
    }
    case RequiredOptionNotFound(message) => {
      println(message)
      printHelp()
    }
    case other => throw other
  }

}


