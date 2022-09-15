package com.data.output

import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame

object WriteOutput extends Writer{
  
  val log = LogManager.getLogger(this.getClass)
  
  override def write(dataFrame: DataFrame): Unit = {
    
  }
  
}