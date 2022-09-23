package com.data.configuration



case class Configuration (
    processName:Option[String],
    schemaName:Option[String],
    tableName:Option[String],
    validationRules: Array[ValidationConfiguration]
   ) {
 require(processName.isDefined, "Process Name Should be defined.")
 require(schemaName.isDefined, "Schema Name Should be defined")
 require(tableName.isDefined,"Table Name should be defined")
 
}

