package com.data.configuration

case class ValidationConfiguration(
       
    var schemaName: Option[String],
    var tableName: Option[String],
    var validationGranualityLevel : Option[String],
    var columnName: Option[String],
    var isRuleActive : Option[String],
    var validationType: Option[String],
    var totalCountGte: Option[String],
    var totalCountLte: Option[String],
    var totalCountEquals: Option[String],
    var totalCountBtw: Option[String],
    var completeness: Option[String],
    var uniqueness: Option[String],
    var hasSelectiveValue: Option[String],
    var hasMin: Option[String],
    var hasMax: Option[String],
    var nonNegative: Option[String],
    var withinRange: Option[String],
    var customSql: Option[String],
    var errorLevel: Option[String],
    var filterCondition :Option[Any]
    
   ) {
  
    //require(filterCondition.isDefined, "Filter name should be defined to churn data based on the condition.")
    
    hasMin = Option(hasMin.getOrElse(""))
    hasMax = Option(hasMax.getOrElse(""))
    isRuleActive  = Option(isRuleActive.getOrElse("False"))
    columnName = Option(columnName.getOrElse(""))
    filterCondition = Option(filterCondition.getOrElse("1:1"))
    errorLevel = Option(errorLevel.getOrElse("warn"))
  }