package com.data.configuration

import com.data.configuration.ValidationConfiguration

case class Configuration (
    processName:String,
    validationRules: Array[ValidationConfiguration]
   ) {
 
}

