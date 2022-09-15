package com.data.configuration

import com.data.configuration.ValidationConfiguration

case class Configuration (
    processname:String,
    validationRules: Array[ValidationConfiguration]
   ) {
 
}

