package com.data.output

import org.apache.spark.sql.DataFrame

trait Writer extends Serializable{
  
  private var mandatoryArguments = Seq[String]()

  def write(dataFrame: DataFrame): Unit

  protected def setMandatoryArguments(mandatoryArgs: String*): Unit = {
    this.mandatoryArguments = mandatoryArgs
  }

  def validateMandatoryArguments(outputOptions: Map[String, String]): Unit = {
    this.mandatoryArguments.foreach {arg =>
      if (!outputOptions.contains(arg)) throw new MissingWriterArgumentException(s"Missing argument $arg for writer ${this.getClass.toString}")
    }
  }

  case class MissingWriterArgumentException(private val message: String = "",
                                            private val cause: Throwable = None.orNull)
    extends Exception(message, cause)
}