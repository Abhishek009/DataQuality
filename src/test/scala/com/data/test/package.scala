package scala.com.data

import com.data.job.Job
import com.data.analyzer._
package object test {
  
  def main(args:Array[String]):Unit =  {
    
    val sparkSession = Job.createSparkSession("DataTest","local")
    
    val df = sparkSession.read.format("csv").option("header", value = true).load("D:\\SampleData\\IPL\\matches.csv");
    df.show()
    try {
    /* var comp = ValidationFunction.completenessCheck(sparkSession, df, "city","100")*/
     
    } catch {
      case e: Exception => {
        println(e)
      }
    }
    
  }
  
}