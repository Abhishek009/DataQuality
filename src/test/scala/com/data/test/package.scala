package scala.com.data

import com.data.job.Job
import com.data.analyzer._
package object test {
  
  def main(args:Array[String]):Unit =  {
    
    val sparkSession = Job.createSparkSession("DataTest","local")
    
    val df = sparkSession.read.format("csv").option("header", value = true).load("./test-data/matches.csv");
    df.show()
    try {
    /*val completenessCheck = new Completeness(sparkSession,df,validationConfig,"df")
    completenessCheck.run()*/
     
    } catch {
      case e: Exception => {
        println(e)
      }
    }
    
  }
  
}