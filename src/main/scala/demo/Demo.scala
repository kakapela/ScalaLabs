package demo
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

import scala.util.Random

object Demo {
  def main(args: Array[String]): Unit ={

  //  val aggregation = new Aggregation
//aggregation.runForAll1()
    //val matrixMultiplication = new MatrixMultiplication
   //matrixMultiplication.run
     val msdcDatabase = new MsdcDatabase
msdcDatabase.runAllQueries()
  //  msdcDatabase.t2_getUsersWithMostTracks()
  }

}
