package demo

import org.apache.spark.{SparkConf, SparkContext}

class MatrixMultiplication {


  val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
  val sc = new SparkContext(conf)

  val matrixM = sc.textFile("M.txt").map(line =>{val t = line.split(" ");(t(1).trim.toInt,(t(0).trim.toInt,t(2).trim.toInt))})

  val matrixN = sc.textFile("N.txt").map(line =>{val t = line.split(" ");(t(0).trim.toInt,(t(1).trim.toInt,t(2).trim.toInt))})

//format wiersz, kolumna, wartość


  def run: Unit = {

    val result = matrixM.join(matrixN)
      .map{ case(j, ((indexM,valueM),(indexN,valueN))) => ((indexM,indexN), valueM * valueN)}.reduceByKey(_ + _)

    result.sortByKey(true)
    result.collect().foreach(println)
  }
}
