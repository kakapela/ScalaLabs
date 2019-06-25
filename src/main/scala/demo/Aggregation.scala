package demo

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

class Aggregation {

  val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
  val sc = new SparkContext(conf)

  //INIT DATA
  val groups = Map(
    0 -> (0, 1),
    1 -> (1, 1),
    2 -> (2, 2),
    3 -> (3, 3),
    4 -> (4, 3),
    5 -> (5, 4),
    6 -> (6, 3),
    7 -> (7, 3),
    8 -> (8, 2),
    9 -> (9, 1),
    10 -> (10, 1)
  )

  val n = 1000000
  val random = Random

  val data = sc.parallelize(
    for (i <- 1 to n) yield {
      val g = random.nextInt(11)
      val (mu, sigma) = groups(g)

      (g, mu + sigma * random.nextGaussian())
    }
  )

  //Data is in format -> (group_id, value)

  // Grupowanie do formatu (GROUP_ID, (Count, Value, Value^2))
  val groupStats = (data
    .map(line => (line._1, (1, line._2, line._2 * line._2)))
    .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))
    )


  def printDataForAll(): Unit = {
    println(dataForAll)
  }

  //FORMAT - (GROUP_ID, VALUE)
  def printData(): Unit = {
    data.take(20).foreach(println)
  }

  def printGroupsStats(): Unit = {
    groupStats.collect().foreach(println)
  }
  def printGroups(): Unit = {
    groups.foreach(println)
  }

  //funkcja zliczająca statystyki dla każdej grupy
  def runForEachGroup(): Unit = {

    groupStats.cache()

    //variance = 1/N ( x^2  - (SUM(x*x)/N)  )

    // Agregujemy (Group ID, (Sum count this group in all records, Average value, Variance))
    val resultForGroup =
    groupStats.map(line => (line._1, (line._2._1, line._2._2 / line._2._1, ((line._2._3 - ((line._2._2 * line._2._2) / line._2._1)) / line._2._1))))
    // Print in new line each group stats
    resultForGroup.collect().foreach(println)
  }


  val dataForAll = (data
    .map(line => (1, line._2, line._2 * line._2))
    .reduce((a, b) => ((a._1 + b._1), (a._2 + b._2), (a._3 + b._3))
    ))


  //korzystajac z obliczen pierwotnych
  def runForAll1(): Unit = {

    println("OBLICZENIA DLA WSZYSTKICH KORZYSTAJAC Z OBLICZEN PIERWOTNYCH")

    val resultForAll =
      (dataForAll._1, dataForAll._2 / dataForAll._1,
        ((dataForAll._3 - ((dataForAll._2 * dataForAll._2) / dataForAll._1)) / dataForAll._1))
    println(resultForAll)

  }

  def runForAll2(): Unit = {

    println("OBLICZENIA DLA WSZYSTKICH KORZYSTAJAC Z OBLICZEN DLA GRUPY")
    groupStats.cache()

    // (0, ( suma ilości ze wszystkich grup, suma wartości ze wszystkich grup, suma potęg ze wszystkich grup  ) )
    val allStats =
      groupStats.reduce((a, b) => (0, (a._2._1 + b._2._1, a._2._2 + b._2._2, a._2._3 + b._2._3)))


    val resultForAll =
      (allStats._2._1, allStats._2._2 / allStats._2._1,
        ((allStats._2._3 - ((allStats._2._2 * allStats._2._2) / allStats._2._1)) / allStats._2._1))

    // Return result as (Total count, Total average, Total variance)
    println(resultForAll)
  }

  def runForAllWithoutCaching(): Unit = {
    val allStats =
      groupStats.reduce((a, b) => (0, (a._2._1 + b._2._1, a._2._2 + b._2._2, a._2._3 + b._2._3)))
    val resultForAll =
      (allStats._2._1, allStats._2._2 / allStats._2._1,
        ((allStats._2._3 - ((allStats._2._2 * allStats._2._2) / allStats._2._1)) / allStats._2._1))

    // Return result as (Total count, Total average, Total variance)
    println(resultForAll)
  }
}
