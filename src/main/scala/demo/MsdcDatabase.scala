package demo
import org.apache.spark.sql.functions._

class MsdcDatabase {

  val spark = org.apache.spark.sql.SparkSession.builder
    .master("local")
    .appName("Spark CSV Reader")
    .getOrCreate;


  val samples = spark.read.format("com.databricks.spark.csv").
    option("sep",",").
    csv("samples.txt").
    toDF("user_id", "song_id", "date_id")

  val songs = spark.read.format("com.databricks.spark.csv").
    option("sep",",").
    csv("tracks1.txt").
    toDF("track_id","song_id","artist","title","nowe_id")

  val dates = spark.read.format("com.databricks.spark.csv").
    option("sep",",").
    csv("dates.txt").
    toDF("date_id","day","month","year")


  def t1_getMostPopularSongs( ) : Unit = {
    samples.groupBy("song_id")
      .count().join(songs,"song_id")
      .select("title","artist","count")
      .orderBy(desc("count"))
      .show(10,false)

  }

  def t2_getUsersWithMostTracks(): Unit= {
    samples.select("user_id", "song_id")
      .groupBy("user_id","song_id")
      .count()
      .select("user_id")
      .groupBy("user_id")
      .count()
      .orderBy(desc("count"))
      .show(10,false)

  }

  def t3_getBestArtist(): Unit = {
    samples.select("song_id").
      join(songs, "song_id").
      groupBy("artist").
      count().
      orderBy(desc("count")).
      show(1, false)
  }


  def t4_getListenedSongsPerMonth(): Unit ={
    samples.select("date_id").
      join(dates, "date_id").
      groupBy("month").
      count().
      orderBy(asc("month")).
      select("month", "count").
      show(false)
  }


  val top3queenSongs = samples.groupBy("song_id").
    count().
    join(songs, "song_id").
    filter(col("artist") === "Queen").
    orderBy(desc("count")).
    select("title", "song_id").
    limit(3)


  def t5_getQueenListeners(): Unit = {
    samples.select("song_id", "user_id").
      join(top3queenSongs, "song_id").
      filter(not(col("title").isNull)).
      groupBy("user_id", "song_id").
      count().
      select("user_id").
      groupBy("user_id").
      count().
      filter(col("count") === 3).
      orderBy(asc("user_id")).
      select("user_id").
      show(10, false)

  }

  def runAllQueries(): Unit = {
    t1_getMostPopularSongs()
    t2_getUsersWithMostTracks()
    t3_getBestArtist()
    t4_getListenedSongsPerMonth()
    t5_getQueenListeners()
  }
}
