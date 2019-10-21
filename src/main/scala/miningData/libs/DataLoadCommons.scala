package miningData.libs


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.IntegerType

object DataLoadCommons {



  def loadRawData(spark : SparkSession, dataSourceDir : String, numberOfPartitions : Int): Map[String, DataFrame] = {

    val players = spark.read.option("header", "true").csv(s"$dataSourceDir/player/*.csv").repartition(numberOfPartitions)
    val matches_raw = spark.read.option("header", "true").csv(s"$dataSourceDir/match/*.csv").repartition(numberOfPartitions)

    val matches = TimeCommons.dateToTimestampInDF(matches_raw, ":date", ":timestamp")
      .drop(":date")
      .withColumnRenamed(":timestamp", ":date")
      .withColumnRenamed(":id", ":match")


    val teams = spark.read.option("header", "true").csv(s"$dataSourceDir/team/*.csv").distinct().repartition(numberOfPartitions)
    val cards_raw = spark.read.option("header", "true").csv(s"$dataSourceDir/card/*.csv").repartition(numberOfPartitions)

    val cards = cards_raw
      .withColumn(":timetmp", cards_raw.col(":time").cast(IntegerType))
      .drop(":time")
      .withColumnRenamed(":timetmp", ":time")

    val goals_raw = spark.read.option("header", "true").csv(s"$dataSourceDir/goal/*.csv").repartition(numberOfPartitions)

    val goals = goals_raw
      .withColumn("timetmp", goals_raw.col(":time").cast(IntegerType))
      .drop(":time")
      .withColumnRenamed("timetmp", ":time")

    val substitution_raw = spark.read.option("header", "true").csv(s"$dataSourceDir/substitution/*.csv").repartition(numberOfPartitions)

    val substitution = substitution_raw
      .withColumn(":intmp", substitution_raw.col(":in").cast(IntegerType))
      .drop(":in")
      .withColumnRenamed(":intmp", ":in")
      .withColumn(":outtmp", substitution_raw.col(":out").cast(IntegerType))
      .drop(":out")
      .withColumnRenamed(":outtmp", ":out")

    players.cache()
    teams.cache()
    matches.cache()
    cards.cache()
    goals.cache()
    substitution.cache()

    val rawDataBundle = Map(
      ("players", players),
      ("teams", teams),
      ("matches", matches),
      ("cards", cards),
      ("goals", goals),
      ("substitution", substitution))

    rawDataBundle
  }



}
