package miningData.libs


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.IntegerType
import java.util.Properties;

object DataLoadCommons {

  def loadRawDataFormTable(spark : SparkSession, url : String, user :String, password : String, numberOfPartitions : Int): Map[String, DataFrame] = {

    val connectionProperties = new Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", password)

    val matchDF = spark.read.jdbc(url,  "match", connectionProperties)
    val playerDF = spark.read.jdbc(url,  "player", connectionProperties)
    val teamDF = spark.read.jdbc(url,  "team", connectionProperties)
    val cardDF = spark.read.jdbc(url,  "card", connectionProperties)
    val goalDF = spark.read.jdbc(url,  "goal", connectionProperties)
    val substitutionDF = spark.read.jdbc(url,  "substitution", connectionProperties)
    val profileDF = spark.read.jdbc(url,  "profile", connectionProperties)

    playerDF.cache()
    teamDF.cache()
    matchDF.cache()
    cardDF.cache()
    goalDF.cache()
    substitutionDF.cache()
    profileDF.cache()

    val rawDataBundle = Map(
      ("players", playerDF),
      ("teams", teamDF),
      ("matches", matchDF),
      ("cards", cardDF),
      ("goals", goalDF),
      ("substitution", substitutionDF),
      ("playerProfiles", profileDF)
    )

    rawDataBundle
  }


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

    val playerProfilesRaw = spark.read.option("header", "true").csv(s"$dataSourceDir/player-profile/*.csv")
    val playerProfiles = TimeCommons.dateToTimestampInDF(playerProfilesRaw, ":birth-date", ":timestamp")
      .drop(":birth-date")
      .withColumnRenamed(":timestamp", ":birth-date")
      .withColumnRenamed(":player-id", ":player")


    players.cache()
    teams.cache()
    matches.cache()
    cards.cache()
    goals.cache()
    substitution.cache()
    playerProfiles.cache()

    val rawDataBundle = Map(
      ("players", players),
      ("teams", teams),
      ("matches", matches),
      ("cards", cards),
      ("goals", goals),
      ("substitution", substitution),
      ("playerProfiles", playerProfiles)
    )

    rawDataBundle
  }



}
