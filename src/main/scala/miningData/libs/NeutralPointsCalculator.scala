package miningData.libs

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{row_number, typedLit}
import org.apache.spark.sql.{DataFrame, SparkSession}

object NeutralPointsCalculator {


  private[this] val offensiveGoalPresentPoint = 1.0
  private[this] val defensiveGoalPresentPoint = 1.0

  private[this] val initOffensiveGoalPresentPoint = 0.0



  def getNeutralPoints(spark: SparkSession, validationTakeLastXMatches: Int, playersPerMatchWithTime: DataFrame, goalDF: DataFrame): DataFrame = {

    val offPointsPerMatch = getNeutralOffensivePresentPointsPerMatch(playersPerMatchWithTime, goalDF, spark)
    val defPointsPerMatch = getNeutralDefensivePresentPointsPerMatch(playersPerMatchWithTime, goalDF, spark)

    val allPoints = getNeutralPointsMerged(playersPerMatchWithTime, offPointsPerMatch, defPointsPerMatch)

    val allMatchesPointLastXMatches = aggregateLastXMatches(allPoints, validationTakeLastXMatches, spark)

    allMatchesPointLastXMatches

  }


  private[this] def getNeutralDefensivePresentPointsPerMatch(playersPerMatchWithTime: DataFrame, goalDF: DataFrame, spark: SparkSession): DataFrame = {

    val initDefGoalPresentPoint = 0
    val defGoalPresentPoint = -1

    val goalPresent = playersPerMatchWithTime.join(goalDF,
      playersPerMatchWithTime.col(":match") === goalDF.col(":match")
        && playersPerMatchWithTime.col(":team") =!= goalDF.col(":team")
        && playersPerMatchWithTime.col(":in-time") <= goalDF.col(":time")
        && goalDF.col(":time") <= playersPerMatchWithTime.col(":out-time"))
      .drop(goalDF.col(":match"))
      .drop(goalDF.col(":team"))


    val zeroInitPlayerPoints = playersPerMatchWithTime.select(":match", ":player")
      .withColumn(":point", typedLit(initDefGoalPresentPoint))


    val playersWithPointsPerMatch = goalPresent.withColumn(":point", typedLit(defGoalPresentPoint))
      .select(":match", ":player", ":point")
      .union(zeroInitPlayerPoints)
      .groupBy(":match", ":player")
      .sum(":point")
      .withColumnRenamed("sum(:point)", ":defPoints")

    playersWithPointsPerMatch
  }

  private[this] def getNeutralOffensivePresentPointsPerMatch(playersPerMatchWithTime: DataFrame, goalDF: DataFrame, spark: SparkSession): DataFrame = {

    val initDefGoalPresentPoint = 0

    val goalPresent = playersPerMatchWithTime.join(goalDF,
      playersPerMatchWithTime.col(":match") === goalDF.col(":match")
        && playersPerMatchWithTime.col(":team") === goalDF.col(":team")
        && playersPerMatchWithTime.col(":in-time") <= goalDF.col(":time")
        && goalDF.col(":time") <= playersPerMatchWithTime.col(":out-time"))
      .drop(goalDF.col(":match")).drop(goalDF.col(":team"))


    val zeroInitPlayerPoints = playersPerMatchWithTime.select(":match", ":player")
      .withColumn(":point", typedLit(initDefGoalPresentPoint))


    val playersWithPointsPerMatch = goalPresent.withColumn(":point", typedLit(offensiveGoalPresentPoint))
      .select(":match", ":player", ":point")
      .union(zeroInitPlayerPoints)
      .groupBy(":match", ":player")
      .sum(":point")
      .withColumnRenamed("sum(:point)", ":offPoints")

    playersWithPointsPerMatch
  }


  private[this] def getNeutralPointsMerged(playersPerMatchWithTime: DataFrame, offPointsPerMatch: DataFrame, defPointsPerMatch: DataFrame) : DataFrame = {

    playersPerMatchWithTime
      .join(offPointsPerMatch, Seq(":player", ":match"))
      .join(defPointsPerMatch, Seq(":player", ":match"))
      .drop(":in-time", ":out-time")

  }


  private[this] def aggregateLastXMatches(allPoints : DataFrame, validationTakeLastXMatches : Int, spark: SparkSession) : DataFrame = {

    import spark.implicits._

    val matchesAndPrevMatches = allPoints
      .select(":player", ":match", ":team", ":tournament", ":saison", ":date")
      .withColumnRenamed(":date", ":target-match-timestamp")
      .join(
        allPoints.select(":player", ":saison", ":date", ":playtime", ":offPoints", ":defPoints"),
        Seq(":player", ":saison"))
      .filter($":target-match-timestamp" >= $":date")

    val orderByTS = Window.partitionBy(":match", ":player", ":target-match-timestamp").orderBy($":date".desc)

    val lastXMatches = matchesAndPrevMatches
      .withColumn("rank", row_number() over orderByTS)
      .filter($"rank" <= validationTakeLastXMatches)

    val aggPoints = lastXMatches
      .groupBy(":player", ":saison", ":match", ":team", ":tournament", ":target-match-timestamp")
      .sum(":playtime", ":offPoints", ":defPoints")
      .withColumnRenamed("sum(:playtime)", ":playtimeLastXMatches")
      .withColumnRenamed("sum(:offPoints)", ":totalOffPointsLastXMatches")
      .withColumnRenamed("sum(:defPoints)", ":totalDefPointsLastXMatches")

    val avgPoints = aggPoints
      .withColumn(":avgOffPointsLastXMatches", $":totalOffPointsLastXMatches" / $":playtimeLastXMatches")
      .withColumn(":avgDefPointsLastXMatches", $":totalDefPointsLastXMatches" / $":playtimeLastXMatches")

    avgPoints
  }


}
