package miningData.libs

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{row_number, typedLit}
import org.apache.spark.sql.{DataFrame, SparkSession}

object NeutralPointsCalculator {


  private[this] val offensiveGoalPresentPoint = 1.0
  private[this] val defensiveGoalPresentPoint = 1.0

  private[this] val initOffensiveGoalPresentPoint = 0.0



  def getNeutralPoints(spark: SparkSession, validationTakeLastXMatches: Int, matchDF: DataFrame, playersPerMatchWithTime: DataFrame, goalDF: DataFrame): DataFrame = {

    val offPointsPerMatch = getNeutralOffensivePresentPointsPerMatch(playersPerMatchWithTime, goalDF, spark)
    val defPointsPerMatch = getNeutralDefensivePresentPointsPerMatch(playersPerMatchWithTime, goalDF, spark)

    val allPoints = getNeutralPointsMerged(playersPerMatchWithTime, offPointsPerMatch, defPointsPerMatch)

    val allMatchesPointLastXMatches = aggregateLastXMatches(matchDF, allPoints, validationTakeLastXMatches, spark)

    allMatchesPointLastXMatches

  }


  private[this] def getNeutralDefensivePresentPointsPerMatch(playersPerMatchWithTime: DataFrame, goalDF: DataFrame, spark: SparkSession): DataFrame = {

    val initDefGoalPresentPoint = 0
    val defGoalPresentPoint = -1

    val goalPresent = playersPerMatchWithTime.join(goalDF,
      playersPerMatchWithTime.col("matchid") === goalDF.col("matchid")
        && playersPerMatchWithTime.col("teamid") =!= goalDF.col("teamid")
        && playersPerMatchWithTime.col("in-time") <= goalDF.col("time")
        && goalDF.col("time") <= playersPerMatchWithTime.col("out-time"))
      .drop(goalDF.col("matchid"))
      .drop(goalDF.col("teamid"))


    val zeroInitPlayerPoints = playersPerMatchWithTime.select("teamid", "playerid")
      .withColumn("point", typedLit(initDefGoalPresentPoint))


    val playersWithPointsPerMatch = goalPresent.withColumn("point", typedLit(defGoalPresentPoint))
      .select("matchid", "playerid", "point")
      .union(zeroInitPlayerPoints)
      .groupBy("matchid", "playerid")
      .sum("point")
      .withColumnRenamed("sum(point)", "defPoints")

    playersWithPointsPerMatch
  }

  private[this] def getNeutralOffensivePresentPointsPerMatch(playersPerMatchWithTime: DataFrame, goalDF: DataFrame, spark: SparkSession): DataFrame = {

    val initDefGoalPresentPoint = 0

    val goalPresent = playersPerMatchWithTime.join(goalDF,
      playersPerMatchWithTime.col("matchid") === goalDF.col("matchid")
        && playersPerMatchWithTime.col("teamid") === goalDF.col("teamid")
        && playersPerMatchWithTime.col("in-time") <= goalDF.col("time")
        && goalDF.col("time") <= playersPerMatchWithTime.col("out-time"))
      .drop(goalDF.col("matchid")).drop(goalDF.col("teamid"))


    val zeroInitPlayerPoints = playersPerMatchWithTime.select("matchid", "playerid")
      .withColumn("point", typedLit(initDefGoalPresentPoint))


    val playersWithPointsPerMatch = goalPresent.withColumn("point", typedLit(offensiveGoalPresentPoint))
      .select("matchid", "playerid", "point")
      .union(zeroInitPlayerPoints)
      .groupBy("matchid", "playerid")
      .sum("point")
      .withColumnRenamed("sum(point)", "offPoints")

    playersWithPointsPerMatch
  }


  private[this] def getNeutralPointsMerged(playersPerMatchWithTime: DataFrame, offPointsPerMatch: DataFrame, defPointsPerMatch: DataFrame) : DataFrame = {

    playersPerMatchWithTime
      .join(offPointsPerMatch, Seq("playerid", "matchid"))
      .join(defPointsPerMatch, Seq("playerid", "matchid"))
      .withColumn("diffPoints", offPointsPerMatch("offPoints") + defPointsPerMatch("defPoints"))
      .drop("in-time", "out-time")

  }


  private[this] def aggregateLastXMatches(matchDF:DataFrame, allPoints : DataFrame, validationTakeLastXMatches : Int, spark: SparkSession) : DataFrame = {

    import spark.implicits._

    val pointsAndDate = allPoints
      .join(matchDF, Seq("matchid"))

    val matchesAndPrevMatches = pointsAndDate
      .select("playerid", "matchid", "teamid",  "matchday")
      .withColumnRenamed("matchday", "target-match-timestamp")
      .join(
        pointsAndDate.select("playerid", "matchday", "playtime", "offPoints", "defPoints", "diffPoints"),
        Seq("playerid"))
      .filter($"target-match-timestamp" >= $"matchday")

    val orderByTS = Window.partitionBy("matchid", "playerid", "target-match-timestamp").orderBy($"matchday".desc)

    val lastXMatches = matchesAndPrevMatches
      .withColumn("rank", row_number() over orderByTS)
      .filter($"rank" <= validationTakeLastXMatches)

    val aggPoints = lastXMatches
      .groupBy("playerid","matchid", "teamid", "target-match-timestamp")
      .sum("playtime", "offPoints", "defPoints", "diffPoints")
      .withColumnRenamed("sum(playtime)", "playtimeLastXMatches")
      .withColumnRenamed("sum(offPoints)", "totalOffPointsLastXMatches")
      .withColumnRenamed("sum(defPoints)", "totalDefPointsLastXMatches")
      .withColumnRenamed("sum(diffPoints)", "totalDiffPointsLastXMatches")

    val avgPoints = aggPoints
      .withColumn("avgOffPointsLastXMatches", $"totalOffPointsLastXMatches" / $"playtimeLastXMatches")
      .withColumn("avgDefPointsLastXMatches", $"totalDefPointsLastXMatches" / $"playtimeLastXMatches")
      .withColumn("avgDiffPointsLastXMatches", $"totalDiffPointsLastXMatches" / $"playtimeLastXMatches")

    avgPoints
  }


}
