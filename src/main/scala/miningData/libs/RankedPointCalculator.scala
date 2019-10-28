package miningData.libs

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, lit, row_number, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

object RankedPointCalculator {


  def getRankedPoints(spark: SparkSession, validationTakeLastXMatches: Int, neutralPointsMatches: DataFrame, playersWithTimeDF: DataFrame, goalDF: DataFrame, matchDF: DataFrame): DataFrame = {

    val rankedOffPointsPerMatch: DataFrame =  getRankedOffensivePresentPointsPerMatch(neutralPointsMatches, playersWithTimeDF, goalDF, matchDF, spark)
    val rankedDefPointsPerMatch: DataFrame = getRankedDefensivePresentPointsPerMatch(neutralPointsMatches, playersWithTimeDF, goalDF, matchDF, spark)

    val allPoints : DataFrame = getRankedPointsMerged(playersWithTimeDF, rankedOffPointsPerMatch, rankedDefPointsPerMatch, spark)

    val allMatchesPointLastXMatches : DataFrame = aggregateLastXMatches(allPoints, validationTakeLastXMatches, spark)

    val allNeutralAndRankedMatchesPointLastXMatches  : DataFrame = joinNeutralAndRanked(neutralPointsMatches, allMatchesPointLastXMatches, spark)

    allNeutralAndRankedMatchesPointLastXMatches

  }



  private[this] def getRankedOffensivePresentPointsPerMatch(allPointsLastXMatches: DataFrame, playersPerMatchWithTime: DataFrame, goals: DataFrame, matches: DataFrame, spark: SparkSession): DataFrame = {

    import spark.implicits._

    val defPlayersPerMatchWithTime = playersPerMatchWithTime.withColumnRenamed(":team", ":defTeam")

    val defencePlayersPresents = goals.join(defPlayersPerMatchWithTime,
      goals.col(":match") === defPlayersPerMatchWithTime.col(":match")
        && goals.col(":team") =!= defPlayersPerMatchWithTime.col(":defTeam")
        && defPlayersPerMatchWithTime.col(":in-time") <= goals.col(":time")
        && goals.col(":time") <= defPlayersPerMatchWithTime.col(":out-time"))
      .drop(defPlayersPerMatchWithTime.col(":match"))
      .join(allPointsLastXMatches
          .withColumnRenamed(":team", ":defTeam")
          .withColumnRenamed(":target-match-timestamp", ":date"),
        Seq(":player", ":date", ":defTeam", ":match"))

    val goalsAndDefence = defencePlayersPresents.groupBy(":time", ":team", ":defTeam", ":match")
      .agg(sum(":avgDefPointsLastXMatches").as("sumDefPointsLastXMatches"))
      .withColumnRenamed(":team", ":offTeam")

    val offPlayersPerMatchWithTime = playersPerMatchWithTime.withColumnRenamed(":team", ":offTeam")

    val offPlayersWithGoalPoints = offPlayersPerMatchWithTime.join(goalsAndDefence,
      goalsAndDefence.col(":match") === offPlayersPerMatchWithTime.col(":match")
        && goalsAndDefence.col(":offTeam") === offPlayersPerMatchWithTime.col(":offTeam")
        && offPlayersPerMatchWithTime.col(":in-time") <= goalsAndDefence.col(":time")
        && goalsAndDefence.col(":time") <= offPlayersPerMatchWithTime.col(":out-time"))
      .drop(goalsAndDefence.col(":match"))
      .drop(goalsAndDefence.col(":offTeam")).cache

    val zeroPoints = playersPerMatchWithTime.select(":player", ":match", ":playtime").distinct()
      .withColumn(":rankedOffPoints", lit(0))

    val balancer = offPlayersWithGoalPoints.select("sumDefPointsLastXMatches").sort($"sumDefPointsLastXMatches".asc).take(1).apply(0).getDouble(0)

    val offPlayersWithRankedGoalPoints = offPlayersWithGoalPoints.withColumn(":rankedOffPoints", lit( - balancer) +  $"sumDefPointsLastXMatches" )
        .select(":player", ":match", ":playtime", ":rankedOffPoints")
      .union(zeroPoints)

    val offPlayersWithRankedGoalPointsPerGame = offPlayersWithRankedGoalPoints
      .groupBy(":player", ":match", ":playtime")
        .agg(sum(":rankedOffPoints").as(":rankedOffPoints"))

    offPlayersWithRankedGoalPointsPerGame
  }


  private[this] def getRankedDefensivePresentPointsPerMatch(allPointsLastXMatches: DataFrame, playersPerMatchWithTime: DataFrame, goals: DataFrame, matches: DataFrame, spark: SparkSession): DataFrame = {

    import spark.implicits._

    val offPlayersPerMatchWithTime = playersPerMatchWithTime.withColumnRenamed(":team", ":offTeam")


    val offPlayersPresents = goals.join(offPlayersPerMatchWithTime,
      goals.col(":match") === offPlayersPerMatchWithTime.col(":match")
        && goals.col(":team") === offPlayersPerMatchWithTime.col(":offTeam")
        && offPlayersPerMatchWithTime.col(":in-time") <= goals.col(":time")
        && goals.col(":time") <= offPlayersPerMatchWithTime.col(":out-time"))
      .drop(offPlayersPerMatchWithTime.col(":match"))
      .join(allPointsLastXMatches
        .withColumnRenamed(":team", ":offTeam")
        .withColumnRenamed(":target-match-timestamp", ":date"),
        Seq(":player", ":date", ":offTeam", ":match"))

    val goalsAndOffence = offPlayersPresents.groupBy(":time",  ":offTeam", ":match")
      .agg(sum(":avgOffPointsLastXMatches").as("sumOffPointsLastXMatches"))


    val defPlayersPerMatchWithTime = playersPerMatchWithTime.withColumnRenamed(":team", ":defTeam")


    val defPlayersWithGoalPoints = defPlayersPerMatchWithTime.join(goalsAndOffence,
      goalsAndOffence.col(":match") === defPlayersPerMatchWithTime.col(":match")
        && goalsAndOffence.col(":offTeam") =!= defPlayersPerMatchWithTime.col(":defTeam")
        && offPlayersPerMatchWithTime.col(":in-time") <= goalsAndOffence.col(":time")
        && goalsAndOffence.col(":time") <= defPlayersPerMatchWithTime.col(":out-time"))
      .drop(goalsAndOffence.col(":match")).cache


    val zeroPoints = playersPerMatchWithTime.select(":player", ":match", ":playtime").distinct()
      .withColumn(":rankedDefPoints", lit(0))

    val balancer = defPlayersWithGoalPoints.select("sumOffPointsLastXMatches").sort($"sumOffPointsLastXMatches".desc).take(1).apply(0).getDouble(0)


    val defPlayersWithRankedGoalPoints = defPlayersWithGoalPoints.withColumn(":rankedDefPoints",  lit( - balancer ) + $"sumOffPointsLastXMatches" )
      .select(":player", ":match", ":playtime", ":rankedDefPoints")
      .union(zeroPoints)


    val defPlayersWithRankedGoalPointsPerGame = defPlayersWithRankedGoalPoints
      .groupBy(":player", ":match", ":playtime")
      .agg(sum(":rankedDefPoints").as(":rankedDefPoints"))


    defPlayersWithRankedGoalPointsPerGame
  }

  private[this] def getRankedPointsMerged(playersWithTimeDF: DataFrame, rankedOffPointsPerMatch: DataFrame, rankedDefPointsPerMatch: DataFrame, spark: SparkSession): DataFrame = {


    playersWithTimeDF.select(":player", ":match", ":team", ":tournament", ":saison", ":date", ":playtime").cache
      .join(rankedOffPointsPerMatch.select(":player",  ":match",  ":rankedOffPoints").cache,
        Seq(":player", ":match"))
      .join(rankedDefPointsPerMatch.select(":player",  ":match", ":rankedDefPoints").cache,
        Seq(":player", ":match"))
      .withColumn(":rankedDiffPoints", rankedOffPointsPerMatch(":rankedOffPoints") - rankedDefPointsPerMatch(":rankedDefPoints"))
  }

  private[this] def aggregateLastXMatches(allPoints : DataFrame, validationTakeLastXMatches : Int, spark: SparkSession) : DataFrame = {

    import spark.implicits._

    val matchesAndPrevMatches = allPoints
      .select(":player", ":match", ":team", ":tournament", ":saison", ":date")
      .withColumnRenamed(":date", ":target-match-timestamp")
      .join(
        allPoints.select(":player", ":saison", ":date", ":playtime", ":rankedOffPoints", ":rankedDefPoints", ":rankedDiffPoints"),
        Seq(":player", ":saison"))
      .filter($":target-match-timestamp" >= $":date")

    val orderByTS = Window.partitionBy(":match", ":player", ":target-match-timestamp").orderBy($":date".desc)

    val lastXMatches = matchesAndPrevMatches
      .withColumn("rank", row_number() over orderByTS)
      .filter($"rank" <= validationTakeLastXMatches)

    val aggPoints = lastXMatches
      .groupBy(":player", ":saison", ":match", ":team", ":tournament", ":target-match-timestamp")
      .sum(":playtime", ":rankedOffPoints", ":rankedDefPoints", ":rankedDiffPoints")
      .withColumnRenamed("sum(:playtime)", ":playtimeLastXMatches")
      .withColumnRenamed("sum(:rankedOffPoints)", ":totalRankedOffPointsLastXMatches")
      .withColumnRenamed("sum(:rankedDefPoints)", ":totalRankedDefPointsLastXMatches")
      .withColumnRenamed("sum(:rankedDiffPoints)", ":totalRankedDiffPointsLastXMatches")

    val avgPoints = aggPoints
      .withColumn(":avgRankedOffPointsLastXMatches", $":totalRankedOffPointsLastXMatches" / $":playtimeLastXMatches")
      .withColumn(":avgRankedDefPointsLastXMatches", $":totalRankedDefPointsLastXMatches" / $":playtimeLastXMatches")
      .withColumn(":avgRankedDiffPointsLastXMatches", $":totalRankedDiffPointsLastXMatches" / $":playtimeLastXMatches")

    avgPoints
  }


  def joinNeutralAndRanked(neutralPointsMatches: DataFrame, rankedPointsMatches: DataFrame, spark: SparkSession): DataFrame = {
    neutralPointsMatches.join(rankedPointsMatches,
      Seq(":player", ":saison", ":match", ":team", ":tournament", ":target-match-timestamp", ":playtimeLastXMatches"))
  }


}
