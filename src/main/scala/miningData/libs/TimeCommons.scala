package miningData.libs

import org.apache.spark.sql.functions.{to_timestamp, typedLit}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TimeCommons {

  val MATCH_TIME_SEGMENT_LENGTH = 5


  def getPlayerPresentTime(playerDF: DataFrame, substitutionDF: DataFrame, cardDF: DataFrame, matchesDF: DataFrame, spark: SparkSession): DataFrame = {

    import spark.implicits._

    // bring in substitution players
    val substitutionInPlayers = substitutionDF.drop(substitutionDF.col(":out"))
      .withColumnRenamed(":in", ":player")
      .withColumnRenamed(":time", ":in-time")

    // set in time of startup players an default value 0
    val startupPlayers = playerDF.withColumn(":in-time", typedLit(0))

    // merge startup and substition players
    val allPlayers = startupPlayers.select($":team", $":player", $":match", $":in-time")
      .union(substitutionInPlayers.select($":team", $":player", $":match", $":in-time"))


    val preparedSubstitutions = substitutionDF.withColumnRenamed(":out", ":player")
      .withColumnRenamed(":time", ":out-time")
      .drop(":in")

    // join players and substitutions for out-times
    val playersJoinedSubs = allPlayers
      .join(preparedSubstitutions, Seq(":player", ":match", ":team"), "left_outer")

    playersJoinedSubs.cache()

    val playerAndMatch = playersJoinedSubs.join(matchesDF, ":match")

    // players that are not substituted
    val filtered90Players = playerAndMatch.filter(playerAndMatch.col(":out-time").isNull)
      .drop(":out-time")
      .withColumn(":out-time", playerAndMatch.col(":match-length"))
      .drop(":home")
      .drop(":guest")
      .drop(":match-length").distinct()

    // out because of red card?
    val redCards = cardDF.filter(cardDF.col(":color") === ":red")
    val filtered90PlayersJoinedCard = filtered90Players.join(redCards, Seq(":player", ":match", ":team"), "left_outer")
    filtered90PlayersJoinedCard.cache()


    // players that are not substituted and get a red card
    val filtered90PlayersWithCards = filtered90PlayersJoinedCard.filter(filtered90PlayersJoinedCard.col(":color").isNotNull)
      .drop(":out-time")
      .withColumnRenamed(":time", ":out-time")
      .select(":player", ":match", ":team", ":tournament", ":saison", ":date", ":in-time", ":out-time")

    // players without substitution and red card
    val filtered90PlayersWithoutCards = filtered90PlayersJoinedCard.filter(filtered90PlayersJoinedCard.col(":color").isNull)
      .select(":player", ":match", ":team", ":tournament", ":saison", ":date", ":in-time", ":out-time")

    // players with substitution
    val filteredNot90Players = playerAndMatch.filter(playerAndMatch.col(":out-time").isNotNull)
      .select(":player", ":match", ":team", ":tournament", ":saison", ":date", ":in-time", ":out-time")

    // merge everything
    val allPlayersInOutTime = filteredNot90Players
      .union(filtered90PlayersWithCards)
      .union(filtered90PlayersWithoutCards)

    import org.apache.spark.sql.functions._

    val withPlaytime = allPlayersInOutTime.withColumn(":playtime", when($":out-time" =!= $":in-time", $":out-time" - $":in-time").otherwise(lit(1)))

    withPlaytime
  }

  def getPlaytimeOfPlayerPerTournamentAndSaison(playersWithTimeDF: DataFrame): DataFrame = {
    playersWithTimeDF
      .select(":player", ":team", ":tournament", ":saison", ":playtime")
      .groupBy(":player", ":team", ":tournament", ":saison")
      .sum(":playtime")
      .withColumnRenamed("sum(:playtime)", ":playtimes")
  }



  def getGoallessMatchSegmentsPerPlayer(spark: SparkSession, goalDF: DataFrame, matchDF: DataFrame, playersPerMatchWithTime: DataFrame) : DataFrame = {

    import spark.implicits._

    val matchPerTeam = matchDF.select(":id", ":match-length", ":home").withColumnRenamed(":home", ":team")
      .union(matchDF.select(":id", ":match-length", ":guest").withColumnRenamed(":guest", ":team"))

    val segmentDF = ((0 to 120 - MATCH_TIME_SEGMENT_LENGTH by MATCH_TIME_SEGMENT_LENGTH),
      (MATCH_TIME_SEGMENT_LENGTH to 120 by MATCH_TIME_SEGMENT_LENGTH))
      .zipped
      .toSeq
      .toDF(":segmentFrom", ":segmentTo")

    val matchSegments = matchPerTeam.join(segmentDF, matchDF.col(":match-length") >= segmentDF.col(":segmentTo"))

    val matchSegmentsAndGoals = matchSegments.join(goalDF,
      matchSegments.col(":id") === goalDF.col(":match") &&
        matchSegments.col(":team") =!= goalDF.col(":team") &&
        matchSegments.col(":segmentFrom") < goalDF.col(":time") &&
        goalDF.col(":time") <= matchSegments.col(":segmentTo"),
      "left_outer")
      .drop(goalDF.col(":team"))
      .drop(":match")
      .withColumnRenamed(":id", ":match")

    val filteredNoGoalSegments = matchSegmentsAndGoals.filter($":scorer".isNull)

    val noGoalSegmentPresents = filteredNoGoalSegments.join(playersPerMatchWithTime,
      filteredNoGoalSegments.col(":match") === playersPerMatchWithTime.col(":match") &&
        filteredNoGoalSegments.col(":team") === playersPerMatchWithTime.col(":team") &&
        filteredNoGoalSegments.col(":segmentFrom") >= playersPerMatchWithTime.col(":in-time") &&
        filteredNoGoalSegments.col(":segmentTo") <= playersPerMatchWithTime.col(":out-time")
    )
      .drop(playersPerMatchWithTime.col(":team"))
      .drop(playersPerMatchWithTime.col(":match"))
      .drop($":scorer")
      .drop($":assist")
      .drop($":own-goal?")
      .drop($":time")
      .withColumn(":target-match-timestamp", to_timestamp($":date", "dd.MM.yyyy"))

    noGoalSegmentPresents
  }

   def dateToTimestampInDF(dateIncludingDF : DataFrame, dateColName : String = ":date", tsColName : String = ":timestamp") : DataFrame = {

     dateIncludingDF.withColumn(tsColName, to_timestamp(dateIncludingDF.col(dateColName), "dd.MM.yyyy"))
   }
}
