package miningData


import java.time.Duration

import miningData.libs.{DataLoadCommons, NeutralPointsCalculator, RankedPointCalculator, TimeCommons}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._


object CalculatePointSetApp{

  val NUMBER_OF_CORES = Runtime.getRuntime().availableProcessors()
  //val NUMBER_OF_CORES = 12

  val VALIDATE_LAST_X_MATCHES_SEQ = Seq(1, 2, 3, 5)


  val OUTPUT_DATA_DIR = "/goalimpacct/spark_data_cache"

  def main(args: Array[String]) {

    val startMillis = System.currentTimeMillis()

    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster(s"local[$NUMBER_OF_CORES]")
    conf.setAppName(this.getClass.getName)
    conf.set("spark.driver.memory", "8g")
    conf.set("spark.executor.memory", "8g")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.appName("CalcPoints").getOrCreate()
    val numberOfPartitions = NUMBER_OF_CORES * 2

    println("loading raw data...")

    val user = "postgres"
    val password = "postgres"
    val url = "jdbc:postgresql://localhost:5432/goalimpacct"

    val rawDataBundle = DataLoadCommons.loadRawDataFormTable(spark, url, user, password, numberOfPartitions)

    import spark.implicits._

    val profileDF = rawDataBundle("playerProfiles").filter($"birthdate".isNotNull)


    val countOfMatches = rawDataBundle("matches").count()
    val countOfPlayerProfiles = profileDF.count();

    println("calculating players present times...")

    val playersPresentTime: DataFrame = TimeCommons.getPlayerPresentTime(rawDataBundle("players"),
      rawDataBundle("substitution"),
      rawDataBundle("cards"),
      rawDataBundle("matches"),
      spark).cache()

    println("Storing player present time ...")


    playersPresentTime.write.
      format("jdbc")
      .mode(SaveMode.Overwrite)
      .option("url", url)
      .option("dbtable", "playertime")
      .option("user", user)
      .option("password", password)
      .save()

    println("Start calculating match data...")

    var endDFList = VALIDATE_LAST_X_MATCHES_SEQ
      .map(i => (i, calcPointData(spark, i, rawDataBundle, playersPresentTime)))
      .map{ case (i : Int, df : DataFrame) =>
        df.withColumnRenamed("playtimeLastXMatches", "playtimeLast"+i+"Matches")
          .withColumnRenamed("totalOffPointsLastXMatches", "totalOffPointsLast"+i+"Matches")
          .withColumnRenamed("totalDefPointsLastXMatches", "totalDefPointsLast"+i+"Matches")
          .withColumnRenamed("avgOffPointsLastXMatches", "avgOffPointsLast"+i+"Matches")
          .withColumnRenamed("avgDefPointsLastXMatches", "avgDefPointsLast"+i+"Matches")
          //.withColumnRenamed(":totalRankedOffPointsLastXMatches", ":totalRankedOffPointsLast"+i+"Matches")
          //.withColumnRenamed(":totalRankedDefPointsLastXMatches", ":totalRankedDefPointsLast"+i+"Matches")
          //.withColumnRenamed(":avgRankedOffPointsLastXMatches", ":avgRankedOffPointsLast"+i+"Matches")
          //.withColumnRenamed(":avgRankedDefPointsLastXMatches", ":avgRankedDefPointsLast"+i+"Matches")
          .withColumnRenamed("totalDiffPointsLastXMatches", "totalDiffPointsLast"+i+"Matches")
          .withColumnRenamed("avgDiffPointsLastXMatches", "avgDiffPointsLast"+i+"Matches")
          //.withColumnRenamed(":totalRankedDiffPointsLastXMatches", ":totalRankedDiffPointsLast"+i+"Matches")
          //.withColumnRenamed(":avgRankedDiffPointsLastXMatches", ":avgRankedDiffPointsLast"+i+"Matches")
      }

    println("Compress match data...")

    val endDF = endDFList
      .reduce{ (df1 : DataFrame, df2 : DataFrame) => df1.join(df2.drop("saison", "tournament", "teamid") , Seq("playerid", "matchid", "target-match-timestamp"))}


    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._

    val windowSpec = Window.partitionBy("playerid","matchid").orderBy($"target-match-timestamp".desc)


    val compressedEndDF = endDF
      .withColumn("lastMatch", first("target-match-timestamp").over(windowSpec))
      .filter($"lastMatch" === $"target-match-timestamp")
      .drop("lastMatch")

    println("Merge data with player profile details...")

    val compressedEndDFjoinProfile = compressedEndDF.join(
      profileDF.select("playerid", "birthdate"),
      endDF("playerid") ===  profileDF("playerid"),
      "left")
      .drop(profileDF("playerid"))

    val compressedEndDFPlusProfile = compressedEndDFjoinProfile
      .withColumn("age", datediff( $"target-match-timestamp", $"birthdate") / 365 )
      .drop("birthdate")


    println("Storing score data ...")

    compressedEndDFPlusProfile.write.
      format("jdbc")
      .mode(SaveMode.Overwrite)
      .option("url", url)
      .option("dbtable", "scores")
      .option("user", user)
      .option("password", password)
      .save()

    println(s"Calculated data of matches:  $countOfMatches")
    println(s"Calculated data of player profiles:  $countOfPlayerProfiles")

    val endMillis = System.currentTimeMillis()
    val runtime =  Duration.ofMillis(endMillis - startMillis).toMinutes

    println(s"Calculating tooks $runtime minutes")


    //scala.io.StdIn.readLine()

    spark.close()

  }

  def calcPointData(spark: SparkSession, validationTakeLastXMatches: Int, rawDataBundle : Map[String, DataFrame], playersPresentTime: DataFrame) :  DataFrame = {

    println(s"calculating data for the last $validationTakeLastXMatches matches...")

    val matches : DataFrame  = rawDataBundle("matches")
    val goals : DataFrame  = rawDataBundle("goals")

    val allNeutralPointsLastXMatches = NeutralPointsCalculator.getNeutralPoints(spark, validationTakeLastXMatches, matches, playersPresentTime, goals).cache()
    //val allRankedPointsLastXMatches = RankedPointCalculator.getRankedPoints(spark, validationTakeLastXMatches, matches, allNeutralPointsLastXMatches, playersPresentTime, goals).cache()

    allNeutralPointsLastXMatches
  }


}
