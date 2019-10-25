package miningData


import java.time.Duration

import miningData.libs.{DataLoadCommons, NeutralPointsCalculator, RankedPointCalculator, TimeCommons}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object CalculatePointSetApp {

  val NUMBER_OF_CORES = Runtime.getRuntime().availableProcessors() + 2
  //val NUMBER_OF_CORES = 12

  val VALIDATE_LAST_X_MATCHES_SEQ = Seq(1)

  val DATA_SOURCE_DIR = "/goalimpacct/data-test"
  //val DATA_SOURCE_DIR = "/goalimpacct/data_compressed"

  val OUTPUT_DATA_DIR = "/goalimpacct/spark_data_cache"

  def main(args: Array[String]) {

    val startMillis = System.currentTimeMillis()

    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster(s"local[$NUMBER_OF_CORES]")
    conf.setAppName(this.getClass.getName)
    conf.set("spark.driver.memory", "16g")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.appName("CalcPoints").getOrCreate()
    val numberOfPartitions = NUMBER_OF_CORES * 2

    println("loading raw data...")

    val rawDataBundle = DataLoadCommons.loadRawData(spark, DATA_SOURCE_DIR, numberOfPartitions)

    // maybe to edit the offset of league quality, but not yet
    //matches.select(":tournament").distinct().withColumn(":offset", lit(1)).coalesce(1).write.option("header", "true").mode("overwrite").csv("/tmp/tournaments.csv")

    import spark.implicits._

    val profileDF = rawDataBundle("playerProfiles").filter($":birth-date".isNotNull)

    val countOfMatches = rawDataBundle("matches").count()

    println("calculating players present times...")

    val playersPresentTime: DataFrame = TimeCommons.getPlayerPresentTime(rawDataBundle("players"),
      rawDataBundle("substitution"),
      rawDataBundle("cards"),
      rawDataBundle("matches"),
      spark).cache()

    playersPresentTime.write.format("parquet").mode(SaveMode.Overwrite).save(OUTPUT_DATA_DIR + "/player_time_parquet")



    var endDF = VALIDATE_LAST_X_MATCHES_SEQ
      .map(i => (i, calcPointData(spark, i, rawDataBundle, playersPresentTime)))
      .map{ case (i : Int, df : DataFrame) =>
        df.withColumnRenamed(":playtimeLastXMatches", ":playtimeLast"+i+"Matches")
          .withColumnRenamed(":totalOffPointsLastXMatches", ":totalOffPointsLast"+i+"Matches")
          .withColumnRenamed(":totalDefPointsLastXMatches", ":totalDefPointsLast"+i+"Matches")
          .withColumnRenamed(":avgOffPointsLastXMatches", ":avgOffPointsLast"+i+"Matches")
          .withColumnRenamed(":avgDefPointsLastXMatches", ":avgDefPointsLast"+i+"Matches")
          .withColumnRenamed(":totalRankedOffPointsLastXMatches", ":totalRankedOffPointsLast"+i+"Matches")
          .withColumnRenamed(":totalRankedDefPointsLastXMatches", ":totalRankedDefPointsLast"+i+"Matches")
          .withColumnRenamed(":avgRankedOffPointsLastXMatches", ":avgRankedOffPointsLast"+i+"Matches")
          .withColumnRenamed(":avgRankedDefPointsLastXMatches", ":avgRankedDefPointsLast"+i+"Matches")}
      .reduce{ (df1 : DataFrame, df2 : DataFrame) => df1.join(df2.drop(":saison", ":tournament", ":team") , Seq(":player", ":match", ":target-match-timestamp"))}


    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._

    val windowSpec = Window.partitionBy(":player",":match").orderBy($":target-match-timestamp".desc)


    val compressedEndDF = endDF
      .withColumn(":lastMatch", first(":target-match-timestamp").over(windowSpec))
      .filter($":lastMatch" === $":target-match-timestamp")
      .drop(":lastMatch")

    var compressedEndDFjoinProfile = compressedEndDF.join(profileDF ,endDF(":player") ===  profileDF(":player"))

    var compressedEndDFPlusProfile = compressedEndDFjoinProfile
      .withColumn("age", ($":target-match-timestamp".cast("long") - $":birth-date".cast("long"))/ (365 * 24 * 60 * 60) )
      .drop(":birth-date")
      .cache()

    compressedEndDFPlusProfile.show

    compressedEndDF.coalesce(1).write
      .mode(SaveMode.Overwrite)
      .option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false") //Avoid creating of crc files
      .option("header","true")
      .csv(OUTPUT_DATA_DIR + "/result_csv")

    compressedEndDF.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(OUTPUT_DATA_DIR + "/result_parquet")


    println(s"Calculated data of matches:  $countOfMatches")

    val endMillis = System.currentTimeMillis()
    val runtime =  Duration.ofMillis(endMillis - startMillis).toMinutes

    println(s"Calculating tooks $runtime minutes")

    //scala.io.StdIn.readLine()
  }

  def calcPointData(spark: SparkSession, validationTakeLastXMatches: Int, rawDataBundle : Map[String, DataFrame], playersPresentTime: DataFrame) :  DataFrame = {

    println(s"calculating data for the last $validationTakeLastXMatches matches...")

    val matches : DataFrame  = rawDataBundle("matches")
    val goals : DataFrame  = rawDataBundle("goals")

    val allNeutralPointsLastXMatches = NeutralPointsCalculator.getNeutralPoints(spark, validationTakeLastXMatches, playersPresentTime, goals).cache()
    val allRankedPointsLastXMatches = RankedPointCalculator.getRankedPoints(spark, validationTakeLastXMatches, allNeutralPointsLastXMatches, playersPresentTime, goals, matches).cache()

    allRankedPointsLastXMatches
  }


}
