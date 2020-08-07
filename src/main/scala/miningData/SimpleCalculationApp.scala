package miningData

import java.time.Duration

import miningData.libs.{DataLoadCommons, NeutralPointsCalculator, RankedPointCalculator, TimeCommons}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SimpleCalculationApp {

  val NUMBER_OF_CORES = Runtime.getRuntime().availableProcessors()
  //val NUMBER_OF_CORES = 12

  val VALIDATE_LAST_X_MATCHES = 1

  val DATA_SOURCE_DIR = "/goalimpacct/data_compressed"
  //val DATA_SOURCE_DIR = "/goalimpacct/data-test"

  def main(args: Array[String]) {

    val startMillis = System.currentTimeMillis()

    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster(s"local[$NUMBER_OF_CORES]")
    conf.setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.appName("CalcPoints").getOrCreate()
    val numberOfPartitions = NUMBER_OF_CORES * 2

    println("loading raw data...")

    val rawDataBundle = DataLoadCommons.loadRawData(spark, DATA_SOURCE_DIR, numberOfPartitions)

    // maybe to edit the offset of league quality, but not yet
    //matches.select(":tournament").distinct().withColumn(":offset", lit(1)).coalesce(1).write.option("header", "true").mode("overwrite").csv("/tmp/tournaments.csv")

    val countOfMatches = rawDataBundle("matches").count()

    println("calculating players present times...")

    val playersPresentTime: DataFrame = TimeCommons.getPlayerPresentTime(rawDataBundle("players"),
      rawDataBundle("substitution"),
      rawDataBundle("cards"),
      rawDataBundle("matches"),
      spark)
      .cache()

    playersPresentTime.show()

    val matches : DataFrame  = rawDataBundle("matches")
    val goals : DataFrame  = rawDataBundle("goals")

    println(s"calculating data for the last $VALIDATE_LAST_X_MATCHES matches...")

    val allNeutralPointsLastXMatches = NeutralPointsCalculator.getNeutralPoints(spark, VALIDATE_LAST_X_MATCHES, matches, playersPresentTime, goals).cache()
    val allRankedPointsLastXMatches = RankedPointCalculator.getRankedPoints(spark, VALIDATE_LAST_X_MATCHES, matches, allNeutralPointsLastXMatches, playersPresentTime, goals).cache()


    allRankedPointsLastXMatches.show()


    println(s"Calculated data of matches:  $countOfMatches")
    val endMillis = System.currentTimeMillis()
    val runtime =  Duration.ofMillis(endMillis - startMillis).toMinutes
    println(s"Calculating tooks $runtime minutes")

    //scala.io.StdIn.readLine()
  }




}
