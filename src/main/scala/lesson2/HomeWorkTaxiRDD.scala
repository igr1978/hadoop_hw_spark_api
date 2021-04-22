package lesson2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

object HomeWorkTaxiRDD extends App {
  Logger.getLogger("org").setLevel(Level.OFF)

  import TaxiUtils._
  import TaxiClasses._

  //RDD
  lazy implicit val spark = SparkSession
    .builder()
    .master("local")
    .appName("spark-api RDD")
    .getOrCreate()

  import spark.implicits._
  def loadTimeTaxiData(factsDF: DataFrame): RDD[String] = {
    val factsRDD: RDD[TaxiRide] = factsDF.as[TaxiRide].rdd
    factsRDD
      //.map(x => (x.tpep_pickup_datetime.substring(11, 16), 1))
      .map(x => (LocalDateTime.parse(x.tpep_pickup_datetime, formatter).format(hh_mm), 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .map(z => z._1 + " " + z._2)
      .coalesce(1, false)
  }

  def buildTimeTaxiData(timeRDD: RDD[String], headerRDD: String): DataFrame = {
    val header: RDD[String] = spark.sparkContext.parallelize(Array(headerRDD))
    header.union(timeRDD)
      .coalesce(1, true)
      .toDF()
  }

  val taxiFactsDF: DataFrame = readParquet(yellow_taxi)

  val taxiTimeRDD = loadTimeTaxiData(taxiFactsDF)
  val taxiTimeDF: DataFrame = buildTimeTaxiData(taxiTimeRDD, "pickup_time count")
  taxiTimeDF.show(false)

  saveDFasText(taxiTimeDF, time_orders)
  println("\nA text file created.\n")

  spark.close()

}

