package lesson2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

object HomeWorkTaxiDF extends App {
  Logger.getLogger("org").setLevel(Level.OFF)

//  val taxiFactsDF = spark.read.load(yellow_taxi)
//  taxiFactsDF.printSchema()
//  println(taxiFactsDF.count())

  import TaxiUtils._

  //DataFrame
  lazy implicit val spark = SparkSession
    .builder()
    .master("local")
    .appName("spark-api DF")
    .getOrCreate()

  import spark.implicits._
  def loadOrdersTaxiData(factsDF: DataFrame, zoneDF: DataFrame): DataFrame = {
    factsDF
      .join(broadcast(zoneDF), $"PULocationID" === $"LocationID")
      .filter($"Borough".isNotNull)
      .groupBy($"Borough")
      .count()
      .orderBy($"count".desc)
  }

  val taxiFactsDF: DataFrame = readParquet(yellow_taxi)
  val taxiZoneDF: DataFrame = readCSV(taxi_zones)

  val taxiOrdersDF: DataFrame = loadOrdersTaxiData(taxiFactsDF, taxiZoneDF)
  taxiOrdersDF.show(false)

  saveDFasParquet(taxiOrdersDF, taxi_orders)
  println("\nA parquet file created.\n")

  spark.close()

}

