package lesson2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object HomeWorkTaxiDS extends App {
  Logger.getLogger("org").setLevel(Level.OFF)

  val tableA =  "trips_all"
  val tableB =  "trips_by_borough"

  import TaxiUtils._
  import TaxiClasses._

  //DataSet
  lazy implicit val spark = SparkSession
    .builder()
    .master("local")
    .appName("spark-api DS")
    .getOrCreate()

  import spark.implicits._
  def loadAggrTaxiData(factsDF: DataFrame): Dataset[TripRideA] = {
    val ds: Dataset[TaxiRide] = factsDF.as[TaxiRide]
    ds
      .filter(x => x.trip_distance > 0)
      .agg(count("*").alias("trip_count"),
        min($"trip_distance").alias("trip_min"),
        max($"trip_distance").alias("trip_max"),
        round(mean($"trip_distance"),3).alias("trip_mean"),
        round(stddev($"trip_distance"), 3).alias("trip_stddev"))
      .toDF().as[TripRideA]
  }

  def loadAggrTaxiData(factsDF: DataFrame, zoneDF: DataFrame): Dataset[TripRideB] = {
    val zoneDF_BC: Broadcast[DataFrame] = spark.sparkContext.broadcast(zoneDF)

    val df: DataFrame = factsDF
      .join(zoneDF_BC.value, $"PULocationID" === $"LocationID")
      .withColumn("trip_from", $"Borough")
      .select($"trip_from", $"trip_distance", $"DOLocationID")
      .join(zoneDF_BC.value, $"DOLocationID" === $"LocationID")
      .withColumn("trip_do", $"Borough")
      .select($"trip_from", $"trip_do", $"trip_distance")

    val ds: Dataset[TripRide] = df.as[TripRide]
    ds
      .filter(x => x.trip_distance > 0)
      .groupBy($"trip_from", $"trip_do")
      .agg(count("*").alias("trip_count"),
        min($"trip_distance").alias("trip_min"),
        max($"trip_distance").alias("trip_max"),
        round(mean($"trip_distance"),3).alias("trip_mean"),
        round(stddev($"trip_distance"), 3).alias("trip_stddev"))
      .orderBy($"trip_count".desc)
      .toDF().as[TripRideB]
  }

  val taxiFactsDF: DataFrame = readParquet(yellow_taxi)
  val taxiZoneDF: DataFrame = readCSV(taxi_zones)

  println("\nAll trips.")
  val taxiFactsDS: Dataset[TripRideA] = loadAggrTaxiData(taxiFactsDF)
  taxiFactsDS.show(false)

  println("\nTrips by Borough.")
  val taxiTripDS: Dataset[TripRideB] = loadAggrTaxiData(taxiFactsDF, taxiZoneDF)
  taxiTripDS.show(false)

  println("\nWrite data tables to database.")
  saveDFasTable(taxiFactsDS.toDF(), tableA)

  saveDFasTable(taxiTripDS.toDF(), tableB)
  println("\nA data tables created.")

  spark.close()

}

