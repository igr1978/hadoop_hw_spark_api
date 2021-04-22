package lesson1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import java.util.Properties

object HomeWorkTaxiDS extends App {
  Logger.getLogger("org").setLevel(Level.OFF)

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/otus"
  val user = "docker"
  val password = "docker"

  val taxi_zones = "src/main/resources/data/taxi_zones.csv"
  val yellow_taxi = "src/main/resources/data/yellow_taxi_jan_25_2018"

  val connProp = new Properties
  connProp.put ("user", user)
  connProp.put ("password", password)
  connProp.put ("driver", driver)

  val tableA =  "trips_all"
  val tableB =  "trips_by_borough"

  //DataSet
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("spark-api DS")
    .getOrCreate()

  import spark.implicits._
  import TaxiClasses._

  val taxiFactsDF: DataFrame = spark
    .read
    .parquet(yellow_taxi)
  //.load(yellow_taxi)

  val taxiZoneDF: DataFrame = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(taxi_zones)

  val taxiZoneDF_BC: Broadcast[DataFrame] = spark.sparkContext.broadcast(taxiZoneDF)

  val taxiFactsDS: Dataset[TripRideA] = taxiFactsDF.as[TaxiRide]
    .filter(x => x.trip_distance > 0)
    .agg(count("*").alias("trip_count"),
      min($"trip_distance").alias("trip_min"),
      max($"trip_distance").alias("trip_max"),
      round(mean($"trip_distance"),3).alias("trip_mean"),
      round(stddev($"trip_distance"), 3).alias("trip_stddev"))
    .toDF().as[TripRideA]

  println("\nAll trips.")
  taxiFactsDS.show(false)


  val taxiTripDF: DataFrame = taxiFactsDF
    .join(taxiZoneDF_BC.value, $"PULocationID" === $"LocationID")
    .withColumn("trip_from", $"Borough")
    .select($"trip_from", $"trip_distance", $"DOLocationID")
    .join(taxiZoneDF_BC.value, $"DOLocationID" === $"LocationID")
    .withColumn("trip_do", $"Borough")
    .select($"trip_from", $"trip_do", $"trip_distance")

  val taxiTripDS: Dataset[TripRideB] = taxiTripDF.as[TripRide]
    .filter(x => x.trip_distance > 0)
    .groupBy($"trip_from", $"trip_do")
    .agg(count("*").alias("trip_count"),
      min($"trip_distance").alias("trip_min"),
      max($"trip_distance").alias("trip_max"),
      round(mean($"trip_distance"),3).alias("trip_mean"),
      round(stddev($"trip_distance"), 3).alias("trip_stddev"))
    .orderBy($"trip_count".desc)
    .toDF().as[TripRideB]

  println("\nTrips by Borough.")
  taxiTripDS.show(false)


  println("\nWrite data tables to database.")

  taxiFactsDS.write
    .mode (SaveMode.Overwrite)
    .jdbc (url, tableA, connProp)

  taxiTripDS.write
    .mode(SaveMode.Overwrite)
    .jdbc (url, tableB, connProp)

  println("\nA data tables created.")

  spark.close()

}

