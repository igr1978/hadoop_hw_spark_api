package lesson1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object HomeWorkTaxiRDD extends App {
  Logger.getLogger("org").setLevel(Level.OFF)

  val yellow_taxi = "src/main/resources/data/yellow_taxi_jan_25_2018"

  val time_orders = "src/main/resources/data/yellow_taxi_orders_time"

  //RDD
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("spark-api RDD")
    .getOrCreate()

  import spark.implicits._
  import TaxiClasses._

  val taxiFactsDF: DataFrame = spark
    .read
    .parquet(yellow_taxi)
  //.load(yellow_taxi)

  val taxiFactsDS: Dataset[TaxiRide] = taxiFactsDF.as[TaxiRide]
  val taxiFactsRDD: RDD[TaxiRide] = taxiFactsDS.rdd

  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  val hh_mm = DateTimeFormatter.ofPattern("HH:mm")

  val taxiTimeRDD = taxiFactsRDD
    //.map(x => (x.tpep_pickup_datetime.substring(11, 16), 1))
    .map(x => (LocalDateTime.parse(x.tpep_pickup_datetime, formatter).format(hh_mm), 1))
    .reduceByKey(_ + _)
    .sortBy(_._2, false)
    .map(z => z._1 + " " + z._2)
    .coalesce(1, false)

  taxiTimeRDD.take(20).foreach(x => println(x))

  val header: RDD[String] = spark.sparkContext.parallelize(Array("pickup_time count"))
  header.union(taxiTimeRDD)
    .coalesce(1 , true)
    //.saveAsTextFile(time_orders)
    .toDF().write.mode(SaveMode.Overwrite).text(time_orders)

  println("\nA text file created.\n")

  spark.close()

}

