import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object HomeWorkTaxiDF extends App {
  Logger.getLogger("org").setLevel(Level.OFF)

  val taxi_zones = "src/main/resources/data/taxi_zones.csv"
  val yellow_taxi = "src/main/resources/data/yellow_taxi_jan_25_2018"

  val taxi_orders = "src/main/resources/data/yellow_taxi_orders_count"

//  val taxiFactsDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
//  taxiFactsDF.printSchema()
//  println(taxiFactsDF.count())

  //DataFrame
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("spark-api DF")
    .getOrCreate()

  import spark.implicits._

  val taxiFactsDF: DataFrame = spark
    .read
    .parquet(yellow_taxi)
    //.load(yellow_taxi)

  val taxiZoneDF: DataFrame = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(taxi_zones)

  val taxiOrdersDF: DataFrame = taxiFactsDF
    .join(broadcast(taxiZoneDF), $"PULocationID" === $"LocationID")
    .filter($"Borough".isNotNull)
    .groupBy($"Borough")
    .count()
    .orderBy($"count".desc)

  taxiOrdersDF.show(false)

  taxiOrdersDF.repartition(1)
    .write
    .option("header", "true")
    .mode(SaveMode.Overwrite)
    .parquet(taxi_orders)

  println("\nA parquet file created.\n")

  spark.close()

}

