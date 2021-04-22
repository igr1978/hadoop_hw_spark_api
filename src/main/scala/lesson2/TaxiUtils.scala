package lesson2

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object TaxiUtils {

  val yellow_taxi = "src/main/resources/data/yellow_taxi_jan_25_2018"
  val taxi_zones = "src/main/resources/data/taxi_zones.csv"

  val time_orders = "src/main/resources/data/yellow_taxi_orders_time"
  val taxi_orders = "src/main/resources/data/yellow_taxi_orders_count"

  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  val hh_mm = DateTimeFormatter.ofPattern("HH:mm")

  val url = "jdbc:postgresql://localhost:5432/otus"
  val user = "docker"
  val pwd = "docker"
  val driver = "org.postgresql.Driver"

  def readParquet(path: String)(implicit spark: SparkSession): DataFrame = {
    spark
      .read
      .parquet(path)
//      .load(path)
  }

  def readCSV(path: String)(implicit spark: SparkSession):DataFrame = {
    spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
  }

  def saveDFasParquet(df: DataFrame, path : String) = {
    df.repartition(1)
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .parquet(path)
  }

  def saveDFasText(df: DataFrame, path : String) = {
    df.repartition(1)
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .text(path)
  }

  def saveDFasTable(df: DataFrame, table: String) = {
    df.repartition(1)
      .write
      .mode (SaveMode.Overwrite)
      .format("jdbc")
      .option("url", url)
      .option("user", user)
      .option("password", pwd)
      .option("dbtable", table)
//      .options(opts)
      .save()
  }

}
