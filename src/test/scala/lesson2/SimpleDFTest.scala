package lesson2

import HomeWorkTaxiDF._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.SharedSparkSession

class SimpleDFTest extends  SharedSparkSession {
  Logger.getLogger("org").setLevel(Level.OFF)

  import TaxiUtils._

  test("Test HomeWorkTaxiDF - loadOrdersTaxiData (join)") {

    val taxiFactsDF = readParquet(yellow_taxi)
    val taxiZoneDF = readCSV(taxi_zones)

    val result = loadOrdersTaxiData(taxiFactsDF, taxiZoneDF)
    checkAnswer(
      result,
      Row("Manhattan", 304266) ::
        Row("Queens", 17712) ::
        Row("Unknown", 6644) ::
        Row("Brooklyn", 3037)::
        Row("Bronx", 211)::
        Row("EWR", 19)::
        Row("Staten Island", 4):: Nil
    )
  }

}