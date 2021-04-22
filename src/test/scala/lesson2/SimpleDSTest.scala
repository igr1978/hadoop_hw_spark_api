package lesson2

import HomeWorkTaxiDS._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class SimpleDSTest extends SharedSparkSession{
  Logger.getLogger("org").setLevel(Level.OFF)

  import TaxiUtils._

  test("Test HomeWorkTaxiDS - loadAggrTaxiData") {
    val taxiFactsDF = readParquet(yellow_taxi)

    val result = loadAggrTaxiData(taxiFactsDF)
    result.collect().foreach { r =>
       (r.trip_count > 0) shouldBe true
       (r.trip_min < r.trip_max) shouldBe true
       (r.trip_min < r.trip_mean) shouldBe true
       (r.trip_max > r.trip_mean) shouldBe true
    }
//    result.foreach(println(_))
  }

}