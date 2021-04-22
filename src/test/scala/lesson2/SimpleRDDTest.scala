package lesson2

import HomeWorkTaxiRDD._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec

class SimpleRDDTest extends AnyFlatSpec{
  Logger.getLogger("org").setLevel(Level.OFF)

  import TaxiUtils._

  it should "Test HomeWorkTaxiRDD - loadTimeTaxiData" in {
    val taxiFactsDF: DataFrame = readParquet(yellow_taxi)

    val result = loadTimeTaxiData(taxiFactsDF)
      .map(x => x.split(" "))
      .collect()

    assert(result.size > 0)
    assert(result.take(10).length === 10)
    assert(result.take(1)(0) === Array("20:27", "408"))
  }

}
