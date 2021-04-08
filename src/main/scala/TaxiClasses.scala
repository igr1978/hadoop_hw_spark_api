object TaxiClasses {

  case class TaxiZone(LocationID: Int,
                      Borough: String,
                      Zone: String,
                      service_zone: String)

  case class TaxiRide( VendorID: Int,
                       tpep_pickup_datetime: String,
                       tpep_dropoff_datetime: String,
                       passenger_count: Int,
                       trip_distance: Double,
                       RatecodeID: Int,
                       store_and_fwd_flag: String,
                       PULocationID: Int,
                       DOLocationID: Int,
                       payment_type: Int,
                       fare_amount: Double,
                       extra: Double,
                       mta_tax: Double,
                       tip_amount: Double,
                       tolls_amount: Double,
                       improvement_surcharge: Double,
                       total_amount: Double)

  case class TripRide( trip_from: String,
                       trip_do: String,
                       trip_distance: Double)

  case class TripRideA( trip_count: BigInt,
                          trip_min: Double,
                          trip_max: Double,
                          trip_mean: Double,
                          trip_stddev: Double)

  case class TripRideB( trip_from: String,
                          trip_do: String,
                          trip_count: BigInt,
                          trip_min: Double,
                          trip_max: Double,
                          trip_mean: Double,
                          trip_stddev: Double)

}
