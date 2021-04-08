
DROP DATABASE IF EXISTS otus;
CREATE DATABASE otus;

CREATE TABLE trips_all (
    trip_count        INTEGER,
    trip_min          DOUBLE,
    trip_max          DOUBLE,
    trip_mean         DOUBLE,
    trip_stddev       DOUBLE
);

CREATE TABLE trips_by_borough (
    trip_from         VARCHAR (150),
    trip_do           VARCHAR (150),
    trip_count        INTEGER,
    trip_min          DOUBLE,
    trip_max          DOUBLE,
    trip_mean         DOUBLE,
    trip_stddev       DOUBLE
);
