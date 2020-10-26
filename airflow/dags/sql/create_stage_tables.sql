drop table if exists stage."weather_{{ ds }}";

drop table if exists stage."trip_{{ ds }}";

drop table if exists stage."zone_{{ ds }}";

create table if not exists stage."weather_{{ ds }}" (
    station varchar,
    name varchar,
    date timestamp,
    awnd float,
    prcp float,
    snow float,
    tmax float,
    tmin float
);

create table if not exists stage."zone_{{ ds }}" (
    LocationID int,
    Borough varchar,
    Zone varchar,
    service_zone varchar
);

create table if not exists stage."trip_{{ ds }}" (
    VendorID int,
    tpep_pickup_datetime timestamp,
    tpep_dropoff_datetime timestamp,
    passenger_count int,
    trip_distance float,
    RatecodeID int,
    store_and_fwd_flag boolean,
    PULocationID int,
    DOLocationID int,
    payment_type int,
    fare_amount float,
    extra float,
    mta_tax float,
    tip_amount float,
    tolls_amount float,
    improvement_surcharge float,
    total_amount float,
    congestion_surcharge float
);