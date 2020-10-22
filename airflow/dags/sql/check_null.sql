select count(*)
from stage.weather
where date is null;

select count(*)
from stage.trip
where
    trip_distance is null or
    tpep_pickup_datetime is null or
    tpep_dropoff_datetime is null or
    PULocationID is null or
    DOLocationID is null or
    total_amount is null;

select count(*)
from stage.zone
where
    LocationID is null or
    Borough is null or
    Zone is null or
    service_zone is null;
