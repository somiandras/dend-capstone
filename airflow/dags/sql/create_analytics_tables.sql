/* Create final data tables under analytics schema */

create table if not exists analytics.zone (
    location_id int primary key,
    borough varchar,
    zone varchar,
    service_zone varchar
) diststyle all;

create table if not exists analytics.weather(
    date date primary key sortkey distkey,
    wind float,
    precip float,
    snow float,
    tmax float,
    tmin float
);

create table if not exists analytics.trip (
    trip_id varchar primary key,
    passenger_count int,
    trip_distance float not null,
    trip_duration_sec int not null,
    pickup_date date not null references analytics.weather(date) sortkey distkey,
    pickup_location_id int references analytics.zone(location_id) not null,
    dropoff_location_id int references analytics.zone(location_id) not null,
    rate_code varchar,
    payment_type varchar,
    fare_amount float,
    extra float,
    mta_tax float,
    tip_amount float,
    tolls_amount float,
    improvement_surcharge float,
    congestion_surcharge float,
    total_amount float not null,
    total_amount_check boolean
);