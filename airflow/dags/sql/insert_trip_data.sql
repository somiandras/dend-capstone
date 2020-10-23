/* Transformed columns from stage for upserting into analytics */
create temp table trip_transformed
distkey(pickup_date)
as (
    -- Removing trips that either have chargeback counter events or
    -- or have missing distance/provider
    with valid_trips as (
        select trip1.*
        from (
            select *
            from stage.trip
            where total_amount > 0
        ) trip1
        left join (
            select *
            from stage.trip
            where total_amount < 0
        ) trip2 using(tpep_pickup_datetime, tpep_dropoff_datetime, pulocationid, dolocationid)
        where
            trip2.tpep_pickup_datetime is null and
            trip1.vendorid is not null
    )
    select
        md5(
            tpep_pickup_datetime ||
            tpep_dropoff_datetime ||
            pulocationid ||
            dolocationid
        ) as trip_id,
        passenger_count,
        trip_distance,
        datediff(second, tpep_pickup_datetime, tpep_dropoff_datetime) as trip_duration_sec,
        date_trunc('day', tpep_pickup_datetime)::date as pickup_date,
        pulocationid as pickup_location_id,
        dolocationid as dropoff_location_id,
        case
            when ratecodeid = 1 then 'Standard rate'
            when ratecodeid = 2 then 'JFK'
            when ratecodeid = 3 then 'Newark'
            when ratecodeid = 4 then 'Nassau or Westchester'
            when ratecodeid = 5 then 'Negotiated fare'
            when ratecodeid = 6 then 'Group ride'
            else 'Unknown'
        end as rate_code,
        case
            when payment_type = 1 then 'Credit card'
            when payment_type = 2 then 'Cash'
            when payment_type = 3 then 'No charge'
            when payment_type = 4 then 'Dispute'
            when payment_type = 5 then 'Unknown'
            when payment_type = 6 then 'Voided trip'
        end as payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        total_amount,
        (
            fare_amount
            + extra
            + mta_tax
            + tip_amount
            + tolls_amount
            + improvement_surcharge
            + congestion_surcharge
        ) = total_amount as total_amount_check
    from valid_trips
);

/* Delete rows from existing that are also in new */
delete from analytics.trip
using trip_transformed
where analytics.trip.trip_id = trip_transformed.trip_id;

/* Insert all new rows */
insert into analytics.trip
select *
from trip_transformed;

/* Drop temp table for good hygiene */
drop table if exists trip_transformed;