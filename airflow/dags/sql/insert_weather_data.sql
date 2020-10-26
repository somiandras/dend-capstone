/* Insert new weather data from staging table. Staging table is
templated for execution date. */

insert into analytics.weather
(
    select
        n.date,
        n.awnd as wind,
        n.prcp as precip,
        n.snow,
        n.tmax,
        n.tmin
    from stage."weather_{{ ds }}" as n
    left join analytics.weather as existing using(date)
    where existing.date is null
);
