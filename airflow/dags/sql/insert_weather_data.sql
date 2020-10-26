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
    where n.date != existing.date
);
