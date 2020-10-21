insert into analytics.weather
(
    select
        n.date,
        n.awnd as wind,
        n.prcp as precip,
        n.snow,
        n.tmax,
        n.tmin
    from stage.weather as n
    left join analytics.weather as existing using(date)
    where n.station = 'USW00094728' and n.date != existing.date
);
