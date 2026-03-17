select
    city,
    date(timestamp) as observation_date,
    avg(temperature_2m) as avg_temperature_2m,
    avg(relative_humidity_2m) as avg_relative_humidity_2m,
    count(*) as observation_count
from weather_hourly_bronze
group by 1, 2
order by 2, 1;
