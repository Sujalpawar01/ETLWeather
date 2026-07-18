SELECT *
FROM weather
ORDER BY weather_time DESC;


SELECT city,
AVG(temperature) AS avg_temperature
FROM weather
GROUP BY city;

SELECT city,
MAX(windspeed)
FROM weather
GROUP BY city;

SELECT city,
MAX(temperature)
FROM weather
GROUP BY city
ORDER BY MAX(temperature) DESC;

SELECT

DATE(weather_time),

AVG(temperature)

FROM weather

GROUP BY DATE(weather_time)

ORDER BY DATE(weather_time);