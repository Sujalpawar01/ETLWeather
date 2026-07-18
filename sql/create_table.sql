CREATE TABLE IF NOT EXISTS weather (

    id SERIAL PRIMARY KEY,

    city VARCHAR(100),

    latitude DOUBLE PRECISION,

    longitude DOUBLE PRECISION,

    temperature DOUBLE PRECISION,

    windspeed DOUBLE PRECISION,

    winddirection DOUBLE PRECISION,

    weathercode INT,

    weather_time TIMESTAMP,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(city, weather_time)

);



CREATE TABLE raw_weather(

id SERIAL PRIMARY KEY,

city VARCHAR(100),

response JSONB,

created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);