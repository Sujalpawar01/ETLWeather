from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta
import logging
import json

# ----------------------------------------------------
# Configuration
# ----------------------------------------------------

POSTGRES_CONN_ID = "postgres_default"
HTTP_CONN_ID = "open_meteo_api"

logger = logging.getLogger(__name__)

default_args = {
    "owner": "Sujal Pawar",
    "start_date": datetime(2025, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=2)
}

# ----------------------------------------------------
# DAG
# ----------------------------------------------------

with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["weather", "etl", "postgres"],
) as dag:

    # ------------------------------------------------
    # Extract
    # ------------------------------------------------

    @task
    def extract():
        """Extract weather data for configured cities."""

        with open("/usr/local/airflow/include/cities.json") as f:
            cities = json.load(f)

        http = HttpHook(
            method="GET",
            http_conn_id=HTTP_CONN_ID
        )

        weather_data = []

        for city in cities:
            endpoint = (
                f"v1/forecast?"
                f"latitude={city['latitude']}"
                f"&longitude={city['longitude']}"
                f"&current_weather=true"
            )

            logger.info(f"Fetching {city['city']}")

            response = http.run(endpoint)
            response.raise_for_status()

            weather_data.append({
                "city": city["city"],
                "latitude": city["latitude"],
                "longitude": city["longitude"],
                "weather": response.json()
            })

        logger.info("Extraction completed")

        return weather_data

    # ------------------------------------------------
    # Validate
    # ------------------------------------------------

    @task
    def validate(weather_data):

        logger.info("Validating data...")

        validated = []

        for city in weather_data:

            current = city["weather"]["current_weather"]

            if current["temperature"] is None:
                raise ValueError("Temperature missing")

            if current["windspeed"] is None:
                raise ValueError("Windspeed missing")

            if current["time"] is None:
                raise ValueError("Timestamp missing")

            validated.append(city)

        logger.info("Validation successful")

        return validated
        
        
       # ------------------------------------------------
    # Transform
    # ------------------------------------------------

    @task
    def transform(validated_data):

        logger.info("Transforming weather data...")

        transformed = []

        for city in validated_data:

            current = city["weather"]["current_weather"]

            transformed.append({

                "city": city["city"],

                "latitude": city["latitude"],

                "longitude": city["longitude"],

                "temperature": float(current["temperature"]),

                "windspeed": float(current["windspeed"]),

                "winddirection": float(current["winddirection"]),

                "weathercode": int(current["weathercode"]),

                "weather_time": datetime.fromisoformat(current["time"])

            })

        logger.info("Transformation completed")

        return transformed
        # ------------------------------------------------
    # Load
    # ------------------------------------------------
    
    
    
    @task
    def data_quality(transformed_data):

        logger.info("Running data quality checks...")

        for row in transformed_data:

            if row["temperature"] < -100 or row["temperature"] > 70:
                raise ValueError(
                    f"Invalid temperature: {row['temperature']}"
                )

            if row["windspeed"] < 0:
                raise ValueError(
                    f"Invalid windspeed: {row['windspeed']}"
                )

            if row["city"] == "":
                raise ValueError("City cannot be empty")

            if row["weather_time"] is None:
                raise ValueError("Timestamp missing")

        logger.info("Data quality passed.")

        return transformed_data

    
    @task
    def load(transformed_data):
        logger.info("Loading data into PostgreSQL...")

        postgres = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        conn = postgres.get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather(
                id SERIAL PRIMARY KEY,
                city VARCHAR(100),
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                temperature DOUBLE PRECISION,
                windspeed DOUBLE PRECISION,
                winddirection DOUBLE PRECISION,
                weathercode INT,
                weather_time TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """)

            cursor.execute("ALTER TABLE weather ADD COLUMN IF NOT EXISTS city VARCHAR(100);")
            cursor.execute("ALTER TABLE weather ADD COLUMN IF NOT EXISTS latitude DOUBLE PRECISION;")
            cursor.execute("ALTER TABLE weather ADD COLUMN IF NOT EXISTS longitude DOUBLE PRECISION;")
            cursor.execute("ALTER TABLE weather ADD COLUMN IF NOT EXISTS temperature DOUBLE PRECISION;")
            cursor.execute("ALTER TABLE weather ADD COLUMN IF NOT EXISTS windspeed DOUBLE PRECISION;")
            cursor.execute("ALTER TABLE weather ADD COLUMN IF NOT EXISTS winddirection DOUBLE PRECISION;")
            cursor.execute("ALTER TABLE weather ADD COLUMN IF NOT EXISTS weathercode INT;")
            cursor.execute("ALTER TABLE weather ADD COLUMN IF NOT EXISTS weather_time TIMESTAMP;")
            cursor.execute("ALTER TABLE weather ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS weather_city_weather_time_idx
                ON weather (city, weather_time);
            """)

            for row in transformed_data:
                cursor.execute("""
                    INSERT INTO weather(
                        city,
                        latitude,
                        longitude,
                        temperature,
                        windspeed,
                        winddirection,
                        weathercode,
                        weather_time
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT(city, weather_time)
                    DO UPDATE SET
                        temperature = EXCLUDED.temperature,
                        windspeed = EXCLUDED.windspeed,
                        winddirection = EXCLUDED.winddirection,
                        weathercode = EXCLUDED.weathercode;
                """,
                (
                    row["city"],
                    row["latitude"],
                    row["longitude"],
                    row["temperature"],
                    row["windspeed"],
                    row["winddirection"],
                    row["weathercode"],
                    row["weather_time"],
                ))

            conn.commit()
            logger.info("Data loaded successfully.")

        except Exception as e:
            conn.rollback()
            logger.exception("Load failed: %s", e)
            raise

        finally:
            cursor.close()
            conn.close()

    #DAG workflow - ETL pipeline
    weather = extract()

    validated = validate(weather)

    transformed = transform(validated)

    quality = data_quality(transformed)

    load(quality)

            
        