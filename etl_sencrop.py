import requests
import json
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os
from datetime import datetime

#logging setup
log_file = "script_logs.log"
def log_message(message):
   timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
   with open(log_file, "a") as f:
       f.write(f"[{timestamp}] {message}\n")

try:
    log_message("Starting script execution")

    load_dotenv()  # Load the .env file
    api_key = os.getenv("WEATHERSTACK_API_KEY")
    if not api_key:
        raise ValueError("API key not found. Check your .env file.")

    # Fetching the weather data of Ikorodu, Lagos, using Latitude & Longititude from weatherstack
    url = f"https://api.weatherstack.com/current?access_key={api_key}"

    querystring = {"query":"6.510739384596194, 3.6162058548917946"}

    response = requests.get(url, params=querystring)

    #print(response.json())
    log_message("Extracted current weather condition successfully")


    # Write the json data into a json file
    data = response.json()

    filename = 'weather_report.json'

    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)



    # Load JSON data from file
    with open("weather_report.json", "r") as file:
        data = json.load(file)

    # Preprocess arrays by flattening them into strings
    def flatten_arrays(obj):
        if isinstance(obj, dict):
            return {k: flatten_arrays(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return ', '.join(map(str, obj))  # Convert lists to comma-separated strings
        return obj

    # Flatten arrays in the JSON
    data_flattened = flatten_arrays(data)

    # Normalize the JSON data into a flat DataFrame
    df = pd.json_normalize(data_flattened)

    # Rename Columns
    df.rename(columns={
        'location.name' : 'location_name',
        'location.country' : 'country',
        'location.region' : 'region',
        'location.lat' : 'latitude',
        'location.lon' : 'longititude',
        'location.timezone_id' : 'timezone_id',
        'location.localtime' : 'localtime',
        'location.localtime_epoch' : 'localtime_epoch',
        'location.utc_offset' : 'utc_offset',
        'current.observation_time' : 'observation_time',
        'current.temperature' : 'temperature',
        'current.weather_code' : 'weather_code',
        'current.weather_icons' : 'weather_icons',
        'current.weather_descriptions' : 'weather_descriptions',
        'current.wind_speed' : 'wind_speed',
        'current.wind_degree' : 'wind_degree',
        'current.wind_dir' : 'wind_dir',
        'current.pressure' : 'pressure',
        'current.precip' : 'precip',
        'current.humidity' : 'humidity',
        'current.cloudcover' : 'cloudcover',
        'current.feelslike' : 'feelslike',
        'current.uv_index' : 'uv_index',
        'current.visibility' : 'visibility',
        'current.is_day' : 'is_day'
    }, inplace=True)

    #Convert LocalTime datatype to date
    df['localtime'] = pd.to_datetime(df['localtime'])

    # Take Necessary data
    data_df = df[['location_name', 'country', 'region', 'latitude', 'longititude','timezone_id', 'localtime', 'localtime_epoch', 'utc_offset','observation_time', 'temperature', 'weather_code', 'weather_icons','weather_descriptions', 'wind_speed', 'wind_degree', 'wind_dir','pressure', 'precip', 'humidity', 'cloudcover', 'feelslike', 'uv_index','visibility', 'is_day']]


    #Extract Location data from dataframe
    location = data_df[['location_name', 'country', 'region', 'latitude', 'longititude', 'timezone_id']].copy().reset_index(drop=True)
    location['location_id'] = range(1, len(location) + 1)
    location = location[['location_id','location_name', 'country', 'region', 'latitude', 'longititude','timezone_id']]
    log_message("Extracted and saved 'location' table data")

    #WeatherDescription Table
    weatherDec = data_df[['weather_code', 'weather_icons', 'weather_descriptions']].copy().reset_index(drop=True)
    weatherDec['weatherDec_id'] = range(1, len(weatherDec) + 1)
    weatherDec = weatherDec[['weatherDec_id','weather_code', 'weather_icons', 'weather_descriptions']]
    log_message("Extracted and saved 'weatherDec' table data")

    # Weather fact table
    weather_fact_table = data_df.merge(location, on=['location_name', 'country', 'region', 'latitude', 'longititude','timezone_id'], how='left') \
                            .merge(weatherDec, on=['weather_code', 'weather_icons', 'weather_descriptions'], how='left') \
                            [['location_id','weatherDec_id','localtime','localtime_epoch','utc_offset','observation_time','temperature','wind_speed','wind_degree','wind_dir','pressure', 'precip', 'humidity', 'cloudcover', 'feelslike', 'uv_index','visibility', 'is_day']].copy().reset_index(drop=True)
    weather_fact_table['Unique_id'] = range(1, len(weather_fact_table) + 1)
    weather_fact_table = weather_fact_table[['Unique_id','location_id','weatherDec_id','localtime','localtime_epoch','utc_offset','observation_time','temperature','wind_speed','wind_degree','wind_dir','pressure', 'precip', 'humidity', 'cloudcover', 'feelslike', 'uv_index','visibility', 'is_day']]
    log_message("Extracted and saved 'weather fact table' table data")


    # Loading to CSV
    location.to_csv('location.csv')
    weatherDec.to_csv('weatherDec.csv')
    weather_fact_table.to_csv('weather_fact_table.csv')

    # Develop a function to get the Database connection
    # Develop a function to get the Database connection
    def get_db_connection():
        connection = psycopg2.connect(
            host = os.getenv("HOST"),
            database = os.getenv("DATABASE"),
            user = os.getenv("USER"),
            port = os.getenv("PORT"),
            password = os.getenv("PASSWORD")
        )
        return connection

    #connect to our database
    conn = get_db_connection()
    log_message("Database connected successfully")


    # Create a function that setups the schema and tables
    def create_tables():
        conn = get_db_connection()
        cursor = conn.cursor()
        create_table_query = '''
                        CREATE SCHEMA IF NOT EXISTS sencrop;

                        CREATE TABLE IF NOT EXISTS sencrop.location (
                            location_id SERIAL PRIMARY KEY,
                            location_name VARCHAR(100000),
                            country VARCHAR(100000),
                            region VARCHAR(100000),
                            latitude VARCHAR(100000),
                            longititude VARCHAR(100000),
                            timezone_id VARCHAR(10000)
                        );

                        CREATE TABLE IF NOT EXISTS sencrop.weatherDec (
                            weatherDec_id SERIAL PRIMARY KEY,
                            weather_code INTEGER,
                            weather_icons VARCHAR(100000),
                            weather_descriptions VARCHAR(100000)
                        );

                        CREATE TABLE IF NOT EXISTS sencrop.weather_fact_table (
                            Unique_id SERIAL PRIMARY KEY,
                            location_id INT,
                            weatherDec_id INT,
                            local_time TIMESTAMP,
                            localtime_epoch INT,
                            utc_offset VARCHAR(100000),
                            observation_time VARCHAR(100000),
                            temperature INT,
                            wind_speed INT,
                            wind_degree INT,
                            wind_dir VARCHAR(100000),
                            pressure INT,
                            precip INT,
                            humidity INT,
                            cloudcover INT,
                            feelslike INT,
                            uv_index INT,
                            visibility INT,
                            is_day VARCHAR(100000),
                            FOREIGN KEY (location_id) REFERENCES sencrop.location(location_id),
                            FOREIGN KEY (weatherDec_id) REFERENCES sencrop.weatherDec(weatherDec_id)
                        );
                            '''
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        conn.close()

    create_tables()
    log_message("Schema and table created successfully")


    ## Loading the data into the db tables
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('SELECT MAX(location_id) FROM sencrop.location')
    last_id = cursor.fetchone()[0] or 0  # If table is empty, start from 0

    # Iterate through rows and insert with incremented IDs
    for i, (_, row) in enumerate(location.iterrows(), start=1):
        new_id = last_id + i
        cursor.execute(
            '''
            INSERT INTO sencrop.location(location_id, location_name, country, region, latitude, longititude, timezone_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''',
            (new_id, row['location_name'], row['country'], row['region'], row['latitude'], row['longititude'], row['timezone_id'])
        )

    for i, (_, row) in enumerate(weatherDec.iterrows(), start=1):
        new_id = last_id + i
        cursor.execute(
            '''INSERT INTO sencrop.weatherDec(weatherDec_id, weather_code, weather_icons,weather_descriptions)
            VALUES (%s, %s, %s, %s)''',
            (new_id, row['weather_code'], row['weather_icons'], row['weather_descriptions'])
        )

    for i, (_, row) in enumerate(weather_fact_table.iterrows(), start=1):
        new_id = last_id + i
        cursor.execute(
            '''INSERT INTO sencrop.weather_fact_table(Unique_id, location_id, weatherDec_id, local_time,localtime_epoch, utc_offset, observation_time, temperature, wind_speed, wind_degree, wind_dir, pressure, precip, humidity, cloudcover, feelslike, uv_index, visibility,is_day)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
            (new_id, row['location_id'], row['weatherDec_id'], row['localtime'], row['localtime_epoch'], row['utc_offset'], row['observation_time'], row['temperature'], row['wind_speed'], row['wind_degree'], row['wind_dir'], row['pressure'], row['precip'], row['humidity'], row['cloudcover'], row['feelslike'], row['uv_index'], row['visibility'], row['is_day'])
        )
    # Commit changes
    conn.commit()

    #close connection
    cursor.close()
    conn.close()
    log_message("Script execution completed successfully")
except Exception as e:
    log_message(f"An error occured: {str(e)}")