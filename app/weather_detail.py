import requests
import time
import json
import os

from dotenv import load_dotenv
from os import path

from kafka_producer import MessageProducer

basedir = path.abspath(path.dirname(__file__))
load_dotenv(path.join(basedir, '.env'))

json_message = None
city_name = None
temperature = None
humidity = None
openweathermap_api_endpoint = None
appid = None


def get_weather_detail(openweathermap_api_endpoint):
    api_response = requests.get(openweathermap_api_endpoint)
    json_data = api_response.json()
    city_name = json_data["name"]
    humidity = json_data["main"]["humidity"]
    temperature = json_data["main"]["temp"]
    json_message = {"CityName": city_name,
                    "Temperature": temperature,
                    "Humidity": humidity,
                    "CreationTime": time.strftime("%Y-%m-%d %H:%M:%S")}
    return json_message
