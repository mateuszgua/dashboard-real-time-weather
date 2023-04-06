import requests
import time
import json


from kafka_producer import MessageProducer
from helpers import get_appid
from config import Config

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


while True:
    city_name = "Warsaw"
    units = "metric"
    appid = get_appid()
    openweathermap_api_endpoint = f"http://api.openweathermap.org/data/2.5/weather?appid={appid}&q={city_name}&units={units}"
    json_message = get_weather_detail(openweathermap_api_endpoint)
    config = Config()
    message_producer = MessageProducer(config.broker, config.topic)
    message_producer.send_msg(json_message)
    print("Published message 1: " + json.dumps(json_message))
    print("Wait for 60 seconds ...")
    time.sleep(60)
