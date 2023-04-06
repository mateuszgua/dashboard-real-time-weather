import os

from dotenv import load_dotenv
from os import path

basedir = path.abspath(path.dirname(__file__))
load_dotenv(path.join(basedir, '.env'))


class Config:
    weather_api_key = os.getenv('WEATHER_KEY')
    broker = os.getenv('BROKER')
    topic = os.getenv('TOPIC')
