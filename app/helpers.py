from config import Config


def get_appid():
    config = Config()
    api_key = config.weather_api_key
    return api_key
