import pandas as pd
import requests
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import geopy
from time import sleep
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
import pickle

proxyip = "http://storm-maplin_area-US_city-LosAngeles_life-1:Homycasa2012@proxy.stormip.cn:1000"
import random
import string

def generate_random_user_agent():
    # 生成一个长度为8的随机字符串，由字母和数字组成
    return ''.join(random.choices(string.ascii_letters + string.digits, k=8))

def get_location(city):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            proxies = {
                'http': proxyip,
                'https': proxyip,
            }
            # 每次调用时随机生成user_agent
            random_user_agent = generate_random_user_agent()
            geolocator = Nominatim(proxies=proxies, user_agent=random_user_agent)
            geocode = RateLimiter(geolocator.geocode, swallow_exceptions=False,max_retries=1)

            location = geocode(city)
            if location:
                city_name = location.raw['display_name'].split(',', 1)[0]
                full_name = location.raw['display_name']
                return city_name, full_name, location.latitude, location.longitude
            else:
                print(f"No location found for {city}")
                return "No location", "No location", "No location", "No location"
        except Exception as e:
            print(
                f"Proxy or GeocoderUnavailable error occurred for {city}. Attempt {attempt + 1} of {max_retries}.")
            if attempt < max_retries - 1:  # 如果不是最后一次尝试，就等待3秒后再次尝试
                sleep(1)
            else:  # 在最后一次尝试后，如果依然失败，就返回None
                return None, None, None, None
if __name__ == '__main__':
    pass