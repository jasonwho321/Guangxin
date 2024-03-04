import pandas as pd
import requests
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import geopy
from time import sleep
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
import pickle
from geopy.exc import GeocoderTimedOut
import requests
proxyip = "http://storm-maplin_area-US_city-LosAngeles_life-1:Homycasa2012@proxy.stormip.cn:1000"
import random
import string

def generate_random_user_agent():
    # 生成一个长度为8的随机字符串，由字母和数字组成
    return ''.join(random.choices(string.ascii_letters + string.digits, k=8))

def get_location_by_coordinates_1(lat, lon):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            proxies = {
                'http': proxyip,
                'https': proxyip,
            }
            random_user_agent = generate_random_user_agent()
            geolocator = Nominatim(proxies=proxies, user_agent=random_user_agent)
            location = geolocator.reverse((lat, lon), language='en')
            address = location.raw['address']
            state = address.get('state', "")
            return state if state else "No Found"
        except (Exception, GeocoderTimedOut) as e:  # 捕获所有异常以及超时异常
            print(f"Proxy or GeocoderUnavailable error occurred for coordinates ({lat}, {lon}). Attempt {attempt + 1} of {max_retries}.")
            if attempt < max_retries - 1:  # 如果不是最后一次尝试，就等待3秒后再次尝试
                sleep(1)
            else:  # 在最后一次尝试后，如果依然失败，就返回 "No Found"
                return "No Found"

def get_location_by_coordinates(row_tuple):
    _, row = row_tuple  # iterrows返回的是(index, Series)元组，我们需要的是第二个元素
    lat = row['latitude']
    lon = row['longitude']
    return get_location_by_coordinates_1(lat, lon)

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

def get_location_google(city):
    api_key = 'AIzaSyBar_MnPWlKrnhjyWU6tr9X9lXkpHs8QPU'
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # 构建请求URL
            base_url = f"https://maps.googleapis.com/maps/api/geocode/json?address={city}&key={api_key}"
            # 使用代理
            proxies = {
                'http': proxyip,
                'https': proxyip,
            }
            # 发起请求
            response = requests.get(base_url, proxies=proxies)
            result = response.json()

            if result['status'] == 'OK':
                location = result['results'][0]
                city_name = location['address_components'][0]['long_name']
                full_name = location['formatted_address']
                latitude = location['geometry']['location']['lat']
                longitude = location['geometry']['location']['lng']
                return city_name, full_name, latitude, longitude
            else:
                print(f"No location found for {city}")
                return "No location", "No location", "No location", "No location"
        except Exception as e:
            print(
                f"Request error occurred for {city}. Attempt {attempt + 1} of {max_retries}.")
            if attempt < max_retries - 1:
                sleep(1)
            else:
                return None, None, None, None

if __name__ == '__main__':
    print(get_location_google('10270 Philadelphia Court, Rancho Cucamonga, CA91730'))
    pass