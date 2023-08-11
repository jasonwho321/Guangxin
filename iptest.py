import requests
proxyip = "http://storm-maplin_area-US_city-LosAngeles_life-1:Homycasa2012@proxy.stormip.cn:1000"

proxies = {
    'http': proxyip,
    'https': proxyip,
}
print(requests.get('http://myip.ipip.net/', proxies=proxies).text)


# print(requests.get('http://httpbin.org/get'))