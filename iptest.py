import requests
import socks
import socket
# 设置默认的 SOCKS5 代理
socks.set_default_proxy(socks.SOCKS5, "127.0.0.1", 7890)
socket.socket = socks.socksocket

proxyip = "http://storm-maplin_area-US_city-LosAngeles_life-1:Homycasa2012@proxy.stormip.cn:1000"

proxies = {
    'http': proxyip,
    'https': proxyip,
}

print(requests.get('http://myip.ipip.net/',proxies=proxies).text)