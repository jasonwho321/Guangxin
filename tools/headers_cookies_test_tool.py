from time import sleep

import requests
from urllib.parse import urlparse, parse_qs
import json
from copy import deepcopy

from bs4 import BeautifulSoup

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
def get_minimal_data_set(data, url, headers):
    def test_key(new_data, key_path):
        sleep(3)
        temp_data = deepcopy(new_data)
        dict_to_update = temp_data
        for key in key_path[:-1]:  # drill down to the next level dictionary
            dict_to_update = dict_to_update[key]
        # delete the key
        dict_to_update.pop(key_path[-1], None)
        response = requests.post(url, data=json.dumps({"variables": temp_data}), headers=headers)

        # 对响应体进行JSON解析
        response_json = response.json()

        print(response_json)
        if response.status_code != 200 or (
                "errors" in response_json and
                response_json["errors"][0]["extensions"]["code"] == "INTERNAL_SERVER_ERROR"
        ) or (
                "data" in response_json and
                "contentLayout" in response_json["data"] and not response_json["data"]["contentLayout"]["modules"] and
                not response_json["data"]["contentLayout"]["pageMetadata"]["p13nMetadata"]
        ):
            print(f'Something went wrong while {key_path}')
        else:
            print(f'fine to delete {key_path}')
            new_data = temp_data
        return new_data

    def deep_keys(data, prefix=[]):
        if isinstance(data, dict):
            for k, v in data.items():
                if isinstance(v, dict):
                    yield from deep_keys(v, prefix=prefix + [k])
                else:
                    yield prefix + [k]

    new_data = deepcopy(data)
    for key_path in deep_keys(data):
        new_data = test_key(new_data, key_path)

    return new_data

def extract_variables(url):
    # 解析URL并提取查询字符串
    parsed_url = urlparse(url)
    params = parse_qs(parsed_url.query)

    # 提取variables参数，并解码
    variables_param = params.get('variables', [])
    if variables_param:
        # 取出列表的第一个元素，然后进行URL解码和JSON解析
        variables_str = variables_param[0]
        variables = json.loads(variables_str)
        return variables
    else:
        return None


def test_headers(headers, url, fail_text):
    min_headers = headers.copy()
    for key in headers:
        min_headers.pop(key, None)
        sleep(3)
        response = requests.get(url, headers=min_headers)
        print(response.text)
        # 如果删除key后的请求失败，则将这个key再放回去
        if response.status_code != 200 or fail_text in response.text:
            min_headers[key] = headers[key]
    return min_headers
def walmart_recommendation():
    headers = {'Content-Type': 'application/json',
               'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
               'X-Apollo-Operation-Name': 'ItemByIdBtf', 'X-O-Platform': 'rweb',
               'X-O-Platform-Version': 'us-web-1.106.1-a4825f50961cb2cde8a88357cc99e8ada972f5fa-1214',
               'X-O-Segment': 'oaoh'}

    # url = 'https://www.walmart.com/orchestra/pdp/graphql/ItemByIdBtf/74215a212deaf10003e79fd6668b072ecd7c08dca644e36b95ae70dbb57f4c82/ip/298635921?variables=%7B%22channel%22%3A%22WWW%22%2C%22version%22%3A%22v1%22%2C%22p13nCls%22%3A%7B%22pageId%22%3A%22298635921%22%2C%22availabilityStatus%22%3A%22IN_STOCK%22%2C%22p13NCallType%22%3A%22BTF%22%2C%22p13nMetadata%22%3A%22TFo0RAAAAPAXeyJ1c2VkTW9kZWxzIjp7fSwiY2F0SWQiOiIwOjQwNDQ6MTAzMTUMAPALMzc6NTg4Nzc4IiwidGFiTW9kZWxzIjp7fX0%3D%22%2C%22lazyModules%22%3A%5B%7B%22configs%22%3A%7B%22__typename%22%3A%22TempoWM_GLASSWWWItemCompleteTheLookConfigsV1%22%2C%22title%22%3Anull%2C%22subTitle%22%3Anull%2C%22products%22%3A%5B%5D%2C%22p13nCtlColumn1%22%3A%5B%5D%2C%22p13nCtlColumn2%22%3A%5B%5D%2C%22p13nCtlColumn3%22%3A%5B%5D%2C%22looks%22%3A%5B%5D%7D%2C%22moduleId%22%3A%228dbf1c37-208a-4339-bfa8-f0637a1df7d2%22%2C%22matchedTrigger%22%3A%7B%22pageType%22%3A%22ItemPageGlobal%22%2C%22pageId%22%3A%22global_evergreen%22%2C%22zone%22%3A%22contentZone36%22%2C%22inheritable%22%3Atrue%7D%2C%22name%22%3A%22Complete%20The%20Look%20Module%20Placeholder%22%2C%22type%22%3A%22CompleteTheLook%22%2C%22version%22%3A9%2C%22status%22%3A%22published%22%2C%22publishedDate%22%3A1667273627375%7D%2C%7B%22configs%22%3A%7B%22__typename%22%3A%22TempoWM_GLASSWWWItemCarouselConfigsV1%22%2C%22products%22%3A%5B%5D%2C%22subTitle%22%3Anull%2C%22showGridView%22%3A%22True%22%2C%22isAtfSectionItemCarousel%22%3A%22False%22%2C%22title%22%3A%22See%20Similar%20Items%22%2C%22minItems%22%3A%224%22%2C%22type%22%3Anull%2C%22spBeaconInfo%22%3Anull%2C%22viewAllLink%22%3Anull%7D%2C%22moduleId%22%3A%221020e0da-74a6-428e-a8a3-1209377af573%22%2C%22matchedTrigger%22%3A%7B%22pageType%22%3A%22ItemPageGlobal%22%2C%22pageId%22%3A%22global_evergreen%22%2C%22zone%22%3A%22contentZone1%22%2C%22inheritable%22%3Afalse%7D%2C%22name%22%3A%22Item%20Carousel%20-%20Recommendations%201%22%2C%22type%22%3A%22ItemCarousel%22%2C%22version%22%3A8%2C%22status%22%3A%22published%22%2C%22publishedDate%22%3A1700514961485%7D%2C%7B%22configs%22%3A%7B%22__typename%22%3A%22TempoWM_GLASSWWWItemCarouselConfigsV1%22%2C%22products%22%3A%5B%5D%2C%22subTitle%22%3Anull%2C%22showGridView%22%3A%22False%22%2C%22isAtfSectionItemCarousel%22%3A%22False%22%2C%22title%22%3A%22Item%20Carousel%20-%20Recommendations%202%22%2C%22minItems%22%3A%224%22%2C%22type%22%3Anull%2C%22spBeaconInfo%22%3Anull%2C%22viewAllLink%22%3Anull%7D%2C%22moduleId%22%3A%2255da6caa-c27a-4e83-953f-90e9816d4fc7%22%2C%22matchedTrigger%22%3A%7B%22pageType%22%3A%22ItemPageGlobal%22%2C%22pageId%22%3A%22global_evergreen%22%2C%22zone%22%3A%22contentZone2%22%2C%22inheritable%22%3Afalse%7D%2C%22name%22%3A%22Item%20Carousel%20-%20Recommendations%202%22%2C%22type%22%3A%22ItemCarousel%22%2C%22version%22%3A12%2C%22status%22%3A%22published%22%2C%22publishedDate%22%3A1700515266344%7D%2C%7B%22configs%22%3A%7B%22__typename%22%3A%22TempoWM_GLASSWWWItemCarouselConfigsV1%22%2C%22products%22%3A%5B%5D%2C%22subTitle%22%3Anull%2C%22showGridView%22%3A%22False%22%2C%22isAtfSectionItemCarousel%22%3A%22False%22%2C%22title%22%3A%22Recommendations%20for%20you%22%2C%22minItems%22%3A%226%22%2C%22type%22%3Anull%2C%22spBeaconInfo%22%3Anull%2C%22viewAllLink%22%3Anull%7D%2C%22moduleId%22%3A%22c86be0b5-6f92-47c7-a3cc-d065abeb5b15%22%2C%22matchedTrigger%22%3A%7B%22pageType%22%3A%22ItemPageGlobal%22%2C%22pageId%22%3A%22global_evergreen%22%2C%22zone%22%3A%22contentZone19%22%2C%22inheritable%22%3Afalse%7D%2C%22name%22%3A%22Item%20Carousel%20-%20Recommendations%203%22%2C%22type%22%3A%22ItemCarousel%22%2C%22version%22%3A9%2C%22status%22%3A%22published%22%2C%22publishedDate%22%3A1700573252631%7D%2C%7B%22configs%22%3A%7B%22__typename%22%3A%22TempoWM_GLASSWWWItemCarouselConfigsV1%22%2C%22products%22%3A%5B%5D%2C%22subTitle%22%3Anull%2C%22showGridView%22%3A%22False%22%2C%22isAtfSectionItemCarousel%22%3A%22False%22%2C%22title%22%3A%22Recommendations%20for%20you%22%2C%22minItems%22%3A%226%22%2C%22type%22%3Anull%2C%22spBeaconInfo%22%3Anull%2C%22viewAllLink%22%3Anull%7D%2C%22moduleId%22%3A%224543ceeb-c491-4b3b-90de-a0dd502b18e1%22%2C%22matchedTrigger%22%3A%7B%22pageType%22%3A%22ItemPageGlobal%22%2C%22pageId%22%3A%22global_evergreen%22%2C%22zone%22%3A%22contentZone23%22%2C%22inheritable%22%3Afalse%7D%2C%22name%22%3A%22Item%20Carousel%20-%20Recommendations%204%22%2C%22type%22%3A%22ItemCarousel%22%2C%22version%22%3A6%2C%22status%22%3A%22published%22%2C%22publishedDate%22%3A1700573464116%7D%2C%7B%22configs%22%3A%7B%22__typename%22%3A%22TempoWM_GLASSWWWSoftBundlesConfigs%22%2C%22products%22%3A%5B%5D%2C%22subTitle%22%3Anull%2C%22sTitle%22%3A%22Frequently%20Bought%20Together%22%2C%22collapse%22%3A%22True%22%2C%22athModule%22%3A%22%22%2C%22title%22%3A%22Frequently%20Bought%20Together%22%7D%2C%22moduleId%22%3A%2262c9f71d-0c09-4769-8f5e-5683596dd21b%22%2C%22matchedTrigger%22%3A%7B%22pageType%22%3A%22ItemPageGlobal%22%2C%22pageId%22%3A%22global_evergreen%22%2C%22zone%22%3A%22contentZone46%22%2C%22inheritable%22%3Afalse%7D%2C%22name%22%3A%22New%20SoftBundles%20Module%2C%20Wednesday%2C%20May%2003%2C%202023%2C%2010%3A17%3A01%3A79%20am%22%2C%22type%22%3A%22SoftBundles%22%2C%22version%22%3A6%2C%22status%22%3A%22published%22%2C%22publishedDate%22%3A1698123664655%7D%2C%7B%22configs%22%3A%7B%22__typename%22%3A%22TempoWM_GLASSWWWItemComparisonChartConfigsV1%22%2C%22compChartItems%22%3A%5B%5D%2C%22tileOptions%22%3A%7B%22addToCart%22%3A%22True%22%2C%22averageRatings%22%3A%22True%22%2C%22displayAveragePriceCondition%22%3A%22True%22%2C%22displayPricePerUnit%22%3A%22True%22%2C%22displayStandardPrice%22%3A%22True%22%2C%22displayWasPrice%22%3A%22True%22%2C%22fulfillmentBadging%22%3A%22True%22%2C%22mediaRatings%22%3A%22True%22%2C%22productFlags%22%3A%22True%22%2C%22productLabels%22%3A%22True%22%2C%22productPrice%22%3A%22True%22%2C%22productTitle%22%3A%22True%22%7D%2C%22title%22%3Anull%7D%2C%22moduleId%22%3A%22ea9001ab-6aae-478f-b80d-e3b4b73d857d%22%2C%22matchedTrigger%22%3A%7B%22pageType%22%3A%22ItemPageGlobal%22%2C%22pageId%22%3A%22global_evergreen%22%2C%22zone%22%3A%22contentZone6%22%2C%22inheritable%22%3Afalse%7D%2C%22name%22%3A%22New%20Comparison%20Chart%20Module%2C%20Thursday%2C%20May%2004%2C%202023%2C%203%3A43%3A45%3A98%20pm%22%2C%22type%22%3A%22ComparisonChart%22%2C%22version%22%3A6%2C%22status%22%3A%22published%22%2C%22publishedDate%22%3A1694454790473%7D%5D%2C%22userClientInfo%22%3A%7B%22isZipLocated%22%3Atrue%2C%22callType%22%3A%22CLIENT%22%7D%2C%22userReqInfo%22%3A%7B%22refererContext%22%3A%7B%22source%22%3A%22itempage%22%2C%22sourceId%22%3Anull%2C%22wmlspartner%22%3Anull%7D%2C%22isMoreOptionsTileEnabled%22%3Atrue%7D%7D%2C%22fetchP13N%22%3Atrue%2C%22fMrkDscrp%22%3Afalse%2C%22pageType%22%3A%22ItemPageGlobal%22%2C%22fIdml%22%3Afalse%2C%22fRev%22%3Afalse%2C%22iId%22%3A%22298635921%22%2C%22bbe%22%3Atrue%2C%22fSId%22%3Atrue%2C%22eSb%22%3Atrue%2C%22enableDetailedBeacon%22%3Afalse%2C%22enableClickTrackingURL%22%3Afalse%2C%22eCc%22%3Atrue%2C%22fIdmlOrMrkDscrp%22%3Afalse%2C%22tenant%22%3A%22WM_GLASS%22%2C%22epsv%22%3Atrue%7D'

    # print(requests.get(url, headers=headers).text)
    # data = {'channel': 'WWW', 'version': 'v1', 'p13nCls': {'pageId': '298635921', 'availabilityStatus': 'IN_STOCK', 'p13NCallType': 'BTF', 'p13nMetadata': 'TFo0RAAAAPAXeyJ1c2VkTW9kZWxzIjp7fSwiY2F0SWQiOiIwOjQwNDQ6MTAzMTUMAPALMzc6NTg4Nzc4IiwidGFiTW9kZWxzIjp7fX0=', 'lazyModules': [{'configs': {'__typename': 'TempoWM_GLASSWWWItemCompleteTheLookConfigsV1', 'title': None, 'subTitle': None, 'products': [], 'p13nCtlColumn1': [], 'p13nCtlColumn2': [], 'p13nCtlColumn3': [], 'looks': []}, 'moduleId': '8dbf1c37-208a-4339-bfa8-f0637a1df7d2', 'matchedTrigger': {'pageType': 'ItemPageGlobal', 'pageId': 'global_evergreen', 'zone': 'contentZone36', 'inheritable': True}, 'name': 'Complete The Look Module Placeholder', 'type': 'CompleteTheLook', 'version': 9, 'status': 'published', 'publishedDate': 1667273627375}, {'configs': {'__typename': 'TempoWM_GLASSWWWItemCarouselConfigsV1', 'products': [], 'subTitle': None, 'showGridView': 'True', 'isAtfSectionItemCarousel': 'False', 'title': 'See Similar Items', 'minItems': '4', 'type': None, 'spBeaconInfo': None, 'viewAllLink': None}, 'moduleId': '1020e0da-74a6-428e-a8a3-1209377af573', 'matchedTrigger': {'pageType': 'ItemPageGlobal', 'pageId': 'global_evergreen', 'zone': 'contentZone1', 'inheritable': False}, 'name': 'Item Carousel - Recommendations 1', 'type': 'ItemCarousel', 'version': 8, 'status': 'published', 'publishedDate': 1700514961485}, {'configs': {'__typename': 'TempoWM_GLASSWWWItemCarouselConfigsV1', 'products': [], 'subTitle': None, 'showGridView': 'False', 'isAtfSectionItemCarousel': 'False', 'title': 'Item Carousel - Recommendations 2', 'minItems': '4', 'type': None, 'spBeaconInfo': None, 'viewAllLink': None}, 'moduleId': '55da6caa-c27a-4e83-953f-90e9816d4fc7', 'matchedTrigger': {'pageType': 'ItemPageGlobal', 'pageId': 'global_evergreen', 'zone': 'contentZone2', 'inheritable': False}, 'name': 'Item Carousel - Recommendations 2', 'type': 'ItemCarousel', 'version': 12, 'status': 'published', 'publishedDate': 1700515266344}, {'configs': {'__typename': 'TempoWM_GLASSWWWItemCarouselConfigsV1', 'products': [], 'subTitle': None, 'showGridView': 'False', 'isAtfSectionItemCarousel': 'False', 'title': 'Recommendations for you', 'minItems': '6', 'type': None, 'spBeaconInfo': None, 'viewAllLink': None}, 'moduleId': 'c86be0b5-6f92-47c7-a3cc-d065abeb5b15', 'matchedTrigger': {'pageType': 'ItemPageGlobal', 'pageId': 'global_evergreen', 'zone': 'contentZone19', 'inheritable': False}, 'name': 'Item Carousel - Recommendations 3', 'type': 'ItemCarousel', 'version': 9, 'status': 'published', 'publishedDate': 1700573252631}, {'configs': {'__typename': 'TempoWM_GLASSWWWItemCarouselConfigsV1', 'products': [], 'subTitle': None, 'showGridView': 'False', 'isAtfSectionItemCarousel': 'False', 'title': 'Recommendations for you', 'minItems': '6', 'type': None, 'spBeaconInfo': None, 'viewAllLink': None}, 'moduleId': '4543ceeb-c491-4b3b-90de-a0dd502b18e1', 'matchedTrigger': {'pageType': 'ItemPageGlobal', 'pageId': 'global_evergreen', 'zone': 'contentZone23', 'inheritable': False}, 'name': 'Item Carousel - Recommendations 4', 'type': 'ItemCarousel', 'version': 6, 'status': 'published', 'publishedDate': 1700573464116}, {'configs': {'__typename': 'TempoWM_GLASSWWWSoftBundlesConfigs', 'products': [], 'subTitle': None, 'sTitle': 'Frequently Bought Together', 'collapse': 'True', 'athModule': '', 'title': 'Frequently Bought Together'}, 'moduleId': '62c9f71d-0c09-4769-8f5e-5683596dd21b', 'matchedTrigger': {'pageType': 'ItemPageGlobal', 'pageId': 'global_evergreen', 'zone': 'contentZone46', 'inheritable': False}, 'name': 'New SoftBundles Module, Wednesday, May 03, 2023, 10:17:01:79 am', 'type': 'SoftBundles', 'version': 6, 'status': 'published', 'publishedDate': 1698123664655}, {'configs': {'__typename': 'TempoWM_GLASSWWWItemComparisonChartConfigsV1', 'compChartItems': [], 'tileOptions': {'addToCart': 'True', 'averageRatings': 'True', 'displayAveragePriceCondition': 'True', 'displayPricePerUnit': 'True', 'displayStandardPrice': 'True', 'displayWasPrice': 'True', 'fulfillmentBadging': 'True', 'mediaRatings': 'True', 'productFlags': 'True', 'productLabels': 'True', 'productPrice': 'True', 'productTitle': 'True'}, 'title': None}, 'moduleId': 'ea9001ab-6aae-478f-b80d-e3b4b73d857d', 'matchedTrigger': {'pageType': 'ItemPageGlobal', 'pageId': 'global_evergreen', 'zone': 'contentZone6', 'inheritable': False}, 'name': 'New Comparison Chart Module, Thursday, May 04, 2023, 3:43:45:98 pm', 'type': 'ComparisonChart', 'version': 6, 'status': 'published', 'publishedDate': 1694454790473}], 'userClientInfo': {'isZipLocated': True, 'callType': 'CLIENT'}, 'userReqInfo': {'refererContext': {'source': 'itempage', 'sourceId': None, 'wmlspartner': None}, 'isMoreOptionsTileEnabled': True}}, 'fetchP13N': True, 'fMrkDscrp': False, 'pageType': 'ItemPageGlobal', 'fIdml': False, 'fRev': False, 'iId': '298635921', 'bbe': True, 'fSId': True, 'eSb': True, 'enableDetailedBeacon': False, 'enableClickTrackingURL': False, 'eCc': True, 'fIdmlOrMrkDscrp': False, 'tenant': 'WM_GLASS', 'epsv': True}
    item_id = "2168543994"
    data = {
        "channel": "WWW",
        "version": "v1",
        "p13nCls": {
            "pageId": "2168543994",
            "availabilityStatus": "IN_STOCK",
            "p13NCallType": "BTF",
            "p13nMetadata": "TFo0RAAAAPAXeyJ1c2VkTW9kZWxzIjp7fSwiY2F0SWQiOiIwOjQwNDQ6MTAzMTUMAPALMzc6NTg4Nzc4IiwidGFiTW9kZWxzIjp7fX0=",
            "lazyModules": [
                {
                    "configs": {
                        "__typename": "TempoWM_GLASSWWWItemCompleteTheLookConfigsV1",
                        "title": None,
                        "subTitle": None,
                        "products": [],
                        "p13nCtlColumn1": [],
                        "p13nCtlColumn2": [],
                        "p13nCtlColumn3": [],
                        "looks": []
                    },
                    "moduleId": "8dbf1c37-208a-4339-bfa8-f0637a1df7d2",
                    "matchedTrigger": {
                        "pageType": "ItemPageGlobal",
                        "pageId": "global_evergreen",
                        "zone": "contentZone36",
                        "inheritable": True
                    },
                    "name": "Complete The Look Module Placeholder",
                    "type": "CompleteTheLook",
                    "version": 9,
                    "status": "published",
                    "publishedDate": 1667273627375
                },
                {
                    "configs": {
                        "__typename": "TempoWM_GLASSWWWItemCarouselConfigsV1",
                        "products": [],
                        "subTitle": None,
                        "showGridView": "True",
                        "isAtfSectionItemCarousel": "False",
                        "title": "See Similar Items",
                        "minItems": "4",
                        "type": None,
                        "spBeaconInfo": None,
                        "viewAllLink": None
                    },
                    "moduleId": "1020e0da-74a6-428e-a8a3-1209377af573",
                    "matchedTrigger": {
                        "pageType": "ItemPageGlobal",
                        "pageId": "global_evergreen",
                        "zone": "contentZone1",
                        "inheritable": False
                    },
                    "name": "Item Carousel - Recommendations 1",
                    "type": "ItemCarousel",
                    "version": 8,
                    "status": "published",
                    "publishedDate": 1700514961485
                },
                {
                    "configs": {
                        "__typename": "TempoWM_GLASSWWWItemCarouselConfigsV1",
                        "products": [],
                        "subTitle": None,
                        "showGridView": "False",
                        "isAtfSectionItemCarousel": "False",
                        "title": "Item Carousel - Recommendations 2",
                        "minItems": "4",
                        "type": None,
                        "spBeaconInfo": None,
                        "viewAllLink": None
                    },
                    "moduleId": "55da6caa-c27a-4e83-953f-90e9816d4fc7",
                    "matchedTrigger": {
                        "pageType": "ItemPageGlobal",
                        "pageId": "global_evergreen",
                        "zone": "contentZone2",
                        "inheritable": False
                    },
                    "name": "Item Carousel - Recommendations 2",
                    "type": "ItemCarousel",
                    "version": 12,
                    "status": "published",
                    "publishedDate": 1700515266344
                },
                {
                    "configs": {
                        "__typename": "TempoWM_GLASSWWWItemCarouselConfigsV1",
                        "products": [],
                        "subTitle": None,
                        "showGridView": "False",
                        "isAtfSectionItemCarousel": "False",
                        "title": "Recommendations for you",
                        "minItems": "6",
                        "type": None,
                        "spBeaconInfo": None,
                        "viewAllLink": None
                    },
                    "moduleId": "c86be0b5-6f92-47c7-a3cc-d065abeb5b15",
                    "matchedTrigger": {
                        "pageType": "ItemPageGlobal",
                        "pageId": "global_evergreen",
                        "zone": "contentZone19",
                        "inheritable": False
                    },
                    "name": "Item Carousel - Recommendations 3",
                    "type": "ItemCarousel",
                    "version": 9,
                    "status": "published",
                    "publishedDate": 1700573252631
                },
                {
                    "configs": {
                        "__typename": "TempoWM_GLASSWWWItemCarouselConfigsV1",
                        "products": [],
                        "subTitle": None,
                        "showGridView": "False",
                        "isAtfSectionItemCarousel": "False",
                        "title": "Recommendations for you",
                        "minItems": "6",
                        "type": None,
                        "spBeaconInfo": None,
                        "viewAllLink": None
                    },
                    "moduleId": "4543ceeb-c491-4b3b-90de-a0dd502b18e1",
                    "matchedTrigger": {
                        "pageType": "ItemPageGlobal",
                        "pageId": "global_evergreen",
                        "zone": "contentZone23",
                        "inheritable": False
                    },
                    "name": "Item Carousel - Recommendations 4",
                    "type": "ItemCarousel",
                    "version": 6,
                    "status": "published",
                    "publishedDate": 1700573464116
                },
                {
                    "configs": {
                        "__typename": "TempoWM_GLASSWWWSoftBundlesConfigs",
                        "products": [],
                        "subTitle": None,
                        "sTitle": "Frequently Bought Together",
                        "collapse": "True",
                        "athModule": "",
                        "title": "Frequently Bought Together"
                    },
                    "moduleId": "62c9f71d-0c09-4769-8f5e-5683596dd21b",
                    "matchedTrigger": {
                        "pageType": "ItemPageGlobal",
                        "pageId": "global_evergreen",
                        "zone": "contentZone46",
                        "inheritable": False
                    },
                    "name": "New SoftBundles Module, Wednesday, May 03, 2023, 10:17:01:79 am",
                    "type": "SoftBundles",
                    "version": 6,
                    "status": "published",
                    "publishedDate": 1698123664655
                },
                {
                    "configs": {
                        "__typename": "TempoWM_GLASSWWWItemComparisonChartConfigsV1",
                        "compChartItems": [],
                        "tileOptions": {
                            "addToCart": "True",
                            "averageRatings": "True",
                            "displayAveragePriceCondition": "True",
                            "displayPricePerUnit": "True",
                            "displayStandardPrice": "True",
                            "displayWasPrice": "True",
                            "fulfillmentBadging": "True",
                            "mediaRatings": "True",
                            "productFlags": "True",
                            "productLabels": "True",
                            "productPrice": "True",
                            "productTitle": "True"
                        },
                        "title": None
                    },
                    "moduleId": "ea9001ab-6aae-478f-b80d-e3b4b73d857d",
                    "matchedTrigger": {
                        "pageType": "ItemPageGlobal",
                        "pageId": "global_evergreen",
                        "zone": "contentZone6",
                        "inheritable": False
                    },
                    "name": "New Comparison Chart Module, Thursday, May 04, 2023, 3:43:45:98 pm",
                    "type": "ComparisonChart",
                    "version": 6,
                    "status": "published",
                    "publishedDate": 1694454790473
                }
            ],
            "userClientInfo": {
                "isZipLocated": True,
                "callType": "CLIENT"
            },
            "userReqInfo": {
                "refererContext": {
                    "source": "itempage",
                    "sourceId": None,
                    "wmlspartner": None
                },
                "isMoreOptionsTileEnabled": True
            }
        },
        "fetchP13N": True,
        "fMrkDscrp": False,
        "pageType": "ItemPageGlobal",
        "fIdml": False,
        "fRev": False,
        "iId": "2168543994",
        "bbe": True,
        "fSId": True,
        "eSb": True,
        "enableDetailedBeacon": False,
        "enableClickTrackingURL": False,
        "eCc": True,
        "fIdmlOrMrkDscrp": False,
        "tenant": "WM_GLASS",
        "epsv": True
    }
    graphql_query = {
        "variables": data  # 你的变量
    }
    # 将数据转换为JSON字符串
    json_data = json.dumps(graphql_query)
    url_short = 'https://www.walmart.com/orchestra/pdp/graphql/ItemByIdBtf/74215a212deaf10003e79fd6668b072ecd7c08dca644e36b95ae70dbb57f4c82'

    # 发送POST请求
    response = requests.post(url_short, data=json_data, headers=headers)

    # 打印响应文本
    print(response.text)
    # new_data = get_minimal_data_set(data, url_short, headers)
    # json_data = json.dumps(new_data)
    # print(json_data)

    # print(test_headers(headers, url,fail_text))
    # print(extract_variables(url))
    # fail_text = 'Access Denied'


def find_shortest_cookies(cookies, url):
    cookie_fields = cookies.split("; ")
    essential_cookies = []

    for i, cookie_field in enumerate(cookie_fields):
        print(i, cookie_field)
        sleep(3)
        temp_cookies = essential_cookies + cookie_fields[i + 1:]
        cookie_str = "; ".join(temp_cookies)
        print(cookie_str)
        headers = {
            'cookie': cookie_str,
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0"
        }
        for i in range(3):
            try:
                sp = requests.get(url, headers=headers,proxies=proxies)
                break  # if the request is successful, the loop will break otherwise it will continue until i < 3
            except Exception as e:
                print(f"SSL Error occurred: {e}")
                print("Retrying...")
                sleep(2)  # optional delay before retrying, you may remove it

        # If removing the cookie_field breaks the page,
        # we consider it as an essential one and add it back.
        if is_verify_identity_page(sp.content):
            print('cookie失效')
            essential_cookies.append(cookie_field)
        else:
            soup = BeautifulSoup(sp.content, 'html.parser')

            # 寻找具有特定id的 <script> 标签
            script_tag = soup.find('script', {'id': '__NEXT_DATA__'})
            if script_tag is not None:
                json_text = script_tag.string

                # 解析 JSON 文本为 Python 数据结构
                data = json.loads(json_text)
                product = data['props']['pageProps']['initialData']['data']['product']
                if product:
                    if 'variantProductIdMap' in product and product['variantProductIdMap']:
                        variantProductIdMap = product['variantProductIdMap']
                        for i in variantProductIdMap:
                            color = i
                            id = variantProductIdMap[i]
                            option = product['variantsMap'][id]
                            price = option['priceInfo']['currentPrice']
                            avlib = option['availabilityStatus']
                            output = [id, color, price, avlib]
                            print(output)
                    elif 'priceInfo' in product and 'currentPrice' in product['priceInfo']:
                        if product['priceInfo']['currentPrice']:
                            price = product['priceInfo']['currentPrice']['price']
                        else:
                            price = None
                        output = [None, None, price, None]
                        print(output)
            else:
                essential_cookies.append(cookie_field)

    shortest_cookies = '; '.join(essential_cookies)
    return shortest_cookies
def is_verify_identity_page(html_content):
   soup = BeautifulSoup(html_content, 'html.parser')
   if soup.title and soup.title.string:
       if 'Verify Your Identity' in soup.title.string:
           return True
   return False

if __name__ == '__main__':
    cookie = '_px3=069c7c4a039cb5ab89950119747a0f79c9f612bef9a96dc090ffe5e16da2abaa:Sk8xzskUaBTZ7A5aZpZuwNIzPoGCtbUaCLBDJt2RCA0phyte7AwP6TSueW0Yfz8XeYBOAbNi561kxhd6CZ42cA==:1000:MxNA48vsF3YdONn9jEe/nzAYwxILLy50SeyVKI8w9l8rnWw2QSBFFalhIPIEVU5mW/OUHWOWCE2AhoWdDCRC7rpaslZa4L/j/+O761v+SrU3vUL7qIATs76o8PsSuESvtRGLzeip9iipgh0mabH8oi8HQQDJ6W3PLSlSAQ+4+/jdgMdzFdWozhYOaO+L81ZwKq9ArVJvWNG8saOa+ARVzvWfD2UA+s2xNS/AJXSVx2I=; _pxde=6649338294ef2b40e77e0639eb920bc0aaa3085697921c5f6740742143e309d1:eyJ0aW1lc3RhbXAiOjE3MDM1ODEwOTQwNTl9; AMCV_C4C6370453309C960A490D44%40AdobeOrg=-1124106680%7CMCMID%7C71426496447101848822163917771433360748%7CMCIDTS%7C19718%7CMCAID%7CNONE%7CMCOPTOUT-1703588287s%7CNONE%7CMCAAMLH-1704185887%7C11%7CMCAAMB-1704185887%7Cj8Odv6LonN4r3an7LhD3WZrU1bUpAkFkkiY1ncBR96t2PTI%7CMCSYNCSOP%7C411-19725%7CvVersion%7C5.2.0; _pxvid=e36b4739-a3cc-11ee-adff-cda1c2915236; _sctr=1%7C1703520000000; cto_bundle=Ydwk119Va1lMcDlabjlNQnZCU0hjN1hMSzIlMkZENnVTaUZ6aFJUVSUyRkVmaTBCa0JTbUNoNDM4VzlaNEl6ZXJjcmM5SnRKTyUyQlc5eDRDMTF6SWJMQkVzc3hYWiUyRmIzdzUlMkI4eHdYV2Ixam5rSVliJTJGUzVKMnpkZ0tTaXBuSUpjalNnS2FOYjdsSG1Ja3glMkJjV04wdGlpaVgzVThiWDRMSFBkZ3FTMUFNWUFvYzg4WDBFQ0s0WSUzRA; pxcts=e36b5bd5-a3cc-11ee-adff-170477a2c6f6; AMCVS_C4C6370453309C960A490D44%40AdobeOrg=1; _cs_mk_aa=0.9473249147304458_1703581087039; _ga=GA1.2.1151002761.1703581087; _ga_N1HN887KY7=GS1.2.1703581087.1.0.1703581087.60.0.0; _gat=1; _gcl_au=1.1.978835587.1703581088; _gid=GA1.2.211958083.1703581087; _scid=8ad90668-ef8e-4255-873b-ab123a867c3f; _scid_r=8ad90668-ef8e-4255-873b-ab123a867c3f; _tt_enable_cookie=1; _ttp=cymy4GTYqk6OFeP7lfcM1qj5EAH; _uetsid=e3348ad0a3cc11eeba010f33e19be386; _uetvid=e3349d40a3cc11eea4258feb88ecc83e; gpv_Page=Bot%20Protection%20Page; kndctr_C4C6370453309C960A490D44_AdobeOrg_cluster=jpn3; kndctr_C4C6370453309C960A490D44_AdobeOrg_identity=CiY3MTQyNjQ5NjQ0NzEwMTg0ODgyMjE2MzkxNzc3MTQzMzM2MDc0OFIRCLLr4arKMRgBKgRKUE4zMAPwAbLr4arKMQ==; s_cc=true; s_ecid=MCMID%7C71426496447101848822163917771433360748; s_gnr=1703581087038-New; s_visit=1; _screload=; bm_sv=BE4382B5352C16A0D75A88F703C1666F~YAAQbqrBF9PaPo+MAQAA7nJYpRbyS6Oq2025H9Q8ZyhI/+RIs7rYzAhbrHURx7j3JN9ZTihjls4PLJt1WrWY9T0e+MEJQmlXTYpGWaWlFSYoWJCv7pRqnPMPScR/hyxMld8Zpjq2a13guXY30hpi1Zx+GNnDMRdooYPDDI6VqEVPNNdeWKmWs+Rtdu7zV+rpKmmnfT7p9+J05OUI1j/6RFV3pHz49hRtVyVxr5TT0DOyjXGwR+7KbmQgHi3AAM4C~1; TSe62c5f0d027=080c383e3cab2000d1010618eeb4652c9a4d9a85d62b0a8e8fc5078027ef38a435ca31d58777773e0883ed03931130003656e170d3c56a8594088d535ac6dfef15e2ddc39e72e1b62b4713fa8b08fabac90c5ab8ed9d9e5d0355c4203da4f834; TS0180da25=01d344435eaca781fcefdc84dc49798881aed2f29fc36c1ec990806e7732c7c7fc95da6a258917bf86990ac5e8a946cf468304a41c; TS01ea8d4c=01d344435eaca781fcefdc84dc49798881aed2f29fc36c1ec990806e7732c7c7fc95da6a258917bf86990ac5e8a946cf468304a41c; vtc=cDDkE3oxG23fbt9g4tG8Sc; wmt.c=0; TS010110a1=01d344435eaca781fcefdc84dc49798881aed2f29fc36c1ec990806e7732c7c7fc95da6a258917bf86990ac5e8a946cf468304a41c; defaultNearestStoreId=1061; deliveryCatchment=1061; localStoreInfo=eyJwb3N0YWxDb2RlIjoiTDVWMk42IiwibG9jYWxTdG9yZUlkIjoiMTA2MSIsInNlbGVjdGVkU3RvcmVJZCI6IjEwNjEiLCJjaXR5IjoiTWlzc2lzc2F1Z2EiLCJzZWxlY3RlZFN0b3JlTmFtZSI6IkhFQVJUTEFORCwgTUlTU0lTU0FVR0EsT04iLCJmdWxmaWxsbWVudFN0b3JlSWQiOiIxMDYxIiwiZnVsZmlsbG1lbnRUeXBlIjoiSU5TVE9SRV9QSUNLVVAifQo=; walmart.nearestPostalCode=L5V2N6; walmart.shippingPostalCode=L5V2N6; walmart.nearestLatLng="43.60822,-79.69387"; ACID=d7179bc5-0c79-468e-b420-a603f5315238; ak_bmsc=9AE1D5D190E35537F4AD7D6AA8E54B7D~000000000000000000000000000000~YAAQbqrBF/SWPY+MAQAAzrEfpRamSDY7YaEhuBjRe1Jcj/4JO5PsqdxiS0V4HsFH6RbugJE95QSTGUsUSpdZdSEy44Bo5dGvNjEsqyYzgicGmWQeNCqQcAiTmSK+TCcsEMqpPy34vERllbayt7T1+C94bLhq2ge9s0HftzQYOsiyVyb/JxG1DglhkOcfqTfBn8hckOUyqy13Hz2Q63TChgsv65+o11ERHxTWpHUmMClRuQjCrOs0jIvBnQm1mDedsLELRGlEefbkpXmre7T9MsrK/wslZz0YkvOM+d0uiR3Fym/uiu1mCoKNDjbM7miIM4tc2R3mSG48c0RLyUoCpxL4nkPkN4Obm2fwebtaLwg8wro7j9A4LRj1jA2J9KkYLOs14OR4H+smSA==; hasLocData=1; locDataV3=eyJwaWNrdXBTdG9yZSI6eyJhZGRyZXNzTGluZU9uZSI6IjgwMCBNYXRoZXNvbiBCbHZkIFciLCJjaXR5IjoiTWlzc2lzc2F1Z2EiLCJzdGF0ZU9yUHJvdmluY2VDb2RlIjoiT04iLCJjb3VudHJ5Q29kZSI6IkNBIiwicG9zdGFsQ29kZSI6Ikw1ViAyTjYiLCJzdG9yZUlkIjoiMTA2MSIsImRpc3BsYXlOYW1lIjoiSEVBUlRMQU5ELCBNSVNTSVNTQVVHQSxPTiIsImdlb1BvaW50Ijp7ImxhdGl0dWRlIjo0My42MDY5ODEsImxvbmdpdHVkZSI6LTc5LjY5MjU0Nn0sImFjY2Vzc1BvaW50SWQiOiJhZTg5Y2E1MS02YjU5LTRlYTctODgxZS01YTMyNzgwZDAwZWIiLCJmdWxmaWxsbWVudFN0b3JlSWQiOiIxMDYxIiwicHJpY2luZ1N0b3JlSWQiOiIxMDYxIiwiZnVsZmlsbG1lbnRPcHRpb24iOiJQSUNLVVAiLCJmdWxmaWxsbWVudFR5cGUiOiJJTlNUT1JFX1BJQ0tVUCJ9LCJzaGlwcGluZyI6eyJwb3N0YWxDb2RlIjoiTDVWIDJONiIsImNpdHkiOiJNaXNzaXNzYXVnYSIsInN0YXRlT3JQcm92aW5jZUNvZGUiOiJPTiIsImNvdW50cnlDb2RlIjoiQ0EiLCJsYXRpdHVkZSI6NDMuNjA2OTgxLCJsb25naXR1ZGUiOi03OS42OTI1NDYsImlzR2lmdEFkZHJlc3MiOmZhbHNlfSwiaW50ZW50IjoiUElDS1VQIiwiaXNFeHBsaWNpdEludGVudCI6ZmFsc2UsInZhbGlkYXRlS2V5IjoicHJvZDp2MjpkNzE3OWJjNS0wYzc5LTQ2OGUtYjQyMC1hNjAzZjUzMTUyMzgifQ%3D%3D; uxcon=enforce=false&p13n=true&ads=true&createdAt=1703577366909&modifiedAt=; userSegment=50-percent; DYN_USER_ID=d7179bc5-0c79-468e-b420-a603f5315238; LT=1703577365749; WM_SEC.AUTH_TOKEN=MTAyOTYyMDE456H4KTzgARtOH5aRYpwMsAq%2FzN4NFqLfg4GOzgW15l2YdofPI7GtoutxZxdgEUR01SCnG2zA4iJtqV%2F9HroItg%2BBDkgSsxSBu9mrOfdMKg%2BICejNJF2g1ea6n8k3kXgTj8OFN4dileb20bpDLeCIlSFd%2FHsc7bnSe4%2BTLU2zbj2g8Vo6KbhOGG5t9va3BsFEtCLVE6XN2KljY%2BP1kMk48d%2Flc9FwNKKGrTgSFHHgRlfb%2FSoGFgAYL9DGZ8K45WCXDCcb9mgycy9jtT1uIyOBHRtzO1dGudAUHGcAwfNfOtaSJCGAApJKahFwupqFQDW3hdT1GE0cexOUhbzge2vlRynOHPprNRpmxDAOprZG%2Fpxuql0S9On4262x2Dg%2BaIz%2BieD1%2FlBBRB3Y6OeGRSlqeA%3D%3D; auth=MTAyOTYyMDE456H4KTzgARtOH5aRYpwMsAq%2FzN4NFqLfg4GOzgW15l2YdofPI7GtoutxZxdgEUR01SCnG2zA4iJtqV%2F9HroItg%2BBDkgSsxSBu9mrOfdMKg%2BICejNJF2g1ea6n8k3kXgTj8OFN4dileb20bpDLeCIlSFd%2FHsc7bnSe4%2BTLU2zbj2g8Vo6KbhOGG5t9va3BsFEtCLVE6XN2KljY%2BP1kMk48d%2Flc9FwNKKGrTgSFHHgRlfb%2FSoGFgAYL9DGZ8K45WCXDCcb9mgycy9jtT1uIyOBHRtzO1dGudAUHGcAwfNfOtaSJCGAApJKahFwupqFQDW3hdT1GE0cexOUhbzge2vlRynOHPprNRpmxDAOprZG%2Fpxuql0S9On4262x2Dg%2BaIz%2BieD1%2FlBBRB3Y6OeGRSlqeA%3D%3D; sizeID=vonsgn0cmvaj09epca1tb318l0; userAppVersion=main-1.73.2-fa21230-1221T2214; wm_route_based_language=en-CA'    #
    url = 'https://www.walmart.com/ip/Highsound-Dining-Chairs-Set-of-4-Contemporary-Upholstered-Dining-Chairs-with-Velvet-Cushion-Seat-Blue/1405275663?from=/search'
    # print(find_shortest_cookies(cookie,url))
    final_cookies_CA = 'userAppVersion=main-1.73.2-fa21230-1221T2214'
    headers = {
        "cookie":final_cookies_CA,
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0"
    }
    sp = requests.get(url, headers=headers, proxies=proxies)
    soup = BeautifulSoup(sp.content, 'html.parser')

    # 寻找具有特定id的 <script> 标签
    script_tag = soup.find('script', {'id': '__NEXT_DATA__'})
    print(soup)
    if script_tag is not None:
        json_text = script_tag.string

        # 解析 JSON 文本为 Python 数据结构
        data = json.loads(json_text)
        product = data['props']['pageProps']['initialData']['data']['product']
        print(product)