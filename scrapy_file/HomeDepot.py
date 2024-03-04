import json
import time

import requests
import pandas as pd
from datetime import datetime
from functools import reduce
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor

def process_us_row(row):
    extracted_data = {}
    us_itemId = row['OMSID']
    print(us_itemId)
    supplier_sku = row['Supplier SKU']
    us_data = fetch_us_data(us_itemId)

    if us_data:
        data_dict = json.loads(us_data.content)
        print(data_dict)

        keys_total_reviews = ['data', 'product', 'reviews', 'ratingsReviews', 'totalReviews']
        total_reviews = safe_get(data_dict, keys_total_reviews) or 0

        keys_price = ['data', 'product', 'pricing', 'value']
        price = safe_get(data_dict, keys_price) or '-'

        keys_average_rating = ['data', 'product', 'reviews', 'ratingsReviews', 'averageRating']
        average_rating = safe_get(data_dict, keys_average_rating) or 0

        if average_rating != "-":
            average_rating = "{:.2f}".format(float(average_rating))

        extracted_data = {
            'OMSID': us_itemId,
            'Supplier SKU': supplier_sku,
            'Price': price,
            'Total Reviews': total_reviews,
            'Average Rating': average_rating
        }

    return extracted_data or {'OMSID': us_itemId, 'Supplier SKU': supplier_sku, 'Price': '-', 'Total Reviews': 0,
                              'Average Rating': '0.00'}


def process_ca_row(row):
    ca_productid = row['THD Article Number']
    print(ca_productid)
    supplier_sku_ca = row['Supplier SKU']

    ca_review_link = f'https://api.bazaarvoice.com/data/display/0.2alpha/product/summary?PassKey=i2qqfxgqsb1f86aabybalrdvf&productid={ca_productid}&contentType=reviews&rev=0'
    ca_price_link = f'https://www.homedepot.ca/api/productsvc/v1/products/{ca_productid}/store/7129?fields=BASIC_SPA&lang=en'

    json_data_review = fetch_ca_data(ca_review_link, proxies)
    json_data_price = fetch_ca_data(ca_price_link, proxies)

    extracted_data_ca = {}

    try:
        data_dict_review = json.loads(json_data_review) if json_data_review else None
        data_dict_price = json.loads(json_data_price) if json_data_price else None
    except json.JSONDecodeError as e:
        print(f"JSON Decode Error: {e}")
        data_dict_review = None
        data_dict_price = None

    keys_price = ['optimizedPrice', 'displayPrice', 'value']
    price_ca = safe_get(data_dict_price, keys_price)
    price_ca = "{:.2f}".format(float(price_ca)) if price_ca else '-'

    keys_reviews = ['reviewSummary', 'numReviews']
    total_reviews_ca = safe_get(data_dict_review, keys_reviews) or '-'

    keys_rating = ['reviewSummary', 'primaryRating', 'average']
    average_rating_ca = safe_get(data_dict_review, keys_rating)
    average_rating_ca = "{:.2f}".format(float(average_rating_ca)) if average_rating_ca else '-'

    extracted_data_ca = {
        'THD Article Number': ca_productid,
        'Supplier SKU': supplier_sku_ca,
        'Price': price_ca,
        'Total Reviews': total_reviews_ca,
        'Average Rating': average_rating_ca
    }

    return extracted_data_ca or {'THD Article Number': ca_productid, 'Supplier SKU': supplier_sku_ca, 'Price': '-',
                                 'Total Reviews': 0, 'Average Rating': '0.00'}


def safe_get(d, keys):
    return reduce(lambda d, key: d.get(key, {}) if isinstance(d, dict) else None, keys, d)

proxyip = "http://storm-maplin_area-US_city-LosAngeles_life-1:Homycasa2012@proxy.stormip.cn:1000"

proxies = {
    'http': proxyip,
    'https': proxyip,
}

sku_list_file = '/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/「晓望集群」/S数据分析/HomeDepot爬虫/SKU_列表_运营维护.xlsx'
# 读取Excel文件
def read_excel(file_path, sheet_name):
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    return df

# 发送GraphQL请求
def fetch_us_data(us_itemId):
    retry_count = 0
    max_retries = 3
    headers = {
        'cookie': '_abck=DBE7E9D86007FF00BF31E14F0F87FDE5~0~YAAQFy0+F3W/EmOKAQAACvy6ZAo36wk/AIRvRibFVrxPJUzywds8C1WtVKiQQkyZUERlcBrRN4NzffztdMurzpivsjMt1mvp3DLFBh16QYeIpusa42TJiLS06DJEo8n1O7hFzlgijBP1ywcVI6IeyPiyQGUd2VkRNjkrkybZ52rxI50I2eaZbT/eOmVG4X2BGqvGSiWLVwXwScXevwv0u4zpcpQql+ZnM+d8xeL4kgHxe+rjPQB2dLFeiXXJwo4Wl2h+b2jp+dSOFj8kLlzB6mVLg1tu8TP0CfIDW1BM1m6sKIBfO75li4VJlKV5dg14y6sNAHRGNJcNUi3NjJ/6TzArsCZ7XV/F0mEW0vEevEesVtrzLofQC61nttYDzLWZiQIRuc1Xby9ZZYOQQQ42KdsVy+qCSS8pQyir+w==~-1~-1~-1',
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.69",
        "X-Experience-Name": "hd-home"
    }

    url = 'https://apionline.homedepot.com/federation-gateway/graphql?opname=productClientOnlyProduct'

    payload= {"variables":{"itemId":us_itemId},"query":"query productClientOnlyProduct($itemId: String!, $dataSource: String, $loyaltyMembershipInput: LoyaltyMembershipInput, $storeId: String, $skipSpecificationGroup: Boolean = false, $zipCode: String, $quantity: Int, $skipSubscribeAndSave: Boolean = false, $skipInstallServices: Boolean = true, $skipKPF: Boolean = false) {\n  product(itemId: $itemId, dataSource: $dataSource, loyaltyMembershipInput: $loyaltyMembershipInput) {\n    itemId\n    dataSources\n    identifiers {\n      canonicalUrl\n      brandName\n      itemId\n      modelNumber\n      productLabel\n      storeSkuNumber\n      upcGtin13\n      skuClassification\n      specialOrderSku\n      toolRentalSkuNumber\n      rentalCategory\n      rentalSubCategory\n      upc\n      productType\n      isSuperSku\n      parentId\n      roomVOEnabled\n      sampleId\n      __typename\n    }\n    availabilityType {\n      discontinued\n      status\n      type\n      buyable\n      __typename\n    }\n    details {\n      description\n      collection {\n        url\n        collectionId\n        name\n        __typename\n      }\n      highlights\n      descriptiveAttributes {\n        name\n        value\n        bulleted\n        sequence\n        __typename\n      }\n      additionalResources {\n        infoAndGuides {\n          name\n          url\n          __typename\n        }\n        installationAndRentals {\n          contentType\n          name\n          url\n          __typename\n        }\n        diyProjects {\n          contentType\n          name\n          url\n          __typename\n        }\n        __typename\n      }\n      installation {\n        leadGenUrl\n        __typename\n      }\n      __typename\n    }\n    media {\n      images {\n        url\n        type\n        subType\n        sizes\n        hotspots {\n          coordinate {\n            xCoordinate\n            yCoordinate\n            __typename\n          }\n          omsIDs\n          __typename\n        }\n        altText\n        __typename\n      }\n      video {\n        shortDescription\n        thumbnail\n        url\n        videoStill\n        uploadDate\n        dateModified\n        link {\n          text\n          url\n          __typename\n        }\n        title\n        type\n        videoId\n        longDescription\n        __typename\n      }\n      threeSixty {\n        id\n        url\n        __typename\n      }\n      augmentedRealityLink {\n        usdz\n        image\n        __typename\n      }\n      richContent {\n        content\n        displayMode\n        salsifyRichContent\n        __typename\n      }\n      __typename\n    }\n    pricing(storeId: $storeId) {\n      promotion {\n        dates {\n          end\n          start\n          __typename\n        }\n        type\n        description {\n          shortDesc\n          longDesc\n          __typename\n        }\n        dollarOff\n        percentageOff\n        savingsCenter\n        savingsCenterPromos\n        specialBuySavings\n        specialBuyDollarOff\n        specialBuyPercentageOff\n        promotionTag\n        experienceTag\n        subExperienceTag\n        __typename\n      }\n      value\n      alternatePriceDisplay\n      alternate {\n        bulk {\n          pricePerUnit\n          thresholdQuantity\n          value\n          __typename\n        }\n        unit {\n          caseUnitOfMeasure\n          unitsOriginalPrice\n          unitsPerCase\n          value\n          __typename\n        }\n        __typename\n      }\n      original\n      mapAboveOriginalPrice\n      message\n      preferredPriceFlag\n      specialBuy\n      unitOfMeasure\n      conditionalPromotions {\n        dates {\n          start\n          end\n          __typename\n        }\n        description {\n          shortDesc\n          longDesc\n          __typename\n        }\n        experienceTag\n        subExperienceTag\n        eligibilityCriteria {\n          itemGroup\n          minPurchaseAmount\n          minPurchaseQuantity\n          relatedSkusCount\n          omsSkus\n          __typename\n        }\n        reward {\n          tiers {\n            minPurchaseAmount\n            minPurchaseQuantity\n            rewardPercent\n            rewardAmountPerOrder\n            rewardAmountPerItem\n            rewardFixedPrice\n            maxAllowedRewardAmount\n            maxPurchaseQuantity\n            __typename\n          }\n          __typename\n        }\n        nvalues\n        brandRefinementId\n        __typename\n      }\n      __typename\n    }\n    reviews {\n      ratingsReviews {\n        averageRating\n        totalReviews\n        __typename\n      }\n      __typename\n    }\n    seo {\n      seoKeywords\n      seoDescription\n      __typename\n    }\n    specificationGroup @skip(if: $skipSpecificationGroup) {\n      specifications {\n        specName\n        specValue\n        __typename\n      }\n      specTitle @skip(if: $skipSpecificationGroup)\n      __typename\n    }\n    taxonomy {\n      breadCrumbs {\n        label\n        url\n        browseUrl\n        creativeIconUrl\n        deselectUrl\n        dimensionName\n        refinementKey\n        __typename\n      }\n      brandLinkUrl\n      __typename\n    }\n    favoriteDetail {\n      count\n      __typename\n    }\n    info {\n      hidePrice\n      ecoRebate\n      quantityLimit\n      sskMin\n      sskMax\n      unitOfMeasureCoverage\n      wasMaxPriceRange\n      wasMinPriceRange\n      fiscalYear\n      productDepartment\n      classNumber\n      hasVisuallySimilar\n      categoryHierarchy\n      productSubType {\n        name\n        link\n        __typename\n      }\n      paintBrand\n      dotComColorEligible\n      bathRenovation\n      label\n      isSponsored\n      sponsoredMetadata {\n        campaignId\n        placementId\n        slotId\n        __typename\n      }\n      sponsoredBeacon {\n        onClickBeacon\n        onViewBeacon\n        __typename\n      }\n      augmentedReality\n      globalCustomConfigurator {\n        customExperience\n        customButtonText\n        customDescription\n        customExperienceUrl\n        customTitle\n        __typename\n      }\n      isLiveGoodsProduct\n      prop65Warning\n      returnable\n      hasSubscription\n      isBuryProduct\n      isGenericProduct\n      samplesAvailable\n      customerSignal {\n        previouslyPurchased\n        __typename\n      }\n      productDepartmentId\n      swatches {\n        isSelected\n        itemId\n        label\n        swatchImgUrl\n        url\n        value\n        __typename\n      }\n      totalNumberOfOptions\n      recommendationFlags {\n        visualNavigation\n        packages\n        pipCollections\n        collections\n        frequentlyBoughtTogether\n        bundles\n        __typename\n      }\n      minimumOrderQuantity\n      projectCalculatorEligible\n      subClassNumber\n      calculatorType\n      pipCalculator {\n        coverageUnits\n        display\n        publisher\n        toggle\n        __typename\n      }\n      protectionPlanSku\n      eligibleProtectionPlanSkus\n      hasServiceAddOns\n      consultationType\n      __typename\n    }\n    fulfillment(storeId: $storeId, zipCode: $zipCode, quantity: $quantity) {\n      backordered\n      fulfillmentOptions {\n        type\n        services {\n          type\n          deliveryTimeline\n          deliveryDates {\n            startDate\n            endDate\n            __typename\n          }\n          deliveryCharge\n          dynamicEta {\n            hours\n            minutes\n            __typename\n          }\n          hasFreeShipping\n          freeDeliveryThreshold\n          locations {\n            curbsidePickupFlag\n            isBuyInStoreCheckNearBy\n            distance\n            inventory {\n              isOutOfStock\n              isInStock\n              isLimitedQuantity\n              isUnavailable\n              quantity\n              maxAllowedBopisQty\n              minAllowedBopisQty\n              __typename\n            }\n            isAnchor\n            locationId\n            state\n            storeName\n            storePhone\n            type\n            storeTimeZone\n            __typename\n          }\n          totalCharge\n          optimalFulfillment\n          __typename\n        }\n        fulfillable\n        __typename\n      }\n      anchorStoreStatus\n      anchorStoreStatusType\n      backorderedShipDate\n      bossExcludedShipStates\n      excludedShipStates\n      seasonStatusEligible\n      onlineStoreStatus\n      onlineStoreStatusType\n      fallbackMode\n      sthExcludedShipState\n      bossExcludedShipState\n      inStoreAssemblyEligible\n      bodfsAssemblyEligible\n      __typename\n    }\n    sizeAndFitDetail {\n      attributeGroups {\n        attributes {\n          attributeName\n          dimensions\n          __typename\n        }\n        dimensionLabel\n        productType\n        __typename\n      }\n      __typename\n    }\n    subscription @skip(if: $skipSubscribeAndSave) {\n      defaultfrequency @skip(if: $skipSubscribeAndSave)\n      discountPercentage @skip(if: $skipSubscribeAndSave)\n      subscriptionEnabled @skip(if: $skipSubscribeAndSave)\n      __typename\n    }\n    badges(storeId: $storeId) {\n      label\n      name\n      color\n      creativeImageUrl\n      endDate\n      message\n      timerDuration\n      timer {\n        timeBombThreshold\n        daysLeftThreshold\n        dateDisplayThreshold\n        message\n        __typename\n      }\n      __typename\n    }\n    dataSource\n    installServices(storeId: $storeId, zipCode: $zipCode) @skip(if: $skipInstallServices) {\n      scheduleAMeasure @skip(if: $skipInstallServices)\n      gccCarpetDesignAndOrderEligible @skip(if: $skipInstallServices)\n      __typename\n    }\n    keyProductFeatures @skip(if: $skipKPF) {\n      keyProductFeaturesItems {\n        features {\n          name\n          refinementId\n          refinementUrl\n          value\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    seoDescription\n    __typename\n  }\n}\n"}

    while retry_count < max_retries:
        try:
            response = requests.post(url, headers=headers, json=payload)

            if response.status_code == 200:
                # 处理正常返回的逻辑
                return response
            else:
                print(f"Request failed with status code {response.status_code}. Retrying...")
                retry_count += 1
                time.sleep(2)  # 延迟2秒再次尝试

        except Exception as e:
            print(f"An error occurred: {e}. Retrying...")
            retry_count += 1
            time.sleep(2)  # 延迟2秒再次尝试

    print("Max retries reached. Returning None.")
    return None
def fetch_ca_data(url, proxies, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            response = requests.get(url, proxies=proxies)
            response.raise_for_status()  # 检查是否请求成功
            return response.content
        except requests.RequestException as e:
            print(f"请求失败，错误：{e}")
            retries += 1
            print(f"重试次数：{retries}")
            time.sleep(2)  # 可以按需要增加延迟
    return None  # 返回None或其他默认值，表明重试次数已耗尽
# 主逻辑

def main():
    sku_list_file = '/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/「晓望集群」/S数据分析/HomeDepot爬虫/SKU_列表_运营维护.xlsx'

    df_us = read_excel(sku_list_file, 'US')
    df_ca = read_excel(sku_list_file, 'CA')

    with ProcessPoolExecutor() as executor:
        print("处理US数据")
        us_data_list = list(tqdm(executor.map(process_us_row, [row for _, row in df_us.iterrows()]), total=df_us.shape[0]))

        print("处理CA数据")
        ca_data_list = list(tqdm(executor.map(process_ca_row, [row for _, row in df_ca.iterrows()]), total=df_ca.shape[0]))

    # 获取当前日期
    current_date = datetime.now().strftime('%Y%m%d')

    # 转换列表为DataFrame
    df_us_output = pd.DataFrame(us_data_list)
    df_ca_output = pd.DataFrame(ca_data_list)

    # 获取保存路径
    save_directory = '/'.join(sku_list_file.split('/')[:-1])

    # 保存DataFrame为CSV
    df_us_output.to_csv(f'{save_directory}/HomeDepot_Output_{current_date}_US.csv', index=False)
    df_ca_output.to_csv(f'{save_directory}/HomeDepot_Output_{current_date}_CA.csv', index=False)

if __name__ == "__main__":
    main()


