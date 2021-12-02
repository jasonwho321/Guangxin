import json
import pandas as pd

dict1 = {"sale_price":[],"listPrice":[],"sku":[],"manufacturerNumber":[]}
df = pd.DataFrame(dict1)
for i in range(1,6):
    print(i)
    path = 'C://Users//Admin//Downloads//chromedriver_win32 (1)//'+str(i)+'.txt'
    f = open(path,encoding='utf-8')
    content = f.read()
    j = json.loads(content)

    results = j['results'][0]['hits']
    for r in results:
        print(results.index(r))
        sale_price = r['price']
        listPrice = r['compare_at_price']
        sku = r['sku']
        try:
            AverageOverallRating = r['named_tags']['AverageOverallRating']
            TotalSubmittedReviews = r['named_tags']['TotalSubmittedReviews']
        except:
            AverageOverallRating = 'error'
            TotalSubmittedReviews = 'error'
        new = {'sale_price':sale_price,'listPrice':listPrice,'sku':sku,'AverageOverallRating':AverageOverallRating,'TotalSubmittedReviews':TotalSubmittedReviews}
        df = df.append(new, ignore_index=True)
df.to_csv("C://Users//Admin//Downloads//chromedriver_win32 (1)//price_output_1130.csv")

