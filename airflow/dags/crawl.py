import requests
import json
import csv
from datetime import datetime
from hdfs import InsecureClient
from fake_useragent import UserAgent

ua = UserAgent()
hdfs_path = "http://localhost:9870"
hdfs_user = 'root'
global client
client = InsecureClient("http://localhost:9870", user='root')

url = f'https://www.lazada.sg/mother-baby/?ajax=true&page=9&spm=a2o42.searchlistcategory.cate_5.1.46281e22mYNSDT'
headers = {
    'User-Agent': ua.random,
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.9',
    'cookie': 'your_cookie_string_here',
}

def flush_to_hdfs(csv_converted_content):
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    hdfs_path = f'/data/lazada_{timestamp}.csv'
    with client.write(hdfs_path, encoding='utf-8', overwrite=True) as writer:
        writer.write(csv_converted_content)

response = requests.get(url, headers=headers)
print("llllllllllllllllllllllllllllllllllllllllllllllllll",response)
data = response.json()
#print(data.get('mods').get('listItems'))
# for key in data['mods']['listItems']:
#    print(key)
for item in data.get('mods').get('listItems'):
    existing_data = [['NAME', 
                              'originalPrice', 'DiscountedPrice', 'Discount',
                              'ratingScore', 'review', 'description',
                              'categories', 'itemSoldCntShow', 'sellerName', 'brandName','location' ]]
    for item in data.get('mods').get('listItems'):
        existing_data.append([item.get('name', '').replace(',', '').replace("'", '').replace('"', ''),
                                      item.get('originalPrice'),
                                      item.get('price'),
                                      item.get('discount'),
                                      item.get('ratingScore'),
                                      item.get('review'),
                                      "Mom and Baby",
                                      item.get('itemSoldCntShow'),
                                      item.get('sellerName'),
                                      item.get('brandName'),
                                      item.get('location')])
    csv_converted_content = '\n'.join([','.join(map(str, row)) for row in existing_data])
    flush_to_hdfs(csv_converted_content)
    # timestamp = datetime.now().strftime('%Y-%m-%d')
    # csv_file_path = f'lazada.csv'  

    # with open(csv_file_path, 'w', encoding='utf-8') as file:
    #     file.write(csv_converted_content)
print("crawling....")
