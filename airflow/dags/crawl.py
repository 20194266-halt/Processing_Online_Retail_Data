# import requests
# import json
# import csv
# from datetime import datetime
# from hdfs import InsecureClient
# from fake_useragent import UserAgent

# class LazSpider:
#     def __init__(self):
#         self.name = 'laz'
#         self.allowed_domains = ['www.lazada.vn']
#         self.ua = UserAgent()
#         self.page = 1
#         self.hdfs_path = "http://localhost:9870"
#         self.hdfs_user = 'root'
#         self.client = InsecureClient("http://localhost:9870", user='root')
#         self.category = 'Electronic Devices'

#     def start_requests(self):
#         url = f'https://www.lazada.vn/dien-thoai/?ajax=true&page={self.page}&spm=a2o42.searchlistcategory.cate_5.1.46281e22mYNSDT'
#         headers = {
#             'User-Agent': self.ua.random,
#             'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
#             'accept-encoding': 'gzip, deflate, br',
#             'accept-language': 'en-US,en;q=0.9',
#             'cookie': 'your_cookie_string_here',
#         }

#         response = requests.get(url, headers=headers)
#         self.parse(response)

#     def flush_to_hdfs(self, csv_converted_content):
#         timestamp = datetime.now().strftime('%Y-%m-%d')
#         hdfs_path = f'/data/lazada_{timestamp}.csv'
#         with self.client.write(hdfs_path, encoding='utf-8', overwrite=True) as writer:
#             writer.write(csv_converted_content)

#     def parse(self, response):
#         data = json.loads(response.text)
#         try:
#             existing_data = [['NAME', 'productUrl', 'imageUrl',
#                               'originalPrice', 'DiscountedPrice', 'Discount',
#                               'ratingScore', 'review', 'description',
#                               'categories', 'itemId', 'page']]
#             for item in data.get('mods').get('listItems'):
#                 existing_data.append([item.get('name', '').replace(',', '').replace("'", '').replace('"', ''),
#                                       item.get('productUrl'),
#                                       item.get('image'),
#                                       item.get('originalPrice'),
#                                       item.get('price'),
#                                       item.get('discount'),
#                                       item.get('ratingScore'),
#                                       item.get('review'),
#                                       item.get('description', 'Electrical Device'),
#                                       self.category,
#                                       item.get('itemSoldCntShow'),
#                                       item.get('sellerName'),
#                                       item.get('brandName'),
#                                       item.get('location')])

#             csv_converted_content = '\n'.join([','.join(map(str, row)) for row in existing_data])
#             # self.flush_to_hdfs(csv_converted_content)
#             self.flush_to_hdfs(csv_converted_content)
#             with open('output.csv', 'w') as file:
#                 file.write(csv_converted_content)

#         except TypeError:
#             print("NO PAGE LEFT TO SCRAPE")
#             sys.exit(0)
#         except AttributeError:
#             print("Blocked on page number:", self.page)
# if __name__ == "__main__":
#     laz_spider = LazSpider()
#     laz_spider.start_requests()
import requests
import json
import csv
from datetime import datetime
from hdfs import InsecureClient
from fake_useragent import UserAgent

ua = UserAgent()
url = f'https://www.lazada.sg/mother-baby/?ajax=true&page=9&spm=a2o42.searchlistcategory.cate_5.1.46281e22mYNSDT'
headers = {
    'User-Agent': ua.random,
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.9',
    'cookie': 'your_cookie_string_here',
}

response = requests.get(url, headers=headers)
data = response.json()
#print(data['mods'])
for key in data['mods']['listItems']:
    print(key)
# for item in data.get('mods').get('listItems'):
#     existing_data = [['NAME', 'productUrl', 'imageUrl',
#                               'originalPrice', 'DiscountedPrice', 'Discount',
#                               'ratingScore', 'review', 'description',
#                               'categories', 'itemId', 'page']]
#     for item in data.get('mods').get('listItems'):
#         existing_data.append([item.get('name', '').replace(',', '').replace("'", '').replace('"', ''),
#                                       item.get('productUrl'),
#                                       item.get('image'),
#                                       item.get('originalPrice'),
#                                       item.get('price'),
#                                       item.get('discount'),
#                                       item.get('ratingScore'),
#                                       item.get('review'),
#                                       item.get('description', 'Electrical Device'),
#                                       self.category,
#                                       item.get('itemSoldCntShow'),
#                                       item.get('sellerName'),
#                                       item.get('brandName'),
#                                       item.get('location')])
#         print(existing_data)

