import os
import scrapy
import csv
import pymongo
# import json


class OtoSpider(scrapy.Spider):
    name = "Oto"
    allowed_domains = ["https://bonbanh.com/"]
    start_urls = ["https://bonbanh.com/oto",
                  "https://bonbanh.com/oto/page,2",   
                  "https://bonbanh.com/oto/page,3",
                  "https://bonbanh.com/oto/page,4",
                  "https://bonbanh.com/oto/page,5",
                  "https://bonbanh.com/oto/page,6",
                  "https://bonbanh.com/oto/page,7",
                  "https://bonbanh.com/oto/page,8",
                  "https://bonbanh.com/oto/page,9",
                  "https://bonbanh.com/oto/page,10",
                  "https://bonbanh.com/oto/page,11",
                  "https://bonbanh.com/oto/page,12",
                  "https://bonbanh.com/oto/page,13",
                  "https://bonbanh.com/oto/page,14",
                  "https://bonbanh.com/oto/page,15",
                  "https://bonbanh.com/oto/page,16",
                  "https://bonbanh.com/oto/page,17",
                  "https://bonbanh.com/oto/page,18",
                  "https://bonbanh.com/oto/page,19",
                  "https://bonbanh.com/oto/page,20",
                  "https://bonbanh.com/oto/page,21",
                  "https://bonbanh.com/oto/page,22",
                  "https://bonbanh.com/oto/page,23",
                  "https://bonbanh.com/oto/page,24",
                  "https://bonbanh.com/oto/page,25",
                  "https://bonbanh.com/oto/page,26",
                  "https://bonbanh.com/oto/page,27",
                  "https://bonbanh.com/oto/page,28",
                  "https://bonbanh.com/oto/page,29",
                  "https://bonbanh.com/oto/page,30",
                  "https://bonbanh.com/oto/page,31",
                  "https://bonbanh.com/oto/page,32",
                  "https://bonbanh.com/oto/page,33",
                  "https://bonbanh.com/oto/page,34",
                  "https://bonbanh.com/oto/page,35",
                  "https://bonbanh.com/oto/page,36",
                  "https://bonbanh.com/oto/page,37",
                  "https://bonbanh.com/oto/page,38",
                  "https://bonbanh.com/oto/page,39",
                  "https://bonbanh.com/oto/page,40",
                  "https://bonbanh.com/oto/page,41",
                  "https://bonbanh.com/oto/page,42",
                  "https://bonbanh.com/oto/page,43",
                  "https://bonbanh.com/oto/page,44",
                  "https://bonbanh.com/oto/page,45",
                  "https://bonbanh.com/oto/page,46",
                  "https://bonbanh.com/oto/page,47",
                  "https://bonbanh.com/oto/page,48",
                  "https://bonbanh.com/oto/page,49",
                  "https://bonbanh.com/oto/page,50",
                  "https://bonbanh.com/oto/page,51",]
    
    
    # headers = {
    #     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    # }
    
    
    def __init__(self, *args, **kwargs):
        econnect = str(os.environ.get('Mongo_HOST', 'localhost'))
        self.client = pymongo.MongoClient(f'mongodb://{econnect}:27017')
        # Thiết lập kết nối tới MongoDB
         # URL MongoDB
        self.db = self.client["oto_db"]  # Tạo hoặc kết nối tới database oto_db
        self.collection = self.db["oto_collection"]  # Tạo hoặc kết nối tới collection oto_collection

        # # Định nghĩa tên file JSON
        # json_file_path = 'oto_data.json'

        # # Lưu DataFrame vào file JSON
        # df.to_json(json_file_path, orient='records', lines=True, force_ascii=False)
        
        
    def parse(self, response):
       
        for oto in response.css('li.car-item'):
            phonenumber = oto.css('div.cb7::text').getall()
            sellername = oto.css('div.cb7 b::text').getall()
            address = oto.css('div.cb7 span::text').getall()
            status = oto.css('div.cb1::text').getall()
            year = oto.css('div.cb1 b::text').getall()
            code = oto.css('div.cb5 span.car_code::text').getall()
            carname = oto.css('div.cb2_02 h3::text').getall()
            price = oto.css('div.cb3 b::text').getall()
            city = oto.css('div.cb4 b::text').getall()
            parameter = oto.css('div.cb6_02::text').getall()
            content = oto.css('div.cb6_02 p::text').getall(),
            data={
                'phonenumber' : phonenumber,
                'sellername' : sellername,
                'address' : address,
                'status' : status,
                'year' : year,
                'code' : code,
                'carname' : carname,
                'price' : price,
                'city' : city,
                'parameter' : parameter,
                'content' : content,
            }        

            self.collection.insert_one(data)

            # Yield dữ liệu (cũng có thể lưu vào file nếu cần)
            yield data

    def close(self, reason):
        # Đóng kết nối MongoDB khi hoàn thành
        self.client.close()

