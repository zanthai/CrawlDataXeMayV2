# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import scrapy
import pymongo
import json
# from bson.objectid import ObjectId
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
import csv
import os
import psycopg2

class JsonDBXeMayPipeline:
    def process_item(self, item, spider):
        with open('jsondataxemay.json', 'a', encoding='utf-8') as file:
            line = json.dumps(dict(item), ensure_ascii=False) + '\n'
            file.write(line)
        return item

class CSVDBXeMayPipeline:
    def open_spider(self, spider):
        self.file = open('csvdataxemay.csv', 'w', encoding='utf-8', newline='')
        self.writer = csv.writer(self.file, delimiter=',')
        self.writer.writerow([
            'Tên Sản Phẩm', 'Giá', 'Thương Hiệu', 'Loại', 
            'Mã Sản Phẩm', 'Năm Đăng Ký', 'Dung Tích', 'Màu Sắc', 
            'Tình Trạng', 'Smartkey', 'Thông Tin Sản Phẩm'
        ])
    
    def close_spider(self, spider):
        self.file.close()
    
    def process_item(self, item, spider):
        self.writer.writerow([
            item.get('TenSP', ''),
            item.get('Gia', ''),
            item.get('ThuongHieu', ''),
            item.get('Loai', ''),
            item.get('MaSanPham', ''),
            item.get('NamDangKy', ''),
            item.get('DungTich', ''),
            item.get('MauSac', ''),
            item.get('TinhTrang', ''),
            item.get('Smartkey', ''),
            item.get('ThongTinSanPham', '')
            
            
        ])
        return item
    
    