# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class CrawlxemayItem(scrapy.Item):
    TenSP = scrapy.Field()
    Gia = scrapy.Field()
    ThuongHieu = scrapy.Field()
    Loai = scrapy.Field()
    MaSanPham = scrapy.Field()
    NamDangKy = scrapy.Field()
    DungTich = scrapy.Field()
    MauSac = scrapy.Field()
    TinhTrang = scrapy.Field()
    SmartKey = scrapy.Field()
    ThongTinSanPham = scrapy.Field()
    