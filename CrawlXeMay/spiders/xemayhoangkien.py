import scrapy
from CrawlXeMay.items import CrawlxemayItem

class XeMayHoangKienSpider(scrapy.Spider):
    name = "XeMayHoangKienCraw"
    allowed_domains = ["xemayhoangkien.com"]

    def start_requests(self):
        # Bắt đầu từ trang đầu tiên
        yield scrapy.Request(url='https://xemayhoangkien.com/honda?page=1', callback=self.parse_list)

    def parse_list(self, response):
        # Lấy danh sách các liên kết xe máy từ trang
        product_links = response.xpath('//*[@id="home"]/div/div/div/div/div/div/div/a/@href').getall()
        if product_links:
            for link in product_links:
                yield scrapy.Request(
                    url=response.urljoin(link),
                    callback=self.parse_product
                )

        # Lấy số trang hiện tại từ URL
        current_page = int(response.url.split('=')[-1])
        next_page = current_page + 1  # Chuyển sang trang tiếp theo
        next_page_url = f'https://xemayhoangkien.com/honda?page={next_page}'

        # Yêu cầu trang tiếp theo nếu nó tồn tại
        yield scrapy.Request(
            url=next_page_url,
            callback=self.parse_list  # Chuyển tiếp đến parse_list để lấy sản phẩm trên trang mới
        )

    def parse_product(self, response):
        # Khởi tạo item để lưu dữ liệu
        item = CrawlxemayItem()
        # Lấy các thông tin cần thiết từ trang sản phẩm
        item['TenSP'] = response.xpath('normalize-space(string(//*[@id="add-to-cart-form"]/h1))').get()
        item['Gia'] = response.xpath('normalize-space(string(//*[@id="add-to-cart-form"]/div[1]/p/span))').get()
        item['ThuongHieu'] = response.xpath('normalize-space(//*[@id="add-to-cart-form"]/div/span[1]/a/text())').get()
        item['Loai'] = response.xpath('normalize-space(string(//*[@id="add-to-cart-form"]/div/span[2]/a/text()))').get()
        item['MaSanPham'] = response.xpath('normalize-space(string(//*[@id="add-to-cart-form"]/div/span[3]/a/text()))').get()
        item['NamDangKy'] = response.xpath('normalize-space(string(//*[@id="add-to-cart-form"]/div/span[4]/a/text()))').get()
        item['DungTich'] = response.xpath('normalize-space(string(//*[@id="add-to-cart-form"]/div/span[5]/a/text()))').get()
        item['MauSac'] = response.xpath('normalize-space(string(//*[@id="add-to-cart-form"]/div/span[6]/a/text()))').get()
        item['TinhTrang'] = response.xpath('normalize-space(//*[@class="stock-status"]/text())').get()
        item['ThongTinSanPham'] = response.xpath('normalize-space(string(//*[@id="description"]/p[1]))').get()
        item['SmartKey'] = "Có" if "smartkey" in item['ThongTinSanPham'].lower() else "Không"

        yield item

