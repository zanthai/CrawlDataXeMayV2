import json
import re

# Đường dẫn tới file JSON (nếu sai thì sửa đường dẫn lại nha)
file_path = 'C:/CrawlXeMayV2/CrawlXeMay/spiders/jsondataxemay.json'

# Load danh sách dữ liệu đã lưu
data = []
with open(file_path, 'r', encoding='utf-8') as file:
    for line in file:
        data.append(json.loads(line.strip()))

# Hàm chuyển đổi giá từ chuỗi sang số nguyên
def xulyGia(price_str):
    if not price_str:
        return 0
    return int(re.sub(r'[^0-9]', '', price_str))

# Hàm chuyển đổi dung tích từ chuỗi sang số nguyên
def xulyDungtich(capacity_str):
    if not capacity_str or re.sub(r'[^0-9]', '', capacity_str) == '':
        return 0
    return int(re.sub(r'[^0-9]', '', capacity_str))

# Hàm xử lý tên sản phẩm để chỉ giữ lại tên chính
def xulyTen(product_name):
    product_name = re.sub(r'\s[A-Za-z0-9\-]+$', '', product_name).strip()
    return product_name

# Hàm chuẩn hóa tên loại xe (chuyển chữ cái đầu thành hoa và sửa tên sai)
def xulyLoai(brand_name):
    brand_name = ' '.join([word.capitalize() for word in brand_name.split()])
    corrections = {
        "Cup": "Cub",
        "Airbade": "AirBlade",  
        "AirBIade": "AirBlade",
        "Airblade": "AirBlade",
         "AirBlde": "Air Blade"
    }
    return corrections.get(brand_name, brand_name)

# Hàm xử lý năm đăng ký (lấy năm đầu tiên từ chuỗi như "2021 (đời 2018)")
def xulyNamdangky(year_str):
    year_match = re.match(r'(\d{4})', year_str)
    if year_match:
        return year_match.group(1)
    return None  # Trả về None nếu không phải là số

# Hàm kiểm tra mục có hợp lệ hay không
def sanphamHopLe(item):
    return all([
        item.get('TenSP', '').strip() != '',
        item.get('Gia', 0) != 0,
        item.get('ThuongHieu', '').strip() != '',
        item.get('Loai', '').strip() != '',
        item.get('MaSanPham', '').strip() != '',
        item.get('NamDangKy') is not None,  # Đảm bảo NamDangKy là số hợp lệ
        item.get('DungTich', 0) != 0,
        item.get('MauSac', '').strip() != '',
        item.get('TinhTrang', '').strip() != '',
        item.get('ThongTinSanPham', '').strip() != '',
    ])

# Xử lý dữ liệu
for item in data:
    # Xử lý tên sản phẩm (loại bỏ phần số và ký tự không cần thiết)
    item['TenSP'] = xulyTen(item.get('TenSP', ''))

    # Xử lý năm đăng ký (lấy năm đầu tiên trong chuỗi)
    item['NamDangKy'] = xulyNamdangky(item.get('NamDangKy', ''))

    # Xử lý giá (với dấu '₫' và các ký tự khác)
    item['Gia'] = xulyGia(item.get('Gia', '0₫'))
    
    # Xử lý dung tích 
    dung_tich = item.get('DungTich', '')
    if dung_tich:
        item['DungTich'] = xulyDungtich(dung_tich)
    else:
        item['DungTich'] = 0
    
    # Rút gọn thông tin sản phẩm
    thong_tin = item.get('ThongTinSanPham', '')
    if len(thong_tin) > 100:
        item['ThongTinSanPham'] = thong_tin[:100] + '...'
    
    # Xử lý loại xe (chuyển chữ cái đầu mỗi từ thành hoa và sửa tên nếu cần)
    item['Loai'] = xulyLoai(item.get('Loai', ''))

# Loại bỏ các mục không hợp lệ
data = [item for item in data if sanphamHopLe(item)]

# Kiểm tra dữ liệu sau khi xử lý
for item in data:
    print(item)

# Lưu dữ liệu đã xử lý vào một file JSON mới
with open('dulieudaxuly.json', 'w', encoding='utf-8') as file:
    json.dump(data, file, ensure_ascii=False, indent=4)
