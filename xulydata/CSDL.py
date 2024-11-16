import pymongo
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from sqlalchemy import text
import os
from graphviz import Digraph
from psycopg2 import sql

# Kết nối MongoDB
mongo_client = pymongo.MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['xemayDB']  
collection = mongo_db['xemaycrawler']
data = list(collection.find({}))  #Lấy tất cả dữ liệu từ collection và chuyển sang danh sách

# Chuyển dữ liệu thành DataFrame
# Dữ liệu từ MongoDB được chuyển thành pandas DataFrame để xử lý dễ dàng hơn
df_xemay = pd.DataFrame(data)

# Tạo bảng Loại và SmartKey
# Lọc và loại bỏ trùng lặp trong các cột "Loai" và "SmartKey", sau đó gán ID tự động
LoaiXeMay = df_xemay[['Loai']].drop_duplicates().reset_index(drop=True)
LoaiXeMay['LoaiID'] = LoaiXeMay.index + 1  # Gán LoaiID tự động
LoaiXeMay = LoaiXeMay[['LoaiID', 'Loai']]  # Chọn lại thứ tự các cột

smartkey = df_xemay[['SmartKey']].drop_duplicates().reset_index(drop=True)
smartkey['SmartKeyID'] = smartkey.index + 1  # Gán SmartKeyID tự động
smartkey = smartkey[['SmartKeyID', 'SmartKey']]  # Chọn lại thứ tự các cột

# Merge các bảng với sản phẩm
# Thêm LoaiID và SmartKeyID vào DataFrame chính dựa trên giá trị "Loai" và "SmartKey"
df_xemay = df_xemay.merge(LoaiXeMay[['LoaiID', 'Loai']], on='Loai', how='left')
df_xemay = df_xemay.merge(smartkey[['SmartKeyID', 'SmartKey']], on='SmartKey', how='left')

# Chọn các cột cần thiết để tạo bảng "XeMay"
xemay = df_xemay[['MaSanPham', 'TenSP', 'Gia', 'NamDangKy', 'DungTich', 'MauSac', 'TinhTrang', 'SmartKeyID', 'ThongTinSanPham', 'LoaiID']]

# Tạo cơ sở dữ liệu nếu chưa có
def create_database(db_name):
    """
    Hàm tạo cơ sở dữ liệu PostgreSQL nếu chưa tồn tại
    :param db_name: Tên cơ sở dữ liệu
    """
    conn = psycopg2.connect(
        dbname='postgres',  # Kết nối tới database mặc định "postgres"
        user='postgres',
        password='123456',
        host='localhost',
        port='5432'
    )
    conn.autocommit = True  # Cho phép chạy lệnh CREATE DATABASE
    cursor = conn.cursor()
    cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{db_name}'")
    exists = cursor.fetchone()  # Kiểm tra xem cơ sở dữ liệu đã tồn tại chưa
    if not exists:
        cursor.execute(f"CREATE DATABASE {db_name};")  # Tạo database nếu chưa tồn tại
        print(f"Cơ sở dữ liệu '{db_name}' đã được tạo.")
    else:
        print(f"Cơ sở dữ liệu '{db_name}' đã tồn tại.")
    cursor.close()
    conn.close()

db_name = 'dbcrawlerxemay'
create_database(db_name)  # Gọi hàm để tạo database

# Hàm kết nối tới PostgreSQL
def connect():
    """
    Hàm kết nối tới cơ sở dữ liệu PostgreSQL
    """
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user='postgres',
            password='123456',
            host='localhost',
            port='5432'
        )
        cur = conn.cursor()
        return conn, cur
    except Exception as e:
        print("Lỗi khi kết nối tới PostgreSQL:", e)

# Hàm tạo bảng 'LoaiXeMay'
def create_LoaiXeMay_table():
    """
    Hàm tạo bảng 'LoaiXeMay' trong PostgreSQL
    """
    conn, cur = connect()
    try:
        print("Đang tạo bảng 'LoaiXeMay'...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.LoaiXeMay (
                LoaiID INTEGER NOT NULL,
                Loai VARCHAR(50) NOT NULL,
                CONSTRAINT loai_pkey PRIMARY KEY (LoaiID)
            );
        """)
        print("Bảng 'LoaiXeMay' đã được tạo thành công.")
        conn.commit()
    except Exception as e:
        print("Lỗi khi tạo bảng 'LoaiXeMay':", e)
    finally:
        conn.close()
        cur.close()

# Hàm tạo bảng 'SmartKey'
def create_smartkey_table():
    """
    Hàm tạo bảng 'SmartKey' trong PostgreSQL
    """
    conn, cur = connect()
    try:
        print("Đang tạo bảng 'SmartKey'...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.SmartKey (
                SmartKeyID INTEGER NOT NULL,
                SmartKey VARCHAR(50) NOT NULL,
                CONSTRAINT smartkey_pkey PRIMARY KEY (SmartKeyID)
            );
        """)
        print("Bảng 'SmartKey' đã được tạo thành công.")
        conn.commit()
    except Exception as e:
        print("Lỗi khi tạo bảng 'SmartKey':", e)
    finally:
        conn.close()
        cur.close()

# Hàm tạo bảng 'XeMay'
def create_xemay_table():
    """
    Hàm tạo bảng 'XeMay' trong PostgreSQL
    """
    conn, cur = connect()
    try:
        print("Đang tạo bảng 'XeMay'...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.XeMay (
                MaSanPham VARCHAR(50) NOT NULL,
                TenSP VARCHAR(255) NOT NULL,
                Gia BIGINT,  -- Sử dụng BIGINT thay cho INTEGER
                NamDangKy BIGINT,  -- Sử dụng BIGINT thay cho INTEGER
                DungTich BIGINT,  -- Sử dụng BIGINT thay cho INTEGER
                MauSac VARCHAR(50),
                TinhTrang VARCHAR(50),
                SmartKeyID INTEGER,
                ThongTinSanPham TEXT,
                LoaiID INTEGER,
                CONSTRAINT xemay_pkey PRIMARY KEY (MaSanPham),
                CONSTRAINT fk_smartkey FOREIGN KEY (SmartKeyID)
                    REFERENCES public.SmartKey(SmartKeyID)
                    ON UPDATE CASCADE
                    ON DELETE CASCADE,
                CONSTRAINT fk_LoaiXeMay FOREIGN KEY (LoaiID)
                    REFERENCES public.LoaiXeMay(LoaiID)
                    ON UPDATE CASCADE
                    ON DELETE CASCADE
            );
        """)
        print("Bảng 'XeMay' đã được tạo thành công.")
        conn.commit()
    except Exception as e:
        print("Lỗi khi tạo bảng 'XeMay':", e)
    finally:
        conn.close()
        cur.close()

# Chạy các hàm tạo bảng
if __name__ == "__main__":
    create_LoaiXeMay_table()
    create_smartkey_table()
    create_xemay_table()
# # Vẽ sơ đồ ERD của hệ thống cơ sở dữ liệu xe máy 
# # Thêm đường dẫn của Graphviz vào PATH
# os.environ["PATH"] += os.pathsep + r"C:\Program Files\Graphviz\bin"

# # Tạo đồ thị
# dot = Digraph(comment="ERD cho hệ thống cơ sở dữ liệu xe máy")

# # Thêm các thực thể và mối quan hệ
# dot.node("LoaiXeMay", "LoaiXeMay\n\n- LoaiID (PK)\n- Loai")
# dot.node("SmartKey", "SmartKey\n\n- SmartKeyID (PK)\n- SmartKey")
# dot.node("XeMay", "XeMay\n\n- MaSanPham (PK)\n- TenSP\n- Gia\n- NamDangKy\n- DungTich\n"
#                   "- MauSac\n- TinhTrang\n- SmartKeyID (FK)\n- ThongTinSanPham\n- LoaiID (FK)")

# dot.edge("LoaiXeMay", "XeMay", label="1:N\nLoaiID")
# dot.edge("SmartKey", "XeMay", label="1:N\nSmartKeyID")

# # Xuất ra file
# dot.format = "png"
# dot.render("xemay_ERD", view=True)

# Hàm chèn dữ liệu vào bảng 'LoaiXeMay'
def insert_LoaiXeMay_data():
    conn, cur = connect()
    try:
        for index, row in LoaiXeMay.iterrows():
            cur.execute("""
                INSERT INTO public.LoaiXeMay (LoaiID, Loai)
                VALUES (%s, %s)
                ON CONFLICT (LoaiID) DO NOTHING;
            """, (row['LoaiID'], row['Loai']))
        print("Dữ liệu vào bảng 'LoaiXeMay' đã được chèn thành công.")
        conn.commit()
    except Exception as e:
        print("Lỗi khi chèn dữ liệu vào bảng 'LoaiXeMay':", e)
    finally:
        conn.close()
        cur.close()

# Hàm chèn dữ liệu vào bảng 'SmartKey'
def insert_smartkey_data():
    conn, cur = connect()
    try:
        for index, row in smartkey.iterrows():
            cur.execute("""
                INSERT INTO public.SmartKey (SmartKeyID, SmartKey)
                VALUES (%s, %s)
                ON CONFLICT (SmartKeyID) DO NOTHING;
            """, (row['SmartKeyID'], row['SmartKey']))
        print("Dữ liệu vào bảng 'SmartKey' đã được chèn thành công.")
        conn.commit()
    except Exception as e:
        print("Lỗi khi chèn dữ liệu vào bảng 'SmartKey':", e)
    finally:
        conn.close()
        cur.close()

# Hàm chèn dữ liệu vào bảng 'XeMay'
def insert_xemay_data():
    conn, cur = connect()
    try:
        for index, row in xemay.iterrows():
            cur.execute("""
                INSERT INTO public.XeMay (MaSanPham, TenSP, Gia, NamDangKy, DungTich, MauSac, TinhTrang, SmartKeyID, ThongTinSanPham, LoaiID)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (MaSanPham) DO NOTHING;
            """, (row['MaSanPham'], row['TenSP'], row['Gia'], row['NamDangKy'], row['DungTich'], row['MauSac'], row['TinhTrang'], row['SmartKeyID'], row['ThongTinSanPham'], row['LoaiID']))
        print("Dữ liệu vào bảng 'XeMay' đã được chèn thành công.")
        conn.commit()
    except Exception as e:
        print("Lỗi khi chèn dữ liệu vào bảng 'XeMay':", e)
    finally:
        conn.close()
        cur.close()

# Chạy các hàm chèn dữ liệu
if __name__ == "__main__":
    insert_LoaiXeMay_data()
    insert_smartkey_data()
    insert_xemay_data()

