import pandas as pd
import psycopg2
from pymongo import MongoClient
import re  # Nhập thư viện regular expressions

# Kết nối đến MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['oto_db']
collection = db['oto_collection']

# Truy vấn dữ liệu từ MongoDB
data = list(collection.find())  # Chuyển đổi dữ liệu từ MongoDB sang list of dictionaries
df = pd.DataFrame(data)  # Chuyển đổi dữ liệu sang DataFrame

# Hàm xử lý số điện thoại
def xuli_phonenumber(phonenumber):
    if isinstance(phonenumber, list):  # Kiểm tra xem phonenumber có phải là list không
        # Kết hợp các phần tử thành một chuỗi
        phonenumber = ' '.join(phonenumber)
    
    if isinstance(phonenumber, str):  # Nếu là chuỗi, xử lý
        cleaned_phonenumber = re.sub(r'[^0-9-]', '', phonenumber)  # Giữ lại số và dấu '-'
        cleaned_phonenumber = re.sub(r'-+', '-', cleaned_phonenumber)  # Xóa các dấu '-' liên tiếp
        return cleaned_phonenumber.strip('-')  # Xóa '-' đầu và cuối nếu có
    else:
        return None  # Trả về None cho các loại khác
# Áp dụng hàm xử lý cho cột 'phonenumber'
df['phonenumber'] = df['phonenumber'].apply(xuli_phonenumber)

# Hàm xử lý mã số xe
def xuli_code(code):
    if isinstance(code, list):  # Kiểm tra xem code có phải là list không
        # Kết hợp các phần tử thành một chuỗi
        code = ' '.join(code)
    
    if isinstance(code, str):  # Nếu là chuỗi, xử lý
        cleaned_code = re.sub(r'[^\d]', '', code)  # Giữ lại chỉ các ký tự số
        return cleaned_code
    else:
        return None  # Trả về None cho các loại khác

# Áp dụng hàm xử lý cho cột 'code'
df['code'] = df['code'].apply(xuli_code)

# Hàm xử lý trạng thái xe
def xuli_status(status):
    if isinstance(status, list):  # Kiểm tra xem status có phải là list không
        # Kết hợp các phần tử thành một chuỗi
        status = ' '.join(status)
    
    if isinstance(status, str):  # Nếu là chuỗi, xử lý
        # Giữ lại tất cả các ký tự chữ cái và dấu, bao gồm cả các ký tự đặc biệt trong tiếng Việt
        cleaned_status = re.sub(r'[^0-9a-zA-ZÀ-ỹ\s]', '', status)  # Giữ lại số, chữ (có dấu) và khoảng trắng
        return cleaned_status.strip()  # Xóa khoảng trắng đầu và cuối
    else:
        return None  # Trả về None cho các loại khác

# Áp dụng hàm xử lý cho cột 'status'
df['status'] = df['status'].apply(xuli_status)

def xuliTien(price):
    # Chuyển price thành chuỗi và xóa mọi khoảng trắng
    price = str(price).replace(" ", "").strip("[]'")  # Loại bỏ ký tự không cần thiết

    # Khởi tạo giá trị tổng
    total_value = 0

    # Kiểm tra và tính toán giá trị
    if 'Tỷ' in price:
        # Tách theo 'Tỷ' và xử lý phần trước
        parts = price.split('Tỷ')
        if parts[0]:  # Kiểm tra nếu có giá trị trước 'Tỷ'
            total_value += int(parts[0]) * 1000  # Chuyển Tỷ thành số

        # Kiểm tra nếu có giá trị triệu sau 'Tỷ'
        if len(parts) > 1:
            parts[1] = parts[1].replace("Triệu", "")  # Làm sạch phần triệu
            if parts[1]:  # Đảm bảo không rỗng
                total_value += int(parts[1]) * 1  # Chuyển Triệu thành số

    elif 'Triệu' in price:
        # Nếu chỉ có triệu
        parts = price.split('Triệu')
        if parts[0]:  # Kiểm tra nếu có giá trị trước 'Triệu'
            total_value += int(parts[0]) * 1  # Chuyển Triệu thành số

    return total_value

# Áp dụng hàm cho cột price trong dataframe
df['price'] = df['price'].apply(xuliTien)



# Hàm làm sạch địa chỉ
def clean_address(address):
    if isinstance(address, list):  # Kiểm tra xem address có phải là list không
        # Kết hợp các phần tử thành một chuỗi
        address = ' '.join(address)

    if isinstance(address, str):  # Nếu là chuỗi, xử lý
        # Xóa ký tự đặc biệt và dấu ngoặc
        cleaned_address = re.sub(r'[^\w\s]', '', address)  # Xóa dấu ngoặc và ký tự đặc biệt
        cleaned_address = cleaned_address.replace("*", "")  # Xóa dấu *
        cleaned_address = ' '.join(cleaned_address.split())  # Xóa khoảng trắng thừa
        return cleaned_address.strip()  # Xóa khoảng trắng đầu và cuối
    else:
        return None  # Trả về None cho các loại khác

# Áp dụng hàm làm sạch cho cột 'address'
df['address'] = df['address'].astype('str').apply(clean_address)


# Hàm làm sạch nội dung
def clean_content(content):
    if isinstance(content, list):  # Kiểm tra xem content có phải là list không
        # Kết hợp các phần tử thành một chuỗi
        content = ' '.join(content)

    if isinstance(content, str):  # Nếu là chuỗi, xử lý
        # Xóa ký tự đặc biệt và dấu ngoặc
        cleaned_content = re.sub(r'[^\w\s]', '', content)  # Xóa dấu ngoặc và ký tự đặc biệt
        cleaned_content = cleaned_content.replace("*", "")  # Xóa dấu *
        cleaned_content = ' '.join(cleaned_content.split())  # Xóa khoảng trắng thừa
        return cleaned_content.strip()  # Xóa khoảng trắng đầu và cuối
    else:
        return None  # Trả về None cho các loại khác

# Áp dụng hàm làm sạch cho cột 'content'
df['content'] = df['content'].astype('str').apply(clean_content)

# Hàm làm sạch thông số
def clean_parameter(parameter):
    if isinstance(parameter, list):  # Kiểm tra xem parameter có phải là list không
        # Kết hợp các phần tử thành một chuỗi
        parameter = ' '.join(parameter)

    if isinstance(parameter, str):  # Nếu là chuỗi, xử lý
        # Xóa ký tự đặc biệt và dấu ngoặc
        cleaned_parameter = re.sub(r'[^\w\s]', '', parameter)  # Xóa dấu ngoặc và ký tự đặc biệt
        cleaned_parameter = cleaned_parameter.replace("*", "")  # Xóa dấu *
        cleaned_parameter = ' '.join(cleaned_parameter.split())  # Xóa khoảng trắng thừa
        return cleaned_parameter.strip()  # Xóa khoảng trắng đầu và cuối
    else:
        return None  # Trả về None cho các loại khác

# Áp dụng hàm làm sạch cho cột 'parameter'
df['parameter'] = df['parameter'].astype('str').apply(clean_parameter)

# Hàm làm sạch cho các cột
def clean_sellername(sellername):
    if isinstance(sellername, list):  # Kiểm tra xem sellername có phải là list không
        # Kết hợp các phần tử thành một chuỗi
        sellername = ' '.join(sellername)

    if isinstance(sellername, str):  # Nếu là chuỗi, xử lý
        cleaned_sellername = re.sub(r'[^\w\s]', '', sellername)  # Xóa dấu ngoặc và ký tự đặc biệt
        cleaned_sellername = cleaned_sellername.replace("*", "")  # Xóa dấu *
        cleaned_sellername = ' '.join(cleaned_sellername.split())  # Xóa khoảng trắng thừa
        return cleaned_sellername.strip()  # Xóa khoảng trắng đầu và cuối
    else:
        return None  # Trả về None cho các loại khác

# Xử lý cho từng cột
df['sellername'] = df['sellername'].astype('str').apply(clean_sellername)

# Hàm làm sạch cho các cột
def clean_carname(carname):
    if isinstance(carname, list):  # Kiểm tra xem carname có phải là list không
        # Kết hợp các phần tử thành một chuỗi
        carname = ' '.join(carname)

    if isinstance(carname, str):  # Nếu là chuỗi, xử lý
        cleaned_carname = re.sub(r'[^\w\s]', '', carname)  # Xóa dấu ngoặc và ký tự đặc biệt
        cleaned_carname = cleaned_carname.replace("*", "")  # Xóa dấu *
        cleaned_carname = ' '.join(cleaned_carname.split())  # Xóa khoảng trắng thừa
        return cleaned_carname.strip()  # Xóa khoảng trắng đầu và cuối
    else:
        return None  # Trả về None cho các loại khác

# Xử lý cho từng cột
df['carname'] = df['carname'].astype('str').apply(clean_carname)

# Hàm làm sạch cho các cột
def clean_city(city):
    if isinstance(city, list):  # Kiểm tra xem city có phải là list không
        # Kết hợp các phần tử thành một chuỗi
        city = ' '.join(city)

    if isinstance(city, str):  # Nếu là chuỗi, xử lý
        cleaned_city = re.sub(r'[^\w\s]', '', city)  # Xóa dấu ngoặc và ký tự đặc biệt
        cleaned_city = cleaned_city.replace("*", "")  # Xóa dấu *
        cleaned_city = ' '.join(cleaned_city.split())  # Xóa khoảng trắng thừa
        return cleaned_city.strip()  # Xóa khoảng trắng đầu và cuối
    else:
        return None  # Trả về None cho các loại khác

# Xử lý cho từng cột
df['city'] = df['city'].astype('str').apply(clean_city)


def clean_id(id):
    if isinstance(id, list):  # Kiểm tra xem id có phải là list không
        # Kết hợp các phần tử thành một chuỗi
        id = ' '.join(id)

    if isinstance(id, str):  # Nếu là chuỗi, xử lý
        cleaned_id = re.sub(r'[^\w\s]', '', id)  # Xóa dấu ngoặc và ký tự đặc biệt
        cleaned_id = cleaned_id.replace("*", "")  # Xóa dấu *
        cleaned_id = ' '.join(cleaned_id.split())  # Xóa khoảng trắng thừa
        return cleaned_id.strip()  # Xóa khoảng trắng đầu và cuối
    else:
        return None  # Trả về None cho các loại khác

# Xử lý cho từng cột
df['_id'] = df['_id'].astype('str').apply(clean_id)


# Hàm làm sạch cho cột year
def clean_year(year):
    if isinstance(year, list):  # Kiểm tra xem year có phải là list không
        # Kết hợp các phần tử thành một chuỗi
        year = ' '.join(map(str, year))

    if isinstance(year, str):  # Nếu là chuỗi, xử lý
        # Xóa tất cả ký tự không phải số
        cleaned_year = re.sub(r'\D', '', year)
        return int(cleaned_year) if cleaned_year else None  # Chuyển thành int nếu có giá trị, nếu không trả về None
    elif isinstance(year, (int, float)):  # Nếu là số
        return int(year)  # Chuyển thành int
    else:
        return None  # Trả về None cho các loại khác

# Xử lý cho từng cột
df['year'] = df['year'].apply(clean_year)


# Định dạng kiểu dữ liệu cho các cột trong DataFrame
df['phonenumber'] = df['phonenumber'].astype(str)  # Định dạng số điện thoại là chuỗi
df['sellername'] = df['sellername'].astype(str)    # Định dạng tên người bán là chuỗi
df['carname'] = df['carname'].astype(str)          # Định dạng tên xe là chuỗi
df['code'] = df['code'].astype(str)                  # Định dạng mã số xe là chuỗi
df['status'] = df['status'].astype(str)              # Định dạng trạng thái xe là chuỗi
df['address'] = df['address'].astype(str)                # Định dạng giá là chuỗi
df['parameter'] = df['parameter'].astype(str)        # Định dạng thông số là chuỗi
df['address'] = df['address'].astype(str)            # Định dạng địa chỉ là chuỗi
df['city'] = df['city'].astype(str)                  # Định dạng thành phố là chuỗi
df['year'] = df['year'].astype(int)                  # Định dạng năm sản xuất là số nguyên
df['_id'] = df['_id'].astype(str)                    # Định dạng ID là chuỗi


# Lưu dữ liệu đã làm sạch vào tệp CSV (nếu cần)
df.to_csv('Oto.Oto.csv', index=False)

# Kết nối đến PostgreSQL
conn = psycopg2.connect(
    dbname="oto_db",  # Tên cơ sở dữ liệu
    user="postgres",  # Tên người dùng PostgreSQL
    password="123456",  # Mật khẩu của người dùng
    host="localhost",  # Địa chỉ máy chủ
    port="5432"  # Cổng (thường là 5432)
)


cursor = conn.cursor()

# Tạo bảng cho thông tin người bán
create_seller_table_query = '''
CREATE TABLE IF NOT EXISTS seller (
    seller_id SERIAL PRIMARY KEY,     -- Mã số tự động
    sellername VARCHAR(100) NOT NULL  -- Tên người bán
);
'''
cursor.execute(create_seller_table_query)

# Tạo bảng cho thông tin xe
create_car_table_query = '''
CREATE TABLE IF NOT EXISTS car (
    car_id SERIAL PRIMARY KEY,         -- Mã số tự động
    carname VARCHAR(100) NOT NULL,     -- Tên xe
    code VARCHAR(10),                   -- Mã số xe
    status VARCHAR(50),                 -- Trạng thái xe
    address VARCHAR(50),                  -- Giá
    parameter TEXT                      -- Thông số
);
'''
cursor.execute(create_car_table_query)

# Tạo bảng cho thông tin địa chỉ
create_address_table_query = '''
CREATE TABLE IF NOT EXISTS address (
    address_id SERIAL PRIMARY KEY,      -- Mã số tự động
    address TEXT NOT NULL,               -- Địa chỉ
    city VARCHAR(50)                     -- Thành phố
);
'''
cursor.execute(create_address_table_query)

# Tạo bảng cho thông tin xe và năm sản xuất
create_year_table_query = '''
CREATE TABLE IF NOT EXISTS car_year (
    year_id SERIAL PRIMARY KEY,          -- Mã số tự động
    year INTEGER NOT NULL,                -- Năm sản xuất
    car_id INT REFERENCES car(car_id)     -- Khóa ngoại tham chiếu đến bảng car
);
'''
cursor.execute(create_year_table_query)

# Tạo bảng cho thông tin liên hệ
create_contact_table_query = '''
CREATE TABLE IF NOT EXISTS contact (
    contact_id SERIAL PRIMARY KEY,      -- Mã số tự động
    phonenumber VARCHAR(15)              -- Số điện thoại
);
'''
cursor.execute(create_contact_table_query)

conn.commit()

# Chèn dữ liệu vào các bảng
for index, row in df.iterrows():
    # Chèn dữ liệu vào bảng seller
    cursor.execute('''
    INSERT INTO seller (sellername) 
    VALUES (%s) 
    ON CONFLICT (sellername) DO NOTHING;''', (row['sellername'],))

    # Chèn dữ liệu vào bảng car
    cursor.execute('''
    INSERT INTO car (carname, code, status, address, parameter) 
    VALUES (%s, %s, %s, %s, %s) 
    RETURNING car_id;''', (row['carname'], row['code'], row['status'], row['address'], row['parameter']))
    
    car_id = cursor.fetchone()[0]  # Lấy car_id vừa được chèn

    # Chèn dữ liệu vào bảng address
    cursor.execute('''
    INSERT INTO address (address, city) 
    VALUES (%s, %s) 
    RETURNING address_id;''', (row['address'], row['city']))
    
    address_id = cursor.fetchone()[0]  # Lấy address_id vừa được chèn

    # Chèn dữ liệu vào bảng car_year
    cursor.execute('''
    INSERT INTO car_year (year, car_id) 
    VALUES (%s, %s);''', (row['year'], car_id))

    # Chèn dữ liệu vào bảng contact
    cursor.execute('''
    INSERT INTO contact (phonenumber) 
    VALUES (%s) 
    ON CONFLICT (phonenumber) DO NOTHING;''', (row['phonenumber'],))

# Commit các thay đổi
conn.commit()

# Đóng con trỏ và kết nối
cursor.close()
conn.close()
