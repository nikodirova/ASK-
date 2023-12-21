import requests
from datetime import datetime, timedelta
import sqlite3
import json
from apscheduler.schedulers.blocking import BlockingScheduler

# Токен и заголовки для запросов
token = 'd6c6424190cdf286445f3d67d5c5ad3e5268ff14'
headers = {"Authorization": f"Bearer {token}", 'Content-Type': 'application/json'}

def get_data_from_mysklad():
    
    url_retailstore = 'https://api.moysklad.ru/api/remap/1.2/entity/retailstore'
    response_retailstore = requests.get(url_retailstore, headers=headers)
    
    url_retaildemand = 'https://api.moysklad.ru/api/remap/1.2/entity/retaildemand'
    response_retaildemand = requests.get(url_retaildemand, headers=headers)
    
    url_retailsalesreturn = 'https://api.moysklad.ru/api/remap/1.2/entity/retailsalesreturn'
    response_retailsalesreturn = requests.get(url_retailsalesreturn, headers=headers)
    
    url_product = 'https://api.moysklad.ru/api/remap/1.2/entity/product'
    response_product = requests.get(url_product, headers=headers)

    data = {}

    # Обработка результатов запросов
    if response_retailstore.status_code == 200:
        retailstore_data = response_retailstore.json()
        data['retailstore'] = retailstore_data
        print("Retail Store Data:", retailstore_data)
    else:
        print("Error in Retail Store Request:", response_retailstore.status_code, response_retailstore.text)

    if response_retaildemand.status_code == 200:
        retaildemand_data = response_retaildemand.json()
        data['retaildemand'] = retaildemand_data
        print("Retail Demand Data:", retaildemand_data)
    else:
        print("Error in Retail Demand Request:", response_retaildemand.status_code, response_retaildemand.text)

    if response_retailsalesreturn.status_code == 200:
        retailsalesreturn_data = response_retailsalesreturn.json()
        data['retailsalesreturn'] = retailsalesreturn_data
        print("Retail Sales Return Data:", retailsalesreturn_data)
    else:
        print("Error in Retail Sales Return Request:", response_retailsalesreturn.status_code, response_retailsalesreturn.text)

    if response_product.status_code == 200:
        product_data = response_product.json()
        data['product'] = product_data
        print("Product Data:", product_data)
    else:
        print("Error in Product Request:", response_product.status_code, response_product.text)

    # Запись данных в JSON файл
    with open('mysklad_data.json', 'w') as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=4)

def send_internal_order(point_of_sale, order_date, order_amount):
    url = 'https://api.moysklad.ru/api/remap/1.2/entity/internalorder'
    
    # Формирование наименования заказа
    order_name = f"{point_of_sale}_{order_date}({order_amount})"

    # Запрос для создания заказа но выдает что необходимо указать организацию 
    payload = {
        "name": order_name
        
    }

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 201:
        print(f"Внутренний заказ успешно создан для точки продаж {point_of_sale}.")
    else:
        print(f"Ошибка при создании внутреннего заказа: {response.status_code}, {response.text}")


def create_database():
    # Подключение к базе данных SQLite и создание таблиц
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()

    cursor.execute('CREATE TABLE IF NOT EXISTS products (id TEXT PRIMARY KEY, name TEXT);')

    
    cursor.execute('CREATE TABLE IF NOT EXISTS sales (id TEXT PRIMARY KEY, product_id INTEGER, amount INTEGER, point_of_sale_id TEXT, FOREIGN KEY(product_id) REFERENCES products(id), FOREIGN KEY(point_of_sale_id) REFERENCES points_of_sale(id));')

    
    cursor.execute('CREATE TABLE IF NOT EXISTS returns (id TEXT PRIMARY KEY, product_id INTEGER, amount INTEGER, point_of_sale_id TEXT, FOREIGN KEY(product_id) REFERENCES products(id), FOREIGN KEY(point_of_sale_id) REFERENCES points_of_sale(id));')

    cursor.execute('CREATE TABLE IF NOT EXISTS points_of_sale (id TEXT PRIMARY KEY, name TEXT);')

    conn.close()

def insert_data_into_database(data, table_name):

    # Вставка данных в базу данных
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()

    if table_name == 'products':
        if 'rows' in data and isinstance(data['rows'], list):
            for entry in data['rows']:
                cursor.execute('INSERT INTO products (id, name) VALUES (?, ?);', (entry['id'], entry['name']))
        else:
            print("Ошибка: Отсутствуют данные о продуктах или они неверного формата.")

    elif table_name == 'sales':
        if 'rows' in data and isinstance(data['rows'], list):
            for entry in data['rows']:
                cursor.execute('INSERT INTO sales (id, product_id, amount, point_of_sale_id) VALUES (?, ?, ?, ?);', (entry['id'], entry['product_id'], entry['amount'], entry.get('point_of_sale_id')))
        else:
            print("Ошибка: Отсутствуют данные о продажах или они неверного формата.")

    elif table_name == 'returns':
        if 'rows' in data and isinstance(data['rows'], list):
            for entry in data['rows']:
                cursor.execute('INSERT INTO returns (id, product_id, amount, point_of_sale_id) VALUES (?, ?, ?, ?);', (entry['id'], entry['product_id'], entry['amount'], entry.get('point_of_sale_id')))
        else:
            print("Ошибка: Отсутствуют данные о возвратах или они неверного формата.")

    elif table_name == 'points_of_sale':
        if 'rows' in data and isinstance(data['rows'], list):
            for entry in data['rows']:
                try:
                    cursor.execute('INSERT INTO points_of_sale (id, name) VALUES (?, ?);', (entry['id'], entry['name']))
                except sqlite3.IntegrityError as e:
                    print(f"Пропуск дублирующейся записи с id {entry['id']}.")

        else:
            print("Ошибка: Отсутствуют данные о точках продаж или они неверного формата.")
            
    conn.commit()
    conn.close()

def analyze_orders():

    # Анализ внутренних заказов
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()

    cursor.execute('SELECT DISTINCT name FROM points_of_sale;')
    points_of_sale = cursor.fetchall()

    orders_data = []

    for point_of_sale in points_of_sale:
        cursor.execute('SELECT SUM(s.amount) FROM sales s JOIN points_of_sale p ON s.point_of_sale_id = p.id WHERE p.name=?;', (point_of_sale[0],))
        total_sales = cursor.fetchone()[0] or 0

        cursor.execute('SELECT SUM(r.amount) FROM returns r JOIN points_of_sale p ON r.point_of_sale_id = p.id WHERE p.name=?;', (point_of_sale[0],))
        total_returns = cursor.fetchone()[0] or 0

        net_sales = total_sales - total_returns

        order_entry = {'point_of_sale': point_of_sale[0], 'net_sales': net_sales}
        orders_data.append(order_entry)

    conn.close()

    return orders_data

def give_out_orders(orders_data):

    # Запись внутенних заказов в JSON файл
    with open('orders_to_send.json', 'w') as json_file:
        json.dump(orders_data, json_file, ensure_ascii=False, indent=4)

def job():

    # Расписание работы
    current_date = datetime.now()
    previous_date = current_date - timedelta(days=1)

    if current_date.weekday() != 6:
        get_data_from_mysklad()
        create_database()

        with open('mysklad_data.json', 'r') as json_file:
            data = json.load(json_file)
            
        if 'product' in data and data['product']:
            insert_data_into_database(data['product'], 'products')
        else:
            print("Нет доступных данных о продуктах.") 
        insert_data_into_database(data['product'], 'products')
        insert_data_into_database(data['retaildemand'], 'sales')
        insert_data_into_database(data['retailsalesreturn'], 'returns')
        insert_data_into_database(data['retailstore'], 'points_of_sale')
        orders_data = analyze_orders()
        give_out_orders(orders_data)

        for order_entry in orders_data:
            send_internal_order(order_entry['point_of_sale'], current_date.strftime("%d.%m.%Y"), order_entry['net_sales'])

scheduler = BlockingScheduler()
scheduler.add_job(job, 'cron', hour=9, minute=42, second=0, timezone='Europe/Moscow')
scheduler.start()

