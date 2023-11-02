import datetime
import psycopg2
import os
import os
import requests
import json
import urllib.parse


conn = psycopg2.connect(
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)

# Retrieve SKUs in batches
batch_size = 1000
offset = 0
nm_ids = []

while True:
    # Create a cursor object
    cur = conn.cursor()
    # Execute the query to retrieve all SKUs
    cur.execute("SELECT DISTINCT sku FROM card OFFSET %s LIMIT %s", (offset, batch_size))
    # Fetch all the results
    skus = cur.fetchall()
    # Close the cursor and connection
    cur.close()
    
    if not skus:
        break

    nm_ids.extend([sku[0] for sku in skus])
    offset += batch_size



# Close the cursor and connection
cur.close()

# Split the list of SKUs into smaller chunks
chunk_size = 100
nm_ids_chunks = [nm_ids[i:i+chunk_size] for i in range(0, len(nm_ids), chunk_size)]



API_DETAIL_ENPOINT  = 'https://product-order-qnt.wildberries.ru/v2/by-nm/?'
DETAIL_HEADERS = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'ru-RU,ru;q=0.9',
        'Connection': 'keep-alive',
        'Host': 'card.wb.ru',
        'Origin': 'https://www.wildberries.ru',
        'sec-ch-ua': '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'
}

data = []

for chunk in nm_ids_chunks:
    nm_ids_str = ','.join([str(nm) for nm in chunk])
    params = {
        "appType": 1,
        "curr": "rub",
        "dest": -1257786,
        "regions": "80,38,4,64,83,33,68,70,69,30,86,75,40,1,66,110,22,31,48,71,114",
        "spp": 0,
        "nm": nm_ids_str
    }
    detail_url = API_DETAIL_ENPOINT + urllib.parse.urlencode(params)
    resp = requests.get(detail_url)
    try:
        orders = json.loads(resp.text)

        for order in orders:
            sku = order['nmId']
            qty = order['qnt']
            data.append((sku, qty))
    except json.decoder.JSONDecodeError as e:
        print(f"{chunk} Error decoding JSON:", e)

conn = psycopg2.connect(
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)

cursor = conn.cursor()

current_time = datetime.datetime.now()
# Insert the data into the stock table
cursor.executemany('''
    INSERT INTO order (sku, qty, created_at)
    VALUES (%s, %s, %s)
''', [(sku, qty, current_time) for sku, qty in data])

# Commit the changes and close the connection
conn.commit()
conn.close()