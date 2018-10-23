import json

from flask import Flask, render_template, request
import os
from kafka import KafkaProducer
app = Flask(__name__)

THIS_DIR = os.path.dirname(__file__)

json_one, json_two, json_three =None, None, None

@app.route('/', methods = ['POST', 'GET'])

def post():
    context = {
        "price_1": '0',
        "price_2": '0',
        "price_3": '0',
    }

    if request.method == 'POST':
        sku_1 = request.form.get('sku_1', '')
        sku_2 = request.form.get('sku_2', '')
        sku_3 = request.form.get('sku_3', '')
        qty_1 = request.form.get('qty_1', '0')
        qty_2 = request.form.get('qty_2', '0')
        qty_3 = request.form.get('qty_3', '0')

        price_1 = int(qty_1) * 2
        price_2 = int(qty_2) * 4
        price_3 = int(qty_3) * 6

        context.update({'price_1': price_1, 'price_2': price_2, 'price_3':price_3})

        json_one = get_config()
        json_two = get_config()
        json_three = get_config()

        json_one['Transaction']['RetailTransaction']['LineItem']['Quantity']['ItemID'] = 12345
        json_two['Transaction']['RetailTransaction']['LineItem']['Quantity']['ItemID'] = 67890
        json_three['Transaction']['RetailTransaction']['LineItem']['Quantity']['ItemID'] = 98078

        json_one['Transaction']['RetailTransaction']['LineItem']['Quantity']['Units'] = qty_1
        json_two['Transaction']['RetailTransaction']['LineItem']['Quantity']['Units'] = qty_2
        json_three['Transaction']['RetailTransaction']['LineItem']['Quantity']['Units'] = qty_3

        json_file_list = [json_one, json_two, json_three]

        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        for json_file in json_file_list:
            future = producer.send('test', json.dumps(json_file).encode())
            result = future.get(timeout=60)

        producer.flush()

    return render_template('home.html', **context)

def get_config():
    """
    Get our config, and store it in memory, for faster subsequent access

    :return: Python dict of config values
    """

    with open(os.path.join(THIS_DIR, 'config.json')) as f:
        CONFIG = json.load(f)

    return CONFIG
