import json

from flask import Flask, render_template, request
import os
from kafka import KafkaProducer
app = Flask(__name__)

THIS_DIR = os.path.dirname(__file__)

@app.route('/', methods = ['POST'])
def post():
    sku_1 = request.form.get('sku_1', '')
    sku_2 = request.form.get('sku_2', '')
    sku_3 = request.form.get('sku_3', '')
    qty_1 = request.form.get('qty_1', '')
    qty_2 = request.form.get('qty_2', '')
    qty_3 = request.form.get('qty_3', '')

    json_one = get_config()
    json_two = get_config()
    json_three = get_config()

    json_one['Transaction']['RetailTransaction']['LineItem']['Quantity']['ItemID'] = sku_1
    json_two['Transaction']['RetailTransaction']['LineItem']['Quantity']['ItemID'] = sku_2
    json_three['Transaction']['RetailTransaction']['LineItem']['Quantity']['ItemID'] = sku_3

    json_one['Transaction']['RetailTransaction']['LineItem']['Quantity']['Units'] = qty_1
    json_two['Transaction']['RetailTransaction']['LineItem']['Quantity']['Units'] = qty_2
    json_three['Transaction']['RetailTransaction']['LineItem']['Quantity']['Units'] = qty_3

    json_file_list = [json_one, json_two, json_three]

    producer = KafkaProducer(bootstrap_servers='localhost:1234')
    for _ in json_file_list:
        future = producer.send('foobar', b'another_message')
        result = future.get(timeout=60)

    producer.flush()

    return render_template('home.html')

def get_config():
    """
    Get our config, and store it in memory, for faster subsequent access

    :return: Python dict of config values
    """

    with open(os.path.join(THIS_DIR, 'config.json')) as f:
        CONFIG = json.load(f)

    return CONFIG
