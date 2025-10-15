from flask import Flask, jsonify, abort
import json

app = Flask(__name__)

@app.route('/products', methods=['GET'])
def get_products():
    try:
        with open('src/data/products.json', 'r') as f:
            products = json.load(f)
        return jsonify(products)
    except FileNotFoundError:
        abort(404, description="Product file not found. Please run setup_all_source_data.py first.")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)