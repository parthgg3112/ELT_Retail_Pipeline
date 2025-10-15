
import pandas as pd
from faker import Faker
import random
from sqlalchemy import create_engine
import time
import os
from dotenv import load_dotenv
import json
from datetime import datetime


load_dotenv()
db_password = os.getenv("POSTGRES_PASSWORD")
if not db_password:
    raise ValueError("POSTGRES_PASSWORD not found in .env file.")
db_url = f'postgresql://postgres:{db_password}@localhost:5432/retail_db'
engine = create_engine(db_url)

fake = Faker()

def setup_sources():
    """
    Generates a complete retail dataset, ensuring logical consistency
    between customer registration and transaction dates.
    """
    start_time = time.time()
    
    
    num_customers = 2000
    num_products = 500
    num_transactions = 20000

    
    print("Generating master data for customers and products...")

    
    customers_data = [{
        'customer_id': i,
        'first_name': fake.first_name(),
        'last_name': fake.last_name(),
        'email': fake.unique.email(),
        'street_address': fake.street_address(),
        'city': fake.city(),
        'state': fake.state_abbr(),
        'registration_date': fake.date_this_decade()
    } for i in range(1, num_customers + 1)]
    customers_df = pd.DataFrame(customers_data)
    
    customers_df['registration_date'] = pd.to_datetime(customers_df['registration_date'])

    
    product_templates = {
        'Electronics': [f"{random.choice(['Quantum', 'Astra', 'Nova'])} {random.randint(24, 75)}-inch TV", f"{fake.word().capitalize()} Wireless Headphones"],
        'Home Goods': [f"{fake.color_name().capitalize()} Cotton Sheet Set", "Stainless Steel Cookware Set"],
        'Apparel': [f"Men's Classic {random.choice(['T-Shirt', 'Jeans'])}", f"Women's {random.choice(['Summer Dress', 'Blouse'])}"],
        'Books': [f"The Mystery of {fake.last_name()}'s Manor", f"A Guide to {fake.word().capitalize()} Cooking"],
    
        'Toys': ["LEGO City Fire Station", "Remote Control Race Car"]
    }
    categories = list(product_templates.keys())
    products_data = []
    for i in range(1, num_products + 1):
        category = random.choice(categories)
        product_name = random.choice(product_templates[category])
        selling_price = round(random.uniform(5.0, 800.0), 2)
        products_data.append({'product_id': i, 'product_name': product_name, 'category': category, 'selling_price': selling_price, 'cost_price': round(selling_price * random.uniform(0.6, 0.8), 2)})
    products_df = pd.DataFrame(products_data)


    
    print("\nGenerating sales data for PostgreSQL...")
    transactions_data = []
    sales_details_data = []

    for i in range(1, num_transactions + 1):
        transaction_id = 10000 + i
        
        customer_id = random.randint(1, num_customers)
        
        registration_date = customers_df.loc[customers_df['customer_id'] == customer_id, 'registration_date'].iloc[0]
        
        transaction_date = fake.date_time_between(start_date=registration_date, end_date='now')
        

        transactions_data.append({
            'transaction_id': transaction_id,
            'customer_id': customer_id, 
            'transaction_date': transaction_date 
        })
        
        num_items = random.randint(1, 5)
        product_ids = random.sample(range(1, num_products + 1), num_items)
        for product_id in product_ids:
            quantity = random.randint(1, 3)
            price = products_df.loc[products_df['product_id'] == product_id, 'selling_price'].iloc[0]
            sales_details_data.append({'transaction_id': transaction_id, 'product_id': product_id, 'quantity': quantity, 'price_at_sale': price})
    
    transactions_df = pd.DataFrame(transactions_data)
    sales_details_df = pd.DataFrame(sales_details_data)

    try:
        transactions_df.to_sql('sales_transactions', engine, if_exists='replace', index=False)
        sales_details_df.to_sql('sales_details', engine, if_exists='replace', index=False)
        print(f"-> Successfully loaded sales data into PostgreSQL.")
    except Exception as e:
        print(f"Error writing to PostgreSQL: {e}")
        return

    print("\nSaving product data to JSON file...")
    if not os.path.exists('data'):
        os.makedirs('data')
    with open('data/products.json', 'w') as f:
        f.write(products_df.to_json(orient='records', indent=4))
    print(f"-> Saved {len(products_df)} products to data/products.json.")

    print("\nSaving customer data to CSV file...")
    customers_df['registration_date'] = customers_df['registration_date'].dt.date 
    customers_df.to_csv('data/customers.csv', index=False)
    print(f"-> Saved {len(customers_df)} customers to data/customers.csv.")
    
    end_time = time.time()
    print(f"\nSource data setup finished in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    setup_sources()
