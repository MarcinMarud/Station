import csv
from datetime import datetime, timedelta
from faker import Faker
import random
from pathlib import Path

fake = Faker('en_US')

# Statusy i typy produktów
CUSTOMER_STATUSES = ['active', 'inactive', 'blocked']
FUEL_TYPES = ['PB95', 'diesel', 'PB98', 'LPG']
TRAILER_STATUSES = ['available', 'rented', 'in_service', 'reserved']
PRODUCT_TYPES = [
    'engine oil', 'windshield fluid', 'car bulb',
    'polishing paste', 'chewing gum', 'instant coffee',
    'energy drink', 'mineral water', 'chips', 'chocolate',
    'gift set', 'lighter', 'ice scraper', 'washing sponge'
]
ORDER_STATUSES = ['placed', 'paid', 'in_progress', 'completed', 'canceled']


def get_output_directory():
    """Tworzy folder docs/raw_data/YYYY_MM dla poprzedniego miesiąca"""
    last_month = (datetime.now().replace(day=1) - timedelta(days=1))
    year_month = last_month.strftime("%Y_%m")

    # Ścieżka do folderu docs/raw_data/YYYY_MM
    # Wychodzimy z core do głównego folderu
    base_dir = Path(__file__).parent.parent
    output_dir = base_dir / 'docs' / 'raw_data' / year_month

    # Utwórz folder jeśli nie istnieje
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def get_last_month_dates():
    """Zwraca daty z poprzedniego miesiąca"""
    today = datetime.now()
    first_day_of_current_month = today.replace(day=1)
    last_day_of_last_month = first_day_of_current_month - timedelta(days=1)
    first_day_of_last_month = last_day_of_last_month.replace(day=1)
    return first_day_of_last_month.date(), last_day_of_last_month.date()


def generate_customers(count=None):
    """Generuje 120-180 klientów z nierównym rozkładem statusów"""
    count = count or random.randint(120, 180)
    data = []
    for i in range(count):
        data.append({
            'customer_id': i + 1,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'customer_status': random.choices(
                CUSTOMER_STATUSES,
                weights=[0.7, 0.2, 0.1],
                k=1
            )[0]
        })
    return data


def generate_fuel_prices():
    """Realistyczne ceny paliw (w groszach) z lekką losowością"""
    return {
        'PB95': random.randint(645, 685),
        'diesel': random.randint(645, 705),
        'PB98': random.randint(685, 745),
        'LPG': random.randint(285, 325)
    }


def generate_fuel(count=None):
    """Generuje 600-900 transakcji paliwowych"""
    count = count or random.randint(600, 900)
    fuel_prices = generate_fuel_prices()
    start_date, end_date = get_last_month_dates()
    data = []

    for i in range(count):
        transaction_date = fake.date_between(
            start_date=start_date, end_date=end_date)
        fuel_type = random.choice(FUEL_TYPES)
        amount = random.randint(5, 80)
        price = fuel_prices[fuel_type]

        data.append({
            'fuel_id': i + 1,
            'fuel_type': fuel_type,
            'amount': amount,
            'fuel_price': price,
            'transaction_date': transaction_date
        })
    return data


def generate_trailers(count=None):
    """Generuje 12-20 przyczep z różnymi okresami wynajmu"""
    count = count or random.randint(12, 20)
    start_date, end_date = get_last_month_dates()
    data = []

    for i in range(count):
        registry_number = f"{random.randint(10, 99)} {fake.license_plate().split()[0]}"
        status = random.choices(
            TRAILER_STATUSES,
            weights=[0.5, 0.3, 0.1, 0.1],
            k=1
        )[0]

        if status == 'rented':
            rental_start = fake.date_between(
                start_date=start_date, end_date=end_date)
            rental_end = min(
                rental_start + timedelta(days=random.randint(1, 21)),
                end_date
            )
        elif status == 'reserved':
            rental_start = fake.date_between(
                start_date=end_date + timedelta(days=1),
                end_date=end_date + timedelta(days=30)
            )
            rental_end = rental_start + timedelta(days=random.randint(1, 14))
        else:
            rental_start = None
            rental_end = None

        data.append({
            'trailer_id': i + 1,
            'registry_number': registry_number,
            'trailer_status': status,
            'start_date': rental_start,
            'end_date': rental_end
        })
    return data


def generate_products():
    """Generuje produkty z realistycznymi cenami i zmiennymi ilościami"""
    data = []
    for i, product in enumerate(PRODUCT_TYPES, start=1):
        quantity = random.randint(10, 150)

        if 'oil' in product:
            price = random.randint(3500, 15000)
        elif 'fluid' in product:
            price = random.randint(800, 2500)
        elif 'bulb' in product:
            price = random.randint(1500, 5000)
        else:
            price = random.randint(100, 2000)

        data.append({
            'product_id': i,
            'product_type': product,
            'quantity': quantity,
            'price': price
        })
    return data


def generate_orders(customers, trailers, products, fuel, count=None):
    """Generuje 1000-1500 zamówień ze zmiennymi proporcjami"""
    count = count or random.randint(1000, 1500)
    start_date, end_date = get_last_month_dates()
    data = []

    for i in range(count):
        order_status = random.choices(
            ORDER_STATUSES,
            weights=[0.2, 0.3, 0.2, 0.2, 0.1],
            k=1
        )[0]

        data.append({
            'order_id': i + 1,
            'order_status': order_status,
            'order_date': fake.date_between(start_date=start_date, end_date=end_date),
            'customer_id': random.choice(customers)['customer_id'],
            'trailer_id': random.choice(trailers)['trailer_id'] if random.random() > 0.75 else None,
            'product_id': random.choice(products)['product_id'] if random.random() > 0.35 else None,
            'fuel_id': random.choice(fuel)['fuel_id']
        })
    return data


def save_to_csv(data, filename, output_dir):
    """Zapisuje dane do pliku CSV w podanym folderze"""
    if not data:
        return
    keys = data[0].keys()
    filepath = output_dir / filename
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)


def main():
    print("Generating data for the previous month...")

    # Pobierz folder docelowy (będzie zawierał datę poprzedniego miesiąca)
    output_dir = get_output_directory()
    print(f"Saving data to: {output_dir}")

    print("Generating customers...")
    customers = generate_customers()
    save_to_csv(customers, 'customers.csv', output_dir)

    print("Generating products...")
    products = generate_products()
    save_to_csv(products, 'products.csv', output_dir)

    print("Generating trailers...")
    trailers = generate_trailers()
    save_to_csv(trailers, 'trailers.csv', output_dir)

    print("Generating fuel transactions...")
    fuel = generate_fuel()
    save_to_csv(fuel, 'fuel.csv', output_dir)

    print("Generating orders...")
    orders = generate_orders(customers, trailers, products, fuel)
    save_to_csv(orders, 'orders.csv', output_dir)

    print("Data successfully generated!")


if __name__ == "__main__":
    main()
