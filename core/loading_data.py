import psycopg2
import csv
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from pathlib import Path

# Ładowanie zmiennych środowiskowych
load_dotenv()

# Konfiguracja połączenia z bazą danych
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'Station'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASS')
}


def get_data_directory():
    """Zwraca ścieżkę do folderu docs/raw_data/YYYY_MM dla poprzedniego miesiąca"""
    current_date = datetime.now()
    # Oblicz pierwszy dzień bieżącego miesiąca i odejmij jeden dzień, aby uzyskać ostatni dzień poprzedniego miesiąca
    first_day_of_current_month = current_date.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    year_month = last_day_of_previous_month.strftime("%Y_%m")

    # Ścieżka do folderu docs/raw_data/YYYY_MM
    # Wychodzimy z core do głównego folderu
    base_dir = Path(__file__).parent.parent
    data_dir = base_dir / 'docs' / 'raw_data' / year_month

    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")

    return data_dir


# Mapowanie plików na tabele i kolumny
TABLE_CONFIG = {
    'customers.csv': {
        'table': 'staging.customers',
        'columns': ['customer_id', 'first_name', 'last_name', 'customer_status']
    },
    'fuel.csv': {
        'table': 'staging.fuel',
        'columns': ['fuel_id', 'fuel_type', 'amount', 'fuel_price', 'transaction_date']
    },
    'trailers.csv': {
        'table': 'staging.trailers',
        'columns': ['trailer_id', 'registry_number', 'trailer_status', 'start_date', 'end_date']
    },
    'products.csv': {
        'table': 'staging.products',
        'columns': ['product_id', 'product_type', 'quantity', 'price']
    },
    'orders.csv': {
        'table': 'staging.orders',
        'columns': ['order_id', 'order_status', 'order_date', 'customer_id',
                    'trailer_id', 'product_id', 'fuel_id']
    }
}


def get_db_connection():
    """Utwórz połączenie z bazą danych"""
    return psycopg2.connect(**DB_CONFIG)


def clear_staging_tables(conn):
    """Wyczyść tabele stagingowe przed załadowaniem nowych danych"""
    with conn.cursor() as cursor:
        cursor.execute("TRUNCATE staging.customers")
        cursor.execute("TRUNCATE staging.fuel")
        cursor.execute("TRUNCATE staging.trailers")
        cursor.execute("TRUNCATE staging.products")
        cursor.execute("TRUNCATE staging.orders")
        conn.commit()
    print("Staging tables cleared successfully")


def load_csv_to_staging(conn, filename, data_dir):
    """Załaduj dane z pliku CSV do odpowiedniej tabeli stagingowej"""
    if filename not in TABLE_CONFIG:
        print(f"Unknown file: {filename}. Skipping...")
        return

    config = TABLE_CONFIG[filename]
    filepath = data_dir / filename

    if not filepath.exists():
        print(f"File {filepath} does not exist. Skipping...")
        return

    # Sprawdź czy plik jest pusty
    if filepath.stat().st_size == 0:
        print(f"File {filename} is empty. Skipping...")
        return

    # Odczytaj dane z CSV
    rows = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except Exception as e:
        print(f"Error reading file {filename}: {e}")
        return

    if not rows:
        print(f"File {filename} contains no data. Skipping...")
        return

    with conn.cursor() as cursor:
        # Przygotuj zapytanie SQL
        columns = ', '.join(config['columns'] + ['source_file'])
        placeholders = ', '.join(['%s'] * (len(config['columns']) + 1))
        query = f"INSERT INTO {config['table']} ({columns}) VALUES ({placeholders})"

        # Wstaw dane
        success_count = 0
        for row in rows:
            try:
                values = []
                for col in config['columns']:
                    # Zamień puste stringi na None
                    value = row.get(col, '')
                    if value == '':
                        value = None
                    values.append(value)
                values.append(filename)  # Dodaj nazwę pliku jako source_file
                cursor.execute(query, values)
                success_count += 1
            except Exception as e:
                print(f"Error inserting record: {e}")
                continue

        conn.commit()

    print(
        f"Loaded {success_count}/{len(rows)} records from {filename} to {config['table']}")


def main():
    print("Starting data loading to staging...")

    try:
        # Pobierz folder z danymi
        data_dir = get_data_directory()
        print(f"Loading data from: {data_dir}")

        conn = get_db_connection()

        # Wyczyść tabele stagingowe
        clear_staging_tables(conn)

        # Załaduj dane z każdego pliku CSV
        for filename in TABLE_CONFIG.keys():
            print(f"\nProcessing file: {filename}")
            try:
                load_csv_to_staging(conn, filename, data_dir)
            except Exception as e:
                print(f"Critical error processing file {filename}: {e}")
                continue

        print("\nData loading to staging completed successfully!")

    except Exception as e:
        print(f"Error during data loading: {e}")
    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    main()
