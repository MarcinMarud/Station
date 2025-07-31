import psycopg2
import os
from dotenv import load_dotenv

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


def get_db_connection():
    """Utwórz połączenie z bazą danych"""
    return psycopg2.connect(**DB_CONFIG)


def clean_and_validate_data(conn):
    """Uproszczona wersja czyszczenia i walidacji danych"""
    with conn.cursor() as cursor:
        try:
            # 1. Podstawowe czyszczenie tabeli customers
            cursor.execute("""
                DELETE FROM staging.customers 
                WHERE first_name IS NULL OR last_name IS NULL;
            """)

            # 2. Podstawowe czyszczenie tabeli fuel
            cursor.execute("""
                DELETE FROM staging.fuel 
                WHERE fuel_type IS NULL OR amount IS NULL OR fuel_price IS NULL;
            """)

            # 3. Podstawowe czyszczenie tabeli trailers
            cursor.execute("""
                DELETE FROM staging.trailers 
                WHERE trailer_status IS NULL;
                
                -- Uproszczona walidacja dat
                DELETE FROM staging.trailers
                WHERE (trailer_status IN ('rented', 'reserved') 
                AND (start_date IS NULL OR end_date IS NULL OR end_date < start_date));
            """)

            # 4. Podstawowe czyszczenie tabeli products
            cursor.execute("""
                DELETE FROM staging.products 
                WHERE quantity < 0 OR price <= 0;
            """)

            # 5. Podstawowe czyszczenie tabeli orders
            cursor.execute("""
                -- Usuń zamówienia bez powiązanych klientów lub paliwa
                DELETE FROM staging.orders 
                WHERE customer_id NOT IN (SELECT customer_id FROM staging.customers)
                   OR fuel_id NOT IN (SELECT fuel_id FROM staging.fuel);
                
                -- Usuń zamówienia z nieistniejącymi przyczepami (jeśli trailer_id jest ustawiony)
                DELETE FROM staging.orders 
                WHERE trailer_id IS NOT NULL 
                  AND trailer_id NOT IN (SELECT trailer_id FROM staging.trailers);
                
                -- Usuń zamówienia z nieistniejącymi produktami (jeśli product_id jest ustawiony)
                DELETE FROM staging.orders 
                WHERE product_id IS NOT NULL 
                  AND product_id NOT IN (SELECT product_id FROM staging.products);
            """)

            conn.commit()
            print("Podstawowe czyszczenie danych zakończone pomyślnie!")

        except Exception as e:
            conn.rollback()
            print(f"Błąd podczas przetwarzania danych: {e}")
            raise


def main():
    print("Rozpoczynanie podstawowego czyszczenia danych stagingowych...")

    try:
        conn = get_db_connection()
        clean_and_validate_data(conn)
        print("Proces zakończony sukcesem!")
    except Exception as e:
        print(f"Błąd podczas przetwarzania danych: {e}")
    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    main()
