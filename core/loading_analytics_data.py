import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from decimal import Decimal

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'Station'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASS')
}


def get_db_connection():
    """Establish database connection"""
    return psycopg2.connect(**DB_CONFIG)


def clean_analytics_tables(conn):
    """Clear all analytics tables before insertion"""
    tables = [
        'fct_orders', 'dim_customer', 'dim_date',
        'dim_product', 'dim_trailer', 'dim_fuel'
    ]
    with conn.cursor() as cursor:
        try:
            cursor.execute("SET CONSTRAINTS ALL DEFERRED")
            for table in tables:
                cursor.execute(f"TRUNCATE TABLE analytics.{table} CASCADE")
            conn.commit()
            print("Analytics tables cleared successfully")
        except Exception as e:
            conn.rollback()
            raise Exception(f"Error clearing analytics tables: {e}")


def populate_dim_customer(conn):
    """Populate customer dimension table"""
    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO analytics.dim_customer (
                customer_id, first_name, last_name, 
                customer_status, full_name, is_active
            )
            SELECT
                customer_id,
                first_name,
                last_name,
                customer_status,
                first_name || ' ' || last_name AS full_name,
                (customer_status = 'active') AS is_active
            FROM public.customers;
        """)
        print(f"Inserted {cursor.rowcount} customers into dim_customer")


def populate_dim_date(conn):
    """Populate date dimension table"""
    start_date = datetime.now().date().replace(year=datetime.now().year - 3)
    end_date = datetime.now().date() + timedelta(days=365)

    dates = []
    current = start_date
    while current <= end_date:
        dates.append((
            current,
            current.day,
            current.month,
            current.year,
            (current.month-1)//3 + 1,
            current.weekday(),
            current.strftime('%A'),
            current.strftime('%B'),
            current.weekday() >= 5
        ))
        current += timedelta(days=1)

    with conn.cursor() as cursor:
        cursor.executemany("""
            INSERT INTO analytics.dim_date (
                full_date, day, month, year, quarter, 
                day_of_week, day_name, month_name, is_weekend
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (full_date) DO NOTHING;
        """, dates)
        print(f"Inserted {len(dates)} dates into dim_date")


def populate_dim_product(conn):
    """Populate product dimension table"""
    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO analytics.dim_product (
                product_id, product_type, price, category
            )
            SELECT
                product_id,
                product_type,
                CAST(price AS NUMERIC)/100.0 AS price,
                CASE 
                    WHEN product_type IN ('engine oil', 'windshield fluid', 'car bulb') 
                        THEN 'Car Maintenance'
                    ELSE 'Convenience Items'
                END AS category
            FROM public.products;
        """)
        print(f"Inserted {cursor.rowcount} products into dim_product")


def populate_dim_trailer(conn):
    """Populate trailer dimension table"""
    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO analytics.dim_trailer (
                trailer_id, registry_number, trailer_status, is_available
            )
            SELECT
                trailer_id,
                registry_number::TEXT,
                trailer_status,
                (trailer_status = 'available') AS is_available
            FROM public.trailers
            WHERE registry_number IS NOT NULL;
        """)
        print(f"Inserted {cursor.rowcount} trailers into dim_trailer")


def populate_dim_fuel(conn):
    """Populate fuel dimension table"""
    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO analytics.dim_fuel (
                fuel_id, fuel_type, price_per_liter
            )
            SELECT
                fuel_id,
                fuel_type,
                CAST(fuel_price AS NUMERIC)/100.0 AS price_per_liter
            FROM public.fuel;
        """)
        print(f"Inserted {cursor.rowcount} fuel types into dim_fuel")


def populate_fct_orders(conn):
    """Populate fact orders table with proper decimal handling"""
    with conn.cursor() as cursor:
        # Get date keys mapping
        cursor.execute("SELECT full_date, date_key FROM analytics.dim_date")
        date_map = {row[0]: row[1] for row in cursor.fetchall()}

        # Fetch order data with related information
        cursor.execute("""
            SELECT
                o.order_id,
                o.order_status,
                o.order_date,
                o.customer_id,
                o.trailer_id,
                o.product_id,
                o.fuel_id,
                f.amount AS fuel_amount,
                f.fuel_price,
                p.price AS product_price,
                CASE 
                    WHEN o.trailer_id IS NOT NULL THEN 50.00
                    ELSE 0.00
                END AS trailer_cost
            FROM public.orders o
            JOIN public.fuel f ON o.fuel_id = f.fuel_id
            LEFT JOIN public.products p ON o.product_id = p.product_id
        """)

        orders = cursor.fetchall()
        fact_records = []

        for order in orders:
            order_date = order[2]
            date_key = date_map.get(order_date)

            if not date_key:
                continue

            # Convert all to Decimal for precise arithmetic
            fuel_price = Decimal(str(order[8])) / Decimal('100')
            product_price = Decimal(str(order[9] or '0')) / Decimal('100')
            trailer_cost = Decimal(str(order[10]))
            fuel_amount = Decimal(str(order[7]))

            fuel_cost = fuel_amount * fuel_price
            total_cost = fuel_cost + product_price + trailer_cost

            fact_records.append((
                order[0],  # order_id
                date_key,
                order[3],  # customer_id
                order[5],  # product_id
                order[4],  # trailer_id
                order[6],  # fuel_id
                order[1],  # order_status
                order_date,
                float(fuel_amount),  # Convert back to float for psycopg2
                float(fuel_cost),
                float(product_price),
                float(trailer_cost),
                float(total_cost)
            ))

        # Insert in batches
        for i in range(0, len(fact_records), 1000):
            batch = fact_records[i:i+1000]
            cursor.executemany("""
                INSERT INTO analytics.fct_orders (
                    order_id, date_key, customer_key, product_key, 
                    trailer_key, fuel_key, order_status, order_date,
                    fuel_amount, fuel_cost, product_cost, 
                    trailer_cost, total_cost
                )
                VALUES (
                    %s, %s,
                    (SELECT customer_key FROM analytics.dim_customer WHERE customer_id = %s),
                    (SELECT product_key FROM analytics.dim_product WHERE product_id = %s),
                    (SELECT trailer_key FROM analytics.dim_trailer WHERE trailer_id = %s),
                    (SELECT fuel_key FROM analytics.dim_fuel WHERE fuel_id = %s),
                    %s, %s, %s, %s, %s, %s, %s
                )
            """, batch)

        print(f"Inserted {len(fact_records)} orders into fct_orders")


def main():
    conn = None
    try:
        conn = get_db_connection()

        print("Starting analytics data population...")

        # Clear all analytics tables first
        clean_analytics_tables(conn)

        # Populate dimension tables
        populate_dim_customer(conn)
        populate_dim_date(conn)
        populate_dim_product(conn)
        populate_dim_trailer(conn)
        populate_dim_fuel(conn)

        # Populate fact table
        populate_fct_orders(conn)

        conn.commit()
        print("Analytics tables populated successfully!")

    except Exception as e:
        print(f"Error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
