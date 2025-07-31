import psycopg2
import os
from dotenv import load_dotenv

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


def modify_table_constraints(conn):
    """Temporarily modify constraints to allow NULL values"""
    with conn.cursor() as cursor:
        try:
            cursor.execute("""
                ALTER TABLE public.orders 
                ALTER COLUMN trailer_id DROP NOT NULL,
                ALTER COLUMN product_id DROP NOT NULL;
            """)
            conn.commit()
            print("Temporarily relaxed NULL constraints on orders table")
        except Exception as e:
            conn.rollback()
            raise Exception(f"Error modifying constraints: {e}")


def clear_production_tables(conn):
    """Clear all data from production tables"""
    with conn.cursor() as cursor:
        try:
            cursor.execute("SET CONSTRAINTS ALL DEFERRED")
            cursor.execute(
                "TRUNCATE TABLE public.orders, public.products, public.trailers, public.fuel, public.customers CASCADE")
            conn.commit()
            print("Production tables cleared successfully")
        except Exception as e:
            conn.rollback()
            raise Exception(f"Error clearing tables: {e}")


def transfer_data(conn):
    """Transfer cleaned data from staging to production tables"""
    with conn.cursor() as cursor:
        try:
            print("Starting data transfer from staging to production...")

            # 1. Transfer customers
            cursor.execute("""
                INSERT INTO public.customers (customer_id, first_name, last_name, customer_status)
                SELECT customer_id, first_name, last_name, customer_status
                FROM staging.customers
            """)
            print(f"Transferred {cursor.rowcount} customers")

            # 2. Transfer fuel data
            cursor.execute("""
                INSERT INTO public.fuel (fuel_id, fuel_type, amount, fuel_price)
                SELECT fuel_id, fuel_type, amount, fuel_price
                FROM staging.fuel
            """)
            print(f"Transferred {cursor.rowcount} fuel records")

            # 3. Transfer trailers with proper registry_number handling
            cursor.execute("""
                INSERT INTO public.trailers (trailer_id, registry_number, trailer_status, start_date, end_date)
                SELECT 
                    trailer_id, 
                    CASE 
                        WHEN registry_number ~ '^[0-9]+$' THEN CAST(registry_number AS INTEGER)
                        ELSE NULL
                    END,
                    trailer_status, 
                    start_date, 
                    end_date
                FROM staging.trailers
                WHERE registry_number ~ '^[0-9]+$' OR registry_number IS NULL
            """)
            print(f"Transferred {cursor.rowcount} trailers")

            # 4. Transfer products
            cursor.execute("""
                INSERT INTO public.products (product_id, product_type, quantity, price)
                SELECT product_id, product_type, quantity, price
                FROM staging.products
            """)
            print(f"Transferred {cursor.rowcount} products")

            # 5. Transfer orders with NULL handling
            cursor.execute("""
                INSERT INTO public.orders (order_id, order_status, order_date, customer_id, trailer_id, product_id, fuel_id)
                SELECT 
                    order_id, 
                    order_status, 
                    order_date, 
                    customer_id, 
                    trailer_id,  -- Now can be NULL
                    product_id, -- Now can be NULL
                    fuel_id
                FROM staging.orders
                WHERE customer_id IN (SELECT customer_id FROM public.customers)
                AND fuel_id IN (SELECT fuel_id FROM public.fuel)
                AND (trailer_id IS NULL OR trailer_id IN (SELECT trailer_id FROM public.trailers))
                AND (product_id IS NULL OR product_id IN (SELECT product_id FROM public.products))
            """)
            print(f"Transferred {cursor.rowcount} orders")

            conn.commit()
            print("Data transfer completed successfully!")

        except Exception as e:
            conn.rollback()
            raise Exception(f"Error during data transfer: {e}")


def restore_constraints(conn):
    """Restore original constraints after transfer"""
    with conn.cursor() as cursor:
        try:
            cursor.execute("""
                ALTER TABLE public.orders 
                ALTER COLUMN trailer_id SET NOT NULL,
                ALTER COLUMN product_id SET NOT NULL;
            """)
            conn.commit()
            print("Restored original constraints on orders table")
        except Exception as e:
            print(f"Warning: Could not restore constraints: {e}")
            conn.rollback()


def main():
    conn = None
    try:
        conn = get_db_connection()

        # Step 1: Temporarily relax constraints
        modify_table_constraints(conn)

        # Step 2: Clear production tables
        clear_production_tables(conn)

        # Step 3: Transfer cleaned data
        transfer_data(conn)

        # Step 4: Restore constraints (optional)
        # restore_constraints(conn)  # Commented out as you likely want to keep NULLs allowed

    except Exception as e:
        print(f"ERROR: {e}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
