from datetime import date, timedelta
import os
import psycopg2
import csv
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def get_previous_month_folder():
    today = date.today()
    first_of_this = today.replace(day=1)
    last_prev = first_of_this - timedelta(days=1)
    return last_prev.strftime("%Y_%m")


def export_tables(conn, output_folder):
    tables = [
        'public.customers',
        'public.orders',
        'public.fuel',
        'public.trailers',
        'public.products'
    ]
    os.makedirs(output_folder, exist_ok=True)
    with conn.cursor() as cur:
        for table in tables:
            cur.execute(f"SELECT *, CURRENT_DATE AS load_date FROM {table};")
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]

            table_name = table.split('.')[-1]
            file_path = os.path.join(output_folder, f"{table_name}.csv")
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(cols)
                writer.writerows(rows)

    print(f"✅ Dane zapisane w: {output_folder}")


def main():
    # Database configuration from environment variables
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'dbname': os.getenv('DB_NAME', 'Station'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASS'),
        'port': int(os.getenv('DB_PORT', 5432))
    }

    # Check if required environment variables are set
    if not db_config['password']:
        print("Error: DB_PASSWORD environment variable is required")
        return

    # Get the absolute path to the parent directory
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # Create the full path to docs/historical_data/YYYY_MM
    folder = os.path.join(parent_dir, 'docs',
                          'historical_data', get_previous_month_folder())

    try:
        conn = psycopg2.connect(**db_config)
        conn.set_client_encoding('UTF8')
        print("Connected to database successfully")

        export_tables(conn, folder)
    except Exception as e:
        print(f"Database connection error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed")


if __name__ == "__main__":
    main()
