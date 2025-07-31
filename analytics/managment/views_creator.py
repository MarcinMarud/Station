import os
import psycopg2
from pathlib import Path
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def create_analytics_views():
    """
    Read SQL files from analytics/queries folder and create views in analytics schema
    """

    # Database configuration from environment variables
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'database': os.getenv('DB_NAME', 'Station'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASS'),
        'port': int(os.getenv('DB_PORT', 5432))
    }

    # Check if required environment variables are set
    if not db_config['password']:
        print("Error: DB_PASSWORD environment variable is required")
        return

    # Path to SQL files
    sql_folder = Path("analytics/queries")

    # Check if folder exists
    if not sql_folder.exists():
        print(f"Error: Folder {sql_folder} does not exist")
        return

    # Get all SQL files
    sql_files = list(sql_folder.glob("*.sql"))

    if not sql_files:
        print(f"No SQL files found in {sql_folder}")
        return

    print(f"Found {len(sql_files)} SQL files")

    # Connect to database
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        print("Connected to database successfully")

        # Create analytics schema if it doesn't exist
        cursor.execute("CREATE SCHEMA IF NOT EXISTS analytics")
        conn.commit()
        print("Analytics schema ready")

        # Process each SQL file
        successful = 0
        failed = 0

        for sql_file in sql_files:
            try:
                # Get view name from filename (remove .sql extension)
                view_name = sql_file.stem

                # Read SQL content
                with open(sql_file, 'r', encoding='utf-8') as f:
                    sql_content = f.read().strip()

                if not sql_content:
                    print(f"Skipping empty file: {sql_file.name}")
                    continue

                # Remove semicolon if present
                if sql_content.endswith(';'):
                    sql_content = sql_content[:-1]

                # Drop view first if it exists (to handle column name changes)
                drop_view_sql = f"DROP VIEW IF EXISTS analytics.{view_name}"
                cursor.execute(drop_view_sql)

                # Create view
                create_view_sql = f"CREATE VIEW analytics.{view_name} AS\n{sql_content}"

                cursor.execute(create_view_sql)
                conn.commit()

                print(f"✅ Created view: analytics.{view_name}")
                successful += 1

            except Exception as e:
                print(f"❌ Failed to create view from {sql_file.name}: {e}")
                failed += 1
                conn.rollback()

        # Print summary
        print(f"\n{'='*50}")
        print("SUMMARY")
        print(f"{'='*50}")
        print(f"Total files: {len(sql_files)}")
        print(f"Successful: {successful}")
        print(f"Failed: {failed}")
        print(f"{'='*50}")

    except Exception as e:
        print(f"Database connection error: {e}")

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        print("Database connection closed")


if __name__ == "__main__":
    create_analytics_views()
