#!/usr/bin/env python3
"""
Power BI Dashboard Data Refresh Script
- Updates existing Power BI dashboard with fresh data from database views
- No pandas or openpyxl dependencies
- Automatically detects and refreshes all analytics views
- Windows compatible with proper logging and Unicode handling
"""

import psycopg2
import json
import os
import csv
import logging
import sys
import subprocess
import time
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set console encoding to UTF-8 if possible
if sys.platform.startswith('win'):
    try:
        import codecs
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.detach())
        sys.stderr = codecs.getwriter('utf-8')(sys.stderr.detach())
    except:
        pass  # Fall back to safe printing

# Configure logging with UTF-8 encoding for file handler
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('powerbi_refresh.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class PowerBIDataRefresher:
    """Refreshes Power BI dashboard data from database views"""

    def __init__(self, dashboard_path=None):
        """Initialize the refresher"""
        self._validate_environment()
        self.db_config = self._get_secure_db_config()

        # Set base path first
        self.base_path = Path(__file__).parent.parent.parent
        self.output_dir = self.base_path / 'output'
        self.output_dir.mkdir(exist_ok=True)

        # Set dashboard path - auto-detect if not provided
        if dashboard_path:
            self.dashboard_path = Path(dashboard_path)
        else:
            self.dashboard_path = self._find_dashboard()

        # Analytics views from database
        self.analytics_views = []

        logger.info(f"Power BI Data Refresher initialized")
        logger.info(f"Dashboard path: {self.dashboard_path}")
        logger.info(f"Output directory: {self.output_dir}")

    def _validate_environment(self):
        """Validate required environment variables"""
        required_vars = ['DB_HOST', 'DB_USER', 'DB_PASS', 'DB_NAME']
        missing = [var for var in required_vars if not os.getenv(var)]

        if missing:
            raise EnvironmentError(f"Missing environment variables: {missing}")

        logger.info("Environment validation passed")

    def _get_secure_db_config(self):
        """Get database configuration from environment"""
        return {
            'host': os.getenv('DB_HOST'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASS'),
            'port': int(os.getenv('DB_PORT', '5432'))
        }

    def _find_dashboard(self):
        """Auto-detect Power BI dashboard file"""
        possible_locations = [
            self.base_path / "dashboard.pbix",
            self.base_path / "visual" / "dashboard.pbix",
            Path(__file__).parent / "dashboard.pbix",
            self.base_path / "dashboard.pbip"  # Power BI Project file
        ]

        for path in possible_locations:
            if path.exists():
                logger.info(f"Found dashboard: {path}")
                return path

        logger.warning("Dashboard file not found in standard locations")
        return self.base_path / "dashboard.pbix"  # Default assumption

    def _connect_database(self):
        """Create database connection"""
        try:
            conn = psycopg2.connect(**self.db_config)
            logger.debug("Database connection established")
            return conn
        except psycopg2.Error as e:
            logger.error(f"Database connection failed: {e}")
            raise

    def _discover_analytics_views(self):
        """Discover all views in the analytics schema"""
        conn = None
        cursor = None

        try:
            conn = self._connect_database()
            cursor = conn.cursor()

            # Query to get all views in analytics schema
            query = """
            SELECT table_name 
            FROM information_schema.views 
            WHERE table_schema = 'analytics'
            ORDER BY table_name;
            """

            cursor.execute(query)
            views = [row[0] for row in cursor.fetchall()]

            if not views:
                logger.warning("No views found in analytics schema")
                # Fallback to known views
                views = [
                    'rolling_orders_sum',
                    'top_customers',
                    'top_fuel_revenue',
                    'top_spenders',
                    'top_weeks_orders',
                    'top_weeks_revenue'
                ]
                logger.info(f"Using fallback views: {views}")
            else:
                logger.info(
                    f"Discovered {len(views)} analytics views: {views}")

            self.analytics_views = views
            return views

        except Exception as e:
            logger.error(f"Failed to discover analytics views: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def _extract_view_data(self, view_name):
        """Extract data from a specific view"""
        conn = None
        cursor = None

        try:
            conn = self._connect_database()
            cursor = conn.cursor()

            # Query the view
            query = f"SELECT * FROM analytics.{view_name}"
            cursor.execute(query)

            # Get column names and data
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            # Save to CSV
            csv_path = self.output_dir / f"{view_name}.csv"
            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(columns)  # Header
                writer.writerows(rows)    # Data

            row_count = len(rows)
            logger.info(
                f"Extracted {view_name}: {row_count} rows -> {csv_path}")

            return {
                'view_name': view_name,
                'file_path': str(csv_path),
                'row_count': row_count,
                'columns': columns,
                'last_updated': datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"Failed to extract {view_name}: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def _update_all_data_sources(self):
        """Update all data sources (CSV files)"""
        logger.info("Updating all data sources...")

        # Discover available views
        self._discover_analytics_views()

        # Extract data from each view
        extraction_results = []
        failed_extractions = []

        for view_name in self.analytics_views:
            try:
                result = self._extract_view_data(view_name)
                extraction_results.append(result)
            except Exception as e:
                logger.error(f"Failed to extract {view_name}: {e}")
                failed_extractions.append({
                    'view_name': view_name,
                    'error': str(e)
                })

        # Create update summary
        summary = {
            'timestamp': datetime.now().isoformat(),
            'total_views': len(self.analytics_views),
            'successful_extractions': len(extraction_results),
            'failed_extractions': len(failed_extractions),
            'total_rows': sum(r['row_count'] for r in extraction_results),
            'updated_files': [r['file_path'] for r in extraction_results],
            'failed_views': failed_extractions
        }

        # Save summary
        summary_path = self.output_dir / 'data_refresh_summary.json'
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2)

        logger.info(
            f"Data update completed: {summary['successful_extractions']}/{summary['total_views']} views updated")

        return summary

    def _refresh_powerbi_desktop(self):
        """Attempt to refresh Power BI Desktop if it's running"""
        try:
            # Check if Power BI Desktop is running
            result = subprocess.run(
                ['tasklist', '/FI', 'IMAGENAME eq PBIDesktop.exe'],
                capture_output=True,
                text=True,
                shell=True
            )

            if 'PBIDesktop.exe' in result.stdout:
                logger.info(
                    "Power BI Desktop is running - data will be refreshed automatically")
                # Note: Power BI Desktop automatically detects file changes and prompts for refresh
                return True
            else:
                logger.info("Power BI Desktop is not currently running")
                return False

        except Exception as e:
            logger.warning(f"Could not check Power BI Desktop status: {e}")
            return False

    def _create_refresh_report(self, data_summary):
        """Create a detailed refresh report"""
        report = {
            'refresh_info': {
                'timestamp': datetime.now().isoformat(),
                'dashboard_path': str(self.dashboard_path),
                'output_directory': str(self.output_dir),
                'refresh_status': 'completed' if data_summary['failed_extractions'] == 0 else 'partial'
            },
            'data_summary': data_summary,
            'files_updated': [
                {
                    'filename': Path(filepath).name,
                    'full_path': filepath,
                    'last_modified': datetime.now().isoformat()
                }
                for filepath in data_summary['updated_files']
            ],
            'next_steps': [
                "Data files have been updated in the output directory",
                "If Power BI Desktop is open, it should prompt for data refresh",
                "If not open, launch Power BI Desktop and open your dashboard",
                "Click 'Refresh' in Power BI to load the new data",
                "Save and publish your dashboard after reviewing the updates"
            ]
        }

        if data_summary['failed_extractions'] > 0:
            report['warnings'] = [
                f"{data_summary['failed_extractions']} views failed to update",
                "Check the logs for specific error details",
                "Some visualizations may show outdated data"
            ]

        return report

    def refresh_dashboard(self):
        """Main method to refresh the dashboard data"""
        logger.info("Starting Power BI dashboard data refresh...")

        try:
            # Step 1: Update all data sources
            data_summary = self._update_all_data_sources()

            # Step 2: Check if Power BI is running
            powerbi_running = self._refresh_powerbi_desktop()

            # Step 3: Create refresh report
            refresh_report = self._create_refresh_report(data_summary)
            refresh_report['powerbi_status'] = {
                'running': powerbi_running,
                'auto_refresh_available': powerbi_running
            }

            # Step 4: Save refresh report
            report_path = self.output_dir / 'refresh_report.json'
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(refresh_report, f, indent=2, ensure_ascii=False)

            # Step 5: Print summary
            self._print_refresh_summary(refresh_report)

            logger.info("Dashboard refresh process completed successfully!")
            return refresh_report

        except Exception as e:
            error_report = {
                'status': 'failed',
                'timestamp': datetime.now().isoformat(),
                'error': str(e),
                'error_type': type(e).__name__
            }

            logger.error(f"Dashboard refresh failed: {e}")

            # Save error report
            error_path = self.output_dir / 'refresh_error.json'
            with open(error_path, 'w', encoding='utf-8') as f:
                json.dump(error_report, f, indent=2)

            raise

    def _safe_print(self, text):
        """Safely print text, handling encoding issues"""
        try:
            print(text)
        except UnicodeEncodeError:
            # Replace problematic Unicode characters with safe alternatives
            safe_text = text.replace('✓', '[OK]').replace(
                '❌', '[FAIL]').replace('⚠', '[WARN]')
            print(safe_text)

    def _print_refresh_summary(self, report):
        """Print formatted refresh summary with safe encoding"""
        self._safe_print("\n" + "="*60)
        self._safe_print("POWER BI DASHBOARD REFRESH - COMPLETED!")
        self._safe_print("="*60)
        self._safe_print(f"Timestamp: {report['refresh_info']['timestamp']}")
        self._safe_print(
            f"Status: {report['refresh_info']['refresh_status'].upper()}")
        self._safe_print(
            f"Views Updated: {report['data_summary']['successful_extractions']}/{report['data_summary']['total_views']}")
        self._safe_print(
            f"Total Records: {report['data_summary']['total_rows']}")
        self._safe_print(
            f"Output Directory: {report['refresh_info']['output_directory']}")

        if report['powerbi_status']['running']:
            self._safe_print("\nPower BI Desktop Status: RUNNING [OK]")
            self._safe_print("- Data refresh should happen automatically")
        else:
            self._safe_print("\nPower BI Desktop Status: NOT RUNNING")
            self._safe_print(
                "- Launch Power BI and manually refresh when ready")

        if 'warnings' in report:
            self._safe_print("\nWarnings:")
            for warning in report['warnings']:
                self._safe_print(f"  [WARN] {warning}")

        self._safe_print(f"\nFiles Updated ({len(report['files_updated'])}):")
        for file_info in report['files_updated']:
            self._safe_print(f"  [OK] {file_info['filename']}")

        self._safe_print("\nNext Steps:")
        for i, step in enumerate(report['next_steps'], 1):
            self._safe_print(f"  {i}. {step}")

        self._safe_print("\nTo schedule automatic refreshes:")
        self._safe_print("  - Set up Windows Task Scheduler")
        self._safe_print("  - Run: python powerbi_refresher.py")
        self._safe_print("  - Or use: refresher.refresh_dashboard()")

    def schedule_refresh(self, interval_minutes=60):
        """Set up scheduled refresh (basic implementation)"""
        logger.info(
            f"Starting scheduled refresh every {interval_minutes} minutes...")

        while True:
            try:
                logger.info("Running scheduled refresh...")
                self.refresh_dashboard()
                logger.info(f"Sleeping for {interval_minutes} minutes...")
                time.sleep(interval_minutes * 60)

            except KeyboardInterrupt:
                logger.info("Scheduled refresh stopped by user")
                break
            except Exception as e:
                logger.error(f"Scheduled refresh error: {e}")
                logger.info(f"Continuing in {interval_minutes} minutes...")
                time.sleep(interval_minutes * 60)


# Factory functions for easy use
def refresh_powerbi_dashboard(dashboard_path=None):
    """Simple function to refresh Power BI dashboard"""
    refresher = PowerBIDataRefresher(dashboard_path)
    return refresher.refresh_dashboard()


def start_scheduled_refresh(interval_minutes=60, dashboard_path=None):
    """Start scheduled refresh process"""
    refresher = PowerBIDataRefresher(dashboard_path)
    refresher.schedule_refresh(interval_minutes)


# CLI interface
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description='Power BI Dashboard Data Refresher')
    parser.add_argument('--dashboard', '-d',
                        help='Path to Power BI dashboard file')
    parser.add_argument('--schedule', '-s', type=int, metavar='MINUTES',
                        help='Schedule refresh every N minutes')
    parser.add_argument('--once', action='store_true',
                        help='Run refresh once and exit')

    args = parser.parse_args()

    if args.schedule:
        logger.info(
            f"Starting scheduled refresh every {args.schedule} minutes")
        start_scheduled_refresh(args.schedule, args.dashboard)
    else:
        # Default: run once
        refresh_powerbi_dashboard(args.dashboard)
