#!/usr/bin/env python3
"""
Station Data Pipeline Orchestrator
==================================
This is the main file that runs the entire data pipeline in the correct order:

1. Data Generation (generator.py)
2. Data Loading to Staging (loading_data.py)
3. Data Cleaning & Validation (cleaning_validating_data.py)
4. Clean Data Insertion to Production (clean_data_insertion.py)
5. Analytics Data Loading (loading_analytics_data.py)
6. Analytics Views Creation (views_creator.py)
7. Historical Data Saving (historical_data_saving.py)
8. Dashboard Data Refresh (dashboard_generator.py)

Usage:
    python main.py                    # Run full pipeline
    python main.py --skip-generation  # Skip data generation step
    python main.py --dashboard-only   # Only refresh dashboard
    python main.py --schedule 60      # Run pipeline every 60 minutes
"""

import os
import sys
import subprocess
import logging
import argparse
import time
import json
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set console encoding to UTF-8 if possible (Windows compatibility)
if sys.platform.startswith('win'):
    try:
        import codecs
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.detach())
        sys.stderr = codecs.getwriter('utf-8')(sys.stderr.detach())
    except:
        pass  # Fall back to safe printing

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('main_pipeline.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class StationDataPipeline:
    """Main orchestrator for the Station data pipeline"""

    def __init__(self):
        """Initialize the pipeline orchestrator"""
        self.base_path = Path(__file__).parent
        self.core_path = self.base_path / 'core'
        self.analytics_path = self.base_path / 'analytics' / 'managment'

        # Pipeline steps configuration
        self.pipeline_steps = [
            {
                'name': 'Data Generation',
                'script': self.core_path / 'generator.py',
                'description': 'Generate synthetic data for the current month',
                'required': True,
                'skip_flag': 'skip_generation'
            },
            {
                'name': 'Data Loading to Staging',
                'script': self.core_path / 'loading_data.py',
                'description': 'Load raw data files into staging tables',
                'required': True,
                'skip_flag': None
            },
            {
                'name': 'Data Cleaning & Validation',
                'script': self.core_path / 'cleaning_validating_data.py',
                'description': 'Clean and validate data in staging tables',
                'required': True,
                'skip_flag': None
            },
            {
                'name': 'Clean Data Insertion',
                'script': self.core_path / 'clean_data_insertion.py',
                'description': 'Move cleaned data from staging to production tables',
                'required': True,
                'skip_flag': None
            },
            {
                'name': 'Analytics Data Loading',
                'script': self.core_path / 'loading_analytics_data.py',
                'description': 'Load data into analytics dimension and fact tables',
                'required': True,
                'skip_flag': None
            },
            {
                'name': 'Analytics Views Creation',
                'script': self.analytics_path / 'views_creator.py',
                'description': 'Create/update analytics views from SQL files',
                'required': True,
                'skip_flag': None
            },
            {
                'name': 'Historical Data Saving',
                'script': self.core_path / 'historical_data_saving.py',
                'description': 'Save current data state to historical archives',
                'required': False,
                'skip_flag': None
            },
            {
                'name': 'Dashboard Data Refresh',
                'script': self.analytics_path / 'dashboard_generator.py',
                'description': 'Refresh Power BI dashboard with latest data',
                'required': True,
                'skip_flag': None
            }
        ]

        # Runtime statistics
        self.execution_stats = {
            'start_time': None,
            'end_time': None,
            'total_duration': None,
            'steps_executed': 0,
            'steps_successful': 0,
            'steps_failed': 0,
            'step_results': []
        }

        logger.info("Station Data Pipeline Orchestrator initialized")

    def _safe_print(self, text):
        """Safely print text, handling encoding issues"""
        try:
            print(text)
        except UnicodeEncodeError:
            # Replace problematic Unicode characters with safe alternatives
            safe_text = (text.replace('✅', '[OK]')
                         .replace('❌', '[FAIL]')
                         .replace('⚠️', '[WARN]')
                         .replace('🎉', '[SUCCESS]')
                         .replace('🔄', '[RUNNING]')
                         .replace('🛑', '[STOPPED]')
                         .replace('⏰', '[SCHEDULED]'))
            print(safe_text)

    def _validate_environment(self):
        """Validate that all required environment variables are set"""
        required_vars = ['DB_HOST', 'DB_USER', 'DB_PASS', 'DB_NAME']
        missing_vars = [var for var in required_vars if not os.getenv(var)]

        if missing_vars:
            raise EnvironmentError(
                f"Missing required environment variables: {missing_vars}")

        logger.info("Environment validation passed")

    def _validate_file_structure(self):
        """Validate that all required script files exist"""
        missing_files = []

        for step in self.pipeline_steps:
            if not step['script'].exists():
                missing_files.append(str(step['script']))

        if missing_files:
            logger.error("Missing required script files:")
            for file in missing_files:
                logger.error(f"  - {file}")
            raise FileNotFoundError(
                f"Missing {len(missing_files)} required script files")

        logger.info("File structure validation passed")

    def _run_script(self, script_path, step_name):
        """Run a single Python script and capture results"""
        logger.info(f"Starting: {step_name}")
        logger.info(f"Script: {script_path}")

        start_time = datetime.now()

        try:
            # Get Python executable (use venv if available)
            python_exe = sys.executable
            venv_python = self.base_path / 'venv' / 'Scripts' / 'python.exe'
            if venv_python.exists():
                python_exe = str(venv_python)

            # Run the script
            result = subprocess.run(
                [python_exe, str(script_path)],
                capture_output=True,
                text=True,
                cwd=str(self.base_path),
                timeout=1800  # 30 minutes timeout
            )

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            step_result = {
                'step_name': step_name,
                'script_path': str(script_path),
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration,
                'return_code': result.returncode,
                'success': result.returncode == 0,
                'stdout': result.stdout,
                'stderr': result.stderr
            }

            if result.returncode == 0:
                logger.info(
                    f"[OK] {step_name} completed successfully ({duration:.1f}s)")
                if result.stdout.strip():
                    logger.info("Output:")
                    for line in result.stdout.strip().split('\n'):
                        logger.info(f"  {line}")
            else:
                logger.error(
                    f"[FAIL] {step_name} failed (exit code: {result.returncode})")
                if result.stderr.strip():
                    logger.error("Error output:")
                    for line in result.stderr.strip().split('\n'):
                        logger.error(f"  {line}")
                if result.stdout.strip():
                    logger.info("Standard output:")
                    for line in result.stdout.strip().split('\n'):
                        logger.info(f"  {line}")

            return step_result

        except subprocess.TimeoutExpired:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            logger.error(f"[FAIL] {step_name} timed out after {duration:.1f}s")

            return {
                'step_name': step_name,
                'script_path': str(script_path),
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration,
                'return_code': -1,
                'success': False,
                'stdout': '',
                'stderr': 'Process timed out after 30 minutes'
            }

        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            logger.error(f"[FAIL] {step_name} failed with exception: {e}")

            return {
                'step_name': step_name,
                'script_path': str(script_path),
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration,
                'return_code': -1,
                'success': False,
                'stdout': '',
                'stderr': str(e)
            }

    def run_full_pipeline(self, skip_generation=False, dashboard_only=False):
        """Run the complete data pipeline"""
        logger.info("="*70)
        logger.info("STARTING STATION DATA PIPELINE")
        logger.info("="*70)

        self.execution_stats['start_time'] = datetime.now()

        try:
            # Validate environment and files
            self._validate_environment()
            self._validate_file_structure()

            # Determine which steps to run
            steps_to_run = []

            if dashboard_only:
                # Only run dashboard refresh
                steps_to_run = [step for step in self.pipeline_steps
                                if step['name'] == 'Dashboard Data Refresh']
                logger.info("Running dashboard-only mode")
            else:
                # Run all steps (potentially skipping generation)
                for step in self.pipeline_steps:
                    if skip_generation and step.get('skip_flag') == 'skip_generation':
                        logger.info(f"Skipping: {step['name']} (as requested)")
                        continue
                    steps_to_run.append(step)

            logger.info(f"Pipeline will execute {len(steps_to_run)} steps")

            # Execute each step
            for i, step in enumerate(steps_to_run, 1):
                logger.info(f"\n[{i}/{len(steps_to_run)}] {step['name']}")
                logger.info(f"Description: {step['description']}")

                result = self._run_script(step['script'], step['name'])
                self.execution_stats['step_results'].append(result)
                self.execution_stats['steps_executed'] += 1

                if result['success']:
                    self.execution_stats['steps_successful'] += 1
                else:
                    self.execution_stats['steps_failed'] += 1

                    # Check if this is a required step
                    if step['required']:
                        logger.error(
                            f"Required step '{step['name']}' failed. Stopping pipeline.")
                        break
                    else:
                        logger.warning(
                            f"Optional step '{step['name']}' failed. Continuing...")

            # Calculate final statistics
            self.execution_stats['end_time'] = datetime.now()
            self.execution_stats['total_duration'] = (
                self.execution_stats['end_time'] -
                self.execution_stats['start_time']
            ).total_seconds()

            # Print final summary
            self._print_pipeline_summary()

            # Save execution report
            self._save_execution_report()

            # Return success status
            return self.execution_stats['steps_failed'] == 0

        except Exception as e:
            logger.error(f"Pipeline failed with critical error: {e}")
            self.execution_stats['end_time'] = datetime.now()
            self.execution_stats['total_duration'] = (
                self.execution_stats['end_time'] -
                self.execution_stats['start_time']
            ).total_seconds() if self.execution_stats['start_time'] else 0

            self._save_execution_report()
            return False

    def _print_pipeline_summary(self):
        """Print a formatted summary of the pipeline execution"""
        stats = self.execution_stats

        self._safe_print("\n" + "="*70)
        self._safe_print("PIPELINE EXECUTION SUMMARY")
        self._safe_print("="*70)
        self._safe_print(
            f"Start Time: {stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        self._safe_print(
            f"End Time: {stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        self._safe_print(
            f"Total Duration: {stats['total_duration']:.1f} seconds ({stats['total_duration']/60:.1f} minutes)")
        self._safe_print(f"Steps Executed: {stats['steps_executed']}")
        self._safe_print(f"Steps Successful: [OK] {stats['steps_successful']}")
        self._safe_print(f"Steps Failed: [FAIL] {stats['steps_failed']}")

        if stats['step_results']:
            self._safe_print("\nStep Details:")
            for result in stats['step_results']:
                status = "[OK]" if result['success'] else "[FAIL]"
                self._safe_print(
                    f"  {status} {result['step_name']} ({result['duration_seconds']:.1f}s)")

        if stats['steps_failed'] == 0:
            self._safe_print("\n[SUCCESS] PIPELINE COMPLETED SUCCESSFULLY!")
        else:
            self._safe_print(
                f"\n[WARN] PIPELINE COMPLETED WITH {stats['steps_failed']} FAILURES")

        self._safe_print("="*70)

    def _save_execution_report(self):
        """Save detailed execution report to JSON file"""
        try:
            report_path = self.base_path / 'pipeline_execution_report.json'

            # Convert datetime objects to strings for JSON serialization
            report_data = {
                'execution_summary': {
                    'start_time': self.execution_stats['start_time'].isoformat() if self.execution_stats['start_time'] else None,
                    'end_time': self.execution_stats['end_time'].isoformat() if self.execution_stats['end_time'] else None,
                    'total_duration_seconds': self.execution_stats['total_duration'],
                    'steps_executed': self.execution_stats['steps_executed'],
                    'steps_successful': self.execution_stats['steps_successful'],
                    'steps_failed': self.execution_stats['steps_failed'],
                    'overall_success': self.execution_stats['steps_failed'] == 0
                },
                'step_details': self.execution_stats['step_results'],
                'environment_info': {
                    'python_version': sys.version,
                    'working_directory': str(self.base_path),
                    'db_host': os.getenv('DB_HOST', 'not_set'),
                    'db_name': os.getenv('DB_NAME', 'not_set')
                }
            }

            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, indent=2, ensure_ascii=False)

            logger.info(f"Execution report saved to: {report_path}")

        except Exception as e:
            logger.warning(f"Could not save execution report: {e}")

    def schedule_pipeline(self, interval_minutes=60, skip_generation=False):
        """Run the pipeline on a schedule"""
        logger.info(
            f"Starting scheduled pipeline execution every {interval_minutes} minutes")
        logger.info("Press Ctrl+C to stop the scheduler")

        run_count = 0

        while True:
            try:
                run_count += 1
                logger.info(f"\n[RUNNING] Starting scheduled run #{run_count}")

                success = self.run_full_pipeline(
                    skip_generation=skip_generation)

                if success:
                    logger.info(
                        f"[OK] Scheduled run #{run_count} completed successfully")
                else:
                    logger.error(f"[FAIL] Scheduled run #{run_count} failed")

                logger.info(
                    f"[SCHEDULED] Next run in {interval_minutes} minutes...")
                time.sleep(interval_minutes * 60)

            except KeyboardInterrupt:
                logger.info("[STOPPED] Scheduled pipeline stopped by user")
                break
            except Exception as e:
                logger.error(
                    f"[FAIL] Scheduled run #{run_count} failed with error: {e}")
                logger.info(
                    f"[SCHEDULED] Continuing with next run in {interval_minutes} minutes...")
                time.sleep(interval_minutes * 60)


def main():
    """Main entry point with command line argument parsing"""
    parser = argparse.ArgumentParser(
        description='Station Data Pipeline Orchestrator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                        # Run full pipeline
  python main.py --skip-generation      # Skip data generation step
  python main.py --dashboard-only       # Only refresh dashboard
  python main.py --schedule 60          # Run pipeline every 60 minutes
  python main.py --schedule 30 --skip-generation  # Scheduled run without generation
        """
    )

    parser.add_argument(
        '--skip-generation',
        action='store_true',
        help='Skip the data generation step (use existing data)'
    )

    parser.add_argument(
        '--dashboard-only',
        action='store_true',
        help='Only refresh the dashboard (skip all data processing steps)'
    )

    parser.add_argument(
        '--schedule',
        type=int,
        metavar='MINUTES',
        help='Run pipeline on schedule every N minutes'
    )

    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )

    args = parser.parse_args()

    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Create pipeline orchestrator
    pipeline = StationDataPipeline()

    try:
        if args.schedule:
            # Run on schedule
            pipeline.schedule_pipeline(
                interval_minutes=args.schedule,
                skip_generation=args.skip_generation
            )
        else:
            # Run once
            success = pipeline.run_full_pipeline(
                skip_generation=args.skip_generation,
                dashboard_only=args.dashboard_only
            )

            # Exit with appropriate code
            sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Critical pipeline error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
