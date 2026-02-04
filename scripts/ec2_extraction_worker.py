#!/usr/bin/env python3
"""
Standalone TTB Extraction Worker for EC2 Instances

This script extracts raw TTB COLA data and writes directly to S3 in a format
compatible with Dagster's S3PickleIOManager.

Usage:
    python ec2_extraction_worker.py --start-date 2000-01-01 --end-date 2001-04-20 --worker-id 1

Requirements:
    pip install requests boto3 watchtower
"""
import argparse
import boto3
import hashlib
import json
import logging
import pickle
import sys
import time
import urllib3
from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Set, Optional

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# CloudWatch Logs configuration
CLOUDWATCH_LOG_GROUP = "/ttb-extraction/workers"
CLOUDWATCH_REGION = "us-east-1"

# Basic logging setup (CloudWatch handler added later with worker_id)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger(__name__)


def setup_cloudwatch_logging(worker_id: str):
    """
    Setup CloudWatch Logs handler for real-time monitoring.

    Log Group: /ttb-extraction/workers
    Log Stream: worker-{worker_id}
    """
    try:
        import watchtower

        # Create CloudWatch handler
        cloudwatch_handler = watchtower.CloudWatchLogHandler(
            log_group=CLOUDWATCH_LOG_GROUP,
            stream_name=f"worker-{worker_id}",
            boto3_client=boto3.client('logs', region_name=CLOUDWATCH_REGION),
            create_log_group=True
        )
        cloudwatch_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        )

        # Add to root logger so all logs go to CloudWatch
        logging.getLogger().addHandler(cloudwatch_handler)
        logger.info(f"CloudWatch logging enabled: {CLOUDWATCH_LOG_GROUP}/worker-{worker_id}")
        return True

    except ImportError:
        logger.warning("watchtower not installed - CloudWatch logging disabled")
        return False
    except Exception as e:
        logger.warning(f"Failed to setup CloudWatch logging: {e}")
        return False

# ============================================================================
# Configuration
# ============================================================================

# S3 Configuration
S3_BUCKET = "ciq-dagster"
S3_PREFIX = "ttb-pre-prod/ttb_raw_data"

# TTB Configuration
TTB_ERROR_PAGE_HASH = "50fa048f9cf8200c3d82d60add59b3b1f78f9e3ebc67f9395051595fc830a9e3"
DATA_TYPES = ["cola-detail", "certificate"]
RECEIPT_METHODS = [0, 1, 2, 3]  # hand-delivered, e-filed, mailed, overnight

# Extraction Configuration
MAX_SEQUENCE_PER_BATCH = 15000
CONSECUTIVE_FAILURE_THRESHOLD = 100
REQUEST_DELAY_SECONDS = 0.5
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30

# ============================================================================
# TTB ID Utilities (inline for standalone operation)
# ============================================================================

def date_to_julian(date_obj: date) -> int:
    """Convert a date to Julian day of year."""
    return date_obj.timetuple().tm_yday


def build_ttb_id(year: int, julian_day: int, receipt_method: int, sequence: int) -> str:
    """
    Build a TTB ID from components.

    TTB ID Structure (14 digits): YYJJJRRRSSSSS
    - YY: Year (last 2 digits)
    - JJJ: Julian day (001-366)
    - RRR: Receipt method (000=hand-delivered, 001=e-filed, 002=mailed, 003=overnight)
    - SSSSSS: Sequence number (000001-999999)
    """
    year_2digit = year % 100
    return f"{year_2digit:02d}{julian_day:03d}{receipt_method:03d}{sequence:06d}"


def is_ttb_error_page(content: bytes) -> bool:
    """Check if response content is a TTB error page by comparing hash."""
    content_hash = hashlib.sha256(content).hexdigest()
    return content_hash == TTB_ERROR_PAGE_HASH


def generate_date_range(start_date: date, end_date: date):
    """Generate dates between start and end date (inclusive)."""
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)

# ============================================================================
# Sequence Tracker
# ============================================================================

class SequenceTracker:
    """Track consecutive failures to detect end of sequence."""

    def __init__(self, max_consecutive_failures: int = 500):
        self.max_consecutive_failures = max_consecutive_failures
        self.consecutive_failures = 0
        self.total_success = 0
        self.total_failures = 0
        self.successful_sequences: Set[int] = set()
        self.min_sequence: Optional[int] = None
        self.max_sequence: Optional[int] = None

    def record_success(self, sequence: int):
        self.consecutive_failures = 0
        self.total_success += 1
        self.successful_sequences.add(sequence)

        if self.min_sequence is None or sequence < self.min_sequence:
            self.min_sequence = sequence
        if self.max_sequence is None or sequence > self.max_sequence:
            self.max_sequence = sequence

    def record_failure(self, sequence: int):
        self.consecutive_failures += 1
        self.total_failures += 1

    def should_stop(self) -> bool:
        return self.consecutive_failures >= self.max_consecutive_failures

    def get_stats(self) -> Dict[str, Any]:
        return {
            "total_success": self.total_success,
            "total_failures": self.total_failures,
            "consecutive_failures": self.consecutive_failures,
            "min_sequence": self.min_sequence,
            "max_sequence": self.max_sequence,
            "sequences_found": len(self.successful_sequences)
        }

# ============================================================================
# TTB Extraction
# ============================================================================

def extract_partition(partition_date: date, delay: float = 1.0) -> Dict[str, Any]:
    """
    Extract all TTB data for a single partition date.

    Returns data in Dagster-compatible format.
    """
    import requests

    logger.info(f"Extracting partition: {partition_date}")

    julian_day = date_to_julian(partition_date)
    date_str = partition_date.isoformat()

    # Initialize tracking
    all_records = []
    records_by_method = {method: [] for method in RECEIPT_METHODS}
    stats_by_method = {
        method: {"total": 0, "cola_detail": 0, "certificate": 0, "failed": 0}
        for method in RECEIPT_METHODS
    }
    completeness_reports = {}
    total_failed = 0

    # Process each receipt method and data type combination
    for receipt_method in RECEIPT_METHODS:
        for data_type in DATA_TYPES:
            logger.info(f"  Processing {data_type}/method-{receipt_method}")

            # Configure URL action based on data type
            if data_type == "cola-detail":
                action_param = "publicDisplaySearchAdvanced"
            else:  # certificate
                action_param = "publicFormDisplay"

            tracker = SequenceTracker(CONSECUTIVE_FAILURE_THRESHOLD)
            failed_count = 0
            sequence = 1

            while sequence <= MAX_SEQUENCE_PER_BATCH:
                ttb_id = build_ttb_id(
                    year=partition_date.year,
                    julian_day=julian_day,
                    receipt_method=receipt_method,
                    sequence=sequence
                )

                url = f"https://ttbonline.gov/colasonline/viewColaDetails.do?action={action_param}&ttbid={ttb_id}"

                try:
                    # Rate limiting
                    time.sleep(delay)

                    # Make request with retries
                    response = None
                    for retry in range(MAX_RETRIES):
                        try:
                            response = requests.get(
                                url,
                                verify=False,
                                timeout=REQUEST_TIMEOUT
                            )
                            break
                        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                            if retry < MAX_RETRIES - 1:
                                wait_time = (retry + 1) * 2
                                logger.warning(f"    Retry {retry + 1} for {ttb_id}: {e}")
                                time.sleep(wait_time)
                            else:
                                raise

                    if response is None:
                        raise Exception("No response after retries")

                    if 200 <= response.status_code < 300:
                        content = response.content

                        if is_ttb_error_page(content):
                            # Error page - treat as failure
                            tracker.record_failure(sequence)
                            failed_count += 1

                            if tracker.should_stop():
                                logger.info(f"    Stopping after {tracker.consecutive_failures} consecutive failures at seq {sequence}")
                                break
                        else:
                            # Valid content
                            tracker.record_success(sequence)

                            record = {
                                "ttb_id": ttb_id,
                                "sequence": sequence,
                                "html_content": content.decode('utf-8', errors='ignore'),
                                "partition_date": date_str,
                                "receipt_method": receipt_method,
                                "data_type": data_type,
                                "extraction_timestamp": datetime.now().isoformat(),
                                "url": url,
                                "size_bytes": len(content),
                                "status_code": response.status_code
                            }

                            all_records.append(record)
                            records_by_method[receipt_method].append(record)
                            stats_by_method[receipt_method]["total"] += 1

                            if data_type == "cola-detail":
                                stats_by_method[receipt_method]["cola_detail"] += 1
                            else:
                                stats_by_method[receipt_method]["certificate"] += 1
                    else:
                        tracker.record_failure(sequence)
                        failed_count += 1

                        if tracker.should_stop():
                            break

                except Exception as e:
                    tracker.record_failure(sequence)
                    failed_count += 1
                    logger.error(f"    Error for {ttb_id}: {e}")

                    if tracker.should_stop():
                        break

                sequence += 1

            # Store completeness report
            report_key = f"{receipt_method}_{data_type}"
            stats = tracker.get_stats()
            completeness_reports[report_key] = stats
            total_failed += failed_count
            stats_by_method[receipt_method]["failed"] += failed_count

            logger.info(f"    Completed: {stats['total_success']} success, {failed_count} failed")

    # Calculate totals
    total_records = len(all_records)
    cert_count = len([r for r in all_records if r['data_type'] == 'certificate'])
    cola_count = len([r for r in all_records if r['data_type'] == 'cola-detail'])
    total_processed = total_records + total_failed
    success_rate = total_records / total_processed if total_processed > 0 else 0

    logger.info(f"Partition {partition_date} complete: {total_records} records ({cola_count} cola-detail, {cert_count} certificate)")

    # Return Dagster-compatible structure
    return {
        "by_receipt_method": {
            method: {
                "records": records_by_method[method],
                "stats": stats_by_method[method],
                "completeness": {
                    k: v for k, v in completeness_reports.items()
                    if k.startswith(f"{method}_")
                }
            }
            for method in RECEIPT_METHODS
        },
        "all_records": all_records,
        "summary": {
            "partition_date": date_str,
            "total_records": total_records,
            "total_failed": total_failed,
            "success_rate": success_rate,
            "per_method_stats": stats_by_method,
            "completeness_reports": completeness_reports,
            "certificate_count": cert_count,
            "cola_detail_count": cola_count
        }
    }

# ============================================================================
# S3 Operations
# ============================================================================

def get_s3_key(partition_date: date) -> str:
    """
    Generate S3 key with year/month partitioning.

    Format: ttb-pre-prod/ttb_raw_data/{year}/{month}/{date}
    Example: ttb-pre-prod/ttb_raw_data/2025/12/2025-12-31
    """
    year = partition_date.year
    month = partition_date.month
    return f"{S3_PREFIX}/{year}/{month:02d}/{partition_date.isoformat()}"


def save_to_s3(s3_client, partition_date: date, data: Dict[str, Any]) -> bool:
    """
    Save partition data to S3 in Dagster-compatible pickle format.

    Path: s3://ciq-dagster/ttb-pre-prod/ttb_raw_data/{year}/{month}/{date}
    """
    s3_key = get_s3_key(partition_date)

    try:
        # Serialize to pickle (Dagster's S3PickleIOManager format)
        pickle_data = pickle.dumps(data)

        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=pickle_data,
            ContentType='application/octet-stream'
        )

        logger.info(f"Saved to s3://{S3_BUCKET}/{s3_key} ({len(pickle_data)} bytes)")
        return True

    except Exception as e:
        logger.error(f"Failed to save {partition_date} to S3: {e}")
        return False


def check_partition_exists(s3_client, partition_date: date) -> bool:
    """Check if a partition already exists in S3."""
    s3_key = get_s3_key(partition_date)

    try:
        s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
        return True
    except:
        return False

# ============================================================================
# Main Worker Logic
# ============================================================================

def run_worker(
    start_date: date,
    end_date: date,
    worker_id: str,
    delay: float = 1.0,
    skip_existing: bool = True
):
    """
    Run the extraction worker for a date range.
    """
    logger.info(f"=" * 60)
    logger.info(f"TTB Extraction Worker {worker_id}")
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Delay: {delay}s, Skip existing: {skip_existing}")
    logger.info(f"=" * 60)

    # Initialize S3 client
    s3_client = boto3.client('s3')

    # Count partitions
    total_partitions = (end_date - start_date).days + 1
    completed = 0
    skipped = 0
    failed = 0

    # Track progress
    progress_file = f"/tmp/worker_{worker_id}_progress.json"

    for partition_date in generate_date_range(start_date, end_date):
        logger.info(f"\n[{completed + skipped + failed + 1}/{total_partitions}] Processing {partition_date}")

        # Check if already exists
        if skip_existing and check_partition_exists(s3_client, partition_date):
            logger.info(f"  Skipping - already exists in S3")
            skipped += 1
            continue

        try:
            # Extract data
            data = extract_partition(partition_date, delay=delay)

            # Save to S3
            if save_to_s3(s3_client, partition_date, data):
                completed += 1
            else:
                failed += 1

            # Update progress file
            progress = {
                "worker_id": worker_id,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "current_date": partition_date.isoformat(),
                "total_partitions": total_partitions,
                "completed": completed,
                "skipped": skipped,
                "failed": failed,
                "last_update": datetime.now().isoformat()
            }

            try:
                with open(progress_file, 'w') as f:
                    json.dump(progress, f)
            except:
                pass  # Ignore progress file errors

        except Exception as e:
            logger.error(f"Failed to process {partition_date}: {e}")
            failed += 1

    # Final summary
    logger.info(f"\n" + "=" * 60)
    logger.info(f"Worker {worker_id} Complete")
    logger.info(f"Total: {total_partitions}, Completed: {completed}, Skipped: {skipped}, Failed: {failed}")
    logger.info(f"=" * 60)

    return completed, skipped, failed

# ============================================================================
# CLI
# ============================================================================

def parse_date(date_str: str) -> date:
    """Parse date string in YYYY-MM-DD format."""
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def main():
    parser = argparse.ArgumentParser(
        description="TTB Extraction Worker for EC2 instances"
    )
    parser.add_argument(
        "--start-date",
        required=True,
        type=parse_date,
        help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date",
        required=True,
        type=parse_date,
        help="End date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--worker-id",
        required=True,
        help="Unique worker identifier"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Delay between requests in seconds (default: 0.5)"
    )
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Re-extract even if partition exists in S3"
    )

    args = parser.parse_args()

    # Setup CloudWatch logging with worker_id
    setup_cloudwatch_logging(args.worker_id)

    # Validate date range
    if args.start_date > args.end_date:
        logger.error("Start date must be before or equal to end date")
        sys.exit(1)

    # Run worker
    completed, skipped, failed = run_worker(
        start_date=args.start_date,
        end_date=args.end_date,
        worker_id=args.worker_id,
        delay=args.delay,
        skip_existing=not args.no_skip_existing
    )

    # Exit with error if any failures
    if failed > 0:
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
