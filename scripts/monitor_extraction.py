#!/usr/bin/env python3
"""
TTB Extraction Fleet Monitor

Monitors the progress of EC2 extraction workers by checking:
- Running EC2 instances
- S3 partition count
- Worker progress files

Usage:
    # One-time check
    python monitor_extraction.py

    # Continuous monitoring (every 60 seconds)
    python monitor_extraction.py --watch

    # Check specific date range
    python monitor_extraction.py --start-date 2000-01-01 --end-date 2025-12-31
"""
import argparse
import boto3
import json
import sys
import time
from datetime import datetime, date, timedelta
from typing import Dict, List, Any


# Configuration
S3_BUCKET = "ciq-dagster"
S3_PREFIX = "ttb-pre-prod/ttb_raw_data"
LOGS_PREFIX = "ttb-pre-prod/logs"
DEFAULT_REGION = "us-east-1"


def count_s3_partitions(s3_client) -> int:
    """Count the number of partition files in S3."""
    paginator = s3_client.get_paginator('list_objects_v2')
    count = 0

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX + "/"):
        if 'Contents' in page:
            # Count only partition files (not directories)
            for obj in page['Contents']:
                if not obj['Key'].endswith('/'):
                    count += 1

    return count


def get_running_instances(ec2_client) -> List[Dict[str, Any]]:
    """Get list of running TTB extraction instances."""
    response = ec2_client.describe_instances(
        Filters=[
            {'Name': 'tag:Project', 'Values': ['ttb-extraction']},
            {'Name': 'instance-state-name', 'Values': ['pending', 'running']}
        ]
    )

    instances = []
    for reservation in response['Reservations']:
        for instance in reservation['Instances']:
            tags = {t['Key']: t['Value'] for t in instance.get('Tags', [])}
            instances.append({
                'instance_id': instance['InstanceId'],
                'state': instance['State']['Name'],
                'launch_time': instance['LaunchTime'],
                'worker_number': tags.get('WorkerNumber', 'N/A'),
                'start_date': tags.get('StartDate', 'N/A'),
                'end_date': tags.get('EndDate', 'N/A'),
                'instance_type': instance['InstanceType']
            })

    return sorted(instances, key=lambda x: int(x['worker_number']) if x['worker_number'].isdigit() else 999)


def get_worker_progress(s3_client) -> List[Dict[str, Any]]:
    """Get progress files from S3."""
    progress_files = []

    try:
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=f"{LOGS_PREFIX}/worker-"
        )

        for obj in response.get('Contents', []):
            if obj['Key'].endswith('-progress.json'):
                try:
                    file_response = s3_client.get_object(
                        Bucket=S3_BUCKET,
                        Key=obj['Key']
                    )
                    progress = json.loads(file_response['Body'].read().decode())
                    progress_files.append(progress)
                except:
                    pass

    except Exception as e:
        print(f"Warning: Could not fetch progress files: {e}")

    return sorted(progress_files, key=lambda x: int(x.get('worker_id', '0')) if str(x.get('worker_id', '0')).isdigit() else 999)


def get_completed_workers(s3_client) -> List[str]:
    """Get list of workers that have completed (uploaded extraction logs)."""
    completed = []

    try:
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=f"{LOGS_PREFIX}/worker-"
        )

        for obj in response.get('Contents', []):
            if obj['Key'].endswith('-extraction.log'):
                # Extract worker number from key
                key = obj['Key']
                # Format: ttb-pre-prod/logs/worker-{N}-extraction.log
                worker_part = key.split('worker-')[1].split('-extraction')[0]
                completed.append(worker_part)

    except:
        pass

    return completed


def calculate_expected_partitions(start_date: date, end_date: date) -> int:
    """Calculate expected number of partitions."""
    return (end_date - start_date).days + 1


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format."""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    return f"{hours}h {minutes}m"


def print_status(
    s3_client,
    ec2_client,
    expected_partitions: int,
    show_workers: bool = True
):
    """Print current extraction status."""
    now = datetime.now()

    print("\n" + "=" * 70)
    print(f"TTB Extraction Monitor - {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # S3 partition count
    partition_count = count_s3_partitions(s3_client)
    progress_pct = (partition_count / expected_partitions * 100) if expected_partitions > 0 else 0

    print(f"\nS3 Partitions: {partition_count:,} / {expected_partitions:,} ({progress_pct:.1f}%)")

    # Progress bar
    bar_width = 50
    filled = int(bar_width * partition_count / expected_partitions) if expected_partitions > 0 else 0
    bar = "=" * filled + "-" * (bar_width - filled)
    print(f"[{bar}]")

    # Running instances
    instances = get_running_instances(ec2_client)
    print(f"\nRunning Workers: {len(instances)}")

    if show_workers and instances:
        print("\n  Worker | Instance ID         | State   | Date Range              | Runtime")
        print("  " + "-" * 75)
        for inst in instances:
            worker = inst['worker_number'].ljust(6)
            inst_id = inst['instance_id'].ljust(19)
            state = inst['state'].ljust(7)
            date_range = f"{inst['start_date']} - {inst['end_date']}"

            # Calculate runtime
            if inst['launch_time']:
                runtime = (now.replace(tzinfo=inst['launch_time'].tzinfo) - inst['launch_time']).total_seconds()
                runtime_str = format_duration(runtime)
            else:
                runtime_str = "N/A"

            print(f"  {worker} | {inst_id} | {state} | {date_range} | {runtime_str}")

    # Completed workers
    completed = get_completed_workers(s3_client)
    if completed:
        print(f"\nCompleted Workers: {len(completed)}")

    # Worker progress
    progress_files = get_worker_progress(s3_client)
    if progress_files:
        print(f"\nWorker Progress (from S3):")
        for p in progress_files:
            worker_id = p.get('worker_id', 'N/A')
            completed_p = p.get('completed', 0)
            total = p.get('total_partitions', 0)
            current = p.get('current_date', 'N/A')
            pct = (completed_p / total * 100) if total > 0 else 0
            print(f"  Worker {worker_id}: {completed_p}/{total} ({pct:.1f}%) - Current: {current}")

    # Estimate completion
    if instances and partition_count > 0:
        avg_instances = len(instances)
        # Rough estimate: 27 minutes per partition per worker
        remaining = expected_partitions - partition_count
        if remaining > 0 and avg_instances > 0:
            # Assume each instance processes ~2 partitions per hour
            partitions_per_hour = avg_instances * 2
            hours_remaining = remaining / partitions_per_hour
            print(f"\nEstimated time remaining: ~{hours_remaining:.1f} hours")

    print("\n" + "=" * 70)


def watch_mode(
    s3_client,
    ec2_client,
    expected_partitions: int,
    interval: int = 60
):
    """Continuously monitor extraction progress."""
    print(f"Watching extraction progress (refresh every {interval}s). Press Ctrl+C to stop.")

    try:
        while True:
            # Clear screen (works on most terminals)
            print("\033[H\033[J", end="")

            print_status(s3_client, ec2_client, expected_partitions)

            # Check if complete
            partition_count = count_s3_partitions(s3_client)
            instances = get_running_instances(ec2_client)

            if partition_count >= expected_partitions and len(instances) == 0:
                print("\nExtraction complete!")
                break

            print(f"\nNext update in {interval} seconds...")
            time.sleep(interval)

    except KeyboardInterrupt:
        print("\nMonitoring stopped.")


def parse_date(date_str: str) -> date:
    """Parse date string."""
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def main():
    parser = argparse.ArgumentParser(
        description="Monitor TTB extraction fleet progress"
    )
    parser.add_argument(
        "--start-date",
        type=parse_date,
        default=date(2000, 1, 1),
        help="Expected start date (default: 2000-01-01)"
    )
    parser.add_argument(
        "--end-date",
        type=parse_date,
        default=date(2025, 12, 31),
        help="Expected end date (default: 2025-12-31)"
    )
    parser.add_argument(
        "--region",
        default=DEFAULT_REGION,
        help=f"AWS region (default: {DEFAULT_REGION})"
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Continuously monitor progress"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Refresh interval in seconds for watch mode (default: 60)"
    )
    parser.add_argument(
        "--no-workers",
        action="store_true",
        help="Don't show individual worker details"
    )

    args = parser.parse_args()

    # Initialize AWS clients
    s3_client = boto3.client('s3', region_name=args.region)
    ec2_client = boto3.client('ec2', region_name=args.region)

    # Calculate expected partitions
    expected = calculate_expected_partitions(args.start_date, args.end_date)

    if args.watch:
        watch_mode(s3_client, ec2_client, expected, args.interval)
    else:
        print_status(s3_client, ec2_client, expected, show_workers=not args.no_workers)


if __name__ == "__main__":
    main()
