#!/usr/bin/env python3
"""
TTB Extraction Fleet Launcher

Launches a fleet of EC2 instances to extract TTB data in parallel.
Each instance extracts a portion of the date range.

Prerequisites:
    1. IAM Role: ttb-extraction-role (with S3 write permissions)
    2. Instance Profile: ttb-extraction-profile
    3. Worker script uploaded to: s3://ciq-dagster/scripts/ec2_extraction_worker.py

Usage:
    # Dry run - see what would be launched
    python launch_fleet.py --dry-run

    # Launch 20 workers for full extraction (2000-2025)
    python launch_fleet.py --workers 20

    # Launch for specific date range
    python launch_fleet.py --start-date 2020-01-01 --end-date 2025-12-31 --workers 5

    # Use specific instance type
    python launch_fleet.py --instance-type t3.medium --workers 10
"""
import argparse
import base64
import boto3
import sys
from datetime import date, datetime, timedelta
from typing import List, Tuple


# Default configuration
DEFAULT_START_DATE = date(2000, 1, 1)
DEFAULT_END_DATE = date(2025, 12, 31)
DEFAULT_NUM_WORKERS = 50
DEFAULT_INSTANCE_TYPE = "t3.small"
DEFAULT_AMI_ID = "ami-0c02fb55956c7d316"  # Amazon Linux 2023 (us-east-1)
DEFAULT_REGION = "us-east-1"
INSTANCE_PROFILE_NAME = "ttb-extraction-profile"
S3_BUCKET = "ciq-dagster"


def calculate_date_ranges(
    start_date: date,
    end_date: date,
    num_workers: int
) -> List[Tuple[int, date, date]]:
    """
    Divide date range evenly among workers.

    Returns list of (worker_number, start_date, end_date) tuples.
    """
    total_days = (end_date - start_date).days + 1
    days_per_worker = total_days // num_workers
    remainder = total_days % num_workers

    ranges = []
    current_start = start_date

    for i in range(num_workers):
        # Add extra day to first 'remainder' workers to distribute evenly
        worker_days = days_per_worker + (1 if i < remainder else 0)
        worker_end = current_start + timedelta(days=worker_days - 1)

        # Ensure we don't exceed end_date
        if worker_end > end_date:
            worker_end = end_date

        ranges.append((i + 1, current_start, worker_end))
        current_start = worker_end + timedelta(days=1)

        if current_start > end_date:
            break

    return ranges


def get_user_data_script() -> str:
    """Get the user data bootstrap script."""
    script = """#!/bin/bash
set -e

exec > >(tee /var/log/ttb-bootstrap.log) 2>&1
echo "TTB Extraction Worker Bootstrap - $(date)"

INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
REGION=$(curl -s http://169.254.169.254/latest/meta-data/placement/region)

yum update -y
yum install -y python3 python3-pip jq
pip3 install 'urllib3<2.0' && pip3 install requests boto3 watchtower

START_DATE=$(aws ec2 describe-tags --region $REGION --filters "Name=resource-id,Values=$INSTANCE_ID" "Name=key,Values=StartDate" --query 'Tags[0].Value' --output text)
END_DATE=$(aws ec2 describe-tags --region $REGION --filters "Name=resource-id,Values=$INSTANCE_ID" "Name=key,Values=EndDate" --query 'Tags[0].Value' --output text)
WORKER_NUMBER=$(aws ec2 describe-tags --region $REGION --filters "Name=resource-id,Values=$INSTANCE_ID" "Name=key,Values=WorkerNumber" --query 'Tags[0].Value' --output text)

aws s3 cp s3://ciq-dagster/scripts/ec2_extraction_worker.py /opt/ec2_extraction_worker.py
chmod +x /opt/ec2_extraction_worker.py
mkdir -p /var/log/ttb

python3 /opt/ec2_extraction_worker.py --start-date "$START_DATE" --end-date "$END_DATE" --worker-id "$WORKER_NUMBER" --delay 0.5 2>&1 | tee /var/log/ttb/extraction.log

aws s3 cp /var/log/ttb/extraction.log "s3://ciq-dagster/ttb-pre-prod/logs/worker-${WORKER_NUMBER}-extraction.log"
aws s3 cp /var/log/ttb-bootstrap.log "s3://ciq-dagster/ttb-pre-prod/logs/worker-${WORKER_NUMBER}-bootstrap.log"

echo "Extraction complete, terminating..."
aws ec2 terminate-instances --region $REGION --instance-ids $INSTANCE_ID
"""
    return base64.b64encode(script.encode()).decode()


def upload_worker_script(s3_client, dry_run: bool = False) -> bool:
    """Upload the worker script to S3."""
    script_path = "scripts/ec2_extraction_worker.py"
    s3_key = "scripts/ec2_extraction_worker.py"

    try:
        # Read local script
        with open(script_path, 'r') as f:
            script_content = f.read()

        if dry_run:
            print(f"[DRY RUN] Would upload {script_path} to s3://{S3_BUCKET}/{s3_key}")
            return True

        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=script_content.encode(),
            ContentType='text/x-python'
        )
        print(f"Uploaded worker script to s3://{S3_BUCKET}/{s3_key}")
        return True

    except FileNotFoundError:
        print(f"ERROR: Worker script not found at {script_path}")
        print("Run this script from the project root directory.")
        return False
    except Exception as e:
        print(f"ERROR: Failed to upload worker script: {e}")
        return False


def check_prerequisites(ec2_client, iam_client, dry_run: bool = False) -> bool:
    """Check that required IAM resources exist."""
    print("Checking prerequisites...")

    # Check instance profile
    try:
        iam_client.get_instance_profile(InstanceProfileName=INSTANCE_PROFILE_NAME)
        print(f"  Instance profile '{INSTANCE_PROFILE_NAME}' exists")
    except iam_client.exceptions.NoSuchEntityException:
        print(f"  ERROR: Instance profile '{INSTANCE_PROFILE_NAME}' not found")
        print(f"  Create it with:")
        print(f"    aws iam create-instance-profile --instance-profile-name {INSTANCE_PROFILE_NAME}")
        print(f"    aws iam add-role-to-instance-profile --instance-profile-name {INSTANCE_PROFILE_NAME} --role-name ttb-extraction-role")
        if not dry_run:
            return False

    return True


def launch_instance(
    ec2_client,
    worker_number: int,
    start_date: date,
    end_date: date,
    instance_type: str,
    ami_id: str,
    dry_run: bool = False
) -> str:
    """Launch a single EC2 instance for extraction."""

    tags = [
        {'Key': 'Name', 'Value': f'ttb-extraction-worker-{worker_number}'},
        {'Key': 'Project', 'Value': 'ttb-extraction'},
        {'Key': 'WorkerNumber', 'Value': str(worker_number)},
        {'Key': 'StartDate', 'Value': start_date.isoformat()},
        {'Key': 'EndDate', 'Value': end_date.isoformat()},
        {'Key': 'AutoTerminate', 'Value': 'true'}
    ]

    if dry_run:
        days = (end_date - start_date).days + 1
        print(f"  [DRY RUN] Worker {worker_number}: {start_date} to {end_date} ({days} days)")
        return "dry-run-instance-id"

    try:
        response = ec2_client.run_instances(
            ImageId=ami_id,
            InstanceType=instance_type,
            MinCount=1,
            MaxCount=1,
            IamInstanceProfile={'Name': INSTANCE_PROFILE_NAME},
            UserData=get_user_data_script(),
            TagSpecifications=[
                {
                    'ResourceType': 'instance',
                    'Tags': tags
                }
            ]
        )

        instance_id = response['Instances'][0]['InstanceId']
        days = (end_date - start_date).days + 1
        print(f"  Worker {worker_number}: {instance_id} - {start_date} to {end_date} ({days} days)")
        return instance_id

    except Exception as e:
        print(f"  ERROR launching worker {worker_number}: {e}")
        return None


def launch_fleet(
    start_date: date,
    end_date: date,
    num_workers: int,
    instance_type: str,
    ami_id: str,
    region: str,
    dry_run: bool = False
):
    """Launch the extraction fleet."""

    print("=" * 60)
    print("TTB Extraction Fleet Launcher")
    print("=" * 60)
    print(f"Date range: {start_date} to {end_date}")
    print(f"Workers: {num_workers}")
    print(f"Instance type: {instance_type}")
    print(f"Region: {region}")
    if dry_run:
        print("MODE: DRY RUN (no instances will be launched)")
    print("=" * 60)

    # Initialize AWS clients
    ec2_client = boto3.client('ec2', region_name=region)
    iam_client = boto3.client('iam', region_name=region)
    s3_client = boto3.client('s3', region_name=region)

    # Check prerequisites
    if not check_prerequisites(ec2_client, iam_client, dry_run):
        print("\nPrerequisites not met. Exiting.")
        sys.exit(1)

    # Upload worker script
    print("\nUploading worker script to S3...")
    if not upload_worker_script(s3_client, dry_run):
        print("\nFailed to upload worker script. Exiting.")
        sys.exit(1)

    # Calculate date ranges
    print("\nCalculating date ranges...")
    ranges = calculate_date_ranges(start_date, end_date, num_workers)

    total_days = (end_date - start_date).days + 1
    print(f"Total partitions: {total_days}")
    print(f"Partitions per worker: ~{total_days // num_workers}")

    # Launch instances
    print("\nLaunching instances...")
    launched_instances = []

    for worker_num, worker_start, worker_end in ranges:
        instance_id = launch_instance(
            ec2_client,
            worker_num,
            worker_start,
            worker_end,
            instance_type,
            ami_id,
            dry_run
        )
        if instance_id:
            launched_instances.append(instance_id)

    # Summary
    print("\n" + "=" * 60)
    print("Launch Summary")
    print("=" * 60)
    print(f"Instances launched: {len(launched_instances)}")
    print(f"Total partitions: {total_days}")

    if not dry_run:
        # Estimate cost
        # t3.small = $0.0208/hour
        hours_estimate = 12
        cost_per_instance = 0.0208 * hours_estimate
        total_cost = cost_per_instance * len(launched_instances)
        print(f"Estimated cost (~{hours_estimate}h): ${total_cost:.2f}")

        print("\nMonitor progress with:")
        print(f"  python scripts/monitor_extraction.py")
        print("\nOr check S3:")
        print(f"  aws s3 ls s3://{S3_BUCKET}/ttb-pre-prod/ttb_raw_data/ | wc -l")

    return launched_instances


def parse_date(date_str: str) -> date:
    """Parse date string."""
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def main():
    parser = argparse.ArgumentParser(
        description="Launch EC2 fleet for TTB data extraction"
    )
    parser.add_argument(
        "--start-date",
        type=parse_date,
        default=DEFAULT_START_DATE,
        help=f"Start date (default: {DEFAULT_START_DATE})"
    )
    parser.add_argument(
        "--end-date",
        type=parse_date,
        default=DEFAULT_END_DATE,
        help=f"End date (default: {DEFAULT_END_DATE})"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_NUM_WORKERS,
        help=f"Number of workers (default: {DEFAULT_NUM_WORKERS})"
    )
    parser.add_argument(
        "--instance-type",
        default=DEFAULT_INSTANCE_TYPE,
        help=f"EC2 instance type (default: {DEFAULT_INSTANCE_TYPE})"
    )
    parser.add_argument(
        "--ami-id",
        default=DEFAULT_AMI_ID,
        help=f"AMI ID (default: {DEFAULT_AMI_ID})"
    )
    parser.add_argument(
        "--region",
        default=DEFAULT_REGION,
        help=f"AWS region (default: {DEFAULT_REGION})"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be launched without actually launching"
    )

    args = parser.parse_args()

    # Validate
    if args.start_date > args.end_date:
        print("ERROR: Start date must be before end date")
        sys.exit(1)

    if args.workers < 1 or args.workers > 100:
        print("ERROR: Workers must be between 1 and 100")
        sys.exit(1)

    # Launch
    launch_fleet(
        start_date=args.start_date,
        end_date=args.end_date,
        num_workers=args.workers,
        instance_type=args.instance_type,
        ami_id=args.ami_id,
        region=args.region,
        dry_run=args.dry_run
    )


if __name__ == "__main__":
    main()
