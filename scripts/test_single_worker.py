#!/usr/bin/env python3
"""
Test Single Worker Launch

Launches a single EC2 instance to test the extraction workflow
before launching the full fleet.

Usage:
    # Test with 3 days (default)
    python test_single_worker.py

    # Test with specific date range
    python test_single_worker.py --start-date 2024-01-01 --end-date 2024-01-03

    # Dry run
    python test_single_worker.py --dry-run
"""
import argparse
import base64
import boto3
import sys
from datetime import date, datetime, timedelta


# Configuration
DEFAULT_INSTANCE_TYPE = "t3.small"
DEFAULT_AMI_ID = "ami-0c02fb55956c7d316"  # Amazon Linux 2023 (us-east-1)
DEFAULT_REGION = "us-east-1"
INSTANCE_PROFILE_NAME = "ttb-extraction-profile"
S3_BUCKET = "ciq-dagster"


def get_user_data_script() -> str:
    """Get the user data bootstrap script."""
    script = """#!/bin/bash
set -e

exec > >(tee /var/log/ttb-bootstrap.log) 2>&1
echo "=========================================="
echo "TTB Extraction Worker Bootstrap - TEST"
echo "Started at: $(date)"
echo "=========================================="

INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
REGION=$(curl -s http://169.254.169.254/latest/meta-data/placement/region)
echo "Instance ID: $INSTANCE_ID"
echo "Region: $REGION"

# Install dependencies
echo "Installing dependencies..."
yum update -y
yum install -y python3 python3-pip jq
pip3 install 'urllib3<2.0' && pip3 install requests boto3 watchtower

# Get instance tags
START_DATE=$(aws ec2 describe-tags --region $REGION --filters "Name=resource-id,Values=$INSTANCE_ID" "Name=key,Values=StartDate" --query 'Tags[0].Value' --output text)
END_DATE=$(aws ec2 describe-tags --region $REGION --filters "Name=resource-id,Values=$INSTANCE_ID" "Name=key,Values=EndDate" --query 'Tags[0].Value' --output text)
WORKER_NUMBER=$(aws ec2 describe-tags --region $REGION --filters "Name=resource-id,Values=$INSTANCE_ID" "Name=key,Values=WorkerNumber" --query 'Tags[0].Value' --output text)

echo "Start Date: $START_DATE"
echo "End Date: $END_DATE"
echo "Worker Number: $WORKER_NUMBER"

# Download extraction script
echo "Downloading extraction script from S3..."
aws s3 cp s3://ciq-dagster/scripts/ec2_extraction_worker.py /opt/ec2_extraction_worker.py
chmod +x /opt/ec2_extraction_worker.py
mkdir -p /var/log/ttb

echo "=========================================="
echo "Starting extraction..."
echo "=========================================="

python3 /opt/ec2_extraction_worker.py \
    --start-date "$START_DATE" \
    --end-date "$END_DATE" \
    --worker-id "$WORKER_NUMBER" \
    --delay 0.5 \
    2>&1 | tee /var/log/ttb/extraction.log

EXTRACTION_EXIT_CODE=$?

echo "=========================================="
echo "Extraction finished with exit code: $EXTRACTION_EXIT_CODE"
echo "=========================================="

# Upload logs to S3
echo "Uploading logs to S3..."
aws s3 cp /var/log/ttb/extraction.log "s3://ciq-dagster/ttb-pre-prod/logs/test-worker-${WORKER_NUMBER}-extraction.log"
aws s3 cp /var/log/ttb-bootstrap.log "s3://ciq-dagster/ttb-pre-prod/logs/test-worker-${WORKER_NUMBER}-bootstrap.log"

if [ -f "/tmp/worker_${WORKER_NUMBER}_progress.json" ]; then
    aws s3 cp "/tmp/worker_${WORKER_NUMBER}_progress.json" "s3://ciq-dagster/ttb-pre-prod/logs/test-worker-${WORKER_NUMBER}-progress.json"
fi

echo "=========================================="
echo "TEST COMPLETE at: $(date)"
echo "Exit code: $EXTRACTION_EXIT_CODE"
echo "=========================================="
echo ""
echo "NOTE: Instance will NOT self-terminate (test mode)"
echo "Terminate manually when done inspecting:"
echo "  aws ec2 terminate-instances --instance-ids $INSTANCE_ID"
echo ""
"""
    return base64.b64encode(script.encode()).decode()


def upload_worker_script(s3_client, dry_run: bool = False) -> bool:
    """Upload the worker script to S3."""
    script_path = "scripts/ec2_extraction_worker.py"
    s3_key = "scripts/ec2_extraction_worker.py"

    try:
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


def launch_test_instance(
    start_date: date,
    end_date: date,
    instance_type: str,
    ami_id: str,
    region: str,
    dry_run: bool = False
):
    """Launch a single test instance."""

    print("=" * 60)
    print("TTB Extraction - Single Worker Test")
    print("=" * 60)
    print(f"Date range: {start_date} to {end_date}")
    print(f"Partitions: {(end_date - start_date).days + 1}")
    print(f"Instance type: {instance_type}")
    print(f"Region: {region}")
    if dry_run:
        print("MODE: DRY RUN")
    print("=" * 60)

    # Initialize AWS clients
    ec2_client = boto3.client('ec2', region_name=region)
    s3_client = boto3.client('s3', region_name=region)

    # Upload worker script
    print("\nUploading worker script to S3...")
    if not upload_worker_script(s3_client, dry_run):
        sys.exit(1)

    # Launch instance
    tags = [
        {'Key': 'Name', 'Value': 'ttb-extraction-TEST'},
        {'Key': 'Project', 'Value': 'ttb-extraction'},
        {'Key': 'WorkerNumber', 'Value': 'TEST'},
        {'Key': 'StartDate', 'Value': start_date.isoformat()},
        {'Key': 'EndDate', 'Value': end_date.isoformat()},
        {'Key': 'TestInstance', 'Value': 'true'}
    ]

    if dry_run:
        print(f"\n[DRY RUN] Would launch instance with:")
        print(f"  Instance type: {instance_type}")
        print(f"  AMI: {ami_id}")
        print(f"  Date range: {start_date} to {end_date}")
        print(f"  Tags: {tags}")
        return

    print("\nLaunching test instance...")
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

        print("\n" + "=" * 60)
        print("TEST INSTANCE LAUNCHED")
        print("=" * 60)
        print(f"Instance ID: {instance_id}")
        print(f"Date range: {start_date} to {end_date}")
        print(f"Partitions: {(end_date - start_date).days + 1}")
        print()
        print("Monitor progress:")
        print(f"  1. Wait ~2-3 minutes for instance to boot and start extraction")
        print(f"  2. Check S3 for partitions:")
        print(f"     aws s3 ls s3://{S3_BUCKET}/ttb-pre-prod/ttb_raw_data/ | grep {start_date.isoformat()[:7]}")
        print()
        print("  3. Check logs (after extraction completes):")
        print(f"     aws s3 cp s3://{S3_BUCKET}/ttb-pre-prod/logs/test-worker-TEST-extraction.log -")
        print()
        print("  4. SSH into instance (if needed):")
        print(f"     aws ssm start-session --target {instance_id}")
        print()
        print("IMPORTANT: Instance will NOT auto-terminate. When done:")
        print(f"  aws ec2 terminate-instances --instance-ids {instance_id}")
        print("=" * 60)

        return instance_id

    except Exception as e:
        print(f"ERROR launching instance: {e}")
        sys.exit(1)


def parse_date(date_str: str) -> date:
    """Parse date string."""
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def main():
    parser = argparse.ArgumentParser(
        description="Launch a single test EC2 instance for TTB extraction"
    )
    parser.add_argument(
        "--start-date",
        type=parse_date,
        default=date(2024, 1, 1),
        help="Start date (default: 2024-01-01)"
    )
    parser.add_argument(
        "--end-date",
        type=parse_date,
        default=date(2024, 1, 3),
        help="End date (default: 2024-01-03, i.e., 3 days)"
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

    if args.start_date > args.end_date:
        print("ERROR: Start date must be before end date")
        sys.exit(1)

    launch_test_instance(
        start_date=args.start_date,
        end_date=args.end_date,
        instance_type=args.instance_type,
        ami_id=args.ami_id,
        region=args.region,
        dry_run=args.dry_run
    )


if __name__ == "__main__":
    main()
