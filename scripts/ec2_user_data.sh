#!/bin/bash
# =============================================================================
# TTB Extraction Worker - EC2 User Data Script
#
# This script bootstraps an EC2 instance for TTB data extraction.
# It installs dependencies, downloads the worker script, and runs extraction.
#
# Instance Tags Required:
#   - StartDate: YYYY-MM-DD format
#   - EndDate: YYYY-MM-DD format
#   - WorkerNumber: Numeric worker ID
# =============================================================================

set -e

# Logging setup
exec > >(tee /var/log/ttb-bootstrap.log) 2>&1
echo "=========================================="
echo "TTB Extraction Worker Bootstrap"
echo "Started at: $(date)"
echo "=========================================="

# Get instance metadata
INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
REGION=$(curl -s http://169.254.169.254/latest/meta-data/placement/region)
echo "Instance ID: $INSTANCE_ID"
echo "Region: $REGION"

# Install dependencies
echo "Installing dependencies..."
yum update -y
yum install -y python3 python3-pip jq

# Install Python packages
pip3 install 'urllib3<2.0' && pip3 install requests boto3 watchtower

# Get instance tags
echo "Fetching instance tags..."
START_DATE=$(aws ec2 describe-tags \
    --region $REGION \
    --filters "Name=resource-id,Values=$INSTANCE_ID" "Name=key,Values=StartDate" \
    --query 'Tags[0].Value' --output text)

END_DATE=$(aws ec2 describe-tags \
    --region $REGION \
    --filters "Name=resource-id,Values=$INSTANCE_ID" "Name=key,Values=EndDate" \
    --query 'Tags[0].Value' --output text)

WORKER_NUMBER=$(aws ec2 describe-tags \
    --region $REGION \
    --filters "Name=resource-id,Values=$INSTANCE_ID" "Name=key,Values=WorkerNumber" \
    --query 'Tags[0].Value' --output text)

echo "Start Date: $START_DATE"
echo "End Date: $END_DATE"
echo "Worker Number: $WORKER_NUMBER"

# Validate tags
if [ -z "$START_DATE" ] || [ "$START_DATE" == "None" ]; then
    echo "ERROR: StartDate tag not found"
    exit 1
fi

if [ -z "$END_DATE" ] || [ "$END_DATE" == "None" ]; then
    echo "ERROR: EndDate tag not found"
    exit 1
fi

if [ -z "$WORKER_NUMBER" ] || [ "$WORKER_NUMBER" == "None" ]; then
    WORKER_NUMBER="${INSTANCE_ID: -4}"
    echo "Using instance ID suffix as worker number: $WORKER_NUMBER"
fi

# Download extraction script from S3
echo "Downloading extraction script..."
aws s3 cp s3://ciq-dagster/scripts/ec2_extraction_worker.py /opt/ec2_extraction_worker.py

# Make script executable
chmod +x /opt/ec2_extraction_worker.py

# Create extraction log directory
mkdir -p /var/log/ttb

# Run extraction
echo "=========================================="
echo "Starting TTB extraction"
echo "Date range: $START_DATE to $END_DATE"
echo "Worker ID: $WORKER_NUMBER"
echo "=========================================="

python3 /opt/ec2_extraction_worker.py \
    --start-date "$START_DATE" \
    --end-date "$END_DATE" \
    --worker-id "$WORKER_NUMBER" \
    --delay 0.5 \
    2>&1 | tee /var/log/ttb/extraction.log

EXTRACTION_EXIT_CODE=$?

# Upload logs to S3
echo "Uploading logs to S3..."
aws s3 cp /var/log/ttb/extraction.log \
    "s3://ciq-dagster/ttb-pre-prod/logs/worker-${WORKER_NUMBER}-extraction.log"

aws s3 cp /var/log/ttb-bootstrap.log \
    "s3://ciq-dagster/ttb-pre-prod/logs/worker-${WORKER_NUMBER}-bootstrap.log"

# Upload final progress
if [ -f "/tmp/worker_${WORKER_NUMBER}_progress.json" ]; then
    aws s3 cp "/tmp/worker_${WORKER_NUMBER}_progress.json" \
        "s3://ciq-dagster/ttb-pre-prod/logs/worker-${WORKER_NUMBER}-progress.json"
fi

echo "=========================================="
echo "Extraction complete at: $(date)"
echo "Exit code: $EXTRACTION_EXIT_CODE"
echo "=========================================="

# Self-terminate instance
echo "Initiating self-termination..."
aws ec2 terminate-instances --region $REGION --instance-ids $INSTANCE_ID

exit $EXTRACTION_EXIT_CODE
