# TTB Pipeline - Distributed EC2 Fleet Extraction Guide

## Overview

This guide documents the approach for extracting all TTB COLA data (2000-2025) using a distributed EC2 fleet, completing in approximately 8-12 hours instead of weeks.

### Architecture

```
PHASE 1: Distributed Extraction (EC2 Fleet - 8-12 hours)
─────────────────────────────────────────────────────────
  EC2 #1      EC2 #2      EC2 #3     ...     EC2 #20
  2000-2001   2002-2003   2004-2005          2024-2025
      │           │           │                  │
      └───────────┴───────────┴──────────────────┘
                          │
                          ▼
                 S3: ciq-dagster/ttb-pre-prod/ttb_raw_data/
                 (9,497 partition files)

PHASE 2: Local Dagster Processing (1-2 days)
─────────────────────────────────────────────────────────
  ttb_raw_data (S3) → processing → analytics → Supabase
```

---

## Phase 1: EC2 Fleet Setup

### Step 1: Create IAM Role

Create an IAM role named `ttb-extraction-role` with the following policy:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::ciq-dagster",
                "arn:aws:s3:::ciq-dagster/ttb-pre-prod/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "arn:aws:logs:*:*:*"
        }
    ]
}
```

**Trust relationship:**
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "ec2.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
```

### Step 2: Create Instance Profile

```bash
aws iam create-instance-profile --instance-profile-name ttb-extraction-profile
aws iam add-role-to-instance-profile \
    --instance-profile-name ttb-extraction-profile \
    --role-name ttb-extraction-role
```

### Step 3: Create EC2 Launch Template

```bash
aws ec2 create-launch-template \
    --launch-template-name ttb-extraction-template \
    --version-description "TTB data extraction worker" \
    --launch-template-data '{
        "ImageId": "ami-0c02fb55956c7d316",
        "InstanceType": "t3.small",
        "IamInstanceProfile": {
            "Name": "ttb-extraction-profile"
        },
        "TagSpecifications": [
            {
                "ResourceType": "instance",
                "Tags": [
                    {"Key": "Project", "Value": "ttb-extraction"},
                    {"Key": "AutoTerminate", "Value": "true"}
                ]
            }
        ]
    }'
```

---

## Worker Configuration

### Extraction Script Requirements

Each EC2 worker runs a standalone Python script that:

1. **Takes command-line arguments:**
   - `--start-date`: First partition date to process (YYYY-MM-DD)
   - `--end-date`: Last partition date to process (YYYY-MM-DD)
   - `--worker-id`: Unique worker identifier for logging
   - `--delay`: Request delay in seconds (default: 1.0)

2. **Extraction logic (from existing `raw.py`):**
   - Processes all 4 receipt methods: 0 (hand-delivered), 1 (e-filed), 2 (mailed), 3 (overnight)
   - Processes both data types: cola-detail and certificate
   - Uses 1 second delay between requests (rate limiting)
   - Stops after 500 consecutive TTB error pages per data type/method
   - Maximum 15,000 sequences per batch

3. **S3 output format (Dagster-compatible pickle):**
   ```
   s3://ciq-dagster/ttb-pre-prod/ttb_raw_data/{date}

   Pickle contents:
   {
       "by_receipt_method": {
           0: {"records": [...], "stats": {...}},
           1: {"records": [...], "stats": {...}},
           2: {"records": [...], "stats": {...}},
           3: {"records": [...], "stats": {...}}
       },
       "all_records": [...],
       "summary": {
           "partition_date": "2004-03-09",
           "total_records": 1226,
           "total_failed": 200,
           "success_rate": 0.86,
           ...
       }
   }
   ```

### TTB ID Structure

TTB IDs are 14 digits: `YYJJJRRRSSSSS`
- **YY**: Year (last 2 digits, e.g., 04 for 2004)
- **JJJ**: Julian day (001-366)
- **RRR**: Receipt method (000=hand-delivered, 001=e-filed, 002=mailed, 003=overnight)
- **SSSSSS**: Sequence number (000001-999999)

Example: `04069000000003` = Year 2004, Julian day 069 (March 9), Hand-delivered, Sequence 3

### TTB Error Page Detection

Invalid TTB IDs return a standard error page. Detection method:
```python
TTB_ERROR_PAGE_HASH = "50fa048f9cf8200c3d82d60add59b3b1f78f9e3ebc67f9395051595fc830a9e3"

def is_ttb_error_page(content: bytes) -> bool:
    import hashlib
    return hashlib.sha256(content).hexdigest() == TTB_ERROR_PAGE_HASH
```

---

## Worker Date Range Distribution

Total partitions: ~9,497 days (2000-01-01 to 2025-12-31)
Workers: 20
Partitions per worker: ~475

| Worker | Start Date  | End Date    | Partitions |
|--------|-------------|-------------|------------|
| 1      | 2000-01-01  | 2001-04-20  | ~475       |
| 2      | 2001-04-21  | 2002-08-09  | ~475       |
| 3      | 2002-08-10  | 2003-11-28  | ~475       |
| 4      | 2003-11-29  | 2005-03-18  | ~475       |
| 5      | 2005-03-19  | 2006-07-07  | ~475       |
| 6      | 2006-07-08  | 2007-10-26  | ~475       |
| 7      | 2007-10-27  | 2009-02-14  | ~475       |
| 8      | 2009-02-15  | 2010-06-05  | ~475       |
| 9      | 2010-06-06  | 2011-09-24  | ~475       |
| 10     | 2011-09-25  | 2013-01-13  | ~475       |
| 11     | 2013-01-14  | 2014-05-04  | ~475       |
| 12     | 2014-05-05  | 2015-08-23  | ~475       |
| 13     | 2015-08-24  | 2016-12-11  | ~475       |
| 14     | 2016-12-12  | 2018-04-02  | ~475       |
| 15     | 2018-04-03  | 2019-07-22  | ~475       |
| 16     | 2019-07-23  | 2020-11-09  | ~475       |
| 17     | 2020-11-10  | 2022-02-28  | ~475       |
| 18     | 2022-03-01  | 2023-06-19  | ~475       |
| 19     | 2023-06-20  | 2024-10-07  | ~475       |
| 20     | 2024-10-08  | 2025-12-31  | ~450       |

---

## EC2 User Data Script

Each EC2 instance boots with a user data script that:

1. Installs Python dependencies (requests, boto3)
2. Downloads the extraction worker script
3. Runs the extraction with assigned date range
4. Auto-terminates on completion

```bash
#!/bin/bash
set -e

# Install dependencies
yum update -y
yum install -y python3 python3-pip
pip3 install requests boto3

# Get instance metadata for worker ID
INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
WORKER_ID=${INSTANCE_ID: -4}  # Last 4 chars of instance ID

# Download extraction script from S3
aws s3 cp s3://ciq-dagster/scripts/ec2_extraction_worker.py /opt/worker.py

# Get assigned date range from instance tags
START_DATE=$(aws ec2 describe-tags --filters "Name=resource-id,Values=$INSTANCE_ID" "Name=key,Values=StartDate" --query 'Tags[0].Value' --output text)
END_DATE=$(aws ec2 describe-tags --filters "Name=resource-id,Values=$INSTANCE_ID" "Name=key,Values=EndDate" --query 'Tags[0].Value' --output text)

# Run extraction
python3 /opt/worker.py \
    --start-date $START_DATE \
    --end-date $END_DATE \
    --worker-id $WORKER_ID \
    --delay 1.0 \
    2>&1 | tee /var/log/ttb-extraction.log

# Upload log to S3
aws s3 cp /var/log/ttb-extraction.log s3://ciq-dagster/ttb-pre-prod/logs/worker-${WORKER_ID}.log

# Self-terminate
aws ec2 terminate-instances --instance-ids $INSTANCE_ID
```

---

## Launching the Fleet

### Option 1: AWS CLI

```bash
# Launch 20 instances with different date ranges
for i in {1..20}; do
    START_DATE=$(python3 -c "from datetime import date, timedelta; d=date(2000,1,1)+timedelta(days=($i-1)*475); print(d.isoformat())")
    END_DATE=$(python3 -c "from datetime import date, timedelta; d=date(2000,1,1)+timedelta(days=$i*475-1); d=min(d, date(2025,12,31)); print(d.isoformat())")

    aws ec2 run-instances \
        --launch-template LaunchTemplateName=ttb-extraction-template \
        --count 1 \
        --tag-specifications "ResourceType=instance,Tags=[{Key=WorkerNumber,Value=$i},{Key=StartDate,Value=$START_DATE},{Key=EndDate,Value=$END_DATE}]"
done
```

### Option 2: Python Script

Create `scripts/launch_fleet.py`:
```python
import boto3
from datetime import date, timedelta

ec2 = boto3.client('ec2', region_name='us-east-1')

START = date(2000, 1, 1)
END = date(2025, 12, 31)
NUM_WORKERS = 20

total_days = (END - START).days + 1
days_per_worker = total_days // NUM_WORKERS

for i in range(NUM_WORKERS):
    worker_start = START + timedelta(days=i * days_per_worker)
    worker_end = START + timedelta(days=(i + 1) * days_per_worker - 1)
    if i == NUM_WORKERS - 1:
        worker_end = END

    ec2.run_instances(
        LaunchTemplate={'LaunchTemplateName': 'ttb-extraction-template'},
        MinCount=1,
        MaxCount=1,
        TagSpecifications=[{
            'ResourceType': 'instance',
            'Tags': [
                {'Key': 'WorkerNumber', 'Value': str(i + 1)},
                {'Key': 'StartDate', 'Value': worker_start.isoformat()},
                {'Key': 'EndDate', 'Value': worker_end.isoformat()},
                {'Key': 'Project', 'Value': 'ttb-extraction'}
            ]
        }]
    )
    print(f"Launched worker {i+1}: {worker_start} to {worker_end}")
```

---

## Monitoring Progress

### Check Running Instances

```bash
aws ec2 describe-instances \
    --filters "Name=tag:Project,Values=ttb-extraction" "Name=instance-state-name,Values=running" \
    --query 'Reservations[].Instances[].[InstanceId,Tags[?Key==`WorkerNumber`].Value|[0],State.Name]' \
    --output table
```

### Check S3 Partition Count

```bash
# Count completed partitions
aws s3 ls s3://ciq-dagster/ttb-pre-prod/ttb_raw_data/ | wc -l

# Should reach ~9,497 when complete
```

### Check Worker Logs

```bash
# List completed worker logs
aws s3 ls s3://ciq-dagster/ttb-pre-prod/logs/

# View specific worker log
aws s3 cp s3://ciq-dagster/ttb-pre-prod/logs/worker-0001.log -
```

### Progress Monitor Script

Create `scripts/monitor_extraction.py`:
```python
import boto3
from datetime import datetime

s3 = boto3.client('s3')
ec2 = boto3.client('ec2')

# Count S3 partitions
response = s3.list_objects_v2(
    Bucket='ciq-dagster',
    Prefix='ttb-pre-prod/ttb_raw_data/',
    Delimiter='/'
)
partition_count = len(response.get('CommonPrefixes', []))

# Count running instances
instances = ec2.describe_instances(
    Filters=[
        {'Name': 'tag:Project', 'Values': ['ttb-extraction']},
        {'Name': 'instance-state-name', 'Values': ['running']}
    ]
)
running_count = sum(len(r['Instances']) for r in instances['Reservations'])

print(f"Time: {datetime.now().isoformat()}")
print(f"Partitions complete: {partition_count} / 9497 ({partition_count/9497*100:.1f}%)")
print(f"Workers running: {running_count} / 20")
```

---

## Cost Estimate

| Resource                        | Cost        |
|---------------------------------|-------------|
| 20 × t3.small × 12 hours        | ~$5-8       |
| S3 storage (temp, ~50GB)        | ~$1-2       |
| Data transfer (outbound)        | ~$1-2       |
| CloudWatch Logs (optional)      | ~$0.50      |
| **Total**                       | **~$10**    |

---

## Phase 2: Local Dagster Processing

After EC2 extraction completes (~8-12 hours):

### 1. Verify S3 Data Integrity

```bash
# Count partitions
aws s3 ls s3://ciq-dagster/ttb-pre-prod/ttb_raw_data/ | wc -l
# Expected: ~9,497

# Spot check a random partition
aws s3 cp s3://ciq-dagster/ttb-pre-prod/ttb_raw_data/2015-06-15 /tmp/test.pkl
python3 -c "import pickle; d=pickle.load(open('/tmp/test.pkl','rb')); print(f\"Records: {len(d['all_records'])}\")"
```

### 2. Run Dagster Processing Pipeline

The local Dagster setup will process the raw S3 data through:

```
ttb_raw_data (S3)
    → ttb_extracted_data
    → ttb_processed_data
    → ttb_consolidated
    → dim_* (dimension tables)
    → fact_cola_applications
    → supabase_fact_cola_applications
```

**Backfill command:**
```bash
# From project directory with Dagster running
dagster asset materialize \
    --select ttb_extracted_data+ \
    --partition-range 2000-01-01...2025-12-31
```

### 3. Verify Supabase Load

```sql
SELECT COUNT(*) FROM ciq.fact_cola_applications;
-- Expected: Millions of records

SELECT
    EXTRACT(YEAR FROM approval_date) as year,
    COUNT(*)
FROM ciq.fact_cola_applications
GROUP BY 1
ORDER BY 1;
```

---

## Cleanup

After successful extraction and verification:

```bash
# Terminate any remaining EC2 instances
aws ec2 describe-instances \
    --filters "Name=tag:Project,Values=ttb-extraction" "Name=instance-state-name,Values=running" \
    --query 'Reservations[].Instances[].InstanceId' \
    --output text | xargs -I {} aws ec2 terminate-instances --instance-ids {}

# Delete launch template (optional)
aws ec2 delete-launch-template --launch-template-name ttb-extraction-template

# Delete IAM resources (optional)
aws iam remove-role-from-instance-profile \
    --instance-profile-name ttb-extraction-profile \
    --role-name ttb-extraction-role
aws iam delete-instance-profile --instance-profile-name ttb-extraction-profile
aws iam delete-role-policy --role-name ttb-extraction-role --policy-name ttb-s3-access
aws iam delete-role --role-name ttb-extraction-role
```

---

## Troubleshooting

### Worker Stuck or Slow

1. Check CloudWatch logs for the instance
2. SSH into instance and check `/var/log/ttb-extraction.log`
3. Verify network connectivity to ttbonline.gov

### Missing Partitions

1. List all partitions: `aws s3 ls s3://ciq-dagster/ttb-pre-prod/ttb_raw_data/`
2. Compare against expected range
3. Re-run failed date ranges manually

### S3 Permission Errors

1. Verify IAM role has correct permissions
2. Check instance profile is attached
3. Test with: `aws s3 ls s3://ciq-dagster/ttb-pre-prod/`

### Rate Limiting from TTB

If TTB starts returning errors or blocks:
1. Increase delay from 1s to 2s
2. Reduce number of concurrent workers
3. Each EC2 has unique IP, so blocking is unlikely

---

## Timeline Summary

| Phase | Duration |
|-------|----------|
| IAM + Launch Template setup | 30 min |
| Upload extraction script to S3 | 5 min |
| Launch EC2 fleet | 10 min |
| Extraction runs | 8-12 hours |
| Verify S3 data | 15 min |
| Cleanup EC2 resources | 10 min |
| Local Dagster processing | 1-2 days |
| **Total** | **~2-3 days** |

---

## Files to Create

| File | Status | Description |
|------|--------|-------------|
| `scripts/ec2_extraction_worker.py` | Pending | Standalone extraction script |
| `scripts/ec2_user_data.sh` | Pending | EC2 bootstrap script |
| `scripts/launch_fleet.py` | Pending | Fleet launcher |
| `scripts/monitor_extraction.py` | Pending | Progress monitor |

When ready to implement, these scripts will be created based on the logic in:
- `src/ciq_test_2/assets/raw.py` (extraction logic)
- `src/ciq_test_2/utils/ttb_utils.py` (TTB ID utilities)
