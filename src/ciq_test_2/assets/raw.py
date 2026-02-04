"""
Raw Data Assets

This module contains assets for raw data extraction from external sources.
Raw assets are the entry point to the TTB pipeline and handle data ingestion.
"""
import os
import time
import tempfile
import urllib3
import hashlib
from datetime import date, datetime, timedelta
from typing import Dict, Any, List

import requests
from dagster import (
    asset,
    Config,
    get_dagster_logger,
    AssetMaterialization,
    MetadataValue,
    AssetExecutionContext
)

from ..utils.ttb_utils import TTBIDUtils, TTBSequenceTracker
from ..utils.ttb_supabase_loader import TTBSupabaseLoader
from ..config.ttb_config import TTBExtractionConfig
from ..config.ttb_partitions import daily_partitions

# Disable SSL warnings for requests with verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# TTB Error Page Detection
# Hash of the standard TTB error page returned for invalid TTB IDs
TTB_ERROR_PAGE_HASH = "50fa048f9cf8200c3d82d60add59b3b1f78f9e3ebc67f9395051595fc830a9e3"


def is_ttb_error_page(content: bytes) -> bool:
    """
    Check if the response content is a TTB error page by comparing its hash.

    Args:
        content: Raw response content bytes

    Returns:
        True if the content matches the known TTB error page hash
    """
    content_hash = hashlib.sha256(content).hexdigest()
    return content_hash == TTB_ERROR_PAGE_HASH


@asset(
    partitions_def=daily_partitions,
    group_name="ttb_raw",
    description="Raw TTB data extraction partitioned by date for all data types and receipt methods",
    metadata={
        "data_type": "raw",
        "source": "ttbonline.gov",
        "format": "pickle"
    }
)
def ttb_raw_data(
    context: AssetExecutionContext,
    config: TTBExtractionConfig
) -> Dict[str, Any]:
    """
    Extract raw TTB data for a specific date, processing all receipt methods and data types.

    This asset implements:
    - Rate limiting (0.5s between requests)
    - Intelligent sequence detection (stop after configurable consecutive failures)
    - Comprehensive logging and metadata
    - Organizes data by receipt method for nested S3 storage
    - Processes both COLA detail and certificate data for all receipt methods

    Partition key format: date (e.g. "2024-01-01")

    Returns:
        Dictionary organized by receipt method:
        {
            "by_receipt_method": {
                0: {"records": [...], "stats": {...}},  # hand-delivered
                1: {"records": [...], "stats": {...}},  # e-filed
                2: {"records": [...], "stats": {...}},  # mailed
                3: {"records": [...], "stats": {...}},  # overnight
            },
            "all_records": [...],  # flat list for backward compatibility
            "summary": {...}  # overall statistics
        }
    """
    logger = get_dagster_logger()

    # Get partition information (now just a date string)
    date_str = context.partition_key
    partition_date = datetime.strptime(date_str, "%Y-%m-%d").date()

    logger.info(f"Processing TTB data for date: {partition_date}")

    # Configure data types and receipt methods for daily processing
    data_types = ["cola-detail", "certificate"]
    receipt_methods = config.receipt_methods  # All methods: [0, 1, 2, 3]

    logger.info(f"Data types: {data_types}")
    logger.info(f"Receipt methods: {receipt_methods} (0=hand-delivered, 1=e-filed, 2=mailed, 3=overnight)")

    # Initialize Supabase loader for resume functionality
    supabase_loader = None
    start_sequences = {method: 1 for method in receipt_methods}  # Default: start from 1

    # Get Supabase credentials from config or environment variables
    supabase_url = config.supabase_url or os.environ.get("SUPABASE_URL", "")
    supabase_key = config.supabase_key or os.environ.get("SUPABASE_KEY", "")

    if config.resume_from_supabase and supabase_url and supabase_key:
        try:
            supabase_loader = TTBSupabaseLoader(
                url=supabase_url,
                key=supabase_key,
                schema=config.supabase_schema
            )
            # Get max sequences for all receipt methods at once (cached)
            max_sequences = supabase_loader.get_max_sequence_per_receipt_method(date_str)

            for method in receipt_methods:
                existing_max = max_sequences.get(method, 0)
                if existing_max > 0:
                    start_sequences[method] = existing_max + 1
                    logger.info(f"Resume enabled: method={method} will start from sequence {start_sequences[method]} (found {existing_max} in Supabase)")
                else:
                    logger.info(f"No existing data in Supabase for method={method}, starting from sequence 1")
        except Exception as e:
            logger.warning(f"Failed to initialize Supabase loader for resume: {e}. Starting from sequence 1 for all methods.")
    elif config.resume_from_supabase:
        logger.warning("Resume from Supabase enabled but missing URL or key. Starting from sequence 1.")

    # Initialize tracking - organize by receipt method
    all_extracted_records = []
    records_by_method = {method: [] for method in receipt_methods}  # Organize records by receipt method
    stats_by_method = {method: {"total": 0, "cola_detail": 0, "certificate": 0, "failed": 0} for method in receipt_methods}
    total_failed_count = 0
    completeness_reports = {}  # Store completeness reports per method/type

    # Generate TTB IDs for this partition
    julian_day = TTBIDUtils.date_to_julian(partition_date)

    try:
        # Process each combination of receipt method and data type
        for receipt_method in receipt_methods:
            for data_type in data_types:
                logger.info(f"Processing {data_type} data for receipt method {receipt_method}")

                # Configure action parameter based on data type
                if data_type == "cola-detail":
                    action_param = "publicDisplaySearchAdvanced"
                elif data_type == "certificate":
                    action_param = "publicFormDisplay"
                else:
                    logger.warning(f"Unknown data_type: {data_type}, skipping...")
                    continue

                # Initialize enhanced tracking for this combination
                sequence_tracker = TTBSequenceTracker(
                    max_consecutive_failures=config.consecutive_failure_threshold,
                    gap_probe_intervals=config.gap_probe_intervals,
                    enable_gap_detection=config.enable_gap_detection
                )
                failed_count = 0

                # Use start sequence from Supabase resume or default to 1
                start_sequence = start_sequences.get(receipt_method, 1)
                sequence = start_sequence
                while sequence <= config.max_sequence_per_batch:
                    # Build TTB ID
                    ttb_id = TTBIDUtils.build_ttb_id(
                        year=partition_date.year,
                        julian_day=julian_day,
                        receipt_method=receipt_method,
                        sequence=sequence
                    )

                    # Build URL with appropriate action parameter
                    url = f"https://ttbonline.gov/colasonline/viewColaDetails.do?action={action_param}&ttbid={ttb_id}"

                    try:
                        # Rate limiting
                        TTBIDUtils.rate_limit_sleep()

                        # Make request with retry logic for connection errors
                        response = None
                        for retry_attempt in range(config.max_retries):
                            try:
                                response = requests.get(url, stream=True, verify=False, timeout=30)
                                break  # Success, exit retry loop
                            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as conn_err:
                                if retry_attempt < config.max_retries - 1:
                                    wait_time = (retry_attempt + 1) * 2  # Exponential backoff: 2, 4, 6 seconds
                                    logger.warning(f"Connection error for {ttb_id}, retrying in {wait_time}s (attempt {retry_attempt + 1}/{config.max_retries}): {conn_err}")
                                    time.sleep(wait_time)
                                else:
                                    raise  # Re-raise on final attempt

                        if response is None:
                            raise requests.exceptions.ConnectionError("Failed after all retries")

                        if 200 <= response.status_code < 300:
                            # Get full response content to check for error page
                            content = response.content

                            # Check if this is a TTB error page
                            if is_ttb_error_page(content):
                                # This is an error page - treat as failure
                                sequence_tracker.record_failure(sequence)
                                failed_count += 1

                                if sequence_tracker.should_stop():
                                    logger.info(f"Stopping {data_type}/{receipt_method} after {sequence_tracker.consecutive_failures} consecutive error pages at sequence {sequence}")
                                    break
                            else:
                                # Valid content - collect the data
                                sequence_tracker.record_success(sequence)

                                # Store the extracted record with metadata
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

                                all_extracted_records.append(record)
                                records_by_method[receipt_method].append(record)
                                stats_by_method[receipt_method]["total"] += 1
                                if data_type == "cola-detail":
                                    stats_by_method[receipt_method]["cola_detail"] += 1
                                else:
                                    stats_by_method[receipt_method]["certificate"] += 1
                                logger.debug(f"Successfully extracted TTB {data_type} ID: {ttb_id}")
                        else:
                            # HTTP error
                            sequence_tracker.record_failure(sequence)
                            failed_count += 1

                            if sequence_tracker.should_stop():
                                logger.info(f"Stopping {data_type}/{receipt_method} after {sequence_tracker.consecutive_failures} consecutive failures at sequence {sequence}")
                                break

                    except Exception as e:
                        sequence_tracker.record_failure(sequence)
                        failed_count += 1
                        logger.error(f"Error processing TTB ID {ttb_id}: {e}")

                        if sequence_tracker.should_stop():
                            logger.info(f"Stopping {data_type}/{receipt_method} after {sequence_tracker.consecutive_failures} consecutive failures at sequence {sequence}")
                            break

                    sequence += 1

                # Store completeness report for this combination
                report_key = f"{receipt_method}_{data_type}"
                completeness_reports[report_key] = sequence_tracker.get_completeness_report()

                # Log stats including gap detection info
                stats = sequence_tracker.get_stats()
                total_failed_count += failed_count
                stats_by_method[receipt_method]["failed"] += failed_count
                logger.info(f"Completed {data_type}/{receipt_method}: {stats['total_success']} successful, {failed_count} failed, {stats['gaps_detected']} gaps detected")

    except Exception as e:
        logger.error(f"Critical error in TTB extraction: {e}")
        raise

    # Calculate overall statistics
    total_processed = len(all_extracted_records) + total_failed_count
    success_rate = len(all_extracted_records) / total_processed if total_processed > 0 else 0

    # Count by data type for metadata
    cert_count = len([r for r in all_extracted_records if r['data_type'] == 'certificate'])
    cola_count = len([r for r in all_extracted_records if r['data_type'] == 'cola-detail'])

    # Log comprehensive results
    logger.info(f"TTB extraction complete for {partition_date}")
    logger.info(f"Total successful extractions: {len(all_extracted_records)} (cert: {cert_count}, cola-detail: {cola_count})")
    logger.info(f"Total failed attempts: {total_failed_count}")
    logger.info(f"Overall success rate: {success_rate:.2%}")

    # Aggregate completeness metrics
    total_gaps = sum(r.get('gaps_detected', 0) for r in completeness_reports.values())
    total_missing = sum(r.get('total_missing_in_gaps', 0) for r in completeness_reports.values())

    # Build per-method summary for metadata
    method_labels = {0: "hand_delivered", 1: "e_filed", 2: "mailed", 3: "overnight"}
    per_method_summary = {}
    for method, stats in stats_by_method.items():
        method_name = method_labels.get(method, f"method_{method}")
        per_method_summary[method_name] = {
            "total_records": stats["total"],
            "cola_detail": stats["cola_detail"],
            "certificate": stats["certificate"],
            "failed": stats["failed"]
        }

    # Add metadata to context
    context.add_output_metadata({
        "partition_date": MetadataValue.text(date_str),
        "successful_extractions": MetadataValue.int(len(all_extracted_records)),
        "certificate_extractions": MetadataValue.int(cert_count),
        "cola_detail_extractions": MetadataValue.int(cola_count),
        "failed_attempts": MetadataValue.int(total_failed_count),
        "success_rate": MetadataValue.float(success_rate),
        "total_sequences_processed": MetadataValue.int(total_processed),
        "data_types_processed": MetadataValue.text(",".join(data_types)),
        "receipt_methods_processed": MetadataValue.text(",".join(map(str, receipt_methods))),
        "gaps_detected": MetadataValue.int(total_gaps),
        "total_missing_in_gaps": MetadataValue.int(total_missing),
        "completeness_reports": MetadataValue.json(completeness_reports),
        "per_method_summary": MetadataValue.json(per_method_summary),
        "resume_from_supabase": MetadataValue.bool(config.resume_from_supabase),
        "start_sequences": MetadataValue.json(start_sequences)
    })

    # Return organized structure for nested S3 storage
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
            for method in receipt_methods
        },
        "all_records": all_extracted_records,  # Flat list for backward compatibility
        "summary": {
            "partition_date": date_str,
            "total_records": len(all_extracted_records),
            "total_failed": total_failed_count,
            "success_rate": success_rate,
            "per_method_stats": stats_by_method,
            "completeness_reports": completeness_reports,
            "gaps_detected": total_gaps,
            "total_missing_in_gaps": total_missing,
            "resume_from_supabase": config.resume_from_supabase,
            "start_sequences": start_sequences
        }
    }