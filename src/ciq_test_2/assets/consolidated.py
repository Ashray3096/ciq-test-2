"""
Consolidated Data Assets

This module contains assets that consolidate data from multiple sources
into unified, cross-dimensional datasets for analytics.
"""
import os
import tempfile
from datetime import datetime, date
from typing import Dict, Any, List, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

from dagster import (
    asset,
    Config,
    get_dagster_logger,
    AssetExecutionContext,
    MetadataValue,
    AssetDep,
    AllPartitionMapping
)
from dagster_aws.s3 import S3Resource

from .processed import ttb_structured_data
from ..config.ttb_partitions import daily_partitions
from ..utils.ttb_consolidated_schema import (
    get_consolidated_ttb_schema,
    get_dimension_schemas,
    get_data_quality_tiers
)


class ConsolidatedConfig(Config):
    """Configuration for consolidated TTB assets."""
    bucket_name: str = "ciq-dagster"
    s3_output_prefix: str = "ttb-pre-prod/ttb_consolidated"
    create_partitioned_output: bool = True
    data_quality_analysis: bool = True
    deduplication_enabled: bool = True


@asset(
    partitions_def=daily_partitions,
    group_name="ttb_consolidated",
    description="Consolidated flat table combining COLA detail and certificate data by date",
    deps=[AssetDep(ttb_structured_data, partition_mapping=AllPartitionMapping())],
    metadata={
        "data_type": "consolidated",
        "stage": "consolidation",
        "format": "pickle"
    }
)
def ttb_consolidated_data(
    context: AssetExecutionContext,
    config: ConsolidatedConfig,
    s3_resource: S3Resource
) -> Dict[str, Any]:
    """
    Consolidate COLA detail and certificate data into a single flat table.

    This asset reads structured output from both COLA detail and certificate
    partitions for a given date and creates a unified table with TTB ID as the key.

    Returns:
        Dictionary containing consolidation statistics and metadata
    """
    logger = get_dagster_logger()

    # Get partition information
    partition_date_str = context.partition_key
    partition_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()

    logger.info(f"Consolidating TTB data for date: {partition_date}")

    s3_client = s3_resource.get_client()
    schema = get_consolidated_ttb_schema()

    # Read data from all method_type partitions for this date
    consolidated_records = []
    processing_stats = {
        'partitions_processed': 0,
        'cola_records_found': 0,
        'cert_records_found': 0,
        'consolidated_records': 0,
        'duplicate_ttb_ids': 0,
        'data_quality_scores': {},
        'processing_errors': []
    }

    try:
        # Define receipt methods and data types to process
        receipt_methods = ["001", "002", "003", "000"]  # e-filed, mailed, overnight, hand delivered
        data_types = ["cola-detail", "certificate"]

        # Process all receipt methods and data types for this date
        for receipt_method in receipt_methods:
            for data_type in data_types:
                method_type = f"{receipt_method}-{data_type}"

                try:
                    # Read structured data from S3
                    s3_prefix = f"ttb-pre-prod/ttb_processed_data/structured/{partition_date_str}/{method_type}/"

                    # List objects for this partition
                    response = s3_client.list_objects_v2(
                        Bucket=config.bucket_name,
                        Prefix=s3_prefix
                    )

                    files = response.get('Contents', [])
                    if not files:
                        logger.info(f"No structured data found for {partition_date_str}/{method_type}")
                        continue

                    processing_stats['partitions_processed'] += 1

                    # Process each file in this partition
                    for file_obj in files:
                        s3_key = file_obj['Key']

                        try:
                            # Download and process file
                            if s3_key.endswith('.parquet'):
                                # Read parquet file
                                file_response = s3_client.get_object(Bucket=config.bucket_name, Key=s3_key)
                                with tempfile.NamedTemporaryFile() as tmp_file:
                                    tmp_file.write(file_response['Body'].read())
                                    tmp_file.flush()
                                    df = pd.read_parquet(tmp_file.name)

                                    # Track record counts
                                    if data_type == "cola-detail":
                                        processing_stats['cola_records_found'] += len(df)
                                    else:
                                        processing_stats['cert_records_found'] += len(df)

                                    # Add to consolidated records
                                    consolidated_records.extend(df.to_dict('records'))

                            elif s3_key.endswith('.json'):
                                # Read JSON file
                                import json
                                file_response = s3_client.get_object(Bucket=config.bucket_name, Key=s3_key)
                                content = file_response['Body'].read().decode('utf-8')
                                records = json.loads(content)

                                if isinstance(records, list):
                                    # Track record counts
                                    if data_type == "cola-detail":
                                        processing_stats['cola_records_found'] += len(records)
                                    else:
                                        processing_stats['cert_records_found'] += len(records)

                                    consolidated_records.extend(records)

                        except Exception as e:
                            logger.error(f"Error processing file {s3_key}: {e}")
                            processing_stats['processing_errors'].append({
                                'file': s3_key,
                                'error': str(e)
                            })

                except Exception as e:
                    logger.error(f"Error processing partition {method_type}: {e}")
                    processing_stats['processing_errors'].append({
                        'partition': method_type,
                        'error': str(e)
                    })

        # Process consolidated records
        if consolidated_records:
            # Convert to DataFrame for processing
            df = pd.DataFrame(consolidated_records)

            # Deduplication if enabled
            if config.deduplication_enabled:
                initial_count = len(df)
                df = df.drop_duplicates(subset=['ttb_id'], keep='first')
                processing_stats['duplicate_ttb_ids'] = initial_count - len(df)

            processing_stats['consolidated_records'] = len(df)

            # Save consolidated data to S3
            output_key = f"{config.s3_output_prefix}/consolidated/{partition_date_str}/consolidated_data.parquet"

            # Convert to parquet and upload
            table = pa.Table.from_pandas(df)

            with tempfile.NamedTemporaryFile() as tmp_file:
                pq.write_table(table, tmp_file.name)
                tmp_file.seek(0)

                s3_client.put_object(
                    Bucket=config.bucket_name,
                    Key=output_key,
                    Body=tmp_file.read(),
                    ContentType='application/octet-stream'
                )

            logger.info(f"Saved {len(df)} consolidated records to {output_key}")

        # Calculate data quality metrics
        total_records = processing_stats['cola_records_found'] + processing_stats['cert_records_found']
        data_quality_score = (
            processing_stats['consolidated_records'] / max(total_records, 1)
        ) if total_records > 0 else 0.0

        # Add metadata
        context.add_output_metadata({
            "partition_date": MetadataValue.text(partition_date_str),
            "partitions_processed": MetadataValue.int(processing_stats['partitions_processed']),
            "cola_records_found": MetadataValue.int(processing_stats['cola_records_found']),
            "cert_records_found": MetadataValue.int(processing_stats['cert_records_found']),
            "consolidated_records": MetadataValue.int(processing_stats['consolidated_records']),
            "duplicate_ttb_ids": MetadataValue.int(processing_stats['duplicate_ttb_ids']),
            "data_quality_score": MetadataValue.float(data_quality_score),
            "processing_errors": MetadataValue.int(len(processing_stats['processing_errors'])),
            "output_location": MetadataValue.text(f"s3://{config.bucket_name}/{config.s3_output_prefix}/consolidated/{partition_date_str}/")
        })

        return {
            "partition_date": partition_date_str,
            "processing_stats": processing_stats,
            "data_quality_score": data_quality_score,
            "consolidated_records_count": processing_stats['consolidated_records']
        }

    except Exception as e:
        logger.error(f"Critical error in consolidation: {e}")
        processing_stats['processing_errors'].append({
            'stage': 'consolidation',
            'error': str(e)
        })
        raise