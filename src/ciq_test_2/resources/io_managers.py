"""
IO Manager Definitions

Standardized IO managers for the TTB pipeline following Dagster best practices.
"""
import tempfile
import pickle
from typing import Any, Dict, List
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import json

from dagster import (
    IOManager,
    io_manager,
    ConfigurableIOManager,
    InputContext,
    OutputContext,
    EnvVar
)
from dagster_aws.s3 import S3PickleIOManager


class TTBS3IOManager(ConfigurableIOManager):
    """
    Custom S3 IO Manager for TTB pipeline data.

    Handles different data formats (JSON, Parquet, CSV) based on asset metadata
    and provides standardized S3 key patterns compatible with legacy structure.
    """

    bucket_name: str = "ciq-dagster"
    region_name: str = "us-east-1"

    # S3 prefixes for ttb-pre-prod environment
    raw_data_prefix: str = "ttb-pre-prod/ttb_raw_data"
    processed_data_prefix: str = "ttb-pre-prod/ttb_processed_data"
    dimensional_data_prefix: str = "ttb-pre-prod/ttb_analytics"

    def _get_s3_client(self):
        """Get configured S3 client."""
        import boto3
        return boto3.client('s3', region_name=self.region_name)

    def _handle_raw_data_output(self, context, s3_client, base_s3_key: str, obj: Dict[str, Any]) -> None:
        """
        Handle raw data output with nested folder structure by receipt method.

        Creates S3 structure:
        {base_s3_key}/
            receipt_method=000/data.pickle
            receipt_method=001/data.pickle
            receipt_method=002/data.pickle
            receipt_method=003/data.pickle
            _summary.json
        """
        method_labels = {0: "hand_delivered", 1: "e_filed", 2: "mailed", 3: "overnight"}
        saved_paths = []

        # Save each receipt method's data to separate folder
        for method, method_data in obj.get('by_receipt_method', {}).items():
            records = method_data.get('records', [])
            stats = method_data.get('stats', {})

            # Create S3 key for this receipt method
            method_s3_key = f"{base_s3_key}/receipt_method={method:03d}/data.pickle"

            # Save records as pickle
            method_output = {
                "records": records,
                "stats": stats,
                "completeness": method_data.get('completeness', {}),
                "receipt_method": method,
                "receipt_method_name": method_labels.get(method, f"method_{method}")
            }

            pickle_content = pickle.dumps(method_output)
            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=method_s3_key,
                Body=pickle_content,
                ContentType='application/octet-stream'
            )

            saved_paths.append(method_s3_key)
            context.log.info(f"Saved {len(records)} records for receipt method {method} ({method_labels.get(method, 'unknown')}) to s3://{self.bucket_name}/{method_s3_key}")

        # Save summary metadata as JSON
        summary_s3_key = f"{base_s3_key}/_summary.json"
        summary = obj.get('summary', {})
        summary['saved_paths'] = saved_paths

        json_content = json.dumps(summary, indent=2, default=str)
        s3_client.put_object(
            Bucket=self.bucket_name,
            Key=summary_s3_key,
            Body=json_content.encode('utf-8'),
            ContentType='application/json'
        )

        context.log.info(f"Saved summary to s3://{self.bucket_name}/{summary_s3_key}")

        # Add S3 location metadata
        context.add_output_metadata({
            "s3_base_location": f"s3://{self.bucket_name}/{base_s3_key}/",
            "receipt_method_paths": {
                method_labels.get(m, f"method_{m}"): f"s3://{self.bucket_name}/{base_s3_key}/receipt_method={m:03d}/data.pickle"
                for m in obj.get('by_receipt_method', {}).keys()
            },
            "summary_path": f"s3://{self.bucket_name}/{summary_s3_key}",
            "format": "pickle"
        })

    def _load_raw_data_input(self, context, s3_client, s3_prefix: str) -> List[Dict[str, Any]]:
        """
        Load raw data from EC2 extraction format.

        EC2 format: {s3_prefix}/{year}/{month:02d}/{date}
        Contains flat pickle with all receipt methods combined.

        Returns:
            List of raw records (all_records from the pickle) for downstream processing.
        """
        partition_key = context.asset_partition_key
        partition_date = datetime.strptime(partition_key, "%Y-%m-%d").date()

        # EC2 worker format: ttb_raw_data/{year}/{month:02d}/{date}
        s3_key = f"{s3_prefix}/{partition_date.year}/{partition_date.month:02d}/{partition_key}"

        context.log.info(f"Loading raw data from s3://{self.bucket_name}/{s3_key}")

        try:
            response = s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            content = response['Body'].read()
            data = pickle.loads(content)

            # Return all_records directly - this is what downstream assets expect
            all_records = data.get('all_records', [])
            context.log.info(f"Loaded {len(all_records)} records from {s3_key}")
            return all_records
        except Exception as e:
            context.log.error(f"Failed to load from {s3_key}: {e}")
            raise

    def _get_s3_key(self, context: OutputContext) -> str:
        """
        Generate S3 key based on asset metadata and partition using legacy-compatible paths.

        Uses asset group name to determine the appropriate S3 path prefix.
        """
        asset_key = context.asset_key
        asset_metadata = context.metadata or {}

        # Simple asset name-based mapping (reliable and straightforward)
        asset_name = asset_key.path[-1]

        # Determine S3 prefix based on asset type
        if asset_name in ['ttb_raw_data']:
            s3_prefix = self.raw_data_prefix
        elif asset_name in ['ttb_extracted_data', 'ttb_cleaned_data', 'ttb_structured_data', 'ttb_consolidated_data']:
            s3_prefix = self.processed_data_prefix
        elif asset_name in ['dim_dates', 'dim_companies', 'dim_products', 'fact_products', 'fact_certificates', 'ttb_reference_data']:
            s3_prefix = self.dimensional_data_prefix
        else:
            s3_prefix = self.processed_data_prefix  # Default fallback

        format_type = asset_metadata.get("format", "parquet")

        # Check if this is a partitioned context
        is_partitioned = hasattr(context, 'has_partition_key') and context.has_partition_key

        # For raw data, we use special handling with nested folders by receipt method
        if asset_name == 'ttb_raw_data' and is_partitioned:
            # Return base path - actual saving handled in handle_output with nested structure
            return f"{s3_prefix}/partition_date={context.partition_key}"

        # Build S3 key for other assets
        if is_partitioned:
            # Handle daily partitioned assets
            return f"{s3_prefix}/partition_date={context.partition_key}/{asset_name}.{format_type}"
        else:
            # Non-partitioned assets
            return f"{s3_prefix}/{asset_name}.{format_type}"

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Handle output to S3 based on data type."""
        s3_client = self._get_s3_client()
        s3_key = self._get_s3_key(context)

        asset_name = context.asset_key.path[-1]
        format_type = context.metadata.get("format", "json")

        # Special handling for ttb_raw_data - nested folders by receipt method
        if asset_name == 'ttb_raw_data' and isinstance(obj, dict) and 'by_receipt_method' in obj:
            self._handle_raw_data_output(context, s3_client, s3_key, obj)
            return

        # Skip IO manager processing if s3_key is None
        if s3_key is None:
            context.log.info("Skipping IO manager output - asset handles its own S3 storage")
            return

        context.log.info(f"Writing {format_type} data to s3://{self.bucket_name}/{s3_key}")

        if format_type == "parquet":
            # Handle DataFrame or dict to Parquet
            if isinstance(obj, pd.DataFrame):
                df = obj
            elif isinstance(obj, dict) and "data" in obj:
                df = pd.DataFrame(obj["data"])
            else:
                # Convert dict to single-row DataFrame
                df = pd.DataFrame([obj] if isinstance(obj, dict) else obj)

            table = pa.Table.from_pandas(df)
            with tempfile.NamedTemporaryFile() as tmp_file:
                pq.write_table(table, tmp_file.name)
                tmp_file.seek(0)

                s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=tmp_file.read(),
                    ContentType='application/octet-stream'
                )

        elif format_type == "json":
            # Handle JSON output
            json_content = json.dumps(obj, indent=2, default=str)
            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json_content.encode('utf-8'),
                ContentType='application/json'
            )

        elif format_type == "csv":
            # Handle CSV output
            if isinstance(obj, pd.DataFrame):
                csv_content = obj.to_csv(index=False)
            else:
                df = pd.DataFrame([obj] if isinstance(obj, dict) else obj)
                csv_content = df.to_csv(index=False)

            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=csv_content.encode('utf-8'),
                ContentType='text/csv'
            )

        elif format_type == "pickle":
            # Handle pickle output
            pickle_content = pickle.dumps(obj)
            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=pickle_content,
                ContentType='application/octet-stream'
            )

        else:
            # Default to JSON for unknown formats
            json_content = json.dumps(obj, indent=2, default=str)
            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json_content.encode('utf-8'),
                ContentType='application/json'
            )

        # Add S3 location metadata
        context.add_output_metadata({
            "s3_location": f"s3://{self.bucket_name}/{s3_key}",
            "s3_key": s3_key,
            "format": format_type
        })

    def load_input(self, context: InputContext) -> Any:
        """Load input from S3 by reconstructing the S3 path from context."""
        s3_client = self._get_s3_client()

        # Reconstruct S3 key using same logic as _get_s3_key but for InputContext
        asset_key = context.asset_key
        asset_name = asset_key.path[-1]

        # Determine S3 prefix based on asset type
        if asset_name in ['ttb_raw_data']:
            s3_prefix = self.raw_data_prefix
            format_type = "pickle"  # Raw data is pickle
        elif asset_name in ['ttb_extracted_data', 'ttb_cleaned_data', 'ttb_structured_data', 'ttb_consolidated_data']:
            s3_prefix = self.processed_data_prefix
            format_type = "pickle"  # Processed data is pickle
        elif asset_name in ['dim_dates', 'dim_companies', 'dim_products', 'fact_products', 'fact_certificates', 'ttb_reference_data']:
            s3_prefix = self.dimensional_data_prefix
            format_type = "pickle"  # Dimensional data is pickle
        else:
            s3_prefix = self.processed_data_prefix  # Default fallback
            format_type = "pickle"

        # Check if this is a partitioned asset
        is_partitioned = hasattr(context, 'has_asset_partitions') and context.has_asset_partitions

        # Special handling for ttb_raw_data - load from nested structure
        if asset_name == 'ttb_raw_data' and is_partitioned:
            return self._load_raw_data_input(context, s3_client, s3_prefix)

        # Build S3 key for daily partitioned assets only
        if is_partitioned:
            # Daily partition
            s3_key = f"{s3_prefix}/partition_date={context.asset_partition_key}/{asset_name}.{format_type}"
        else:
            # Non-partitioned assets
            s3_key = f"{s3_prefix}/{asset_name}.{format_type}"

        context.log.info(f"Loading {format_type} data from s3://{self.bucket_name}/{s3_key}")

        try:
            file_response = s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)

            if format_type == "parquet":
                with tempfile.NamedTemporaryFile() as tmp_file:
                    tmp_file.write(file_response['Body'].read())
                    tmp_file.flush()
                    return pd.read_parquet(tmp_file.name)

            elif format_type == "json":
                content = file_response['Body'].read().decode('utf-8')
                return json.loads(content)

            elif format_type == "csv":
                content = file_response['Body'].read().decode('utf-8')
                return pd.read_csv(content)

            elif format_type == "pickle":
                content = file_response['Body'].read()
                return pickle.loads(content)

            else:
                # Default to pickle for unknown formats
                content = file_response['Body'].read()
                return pickle.loads(content)

        except Exception as e:
            context.log.error(f"Failed to load data from S3 key {s3_key}: {str(e)}")
            raise ValueError(f"Could not load input {context.asset_key} from s3://{self.bucket_name}/{s3_key}: {str(e)}")


@io_manager(
    config_schema={
        "bucket_name": str,
        "region_name": str
    }
)
def ttb_s3_io_manager(context) -> TTBS3IOManager:
    """
    TTB S3 IO Manager factory.

    Provides standardized S3 storage for TTB pipeline assets.
    """
    return TTBS3IOManager(
        bucket_name=context.resource_config["bucket_name"],
        region_name=context.resource_config["region_name"]
    )


@io_manager(
    config_schema={
        "bucket_name": str,
        "region_name": str
    }
)
def ttb_parquet_io_manager(context) -> TTBS3IOManager:
    """
    TTB Parquet-specific IO Manager factory.

    Optimized for Parquet format with automatic partitioning support.
    """
    return TTBS3IOManager(
        bucket_name=context.resource_config["bucket_name"],
        region_name=context.resource_config["region_name"]
    )