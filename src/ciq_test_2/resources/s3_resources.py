"""
S3 Resource Definitions

Standardized S3 resources for the TTB pipeline following Dagster best practices.
"""
from dagster import ConfigurableResource, EnvVar
from dagster_aws.s3 import S3Resource
from typing import Optional


class TTBStorageResource(ConfigurableResource):
    """
    TTB-specific storage resource with configured S3 settings.

    This resource provides standardized S3 configuration for the TTB pipeline
    with environment-based configuration support.
    """

    bucket_name: str = "ciq-dagster"
    region_name: str = "us-east-1"
    raw_data_prefix: str = "ttb-pre-prod/ttb_raw_data"
    processed_data_prefix: str = "ttb-pre-prod/ttb_processed_data"
    consolidated_data_prefix: str = "ttb-pre-prod/ttb_consolidated"
    analytics_data_prefix: str = "ttb-pre-prod/ttb_analytics"

    def get_s3_client(self):
        """Get configured S3 client."""
        import boto3
        return boto3.client('s3', region_name=self.region_name)

    def get_raw_data_path(self, partition_date: str, method_type: str) -> str:
        """Get S3 path for raw data."""
        return f"{self.raw_data_prefix}/{partition_date}/{method_type}/"

    def get_processed_data_path(self, stage: str, partition_date: str, method_type: str) -> str:
        """Get S3 path for processed data."""
        return f"{self.processed_data_prefix}/{stage}/{partition_date}/{method_type}/"

    def get_consolidated_data_path(self, partition_date: str) -> str:
        """Get S3 path for consolidated data."""
        return f"{self.consolidated_data_prefix}/consolidated/{partition_date}/"

    def get_analytics_data_path(self, data_type: str, partition_date: str = None) -> str:
        """Get S3 path for analytics data."""
        if partition_date:
            return f"{self.analytics_data_prefix}/{data_type}/partition_date={partition_date}/"
        else:
            return f"{self.analytics_data_prefix}/{data_type}/"


def get_s3_resource() -> S3Resource:
    """
    Get standard S3 resource with environment-based configuration.

    Environment Variables:
        AWS_REGION: AWS region (default: us-east-1)
        AWS_ACCESS_KEY_ID: AWS access key
        AWS_SECRET_ACCESS_KEY: AWS secret key

    Returns:
        Configured S3Resource instance
    """
    return S3Resource(
        region_name=EnvVar("AWS_REGION").get_value("us-east-1")
    )