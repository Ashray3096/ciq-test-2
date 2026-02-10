"""
TTB Pipeline Definitions

Modern Dagster definitions following best practices with proper asset organization,
resource management, and configuration.
"""
import os
from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules, EnvVar, FilesystemIOManager
from dagster_aws.s3 import S3PickleIOManager

# Import custom IO manager for EC2-extracted data
from .resources.io_managers import TTBS3IOManager

# Import organized asset modules
from . import assets
from . import checks
from .jobs import (
    # Production jobs
    ttb_raw_pipeline,
    ttb_analytics_pipeline,
    ttb_extraction_only,
    ttb_analytics_only,
    ttb_backfill,
    # Production schedules
    ttb_daily_schedule,
    ttb_analytics_schedule,
    ttb_weekend_schedule,
    # Sensors
    ttb_pipeline_health_sensor,
    ttb_data_quality_sensor
)

# Import standardized resources
from .resources import get_s3_resource, TTBStorageResource, SupabaseResource, SupabaseIOManager


def get_environment() -> str:
    """Get current environment from environment variable."""
    return EnvVar("DAGSTER_ENVIRONMENT").get_value("development")


def get_resources_for_environment(environment: str) -> dict:
    """Get resources configured for the specified environment."""
    # Check if valid AWS credentials are available
    aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    has_valid_aws = aws_access_key and not aws_access_key.startswith("your-")

    # Use local filesystem for development without valid AWS credentials
    if environment == "development" and not has_valid_aws:
        io_manager = FilesystemIOManager(
            base_dir="./dagster_storage"  # Local storage directory
        )
    else:
        # Use custom TTBS3IOManager for S3 - handles EC2-extracted data format
        # All pipeline data stored under s3://ciq-dagster/ttb-pre-prod/
        io_manager = TTBS3IOManager(
            bucket_name=EnvVar("TTB_S3_BUCKET").get_value("ciq-dagster"),
            region_name=EnvVar("AWS_REGION").get_value("us-east-1")
        )

    base_resources = {
        # Standard S3 resource
        "s3_resource": get_s3_resource(),

        # TTB-specific storage resource
        "ttb_storage": TTBStorageResource(
            bucket_name=EnvVar("TTB_S3_BUCKET").get_value("ciq-dagster"),
            region_name=EnvVar("AWS_REGION").get_value("us-east-1")
        ),

        # IO Manager - local filesystem for dev, S3 for production
        "io_manager": io_manager,

        # Supabase resources
        "supabase_resource": SupabaseResource(),
        "supabase_io_manager": SupabaseIOManager(
            supabase_resource=SupabaseResource()
        )
    }

    return base_resources


# Create definitions with organized structure
defs = Definitions(
    # Load assets from organized modules
    assets=load_assets_from_modules([
        assets.raw,
        assets.processed,
        assets.reference,
        assets.dimensional,
        assets.facts,
        assets.supabase_export
    ]),

    # Asset checks for data quality
    asset_checks=load_asset_checks_from_modules([checks.ttb_asset_checks]),

    # Production jobs
    jobs=[
        ttb_raw_pipeline,           # Raw data extraction pipeline
        ttb_analytics_pipeline,     # Analytics pipeline (consolidated → facts & dimensions)
        ttb_extraction_only,        # Raw extraction only
        ttb_analytics_only,         # Analytics refresh only
        ttb_backfill,              # Legacy backfill
    ],

    # Production schedules
    schedules=[
        ttb_daily_schedule,         # 6 AM UTC daily pipeline
        ttb_analytics_schedule,     # 8 AM UTC analytics refresh
        ttb_weekend_schedule,       # Sunday catch-up
    ],

    # Monitoring sensors
    sensors=[
        ttb_pipeline_health_sensor,
        ttb_data_quality_sensor,
    ],

    # Resources configured for current environment
    resources=get_resources_for_environment(get_environment())
)