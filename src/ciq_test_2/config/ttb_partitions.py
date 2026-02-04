"""
Centralized partition definitions for TTB pipeline.

This module provides configurable partition definitions that can be used
across all TTB assets to ensure consistency and enable testing with minimal partitions.
"""
from dagster import (
    DailyPartitionsDefinition,
    EnvVar
)


def get_configurable_daily_partitions() -> DailyPartitionsDefinition:
    """
    Get configurable daily partitions based on environment variables.

    Environment Variables:
        TTB_PARTITION_START_DATE: Start date (default: 2000-01-01 for 25+ years)
        TTB_PARTITION_END_DATE: End date (default: 2025-12-31)

    Logic: Both start_date and end_date are always included, along with all days in between.

    Default: 25+ years of data (2000-01-01 to 2025-12-31)
    ~9,497 daily partitions
    """
    start_date = EnvVar("TTB_PARTITION_START_DATE").get_value("2000-01-01")
    end_date = EnvVar("TTB_PARTITION_END_DATE").get_value("2025-12-31")

    from datetime import datetime, timedelta

    # Parse dates and calculate proper end_date
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    # Add one day to end_date to make it inclusive
    inclusive_end_date = (end_dt + timedelta(days=1)).strftime("%Y-%m-%d")

    return DailyPartitionsDefinition(
        start_date=start_date,
        end_date=inclusive_end_date,
    )






# Create the actual partition definitions that assets will use
daily_partitions = get_configurable_daily_partitions()


# For backward compatibility and convenience
__all__ = [
    "daily_partitions",
    "get_configurable_daily_partitions"
]