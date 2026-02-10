"""
Asset Checks

This module contains data quality checks and validation logic for TTB pipeline assets.
Checks are organized by asset type and validation category.
"""

from .ttb_asset_checks import (
    # ttb_extracted_data checks
    check_extraction_success_rate,
    check_field_completeness,
    check_html_artifact_detection,
    check_date_field_quality,
    check_record_count_consistency,
    check_data_type_balance,
    check_extraction_error_analysis,
    # ttb_cleaned_data checks
    check_transformation_validation_rates,
    # ttb_structured_data checks
    check_schema_compliance,
    # ttb_raw_data checks
    check_reference_data_freshness,
    check_sequence_completeness,
    check_receipt_method_coverage,
    # dimension & fact checks
    check_fact_table_integrity,
    check_dimensional_data_quality,
    check_reference_data_coverage,
    check_certificate_compliance_monitoring,
)

__all__ = [
    # ttb_extracted_data checks
    "check_extraction_success_rate",
    "check_field_completeness",
    "check_html_artifact_detection",
    "check_date_field_quality",
    "check_record_count_consistency",
    "check_data_type_balance",
    "check_extraction_error_analysis",
    # ttb_cleaned_data checks
    "check_transformation_validation_rates",
    # ttb_structured_data checks
    "check_schema_compliance",
    # ttb_raw_data checks
    "check_reference_data_freshness",
    "check_sequence_completeness",
    "check_receipt_method_coverage",
    # dimension & fact checks
    "check_fact_table_integrity",
    "check_dimensional_data_quality",
    "check_reference_data_coverage",
    "check_certificate_compliance_monitoring",
]
