"""
TTB data quality asset checks and monitoring.

This module defines Dagster asset checks that monitor data quality,
parsing success rates, validation metrics, and system health for the TTB pipeline.
"""
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import json

from dagster import (
    asset_check,
    AssetCheckResult,
    AssetCheckSeverity,
    get_dagster_logger,
    AssetCheckExecutionContext,
    MetadataValue
)

from ..assets.raw import ttb_raw_data
from ..assets.processed import ttb_extracted_data, ttb_cleaned_data, ttb_structured_data
from ..assets.dimensional import dim_companies, dim_products, dim_dates
from ..assets.facts import fact_products, fact_certificates
from ..assets.reference import ttb_reference_data
from ..utils.ttb_transformations import load_ttb_reference_data
from ..config.ttb_partitions import daily_partitions


@asset_check(
    asset=ttb_raw_data,
    name="parsing_success_rate",
    description="Check that TTB HTML parsing success rate is above threshold"
)
def check_parsing_success_rate(context: AssetCheckExecutionContext, ttb_raw_data) -> AssetCheckResult:
    """
    Verify that the parsing success rate for TTB HTML files is acceptable.

    Fails if success rate is below 85%, warns if below 95%.
    """
    logger = get_dagster_logger()

    # Check if we have a specific partition context
    if hasattr(context, 'partition_key') and context.partition_key:
        logger.info(f"Running parsing success rate check for partition: {context.partition_key}")

    try:
        # Handle case where ttb_raw_data might be a dict or have stats
        if isinstance(ttb_raw_data, dict):
            stats = ttb_raw_data.get('processing_stats', {})
        else:
            # If it's not a dict, try to get stats attribute or default to empty
            stats = getattr(ttb_raw_data, 'processing_stats', {})
        total_files = stats.get('total_files', 0)
        successful_parses = stats.get('successful_parses', 0)
        failed_parses = stats.get('failed_parses', 0)

        if total_files == 0:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.WARN,
                description="No files found to process",
                metadata={
                    "total_files": total_files,
                    "partition": context.partition_key
                }
            )

        success_rate = (successful_parses / total_files) * 100

        # Define thresholds
        failure_threshold = 85.0  # Below this is a failure
        warning_threshold = 95.0  # Below this is a warning

        passed = success_rate >= failure_threshold
        severity = AssetCheckSeverity.ERROR if success_rate < failure_threshold else (
            AssetCheckSeverity.WARN if success_rate < warning_threshold else None
        )

        description = f"Parsing success rate: {success_rate:.1f}% ({successful_parses}/{total_files} files)"

        if not passed:
            description += f" - Below failure threshold of {failure_threshold}%"
        elif severity == AssetCheckSeverity.WARN:
            description += f" - Below warning threshold of {warning_threshold}%"

        return AssetCheckResult(
            passed=passed,
            severity=severity,
            description=description,
            metadata={
                "success_rate_percent": success_rate,
                "successful_parses": successful_parses,
                "failed_parses": failed_parses,
                "total_files": total_files,
                "failure_threshold": failure_threshold,
                "warning_threshold": warning_threshold,
                "processing_errors": stats.get('processing_errors', [])[:5]  # Show first 5 errors
            }
        )

    except Exception as e:
        logger.error(f"Error in parsing success rate check: {str(e)}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Check failed due to error: {str(e)}",
            metadata={"error": str(e)}
        )


@asset_check(
    asset=ttb_raw_data,
    name="field_extraction_completeness",
    description="Check that key fields are being extracted from TTB data"
)
def check_field_extraction_completeness(context: AssetCheckExecutionContext, ttb_raw_data) -> AssetCheckResult:
    """
    Verify that essential fields are being extracted from TTB HTML files.

    Checks for presence of key fields like TTB ID, dates, applicant info.
    """
    logger = get_dagster_logger()

    try:
        parsed_records = ttb_raw_data.get('parsed_records', [])
        partition_info = ttb_raw_data.get('partition_info', {})
        data_type = partition_info.get('data_type', 'unknown')

        if not parsed_records:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.WARN,
                description="No parsed records found",
                metadata={
                    "record_count": 0,
                    "data_type": data_type
                }
            )

        # Define essential fields by data type
        if data_type == "cola-detail":
            essential_fields = [
                'ttb_id', 'serial_number', 'filing_date', 'applicant_business_name',
                'brand_name', 'product_description', 'net_contents'
            ]
        elif data_type == "certificate":
            essential_fields = [
                'ttb_id', 'certificate_number', 'effective_date', 'trade_name',
                'premises_address', 'permit_type'
            ]
        else:
            essential_fields = ['ttb_id']

        # Calculate field completeness
        field_stats = {}
        total_records = len(parsed_records)

        for field in essential_fields:
            non_empty_count = sum(1 for record in parsed_records
                                if record.get(field) and str(record[field]).strip())
            completeness_rate = (non_empty_count / total_records) * 100
            field_stats[field] = {
                'completeness_rate': completeness_rate,
                'non_empty_count': non_empty_count,
                'total_records': total_records
            }

        # Check if any essential field has low completeness
        min_completeness_threshold = 70.0  # Minimum acceptable completeness rate
        failing_fields = [
            field for field, stats in field_stats.items()
            if stats['completeness_rate'] < min_completeness_threshold
        ]

        overall_completeness = sum(stats['completeness_rate'] for stats in field_stats.values()) / len(field_stats)

        passed = len(failing_fields) == 0
        severity = AssetCheckSeverity.WARN if failing_fields else None

        description = f"Field extraction completeness: {overall_completeness:.1f}% average"
        if failing_fields:
            description += f" - Low completeness fields: {', '.join(failing_fields)}"

        return AssetCheckResult(
            passed=passed,
            severity=severity,
            description=description,
            metadata={
                "overall_completeness_percent": overall_completeness,
                "field_statistics": field_stats,
                "failing_fields": failing_fields,
                "total_records": total_records,
                "data_type": data_type,
                "min_threshold": min_completeness_threshold
            }
        )

    except Exception as e:
        logger.error(f"Error in field extraction completeness check: {str(e)}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Check failed due to error: {str(e)}",
            metadata={"error": str(e)}
        )


@asset_check(
    asset=ttb_cleaned_data,
    name="transformation_validation_rates",
    description="Check transformation and validation success rates"
)
def check_transformation_validation_rates(context: AssetCheckExecutionContext, ttb_cleaned_data) -> AssetCheckResult:
    """
    Monitor transformation success rates and reference data validation results.

    Checks TTB ID validation, reference data validation, and transformation errors.
    """
    logger = get_dagster_logger()

    try:
        transformed_records = ttb_cleaned_data.get('transformed_records', [])
        transformation_metadata = ttb_cleaned_data.get('transformation_metadata', {})

        if not transformed_records:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.WARN,
                description="No transformed records found",
                metadata={"record_count": 0}
            )

        total_records = len(transformed_records)

        # Check TTB ID validation rates
        valid_ttb_ids = sum(1 for record in transformed_records
                           if record.get('ttb_id_valid', False))
        ttb_id_validation_rate = (valid_ttb_ids / total_records) * 100

        # Check reference data validation rates
        valid_product_classes = sum(1 for record in transformed_records
                                  if record.get('product_class_validation', {}).get('is_valid', False))
        product_class_validation_rate = (valid_product_classes / total_records) * 100 if total_records > 0 else 0

        valid_origin_codes = sum(1 for record in transformed_records
                               if record.get('origin_code_validation', {}).get('is_valid', False))
        origin_code_validation_rate = (valid_origin_codes / total_records) * 100 if total_records > 0 else 0

        # Transformation error analysis
        transformation_stats = transformation_metadata.get('transformation_stats', {})
        transformation_errors = transformation_stats.get('transformation_errors', [])
        error_rate = (len(transformation_errors) / total_records) * 100 if total_records > 0 else 0

        # Define thresholds
        min_ttb_id_rate = 90.0
        max_error_rate = 10.0

        # Determine overall health
        issues = []
        if ttb_id_validation_rate < min_ttb_id_rate:
            issues.append(f"Low TTB ID validation rate: {ttb_id_validation_rate:.1f}%")
        if error_rate > max_error_rate:
            issues.append(f"High transformation error rate: {error_rate:.1f}%")

        passed = len(issues) == 0
        severity = AssetCheckSeverity.WARN if not passed else None

        description = f"Validation rates - TTB ID: {ttb_id_validation_rate:.1f}%, Product Class: {product_class_validation_rate:.1f}%, Origin: {origin_code_validation_rate:.1f}%"
        if issues:
            description += f" - Issues: {'; '.join(issues)}"

        return AssetCheckResult(
            passed=passed,
            severity=severity,
            description=description,
            metadata={
                "ttb_id_validation_rate": ttb_id_validation_rate,
                "product_class_validation_rate": product_class_validation_rate,
                "origin_code_validation_rate": origin_code_validation_rate,
                "transformation_error_rate": error_rate,
                "total_records": total_records,
                "valid_ttb_ids": valid_ttb_ids,
                "valid_product_classes": valid_product_classes,
                "valid_origin_codes": valid_origin_codes,
                "transformation_errors": transformation_errors[:3],  # Show first 3 errors
                "issues": issues
            }
        )

    except Exception as e:
        logger.error(f"Error in transformation validation rates check: {str(e)}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Check failed due to error: {str(e)}",
            metadata={"error": str(e)}
        )


@asset_check(
    asset=ttb_structured_data,
    name="schema_compliance",
    description="Check that structured output complies with expected schema"
)
def check_schema_compliance(context: AssetCheckExecutionContext, ttb_structured_data) -> AssetCheckResult:
    """
    Verify that the structured output data complies with the defined schema.

    Checks field types, required fields, and data consistency.
    """
    logger = get_dagster_logger()

    try:
        dataset_metadata = ttb_structured_data.get('dataset_metadata', {})
        validation_results = ttb_structured_data.get('validation_results', {})

        if not validation_results:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.WARN,
                description="No validation results found",
                metadata={"dataset_metadata": dataset_metadata}
            )

        total_records = validation_results.get('total_records', 0)
        valid_records = validation_results.get('valid_records', 0)
        validation_errors = validation_results.get('validation_errors', [])

        if total_records == 0:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.WARN,
                description="No records found for schema validation",
                metadata={"total_records": 0}
            )

        compliance_rate = (valid_records / total_records) * 100
        error_rate = (len(validation_errors) / total_records) * 100

        # Define thresholds
        min_compliance_rate = 95.0
        max_error_rate = 5.0

        passed = compliance_rate >= min_compliance_rate and error_rate <= max_error_rate
        severity = AssetCheckSeverity.WARN if not passed else None

        description = f"Schema compliance: {compliance_rate:.1f}% ({valid_records}/{total_records} records)"
        if error_rate > max_error_rate:
            description += f", Error rate: {error_rate:.1f}%"

        # Field-level validation statistics
        field_validation = validation_results.get('field_validation', {})
        problematic_fields = [
            field for field, stats in field_validation.items()
            if stats.get('type_errors', 0) > 0
        ]

        return AssetCheckResult(
            passed=passed,
            severity=severity,
            description=description,
            metadata={
                "compliance_rate": compliance_rate,
                "error_rate": error_rate,
                "total_records": total_records,
                "valid_records": valid_records,
                "validation_errors_count": len(validation_errors),
                "problematic_fields": problematic_fields,
                "field_validation_summary": field_validation,
                "dataset_info": {
                    "files_written": dataset_metadata.get('files_written', 0),
                    "file_size_mb": dataset_metadata.get('file_size_bytes', 0) / (1024 * 1024),
                    "output_format": dataset_metadata.get('partitioned', False)
                },
                "sample_errors": validation_errors[:3]  # Show first 3 validation errors
            }
        )

    except Exception as e:
        logger.error(f"Error in schema compliance check: {str(e)}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Check failed due to error: {str(e)}",
            metadata={"error": str(e)}
        )


@asset_check(
    asset=ttb_raw_data,  # Associate with an asset for now
    name="reference_data_freshness",
    description="Check that TTB reference data is fresh and accessible"
)
def check_reference_data_freshness(context: AssetCheckExecutionContext) -> AssetCheckResult:
    """
    Verify that TTB reference data is accessible and reasonably fresh.

    Checks both the availability of reference lookup URLs and cached data age.
    """
    logger = get_dagster_logger()

    try:
        # Try to load reference data
        reference_data = load_ttb_reference_data()

        if not reference_data:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description="Failed to load TTB reference data",
                metadata={"reference_data_loaded": False}
            )

        # Check data completeness
        product_codes_count = len(reference_data.get('product_class_types', {}).get('by_code', {}))
        origin_codes_count = len(reference_data.get('origin_codes', {}).get('by_code', {}))

        # Expected minimum counts (based on current data)
        min_product_codes = 500
        min_origin_codes = 200

        issues = []
        if product_codes_count < min_product_codes:
            issues.append(f"Low product code count: {product_codes_count} (expected ≥ {min_product_codes})")
        if origin_codes_count < min_origin_codes:
            issues.append(f"Low origin code count: {origin_codes_count} (expected ≥ {min_origin_codes})")

        passed = len(issues) == 0
        severity = AssetCheckSeverity.WARN if not passed else None

        description = f"Reference data loaded: {product_codes_count} product codes, {origin_codes_count} origin codes"
        if issues:
            description += f" - Issues: {'; '.join(issues)}"

        return AssetCheckResult(
            passed=passed,
            severity=severity,
            description=description,
            metadata={
                "reference_data_loaded": True,
                "product_codes_count": product_codes_count,
                "origin_codes_count": origin_codes_count,
                "min_product_codes": min_product_codes,
                "min_origin_codes": min_origin_codes,
                "issues": issues,
                "sample_product_codes": list(reference_data.get('product_class_types', {}).get('by_code', {}).keys())[:10],
                "sample_origin_codes": list(reference_data.get('origin_codes', {}).get('by_code', {}).keys())[:10]
            }
        )

    except Exception as e:
        logger.error(f"Error in reference data freshness check: {str(e)}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Reference data check failed: {str(e)}",
            metadata={"error": str(e), "reference_data_loaded": False}
        )


@asset_check(
    asset=fact_products,
    name="fact_table_integrity",
    description="Check fact table data integrity and foreign key relationships"
)
def check_fact_table_integrity(context: AssetCheckExecutionContext, fact_products) -> AssetCheckResult:
    """
    Verify fact table data integrity, foreign key relationships, and business logic.

    Checks foreign key completeness, data quality scores, and business metrics.
    """
    logger = get_dagster_logger()

    try:
        fact_records = fact_products.get('records', [])
        fact_stats = fact_products.get('statistics', {})

        if not fact_records:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description="No fact table records found",
                metadata={"record_count": 0}
            )

        total_records = len(fact_records)

        # Check foreign key integrity
        missing_company_keys = fact_stats.get('missing_company_keys', 0)
        missing_product_keys = fact_stats.get('missing_product_keys', 0)

        company_key_integrity = ((total_records - missing_company_keys) / total_records) * 100
        product_key_integrity = ((total_records - missing_product_keys) / total_records) * 100

        # Check data quality metrics
        quality_scores = fact_stats.get('quality_scores', [])
        avg_quality_score = sum(quality_scores) / len(quality_scores) if quality_scores else 0

        # Business logic checks
        records_with_dates = sum(1 for record in fact_records
                               if record.get('filing_date') and record.get('approval_date'))
        date_completeness = (records_with_dates / total_records) * 100

        # Define thresholds
        min_foreign_key_integrity = 95.0
        min_quality_score = 0.6
        min_date_completeness = 80.0

        issues = []
        if company_key_integrity < min_foreign_key_integrity:
            issues.append(f"Low company foreign key integrity: {company_key_integrity:.1f}%")
        if product_key_integrity < min_foreign_key_integrity:
            issues.append(f"Low product foreign key integrity: {product_key_integrity:.1f}%")
        if avg_quality_score < min_quality_score:
            issues.append(f"Low average quality score: {avg_quality_score:.2f}")
        if date_completeness < min_date_completeness:
            issues.append(f"Low date completeness: {date_completeness:.1f}%")

        passed = len(issues) == 0
        severity = AssetCheckSeverity.WARN if not passed else None

        description = f"Fact table integrity: {total_records} records, FK integrity {min(company_key_integrity, product_key_integrity):.1f}%, avg quality {avg_quality_score:.2f}"
        if issues:
            description += f" - Issues: {'; '.join(issues)}"

        return AssetCheckResult(
            passed=passed,
            severity=severity,
            description=description,
            metadata={
                "total_records": total_records,
                "company_key_integrity_percent": company_key_integrity,
                "product_key_integrity_percent": product_key_integrity,
                "missing_company_keys": missing_company_keys,
                "missing_product_keys": missing_product_keys,
                "average_quality_score": avg_quality_score,
                "date_completeness_percent": date_completeness,
                "records_with_dates": records_with_dates,
                "issues": issues,
                "business_metrics": {
                    "has_cola_detail_count": sum(1 for r in fact_records if r.get('has_cola_detail_data')),
                    "has_certificate_count": sum(1 for r in fact_records if r.get('has_certificate_data')),
                    "approval_rate": len([r for r in fact_records if 'APPROVED' in str(r.get('status', '')).upper()]) / total_records * 100
                }
            }
        )

    except Exception as e:
        logger.error(f"Error in fact table integrity check: {str(e)}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Check failed due to error: {str(e)}",
            metadata={"error": str(e)}
        )


@asset_check(
    asset=dim_companies,
    name="dimensional_data_quality",
    description="Check dimensional table data quality and deduplication effectiveness"
)
def check_dimensional_data_quality(context: AssetCheckExecutionContext, dim_companies) -> AssetCheckResult:
    """
    Monitor dimensional table data quality, deduplication, and consistency.

    Checks company deduplication, data quality scores, and dimensional integrity.
    """
    logger = get_dagster_logger()

    try:
        company_records = dim_companies.get('records', [])

        if not company_records:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.WARN,
                description="No company dimension records found",
                metadata={"record_count": 0}
            )

        total_companies = len(company_records)

        # Check data quality metrics
        quality_scores = [record.get('data_quality_score', 0) for record in company_records]
        avg_quality_score = sum(quality_scores) / len(quality_scores) if quality_scores else 0
        low_quality_companies = sum(1 for score in quality_scores if score < 0.5)

        # Check contact information completeness
        companies_with_phone = sum(1 for record in company_records if record.get('phone'))
        companies_with_email = sum(1 for record in company_records if record.get('email'))
        companies_with_address = sum(1 for record in company_records if record.get('mailing_address'))

        phone_completeness = (companies_with_phone / total_companies) * 100
        email_completeness = (companies_with_email / total_companies) * 100
        address_completeness = (companies_with_address / total_companies) * 100

        # Check for potential duplicates (similar names)
        business_names = [record.get('business_name', '').upper().strip() for record in company_records]
        unique_names = set(business_names)
        potential_duplicates = total_companies - len(unique_names)

        # Application volume analysis
        total_applications = sum(record.get('total_applications', 0) for record in company_records)
        avg_applications_per_company = total_applications / total_companies if total_companies > 0 else 0

        # Define thresholds
        min_quality_score = 0.6
        max_low_quality_percentage = 20.0
        min_address_completeness = 90.0

        issues = []
        if avg_quality_score < min_quality_score:
            issues.append(f"Low average quality score: {avg_quality_score:.2f}")

        low_quality_percentage = (low_quality_companies / total_companies) * 100
        if low_quality_percentage > max_low_quality_percentage:
            issues.append(f"High percentage of low-quality companies: {low_quality_percentage:.1f}%")

        if address_completeness < min_address_completeness:
            issues.append(f"Low address completeness: {address_completeness:.1f}%")

        if potential_duplicates > 0:
            issues.append(f"Potential duplicate companies detected: {potential_duplicates}")

        passed = len(issues) == 0
        severity = AssetCheckSeverity.WARN if not passed else None

        description = f"Company dimension: {total_companies} unique companies, avg quality {avg_quality_score:.2f}, {avg_applications_per_company:.1f} avg applications"
        if issues:
            description += f" - Issues: {'; '.join(issues)}"

        return AssetCheckResult(
            passed=passed,
            severity=severity,
            description=description,
            metadata={
                "total_companies": total_companies,
                "average_quality_score": avg_quality_score,
                "low_quality_companies": low_quality_companies,
                "low_quality_percentage": low_quality_percentage,
                "contact_completeness": {
                    "phone_percent": phone_completeness,
                    "email_percent": email_completeness,
                    "address_percent": address_completeness
                },
                "potential_duplicates": potential_duplicates,
                "total_applications": total_applications,
                "avg_applications_per_company": avg_applications_per_company,
                "issues": issues,
                "top_companies_by_volume": sorted(
                    [{"name": r.get('business_name'), "applications": r.get('total_applications', 0)}
                     for r in company_records],
                    key=lambda x: x['applications'],
                    reverse=True
                )[:5]
            }
        )

    except Exception as e:
        logger.error(f"Error in dimensional data quality check: {str(e)}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Check failed due to error: {str(e)}",
            metadata={"error": str(e)}
        )


@asset_check(
    asset=ttb_reference_data,
    name="reference_data_coverage",
    description="Check TTB reference data coverage and accuracy"
)
def check_reference_data_coverage(context: AssetCheckExecutionContext, ttb_reference_data) -> AssetCheckResult:
    """
    Monitor TTB reference data coverage, accuracy, and freshness.

    Checks reference data completeness and validates against known data patterns.
    """
    logger = get_dagster_logger()

    try:
        product_class_data = ttb_reference_data.get('product_class_types', {})
        origin_codes_data = ttb_reference_data.get('origin_codes', {})
        combined_stats = ttb_reference_data.get('statistics', {})

        product_codes_count = product_class_data.get('total_records', 0)
        origin_codes_count = origin_codes_data.get('total_records', 0)
        total_reference_records = combined_stats.get('total_reference_records', 0)

        # Check for extraction errors
        has_product_errors = combined_stats.get('has_product_errors', False)
        has_origin_errors = combined_stats.get('has_origin_errors', False)

        # Validate reference data content quality
        product_by_code = product_class_data.get('by_code', {})
        origin_by_code = origin_codes_data.get('by_code', {})

        # Check for empty or invalid entries
        empty_product_descriptions = sum(1 for desc in product_by_code.values() if not desc or len(desc.strip()) < 3)
        empty_origin_descriptions = sum(1 for desc in origin_by_code.values() if not desc or len(desc.strip()) < 3)

        # Expected minimum counts based on current TTB data
        min_product_codes = 500
        min_origin_codes = 200
        max_empty_percentage = 5.0

        issues = []

        if product_codes_count < min_product_codes:
            issues.append(f"Low product code count: {product_codes_count} (expected ≥ {min_product_codes})")

        if origin_codes_count < min_origin_codes:
            issues.append(f"Low origin code count: {origin_codes_count} (expected ≥ {min_origin_codes})")

        if has_product_errors:
            issues.append("Product class extraction errors detected")

        if has_origin_errors:
            issues.append("Origin code extraction errors detected")

        # Check data quality
        product_empty_percentage = (empty_product_descriptions / product_codes_count) * 100 if product_codes_count > 0 else 0
        origin_empty_percentage = (empty_origin_descriptions / origin_codes_count) * 100 if origin_codes_count > 0 else 0

        if product_empty_percentage > max_empty_percentage:
            issues.append(f"High percentage of empty product descriptions: {product_empty_percentage:.1f}%")

        if origin_empty_percentage > max_empty_percentage:
            issues.append(f"High percentage of empty origin descriptions: {origin_empty_percentage:.1f}%")

        # Check extraction timestamps for freshness
        product_extraction_time = product_class_data.get('extraction_timestamp')
        origin_extraction_time = origin_codes_data.get('extraction_timestamp')

        # Parse timestamps and check if data is recent (within last 7 days)
        freshness_issues = []
        if product_extraction_time:
            try:
                product_time = datetime.fromisoformat(product_extraction_time.replace('Z', '+00:00'))
                if (datetime.now() - product_time).days > 7:
                    freshness_issues.append(f"Product codes data is {(datetime.now() - product_time).days} days old")
            except:
                freshness_issues.append("Unable to parse product codes extraction timestamp")

        if origin_extraction_time:
            try:
                origin_time = datetime.fromisoformat(origin_extraction_time.replace('Z', '+00:00'))
                if (datetime.now() - origin_time).days > 7:
                    freshness_issues.append(f"Origin codes data is {(datetime.now() - origin_time).days} days old")
            except:
                freshness_issues.append("Unable to parse origin codes extraction timestamp")

        issues.extend(freshness_issues)

        passed = len(issues) == 0
        severity = AssetCheckSeverity.WARN if not passed else None

        description = f"Reference data: {product_codes_count} product codes, {origin_codes_count} origin codes, total {total_reference_records} records"
        if issues:
            description += f" - Issues: {'; '.join(issues)}"

        metadata = {
            "product_codes_count": product_codes_count,
            "origin_codes_count": origin_codes_count,
            "total_reference_records": total_reference_records,
            "has_extraction_errors": has_product_errors or has_origin_errors,
            "data_quality": {
                "empty_product_descriptions": empty_product_descriptions,
                "empty_origin_descriptions": empty_origin_descriptions,
                "product_empty_percentage": product_empty_percentage,
                "origin_empty_percentage": origin_empty_percentage
            },
            "extraction_timestamps": {
                "product_codes": product_extraction_time,
                "origin_codes": origin_extraction_time
            },
            "issues": issues,
            "sample_data": {
                "product_codes": list(product_by_code.keys())[:10],
                "origin_codes": list(origin_by_code.keys())[:10]
            }
        }

        if passed:
            return AssetCheckResult(
                passed=True,
                description=description,
                metadata=metadata
            )
        else:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.WARN,
                description=description,
                metadata=metadata
            )

    except Exception as e:
        logger.error(f"Error in reference data coverage check: {str(e)}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Check failed due to error: {str(e)}",
            metadata={"error": str(e)}
        )


@asset_check(
    asset=fact_certificates,
    name="certificate_compliance_monitoring",
    description="Monitor certificate approval rates and compliance patterns"
)
def check_certificate_compliance_monitoring(context: AssetCheckExecutionContext, fact_certificates) -> AssetCheckResult:
    """
    Monitor certificate compliance metrics, approval rates, and regulatory patterns.

    Tracks approval rates, processing times, and compliance trends.
    """
    logger = get_dagster_logger()

    try:
        cert_records = fact_certificates.get('records', [])
        cert_stats = fact_certificates.get('statistics', {})

        if not cert_records:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.WARN,
                description="No certificate records found",
                metadata={"record_count": 0}
            )

        total_certificates = len(cert_records)
        approved_certificates = cert_stats.get('approved_certificates', 0)
        approval_rate = (approved_certificates / total_certificates) * 100 if total_certificates > 0 else 0

        # Analyze certificate types
        cert_types = {}
        for record in cert_records:
            cert_type = record.get('certificate_type', 'Unknown')
            cert_types[cert_type] = cert_types.get(cert_type, 0) + 1

        # Check data quality for certificates
        quality_scores = [record.get('final_quality_score', 0) for record in cert_records]
        avg_quality_score = sum(quality_scores) / len(quality_scores) if quality_scores else 0

        # Check completeness of key certificate fields
        records_with_serial = sum(1 for record in cert_records if record.get('serial_number'))
        records_with_plant_registry = sum(1 for record in cert_records if record.get('plant_registry_number'))
        records_with_dates = sum(1 for record in cert_records
                               if record.get('application_date') and record.get('approval_date'))

        serial_completeness = (records_with_serial / total_certificates) * 100
        plant_completeness = (records_with_plant_registry / total_certificates) * 100
        date_completeness = (records_with_dates / total_certificates) * 100

        # Compliance thresholds
        min_approval_rate = 80.0  # Expect most certificates to be approved
        min_quality_score = 0.6
        min_field_completeness = 90.0

        issues = []

        if approval_rate < min_approval_rate:
            issues.append(f"Low approval rate: {approval_rate:.1f}%")

        if avg_quality_score < min_quality_score:
            issues.append(f"Low average quality score: {avg_quality_score:.2f}")

        if serial_completeness < min_field_completeness:
            issues.append(f"Low serial number completeness: {serial_completeness:.1f}%")

        if plant_completeness < min_field_completeness:
            issues.append(f"Low plant registry completeness: {plant_completeness:.1f}%")

        if date_completeness < min_field_completeness:
            issues.append(f"Low date completeness: {date_completeness:.1f}%")

        passed = len(issues) == 0
        severity = AssetCheckSeverity.WARN if not passed else None

        description = f"Certificate compliance: {approval_rate:.1f}% approval rate ({approved_certificates}/{total_certificates}), avg quality {avg_quality_score:.2f}"
        if issues:
            description += f" - Issues: {'; '.join(issues)}"

        return AssetCheckResult(
            passed=passed,
            severity=severity,
            description=description,
            metadata={
                "total_certificates": total_certificates,
                "approved_certificates": approved_certificates,
                "approval_rate_percent": approval_rate,
                "average_quality_score": avg_quality_score,
                "certificate_types": cert_types,
                "field_completeness": {
                    "serial_number_percent": serial_completeness,
                    "plant_registry_percent": plant_completeness,
                    "dates_percent": date_completeness
                },
                "compliance_metrics": {
                    "records_with_serial": records_with_serial,
                    "records_with_plant_registry": records_with_plant_registry,
                    "records_with_dates": records_with_dates
                },
                "issues": issues,
                "most_common_cert_type": max(cert_types.items(), key=lambda x: x[1])[0] if cert_types else "None"
            }
        )

    except Exception as e:
        logger.error(f"Error in certificate compliance monitoring check: {str(e)}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Check failed due to error: {str(e)}",
            metadata={"error": str(e)}
        )


@asset_check(
    asset=ttb_raw_data,
    name="sequence_completeness",
    description="Check that TTB sequence extraction is complete without significant gaps"
)
def check_sequence_completeness(context: AssetCheckExecutionContext, ttb_raw_data) -> AssetCheckResult:
    """
    Verify that TTB sequence extraction is complete.

    Checks for:
    - All receipt methods processed
    - No significant gaps in sequences
    - Expected volume ranges
    """
    logger = get_dagster_logger()

    try:
        # Handle different data formats
        if isinstance(ttb_raw_data, list):
            records = ttb_raw_data
            # Try to get completeness reports from context metadata
            completeness_reports = {}
        elif isinstance(ttb_raw_data, dict):
            records = ttb_raw_data.get('records', [])
            completeness_reports = ttb_raw_data.get('completeness_reports', {})
        else:
            records = []
            completeness_reports = {}

        issues = []
        warnings = []

        # Expected receipt methods
        expected_methods = {0, 1, 2, 3}
        found_methods = set()

        total_records = len(records) if isinstance(records, list) else 0
        total_gaps = 0
        total_missing = 0

        # Analyze completeness reports if available
        if completeness_reports:
            for key, report in completeness_reports.items():
                parts = key.split('_')
                if len(parts) >= 1:
                    try:
                        found_methods.add(int(parts[0]))
                    except ValueError:
                        pass

                total_gaps += report.get('gaps_detected', 0)
                total_missing += report.get('total_missing_in_gaps', 0)

                # Check completeness ratio
                ratio = report.get('completeness_ratio', 0)
                if ratio < 0.95 and ratio > 0:
                    warnings.append(f"{key}: Low completeness ratio {ratio:.1%}")

                # Check for significant gaps
                gap_details = report.get('gap_details', [])
                large_gaps = [g for g in gap_details if g.get('size', 0) > 10]
                if large_gaps:
                    issues.append(f"{key}: {len(large_gaps)} large gaps (>10 sequences)")
        else:
            # Analyze records directly if no completeness reports
            for record in records:
                receipt_method = record.get('receipt_method')
                if receipt_method is not None:
                    found_methods.add(receipt_method)

        # Check all receipt methods processed
        missing_methods = expected_methods - found_methods
        if missing_methods and len(found_methods) > 0:
            # Only warn if we have some methods but not all
            method_labels = {0: "hand-delivered", 1: "e-filed", 2: "mailed", 3: "overnight"}
            missing_labels = [method_labels.get(m, str(m)) for m in missing_methods]
            warnings.append(f"Missing receipt methods: {missing_labels}")

        # Check volume thresholds
        if total_records < 10 and total_records > 0:
            warnings.append(f"Low record count: {total_records}")

        passed = len(issues) == 0
        severity = AssetCheckSeverity.ERROR if issues else (
            AssetCheckSeverity.WARN if warnings else None
        )

        description = f"Sequence completeness: {total_records} records"
        if total_gaps > 0:
            description += f", {total_gaps} gaps ({total_missing} missing)"
        if issues:
            description += f" - ISSUES: {'; '.join(issues)}"
        elif warnings:
            description += f" - WARNINGS: {'; '.join(warnings)}"

        return AssetCheckResult(
            passed=passed,
            severity=severity,
            description=description,
            metadata={
                "total_records": total_records,
                "total_gaps": total_gaps,
                "total_missing_sequences": total_missing,
                "receipt_methods_found": list(found_methods),
                "receipt_methods_missing": list(missing_methods),
                "issues": issues,
                "warnings": warnings,
                "completeness_reports": completeness_reports
            }
        )

    except Exception as e:
        logger.error(f"Error in sequence completeness check: {e}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Check failed: {e}",
            metadata={"error": str(e)}
        )


@asset_check(
    asset=ttb_raw_data,
    name="receipt_method_coverage",
    description="Verify all TTB receipt methods are being processed"
)
def check_receipt_method_coverage(context: AssetCheckExecutionContext, ttb_raw_data) -> AssetCheckResult:
    """
    Ensure all TTB receipt methods are being processed.

    Verifies that the extraction covers:
    - 0: Hand-delivered
    - 1: E-filed (primary, should always have data)
    - 2: Mailed
    - 3: Overnight
    """
    logger = get_dagster_logger()

    try:
        # Handle different data formats
        if isinstance(ttb_raw_data, list):
            records = ttb_raw_data
        elif isinstance(ttb_raw_data, dict):
            records = ttb_raw_data.get('records', ttb_raw_data)
            if not isinstance(records, list):
                records = []
        else:
            records = []

        # Count records by receipt method
        method_counts = {0: 0, 1: 0, 2: 0, 3: 0}

        for record in records:
            method = record.get('receipt_method')
            if method in method_counts:
                method_counts[method] += 1

        # Check coverage
        missing_methods = [m for m, count in method_counts.items() if count == 0]

        method_labels = {
            0: "hand-delivered",
            1: "e-filed",
            2: "mailed",
            3: "overnight"
        }

        # E-filed (method 1) should always have data if we have any records
        total_records = sum(method_counts.values())
        passed = True
        severity = None

        description = "Receipt method coverage: " + ", ".join(
            f"{method_labels[m]}={count}" for m, count in method_counts.items()
        )

        if total_records > 0:
            if 1 in missing_methods:
                # E-filed is the primary method - should always have data
                passed = False
                severity = AssetCheckSeverity.ERROR
                description += " - ERROR: No e-filed records found!"
            elif missing_methods:
                # Other methods may legitimately have zero records on some days
                severity = AssetCheckSeverity.WARN
                description += f" - Note: No records for {[method_labels[m] for m in missing_methods]}"
        elif total_records == 0:
            passed = False
            severity = AssetCheckSeverity.ERROR
            description = "No records found for any receipt method"

        return AssetCheckResult(
            passed=passed,
            severity=severity,
            description=description,
            metadata={
                "method_counts": method_counts,
                "method_labels": method_labels,
                "missing_methods": missing_methods,
                "total_records": total_records,
                "e_filed_coverage": method_counts[1] > 0,
                "all_methods_covered": len(missing_methods) == 0
            }
        )

    except Exception as e:
        logger.error(f"Error in receipt method coverage check: {e}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Check failed: {e}",
            metadata={"error": str(e)}
        )