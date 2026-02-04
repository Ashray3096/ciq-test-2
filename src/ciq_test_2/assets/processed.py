"""
Processed Data Assets

This module contains assets for data processing and transformation stages.
These assets take raw data and convert it into structured, validated formats.
"""
import tempfile
from datetime import datetime
from typing import Dict, Any, List
from pathlib import Path

from dagster import (
    asset,
    Config,
    get_dagster_logger,
    AssetExecutionContext,
    MetadataValue,
    AssetDep
)
from .raw import ttb_raw_data
from ..utils.ttb_data_extraction import parse_ttb_html
from ..config.ttb_partitions import daily_partitions
from ..utils.ttb_transformations import apply_field_transformations, transformation_registry, load_ttb_reference_data
from ..utils.ttb_schema import (
    schema_registry,
    create_parquet_dataset,
    validate_records_against_schema
)


class TTBProcessingConfig(Config):
    """Configuration for TTB processing assets."""
    processing_batch_size: int = 100
    enable_parsing_validation: bool = True
    data_quality_analysis: bool = True


class TTBCleaningConfig(Config):
    """Configuration for TTB cleaning assets."""
    s3_bucket: str = "ciq-dagster"
    s3_output_prefix: str = "ttb-pre-prod/ttb_processed_data"
    enable_deduplication: bool = True
    data_quality_analysis: bool = True
    create_partitioned_output: bool = True


class TTBStructuringConfig(Config):
    """Configuration for TTB structuring assets."""
    s3_bucket: str = "ciq-dagster"
    s3_output_prefix: str = "ttb-pre-prod/ttb_processed_data"
    create_partitioned_output: bool = True
    validate_schema: bool = True
    data_quality_analysis: bool = True


def merge_ttb_records(certificate_record: Dict[str, Any], cola_detail_record: Dict[str, Any], ttb_id: str) -> Dict[str, Any]:
    """
    Merge certificate and cola-detail records at the field level.

    Args:
        certificate_record: Cleaned certificate data (or None)
        cola_detail_record: Cleaned cola-detail data (or None)
        ttb_id: TTB ID for the merged record

    Returns:
        Merged record with field-level consolidation
    """
    # Start with the record that has data, or create empty structure
    if cola_detail_record:
        merged = cola_detail_record.copy()
    elif certificate_record:
        merged = certificate_record.copy()
    else:
        merged = {'ttb_id': ttb_id}

    # Set data source indicators
    merged['has_cola_detail'] = cola_detail_record is not None
    merged['has_certificate'] = certificate_record is not None

    # Merge certificate data if available
    if certificate_record and cola_detail_record:
        # Add certificate-specific fields with 'cert_' prefix
        for key, value in certificate_record.items():
            if key not in ['ttb_id', 'data_type', 'cleaning_timestamp', 'cleaning_version']:
                cert_key = f"cert_{key}" if not key.startswith('cert_') else key
                merged[cert_key] = value

        # Merge common applicant info (prefer cola-detail, fallback to certificate)
        applicant_fields = ['applicant_business_name', 'applicant_mailing_address',
                           'applicant_phone', 'applicant_email', 'applicant_fax']
        for field in applicant_fields:
            if not merged.get(field) and certificate_record.get(field):
                merged[field] = certificate_record[field]

    elif certificate_record:
        # Only certificate data available - add cert_ prefixes where appropriate
        for key, value in certificate_record.items():
            if key in ['certificate_number', 'certificate_status', 'certificate_type',
                      'application_date', 'approval_date', 'plant_registry_number']:
                merged[f"cert_{key}"] = value
            else:
                merged[key] = value

    # Add computed fields
    merged.update(calculate_merged_fields(merged))

    return merged


def calculate_merged_fields(record: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate computed fields for merged records."""
    computed = {}

    # Data completeness score
    important_fields = [
        'ttb_id', 'applicant_business_name', 'applicant_mailing_address'
    ]
    if record.get('has_cola_detail'):
        important_fields.extend(['brand_name', 'product_description', 'approval_date'])

    filled_fields = sum(1 for field in important_fields
                       if record.get(field) is not None and record.get(field) != '')
    computed['data_completeness_score'] = filled_fields / len(important_fields)

    # Calculate days to approval (for cola-detail records)
    if record.get('has_cola_detail'):
        filing_date = record.get('filing_date')
        approval_date = record.get('approval_date')
        if filing_date and approval_date:
            try:
                if isinstance(filing_date, str):
                    filing_date = datetime.fromisoformat(filing_date.replace('Z', '+00:00')).date()
                if isinstance(approval_date, str):
                    approval_date = datetime.fromisoformat(approval_date.replace('Z', '+00:00')).date()
                computed['days_to_approval'] = (approval_date - filing_date).days
            except (ValueError, TypeError):
                pass

    # Product classification
    if record.get('product_description'):
        product_desc = str(record['product_description']).lower()
        if any(word in product_desc for word in ['wine', 'vintner', 'winery']):
            computed['product_category'] = 'WINE'
        elif any(word in product_desc for word in ['beer', 'ale', 'lager']):
            computed['product_category'] = 'BEER'
        elif any(word in product_desc for word in ['spirit', 'whiskey', 'vodka']):
            computed['product_category'] = 'SPIRITS'
        else:
            computed['product_category'] = 'OTHER'

    return computed


def apply_final_schema_structure(record: Dict[str, Any]) -> Dict[str, Any]:
    """Apply final schema standardization to a cleaned record."""
    structured = record.copy()

    # Standardize date fields to ISO format
    date_fields = ['filing_date', 'approval_date', 'expiration_date', 'cert_application_date', 'cert_approval_date']
    for field in date_fields:
        if field in structured and structured[field]:
            try:
                # Convert date objects to ISO strings
                if hasattr(structured[field], 'isoformat'):
                    structured[field] = structured[field].isoformat()
                elif isinstance(structured[field], str):
                    # Ensure consistent ISO format
                    from datetime import datetime
                    try:
                        dt = datetime.fromisoformat(structured[field].replace('Z', '+00:00'))
                        structured[field] = dt.date().isoformat()
                    except ValueError:
                        pass  # Keep original if parsing fails
            except Exception:
                pass  # Keep original value if conversion fails

    # Standardize boolean fields
    boolean_fields = ['has_cola_detail', 'has_certificate', 'cert_applicant_signature_present', 'cert_authorized_signature_present']
    for field in boolean_fields:
        if field in structured:
            structured[field] = bool(structured[field])

    # Standardize numeric fields
    numeric_fields = ['data_completeness_score', 'final_quality_score', 'days_to_approval']
    for field in numeric_fields:
        if field in structured and structured[field] is not None:
            try:
                structured[field] = float(structured[field])
            except (ValueError, TypeError):
                structured[field] = 0.0

    # Ensure TTB ID is string
    if 'ttb_id' in structured:
        structured['ttb_id'] = str(structured['ttb_id'])

    # Standardize text fields (trim whitespace)
    text_fields = ['applicant_business_name', 'applicant_mailing_address', 'brand_name',
                  'fanciful_name', 'product_description', 'cert_certificate_status', 'cert_certificate_type']
    for field in text_fields:
        if field in structured and isinstance(structured[field], str):
            structured[field] = structured[field].strip()

    return structured


def calculate_final_quality_score(record: Dict[str, Any]) -> float:
    """Calculate final data quality score for a structured record."""
    score_components = []

    # Base completeness (from earlier calculation)
    base_completeness = record.get('data_completeness_score', 0.0)
    score_components.append(base_completeness * 0.4)  # 40% weight

    # Data source diversity bonus
    source_bonus = 0.0
    if record.get('has_cola_detail') and record.get('has_certificate'):
        source_bonus = 0.3  # Both sources available
    elif record.get('has_cola_detail') or record.get('has_certificate'):
        source_bonus = 0.15  # One source available
    score_components.append(source_bonus)

    # Field transformation success
    transformation_fields = [k for k in record.keys() if k.endswith('_transformed') or k.endswith('_validated')]
    if transformation_fields:
        transformation_score = min(len(transformation_fields) / 10, 0.2)  # Cap at 20%
        score_components.append(transformation_score)

    # Critical field presence
    critical_fields = ['ttb_id', 'applicant_business_name']
    critical_present = sum(1 for field in critical_fields if record.get(field))
    critical_score = (critical_present / len(critical_fields)) * 0.1  # 10% weight
    score_components.append(critical_score)

    return sum(score_components)


def validate_final_schema(record: Dict[str, Any]) -> Dict[str, Any]:
    """Validate record against final schema requirements."""
    validation_result = {
        'is_valid': True,
        'errors': [],
        'warnings': []
    }

    # Required fields validation
    required_fields = ['ttb_id']
    for field in required_fields:
        if not record.get(field):
            validation_result['errors'].append(f"Missing required field: {field}")
            validation_result['is_valid'] = False

    # TTB ID format validation
    ttb_id = record.get('ttb_id')
    if ttb_id and not str(ttb_id).isdigit():
        validation_result['warnings'].append(f"TTB ID format may be invalid: {ttb_id}")

    # Data type consistency
    if record.get('has_cola_detail') and not any(field in record for field in ['brand_name', 'product_description']):
        validation_result['warnings'].append("Record claims to have cola detail but missing typical cola fields")

    if record.get('has_certificate') and not any(field.startswith('cert_') for field in record.keys()):
        validation_result['warnings'].append("Record claims to have certificate but missing cert_ prefixed fields")

    # Date field validation
    date_fields = ['filing_date', 'approval_date', 'expiration_date']
    for field in date_fields:
        if field in record and record[field]:
            try:
                from datetime import datetime
                if isinstance(record[field], str):
                    datetime.fromisoformat(record[field])
            except ValueError:
                validation_result['warnings'].append(f"Invalid date format in {field}: {record[field]}")

    return validation_result


def get_field_transformation_rules(data_type: str) -> Dict[str, List[Dict[str, Any]]]:
    """Get field transformation rules for a specific data type."""
    if data_type == "cola-detail":
        return {
            'filing_date': [
                {'transform': 'clean_text'},
                {'transform': 'parse_date'}
            ],
            'approval_date': [
                {'transform': 'clean_text'},
                {'transform': 'parse_date'}
            ],
            'expiration_date': [
                {'transform': 'clean_text'},
                {'transform': 'parse_date'}
            ],
            'brand_name': [
                {'transform': 'clean_text'}
            ],
            'fanciful_name': [
                {'transform': 'clean_text'}
            ],
            'product_description': [
                {'transform': 'clean_text'}
            ],
            'applicant_business_name': [
                {'transform': 'clean_text'}
            ],
            'applicant_mailing_address': [
                {'transform': 'clean_text'},
                {'transform': 'normalize_address'}
            ],
            'applicant_phone': [
                {'transform': 'clean_phone'}
            ],
            'applicant_email': [
                {'transform': 'clean_text'}
            ],
            'alcohol_content': [
                {'transform': 'extract_alcohol_content'}
            ],
            'net_contents': [
                {'transform': 'standardize_container_size'}
            ]
        }
    elif data_type == "certificate":
        return {
            'application_date': [
                {'transform': 'clean_text'},
                {'transform': 'parse_date'}
            ],
            'approval_date': [
                {'transform': 'clean_text'},
                {'transform': 'parse_date'}
            ],
            'certificate_status': [
                {'transform': 'clean_text'}
            ],
            'certificate_type': [
                {'transform': 'clean_text'}
            ],
            'applicant_business_name': [
                {'transform': 'clean_text'}
            ],
            'applicant_mailing_address': [
                {'transform': 'clean_text'},
                {'transform': 'normalize_address'}
            ],
            'applicant_phone': [
                {'transform': 'clean_phone'}
            ],
            'applicant_email': [
                {'transform': 'clean_text'}
            ]
        }
    else:
        return {}


@asset(
    partitions_def=daily_partitions,
    group_name="ttb_processing",
    description="Extracted structured data from raw TTB HTML files",
    deps=[AssetDep(ttb_raw_data)],
    metadata={
        "data_type": "extracted",
        "stage": "processing",
        "format": "pickle"
    }
)
def ttb_extracted_data(
    context: AssetExecutionContext,
    config: TTBProcessingConfig,
    ttb_raw_data: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Extract structured data from raw TTB HTML data.

    This asset processes raw HTML data from the pickled ttb_raw_data and extracts
    structured fields using parsing logic.

    Args:
        ttb_raw_data: List of dictionaries containing TTB records with HTML content

    Returns:
        Dictionary containing extracted structured data
    """
    logger = get_dagster_logger()

    # Get partition information (now just a date string)
    date_str = context.partition_key

    logger.info(f"Processing TTB extraction for {date_str}")
    logger.info(f"Received {len(ttb_raw_data)} raw records to process")

    extracted_records = []
    processing_stats = {
        "total_records": len(ttb_raw_data),
        "successful_extractions": 0,
        "failed_extractions": 0,
        "total_fields_extracted": 0
    }

    try:
        for raw_record in ttb_raw_data:
            try:
                # Extract HTML content and metadata from raw record
                html_content = raw_record.get('html_content', '')
                ttb_id = raw_record.get('ttb_id', '')
                data_type = raw_record.get('data_type', '')

                if not html_content:
                    logger.warning(f"No HTML content found for TTB ID {ttb_id}")
                    processing_stats["failed_extractions"] += 1
                    continue

                if not data_type:
                    logger.warning(f"No data type found for TTB ID {ttb_id}")
                    processing_stats["failed_extractions"] += 1
                    continue

                # Parse HTML content
                extracted_data = parse_ttb_html(html_content, data_type)

                if extracted_data:
                    # Add metadata from raw record and processing context
                    extracted_data['ttb_id'] = ttb_id
                    extracted_data['sequence'] = raw_record.get('sequence', None)
                    extracted_data['extraction_timestamp'] = datetime.now().isoformat()
                    extracted_data['partition_date'] = date_str
                    extracted_data['receipt_method'] = raw_record.get('receipt_method', None)
                    extracted_data['data_type'] = raw_record.get('data_type', None)
                    extracted_data['original_url'] = raw_record.get('url', '')
                    extracted_data['original_size_bytes'] = raw_record.get('size_bytes', 0)
                    extracted_data['raw_extraction_timestamp'] = raw_record.get('extraction_timestamp', '')

                    extracted_records.append(extracted_data)
                    processing_stats["successful_extractions"] += 1
                    processing_stats["total_fields_extracted"] += len(extracted_data)

                    logger.debug(f"Successfully extracted data from TTB ID {ttb_id}")
                else:
                    logger.warning(f"No data extracted from TTB ID {ttb_id}")
                    processing_stats["failed_extractions"] += 1

            except Exception as e:
                logger.error(f"Error processing TTB ID {ttb_id}: {e}")
                processing_stats["failed_extractions"] += 1

        # Calculate success rate
        success_rate = (
            processing_stats["successful_extractions"] / max(processing_stats["total_records"], 1)
        )

        logger.info(f"Extraction complete: {processing_stats['successful_extractions']}/{processing_stats['total_records']} records processed successfully")
        logger.info(f"Success rate: {success_rate:.2%}")

        # Count by data type for metadata
        cert_count = len([r for r in extracted_records if r.get('data_type') == 'certificate'])
        cola_count = len([r for r in extracted_records if r.get('data_type') == 'cola-detail'])

        # Add metadata
        context.add_output_metadata({
            "total_records": MetadataValue.int(processing_stats["total_records"]),
            "successful_extractions": MetadataValue.int(processing_stats["successful_extractions"]),
            "certificate_extractions": MetadataValue.int(cert_count),
            "cola_detail_extractions": MetadataValue.int(cola_count),
            "failed_extractions": MetadataValue.int(processing_stats["failed_extractions"]),
            "success_rate": MetadataValue.float(success_rate),
            "total_fields_extracted": MetadataValue.int(processing_stats["total_fields_extracted"]),
            "partition_date": MetadataValue.text(date_str)
        })

        return {
            "partition_date": date_str,
            "extracted_records": extracted_records,
            "processing_stats": processing_stats
        }

    except Exception as e:
        logger.error(f"Critical error in extraction: {e}")
        raise


@asset(
    partitions_def=daily_partitions,
    group_name="ttb_processing",
    description="Cleaned and validated TTB data with transformations applied",
    deps=[AssetDep(ttb_extracted_data)],
    metadata={
        "data_type": "cleaned",
        "stage": "processing",
        "format": "pickle"
    }
)
def ttb_cleaned_data(
    context: AssetExecutionContext,
    config: TTBCleaningConfig,
    ttb_extracted_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Clean and validate extracted TTB data.

    This asset applies data cleaning transformations, deduplication,
    and validation to the extracted data.

    Args:
        ttb_extracted_data: Dictionary containing extracted structured data

    Returns:
        Dictionary containing cleaned data and statistics
    """
    logger = get_dagster_logger()

    # Get partition information (now just a date string)
    date_str = context.partition_key

    logger.info(f"Cleaning TTB data for {date_str}")

    # Load extracted records
    extracted_records = ttb_extracted_data.get("extracted_records", [])
    logger.info(f"Processing {len(extracted_records)} extracted records")

    cleaning_stats = {
        "records_processed": len(extracted_records),
        "records_cleaned": 0,
        "duplicates_removed": 0,
        "validation_errors": 0,
        "certificate_records": 0,
        "cola_detail_records": 0,
        "field_transformations": 0
    }

    cleaned_records = []

    try:
        # Load reference data for transformations
        reference_data = load_ttb_reference_data()

        # Track records by TTB ID for field-level merging
        ttb_id_records = {}

        # First pass: collect and transform all records by TTB ID
        for record in extracted_records:
            try:
                ttb_id = record.get('ttb_id')
                data_type = record.get('data_type')

                if not ttb_id or not data_type:
                    cleaning_stats["validation_errors"] += 1
                    logger.warning(f"Missing TTB ID or data_type: {ttb_id}, {data_type}")
                    continue

                # Apply field transformations based on data type
                field_rules = get_field_transformation_rules(data_type)
                cleaned_record = apply_field_transformations(record, field_rules)

                # Count transformations applied
                cleaning_stats["field_transformations"] += len([
                    k for k in cleaned_record.keys()
                    if k.endswith('_transformed') or k.endswith('_validated')
                ])

                # Add cleaning metadata
                cleaned_record['cleaning_timestamp'] = datetime.now().isoformat()
                cleaned_record['cleaning_version'] = '1.0'

                # Group by TTB ID
                if ttb_id not in ttb_id_records:
                    ttb_id_records[ttb_id] = {
                        'certificate': None,
                        'cola-detail': None
                    }

                ttb_id_records[ttb_id][data_type] = cleaned_record

            except Exception as e:
                cleaning_stats["validation_errors"] += 1
                logger.error(f"Error cleaning record {record.get('ttb_id', 'unknown')}: {e}")

        # Second pass: merge records by TTB ID at field level
        for ttb_id, record_pair in ttb_id_records.items():
            try:
                certificate_record = record_pair['certificate']
                cola_detail_record = record_pair['cola-detail']

                # Create merged record using field-level merging
                merged_record = merge_ttb_records(certificate_record, cola_detail_record, ttb_id)

                # Skip schema validation for now to avoid errors
                # TODO: Implement proper schema validation
                if config.data_quality_analysis:
                    # Simple data quality check - ensure required fields exist
                    required_fields = ['ttb_id']
                    has_required = all(merged_record.get(field) for field in required_fields)
                    if not has_required:
                        cleaning_stats["validation_errors"] += 1
                        logger.warning(f"Missing required fields for merged TTB ID {ttb_id}")
                    else:
                        # Add simple validation metadata
                        merged_record['data_quality_validation'] = {
                            'has_required_fields': True,
                            'validation_timestamp': datetime.now().isoformat(),
                            'has_certificate_data': certificate_record is not None,
                            'has_cola_detail_data': cola_detail_record is not None
                        }

                cleaned_records.append(merged_record)
                cleaning_stats["records_cleaned"] += 1

                # Count duplicates found
                if certificate_record and cola_detail_record:
                    cleaning_stats["duplicates_removed"] += 1  # One duplicate merged

                # Count by data source
                if certificate_record:
                    cleaning_stats["certificate_records"] += 1
                if cola_detail_record:
                    cleaning_stats["cola_detail_records"] += 1

            except Exception as e:
                cleaning_stats["validation_errors"] += 1
                logger.error(f"Error merging record {ttb_id}: {e}")

        # Calculate success rate
        success_rate = (
            cleaning_stats["records_cleaned"] / max(cleaning_stats["records_processed"], 1)
        )

        logger.info(f"Cleaning complete: {cleaning_stats['records_cleaned']}/{cleaning_stats['records_processed']} records cleaned successfully")
        logger.info(f"Success rate: {success_rate:.2%}")
        logger.info(f"Duplicates removed: {cleaning_stats['duplicates_removed']}")
        logger.info(f"Field transformations applied: {cleaning_stats['field_transformations']}")

        # Add metadata
        context.add_output_metadata({
            "records_processed": MetadataValue.int(cleaning_stats["records_processed"]),
            "records_cleaned": MetadataValue.int(cleaning_stats["records_cleaned"]),
            "certificate_records": MetadataValue.int(cleaning_stats["certificate_records"]),
            "cola_detail_records": MetadataValue.int(cleaning_stats["cola_detail_records"]),
            "duplicates_removed": MetadataValue.int(cleaning_stats["duplicates_removed"]),
            "validation_errors": MetadataValue.int(cleaning_stats["validation_errors"]),
            "field_transformations": MetadataValue.int(cleaning_stats["field_transformations"]),
            "success_rate": MetadataValue.float(success_rate),
            "partition_date": MetadataValue.text(date_str)
        })

        return {
            "partition_date": date_str,
            "cleaned_records": cleaned_records,
            "cleaning_stats": cleaning_stats
        }

    except Exception as e:
        logger.error(f"Critical error in cleaning: {e}")
        raise


@asset(
    partitions_def=daily_partitions,
    group_name="ttb_processing",
    description="Structured TTB data in final schema format",
    deps=[AssetDep(ttb_cleaned_data)],
    metadata={
        "data_type": "structured",
        "stage": "processing",
        "format": "pickle"
    }
)
def ttb_structured_data(
    context: AssetExecutionContext,
    config: TTBStructuringConfig,
    ttb_cleaned_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Structure cleaned TTB data into final schema format.

    This asset applies final schema transformations and validates
    the data against the target schema.

    Args:
        ttb_cleaned_data: Dictionary containing cleaned and merged data

    Returns:
        Dictionary containing structured data and statistics
    """
    logger = get_dagster_logger()

    # Get partition information (now just a date string)
    date_str = context.partition_key

    logger.info(f"Structuring TTB data for {date_str}")

    # Load cleaned records
    cleaned_records = ttb_cleaned_data.get("cleaned_records", [])
    logger.info(f"Processing {len(cleaned_records)} cleaned records")

    structuring_stats = {
        "records_processed": len(cleaned_records),
        "records_structured": 0,
        "schema_violations": 0,
        "data_quality_score": 0.0,
        "field_standardizations": 0,
        "validation_passes": 0
    }

    structured_records = []

    try:
        for record in cleaned_records:
            try:
                # Apply final schema standardization
                structured_record = apply_final_schema_structure(record)

                # Calculate data quality metrics
                quality_score = calculate_final_quality_score(structured_record)
                structured_record['final_quality_score'] = quality_score

                # Add structuring metadata
                structured_record['structuring_timestamp'] = datetime.now().isoformat()
                structured_record['structuring_version'] = '1.0'

                # Basic schema validation
                if config.validate_schema:
                    validation_result = validate_final_schema(structured_record)
                    if validation_result['is_valid']:
                        structuring_stats["validation_passes"] += 1
                    else:
                        structuring_stats["schema_violations"] += 1
                        logger.warning(f"Schema violation for TTB ID {structured_record.get('ttb_id')}: {validation_result.get('errors', [])}")

                    structured_record['schema_validation'] = validation_result

                structured_records.append(structured_record)
                structuring_stats["records_structured"] += 1

            except Exception as e:
                structuring_stats["schema_violations"] += 1
                logger.error(f"Error structuring record {record.get('ttb_id', 'unknown')}: {e}")

        # Calculate overall data quality score
        if structured_records:
            total_quality = sum(r.get('final_quality_score', 0) for r in structured_records)
            structuring_stats["data_quality_score"] = total_quality / len(structured_records)

        # Calculate success rate
        success_rate = (
            structuring_stats["records_structured"] / max(structuring_stats["records_processed"], 1)
        )

        logger.info(f"Structuring complete: {structuring_stats['records_structured']}/{structuring_stats['records_processed']} records structured successfully")
        logger.info(f"Success rate: {success_rate:.2%}")
        logger.info(f"Average data quality score: {structuring_stats['data_quality_score']:.2f}")
        logger.info(f"Schema violations: {structuring_stats['schema_violations']}")

        # Add metadata
        context.add_output_metadata({
            "records_processed": MetadataValue.int(structuring_stats["records_processed"]),
            "records_structured": MetadataValue.int(structuring_stats["records_structured"]),
            "schema_violations": MetadataValue.int(structuring_stats["schema_violations"]),
            "validation_passes": MetadataValue.int(structuring_stats["validation_passes"]),
            "data_quality_score": MetadataValue.float(structuring_stats["data_quality_score"]),
            "success_rate": MetadataValue.float(success_rate),
            "partition_date": MetadataValue.text(date_str)
        })

        return {
            "partition_date": date_str,
            "structured_records": structured_records,
            "structuring_stats": structuring_stats
        }

    except Exception as e:
        logger.error(f"Critical error in structuring: {e}")
        raise