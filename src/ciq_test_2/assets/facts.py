"""
Fact Table Assets

This module creates fact tables for the TTB star schema.
Fact tables contain foreign keys to dimensions and measurable business metrics.
"""
import hashlib
from datetime import datetime, date
from typing import Dict, Any, List, Optional

from dagster import (
    asset,
    Config,
    get_dagster_logger,
    AssetExecutionContext,
    MetadataValue,
    AssetDep
)

from ..config.ttb_partitions import daily_partitions
from .processed import ttb_structured_data
from .dimensional import dim_companies, dim_products, dim_dates, _infer_product_category, _resolve_business_name


class FactConfig(Config):
    """Configuration for fact table assets."""
    enable_data_quality_metrics: bool = True
    calculate_business_metrics: bool = True


@asset(
    partitions_def=daily_partitions,
    group_name="ttb_facts",
    description="Product fact table with foreign keys to dimensions",
    deps=[AssetDep(ttb_structured_data), AssetDep(dim_companies), AssetDep(dim_products)],
    metadata={
        "data_type": "fact",
        "schema": "star",
        "format": "pickle"
    }
)
def fact_products(
    context: AssetExecutionContext,
    config: FactConfig,
    ttb_structured_data: Dict[str, Any],
    dim_companies: Dict[str, Any],
    dim_products: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Create product fact table with foreign keys to dimensions.

    Links structured TTB data to company and product dimensions
    for star schema analytics.

    Args:
        ttb_structured_data: Structured TTB data
        dim_companies: Company dimension data
        dim_products: Product dimension data

    Returns:
        Dictionary containing fact table data
    """
    logger = get_dagster_logger()

    date_str = context.partition_key
    logger.info(f"Creating product fact table for {date_str}")

    # Get structured records
    structured_records = ttb_structured_data.get('structured_records', [])

    # Create lookup dictionaries for foreign keys
    company_lookup = _create_company_lookup(dim_companies.get('records', []))
    product_lookup = _create_product_lookup(dim_products.get('records', []))

    fact_records = []
    stats = {
        'total_records': len(structured_records),
        'fact_records_created': 0,
        'missing_company_keys': 0,
        'missing_product_keys': 0,
        'quality_scores': []
    }

    for record in structured_records:
        # Only process records with COLA detail data for products
        if not record.get('has_cola_detail'):
            continue

        try:
            # Get foreign keys
            company_id = _get_company_foreign_key(record, company_lookup)
            product_id = _get_product_foreign_key(record, product_lookup)

            # Create date keys (fall back to cert_* fields when top-level is None)
            filing_date_id = _create_date_id(record.get('filing_date') or record.get('cert_application_date'))
            approval_date_id = _create_date_id(record.get('approval_date'))
            expiration_date_id = _create_date_id(record.get('expiration_date'))

            # Track missing keys
            if not company_id:
                stats['missing_company_keys'] += 1
            if not product_id:
                stats['missing_product_keys'] += 1

            # Create fact record
            fact_record = {
                # Primary key
                'product_fact_id': _create_product_fact_id(record.get('ttb_id')),
                'ttb_id': record.get('ttb_id'),

                # Foreign keys to dimensions
                'company_id': company_id,
                'product_id': product_id,
                'filing_date_id': filing_date_id,
                'approval_date_id': approval_date_id,
                'expiration_date_id': expiration_date_id,

                # Measures and metrics
                'final_quality_score': record.get('final_quality_score', 0.0),
                'data_completeness_score': record.get('data_completeness_score', 0.0),
                'days_to_approval': record.get('days_to_approval'),
                'has_certificate_data': record.get('has_certificate', False),
                'has_cola_detail_data': record.get('has_cola_detail', False),

                # Business attributes
                'class_type_code': record.get('class_type_code', ''),
                'origin_code': record.get('origin_code', ''),
                'product_category': record.get('product_category') or _infer_product_category(record.get('class_type_code', '')),
                'status': record.get('status', ''),
                'serial_number': record.get('serial_number'),
                'vendor_code': record.get('vendor_code'),

                # Dates (fall back to cert_* fields when top-level is None)
                'filing_date': record.get('filing_date') or record.get('cert_application_date'),
                'approval_date': record.get('approval_date'),
                'expiration_date': record.get('expiration_date'),

                # Technical metadata
                'partition_date': date_str,
                'fact_creation_timestamp': datetime.now().isoformat(),
                'source_extraction_timestamp': record.get('extraction_timestamp'),
                'source_cleaning_timestamp': record.get('cleaning_timestamp'),
                'source_structuring_timestamp': record.get('structuring_timestamp')
            }

            fact_records.append(fact_record)
            stats['fact_records_created'] += 1
            stats['quality_scores'].append(record.get('final_quality_score', 0.0))

        except Exception as e:
            logger.error(f"Error creating fact record for TTB ID {record.get('ttb_id')}: {e}")

    # Calculate statistics
    avg_quality_score = sum(stats['quality_scores']) / len(stats['quality_scores']) if stats['quality_scores'] else 0.0

    logger.info(f"Created {stats['fact_records_created']} product fact records")
    logger.info(f"Missing foreign keys: {stats['missing_company_keys']} companies, {stats['missing_product_keys']} products")
    logger.info(f"Average quality score: {avg_quality_score:.2f}")

    # Add metadata
    context.add_output_metadata({
        "fact_records_created": MetadataValue.int(stats['fact_records_created']),
        "source_records": MetadataValue.int(stats['total_records']),
        "missing_company_keys": MetadataValue.int(stats['missing_company_keys']),
        "missing_product_keys": MetadataValue.int(stats['missing_product_keys']),
        "average_quality_score": MetadataValue.float(avg_quality_score),
        "partition_date": MetadataValue.text(date_str)
    })

    return {
        'fact_name': 'products',
        'records': fact_records,
        'primary_key': 'product_fact_id',
        'foreign_keys': ['company_id', 'product_id', 'filing_date_id', 'approval_date_id', 'expiration_date_id'],
        'record_count': len(fact_records),
        'partition_date': date_str,
        'statistics': stats,
        'generation_timestamp': datetime.now().isoformat()
    }


@asset(
    partitions_def=daily_partitions,
    group_name="ttb_facts",
    description="Certificate fact table with regulatory and compliance metrics",
    deps=[AssetDep(ttb_structured_data), AssetDep(dim_companies), AssetDep(dim_products)],
    metadata={
        "data_type": "fact",
        "schema": "star",
        "format": "pickle"
    }
)
def fact_certificates(
    context: AssetExecutionContext,
    config: FactConfig,
    ttb_structured_data: Dict[str, Any],
    dim_companies: Dict[str, Any],
    dim_products: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Create certificate fact table for regulatory compliance analytics.

    Links certificate data to company and product dimensions for compliance tracking.

    Args:
        ttb_structured_data: Structured TTB data
        dim_companies: Company dimension data
        dim_products: Product dimension data

    Returns:
        Dictionary containing certificate fact table data
    """
    logger = get_dagster_logger()

    date_str = context.partition_key
    logger.info(f"Creating certificate fact table for {date_str}")

    # Get structured records
    structured_records = ttb_structured_data.get('structured_records', [])

    # Create lookup dictionaries for foreign keys
    company_lookup = _create_company_lookup(dim_companies.get('records', []))
    product_lookup = _create_product_lookup(dim_products.get('records', []))

    fact_records = []
    stats = {
        'total_records': len(structured_records),
        'fact_records_created': 0,
        'missing_company_keys': 0,
        'missing_product_keys': 0,
        'approved_certificates': 0,
        'quality_scores': []
    }

    for record in structured_records:
        # Only process records with certificate data
        if not record.get('has_certificate'):
            continue

        try:
            # Get foreign keys
            company_id = _get_company_foreign_key(record, company_lookup)
            product_id = _get_product_foreign_key(record, product_lookup)

            # Create date keys
            application_date_id = _create_date_id(record.get('cert_application_date'))
            approval_date_id = _create_date_id(record.get('cert_approval_date'))

            # Track missing keys
            if not company_id:
                stats['missing_company_keys'] += 1
            if not product_id:
                stats['missing_product_keys'] += 1

            # Check if approved
            cert_status = record.get('cert_status', '').upper()
            is_approved = 'APPROVED' in cert_status
            if is_approved:
                stats['approved_certificates'] += 1

            # Create certificate fact record
            fact_record = {
                # Primary key
                'certificate_fact_id': _create_certificate_fact_id(record.get('ttb_id')),
                'ttb_id': record.get('ttb_id'),

                # Foreign keys to dimensions
                'company_id': company_id,
                'product_id': product_id,
                'application_date_id': application_date_id,
                'approval_date_id': approval_date_id,

                # Certificate measures and metrics
                'final_quality_score': record.get('final_quality_score', 0.0),
                'data_completeness_score': record.get('data_completeness_score', 0.0),
                'is_approved': is_approved,
                'has_cola_detail_data': record.get('has_cola_detail', False),

                # Certificate attributes
                'certificate_status': record.get('cert_status', ''),
                'certificate_type': record.get('cert_certificate_type', ''),
                'serial_number': record.get('cert_serial_number'),
                'plant_registry_number': record.get('cert_plant_registry_number', ''),

                # Receipt method (0=hand-delivered, 1=e-filed, 2=mailed, 3=overnight)
                'receipt_method': record.get('receipt_method'),

                # Dates
                'application_date': record.get('cert_application_date'),
                'approval_date': record.get('cert_approval_date'),

                # Technical metadata
                'partition_date': date_str,
                'fact_creation_timestamp': datetime.now().isoformat(),
                'source_extraction_timestamp': record.get('extraction_timestamp'),
                'source_cleaning_timestamp': record.get('cleaning_timestamp'),
                'source_structuring_timestamp': record.get('structuring_timestamp')
            }

            fact_records.append(fact_record)
            stats['fact_records_created'] += 1
            stats['quality_scores'].append(record.get('final_quality_score', 0.0))

        except Exception as e:
            logger.error(f"Error creating certificate fact record for TTB ID {record.get('ttb_id')}: {e}")

    # Calculate statistics
    avg_quality_score = sum(stats['quality_scores']) / len(stats['quality_scores']) if stats['quality_scores'] else 0.0
    approval_rate = stats['approved_certificates'] / stats['fact_records_created'] if stats['fact_records_created'] else 0.0

    logger.info(f"Created {stats['fact_records_created']} certificate fact records")
    logger.info(f"Approval rate: {approval_rate:.2%}")
    logger.info(f"Missing foreign keys: {stats['missing_company_keys']} companies, {stats['missing_product_keys']} products")

    # Add metadata
    context.add_output_metadata({
        "fact_records_created": MetadataValue.int(stats['fact_records_created']),
        "source_records": MetadataValue.int(stats['total_records']),
        "approved_certificates": MetadataValue.int(stats['approved_certificates']),
        "approval_rate": MetadataValue.float(approval_rate),
        "missing_company_keys": MetadataValue.int(stats['missing_company_keys']),
        "missing_product_keys": MetadataValue.int(stats['missing_product_keys']),
        "average_quality_score": MetadataValue.float(avg_quality_score),
        "partition_date": MetadataValue.text(date_str)
    })

    return {
        'fact_name': 'certificates',
        'records': fact_records,
        'primary_key': 'certificate_fact_id',
        'foreign_keys': ['company_id', 'product_id', 'application_date_id', 'approval_date_id'],
        'record_count': len(fact_records),
        'partition_date': date_str,
        'statistics': stats,
        'generation_timestamp': datetime.now().isoformat()
    }


# Helper functions

def _create_company_lookup(company_records: List[Dict[str, Any]]) -> Dict[str, int]:
    """Create lookup dictionary for company foreign keys."""
    lookup = {}
    for company in company_records:
        business_name = company.get('business_name', '').strip().upper()
        mailing_address = company.get('mailing_address', '').strip().upper()
        key = f"{business_name}|{mailing_address}"
        lookup[key] = company.get('company_id')
    return lookup


def _create_product_lookup(product_records: List[Dict[str, Any]]) -> Dict[str, int]:
    """Create lookup dictionary for product foreign keys."""
    lookup = {}
    for product in product_records:
        brand_name = product.get('brand_name', '').strip().upper()
        fanciful_name = product.get('fanciful_name', '').strip().upper()
        key = f"{brand_name}|{fanciful_name}"
        lookup[key] = product.get('product_id')
    return lookup


def _get_company_foreign_key(record: Dict[str, Any], company_lookup: Dict[str, int]) -> Optional[int]:
    """Get company foreign key for a record."""
    business_name = _resolve_business_name(record)
    business_name = (business_name or '').strip().upper()
    mailing_address = record.get('applicant_mailing_address', '').strip().upper()
    key = f"{business_name}|{mailing_address}"
    return company_lookup.get(key)


def _get_product_foreign_key(record: Dict[str, Any], product_lookup: Dict[str, int]) -> Optional[int]:
    """Get product foreign key for a record."""
    brand_name = record.get('brand_name', '').strip().upper()
    fanciful_name = record.get('fanciful_name', '').strip().upper()
    key = f"{brand_name}|{fanciful_name}"
    return product_lookup.get(key)


def _create_date_id(date_val) -> Optional[int]:
    """Convert date to date_id format (YYYYMMDD)."""
    if not date_val:
        return None

    try:
        if isinstance(date_val, str):
            # Parse ISO date string
            dt = datetime.fromisoformat(date_val.replace('Z', '+00:00'))
            return int(dt.strftime('%Y%m%d'))
        elif isinstance(date_val, datetime):
            return int(date_val.strftime('%Y%m%d'))
        elif isinstance(date_val, date):
            return int(date_val.strftime('%Y%m%d'))
    except Exception:
        pass

    return None


def _create_product_fact_id(ttb_id: str) -> int:
    """Create unique product fact ID."""
    key_string = f"product_{ttb_id}"
    return int(hashlib.md5(key_string.encode()).hexdigest()[:8], 16)


def _create_certificate_fact_id(ttb_id: str) -> int:
    """Create unique certificate fact ID."""
    key_string = f"certificate_{ttb_id}"
    return int(hashlib.md5(key_string.encode()).hexdigest()[:8], 16)