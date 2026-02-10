"""
Dimensional Modeling Assets

This module creates star schema dimension tables from structured TTB data.
Implements proper dimensional modeling with surrogate keys and slowly changing dimensions.
"""
import pandas as pd
import hashlib
from datetime import datetime, date, timedelta
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


class DimensionalConfig(Config):
    """Configuration for dimensional modeling assets."""
    date_dimension_start_year: int = 2015
    date_dimension_end_year: int = 2030
    enable_data_quality_flags: bool = True


@asset(
    group_name="ttb_dimensions",
    description="Date dimension table with comprehensive date attributes",
    metadata={
        "data_type": "dimension",
        "schema": "star",
        "format": "pickle"
    }
)
def dim_dates(
    context: AssetExecutionContext,
    config: DimensionalConfig
) -> Dict[str, Any]:
    """
    Generate comprehensive date dimension table.

    Creates a date dimension with business calendar attributes
    for time-based analysis and reporting.

    Returns:
        Dictionary containing date dimension data
    """
    logger = get_dagster_logger()

    logger.info(f"Generating date dimension from {config.date_dimension_start_year} to {config.date_dimension_end_year}")

    # Generate date range
    start_date = date(config.date_dimension_start_year, 1, 1)
    end_date = date(config.date_dimension_end_year, 12, 31)

    date_range = pd.date_range(start=start_date, end=end_date, freq='D')

    date_records = []
    for dt in date_range:
        date_id = int(dt.strftime('%Y%m%d'))
        fiscal_year = dt.year if dt.month >= 10 else dt.year - 1  # Oct-Sep fiscal year
        fiscal_quarter = ((dt.month - 10) % 12) // 3 + 1 if dt.month >= 10 else ((dt.month + 2) // 3)

        record = {
            'date_id': date_id,
            'date': dt.date().isoformat(),
            'year': dt.year,
            'quarter': (dt.month - 1) // 3 + 1,
            'month': dt.month,
            'day': dt.day,
            'day_of_week': dt.dayofweek + 1,  # 1 = Monday
            'day_of_year': dt.dayofyear,
            'week_of_year': dt.isocalendar()[1],
            'fiscal_year': fiscal_year,
            'fiscal_quarter': fiscal_quarter,
            'is_weekend': dt.dayofweek >= 5,
            'is_holiday': _is_us_holiday(dt.date()),
            'month_name': dt.strftime('%B'),
            'day_name': dt.strftime('%A'),
            'quarter_name': f'Q{(dt.month - 1) // 3 + 1}',
            'season': _get_season(dt.month),
            'days_from_epoch': (dt.date() - date(1970, 1, 1)).days
        }
        date_records.append(record)

    logger.info(f"Generated {len(date_records)} date dimension records")

    # Add metadata
    context.add_output_metadata({
        "total_dates": MetadataValue.int(len(date_records)),
        "start_date": MetadataValue.text(start_date.isoformat()),
        "end_date": MetadataValue.text(end_date.isoformat()),
        "years_covered": MetadataValue.int(config.date_dimension_end_year - config.date_dimension_start_year + 1),
        "generation_timestamp": MetadataValue.text(datetime.now().isoformat())
    })

    return {
        'dimension_name': 'dates',
        'records': date_records,
        'primary_key': 'date_id',
        'record_count': len(date_records),
        'generation_timestamp': datetime.now().isoformat()
    }


@asset(
    partitions_def=daily_partitions,
    group_name="ttb_dimensions",
    description="Company dimension table with deduplication and data quality",
    deps=[AssetDep(ttb_structured_data)],
    metadata={
        "data_type": "dimension",
        "schema": "star",
        "format": "pickle"
    }
)
def dim_companies(
    context: AssetExecutionContext,
    config: DimensionalConfig,
    ttb_structured_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Create company dimension from structured TTB data.

    Deduplicates companies and creates surrogate keys for star schema.

    Args:
        ttb_structured_data: Structured TTB data containing company information

    Returns:
        Dictionary containing company dimension data
    """
    logger = get_dagster_logger()

    date_str = context.partition_key
    logger.info(f"Creating company dimension for {date_str}")

    # Extract company information from structured records
    structured_records = ttb_structured_data.get('structured_records', [])

    # Track unique companies
    companies_map = {}

    for record in structured_records:
        business_name = _resolve_business_name(record)
        mailing_address = record.get('applicant_mailing_address')

        if not business_name:
            logger.warning(f"Skipping record {record.get('ttb_id')} - no business name found")
            continue

        if business_name:
            # Create company identifier
            company_key = _create_company_key(business_name, mailing_address)

            if company_key not in companies_map:
                companies_map[company_key] = {
                    'company_id': _create_company_id(business_name, mailing_address),
                    'business_name': business_name,
                    'mailing_address': mailing_address or '',
                    'phone': record.get('applicant_phone', ''),
                    'email': record.get('applicant_email', ''),
                    'fax': record.get('applicant_fax', ''),
                    'first_seen_date': date_str,
                    'last_seen_date': date_str,
                    'total_applications': 0,
                    'data_quality_score': _calculate_company_quality_score(record),
                    'source_ttb_ids': []
                }

            # Update company info
            company = companies_map[company_key]
            company['last_seen_date'] = date_str
            company['total_applications'] += 1
            company['source_ttb_ids'].append(record.get('ttb_id'))

            # Update contact info if better quality available
            if not company['phone'] and record.get('applicant_phone'):
                company['phone'] = record.get('applicant_phone')
            if not company['email'] and record.get('applicant_email'):
                company['email'] = record.get('applicant_email')

    company_records = list(companies_map.values())

    logger.info(f"Created {len(company_records)} unique company dimension records")

    # Add metadata
    context.add_output_metadata({
        "unique_companies": MetadataValue.int(len(company_records)),
        "partition_date": MetadataValue.text(date_str),
        "source_records": MetadataValue.int(len(structured_records)),
        "avg_quality_score": MetadataValue.float(
            sum(c['data_quality_score'] for c in company_records) / len(company_records) if company_records else 0
        )
    })

    return {
        'dimension_name': 'companies',
        'records': company_records,
        'primary_key': 'company_id',
        'record_count': len(company_records),
        'partition_date': date_str,
        'generation_timestamp': datetime.now().isoformat()
    }


@asset(
    partitions_def=daily_partitions,
    group_name="ttb_dimensions",
    description="Product dimension table with classification and attributes",
    deps=[AssetDep(ttb_structured_data)],
    metadata={
        "data_type": "dimension",
        "schema": "star",
        "format": "pickle"
    }
)
def dim_products(
    context: AssetExecutionContext,
    config: DimensionalConfig,
    ttb_structured_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Create product dimension from structured TTB data.

    Creates product dimension with classification and quality attributes.

    Args:
        ttb_structured_data: Structured TTB data containing product information

    Returns:
        Dictionary containing product dimension data
    """
    logger = get_dagster_logger()

    date_str = context.partition_key
    logger.info(f"Creating product dimension for {date_str}")

    # Extract product information from structured records
    structured_records = ttb_structured_data.get('structured_records', [])

    # Track unique products
    products_map = {}

    for record in structured_records:
        # Only process records with COLA detail data
        if not record.get('has_cola_detail'):
            continue

        brand_name = record.get('brand_name')
        fanciful_name = record.get('fanciful_name')

        if brand_name:
            # Create product identifier
            product_key = _create_product_key(brand_name, fanciful_name)

            if product_key not in products_map:
                products_map[product_key] = {
                    'product_id': _create_product_id(brand_name, fanciful_name),
                    'brand_name': brand_name,
                    'fanciful_name': fanciful_name or '',
                    'product_description': record.get('product_description', ''),
                    'class_type_code': record.get('class_type_code', ''),
                    'origin_code': record.get('origin_code', ''),
                    'product_category': record.get('product_category') or _infer_product_category(record.get('class_type_code', '')),
                    'grape_varietals': record.get('grape_varietals', ''),
                    'wine_appellation': record.get('cert_wine_appellation', ''),
                    'alcohol_content': record.get('alcohol_content'),
                    'net_contents': record.get('net_contents', ''),
                    'first_seen_date': date_str,
                    'last_seen_date': date_str,
                    'total_labels': 0,
                    'data_quality_score': _calculate_product_quality_score(record),
                    'source_ttb_ids': []
                }

            # Update product info
            product = products_map[product_key]
            product['last_seen_date'] = date_str
            product['total_labels'] += 1
            product['source_ttb_ids'].append(record.get('ttb_id'))

    product_records = list(products_map.values())

    logger.info(f"Created {len(product_records)} unique product dimension records")

    # Add metadata
    context.add_output_metadata({
        "unique_products": MetadataValue.int(len(product_records)),
        "partition_date": MetadataValue.text(date_str),
        "source_records": MetadataValue.int(len(structured_records)),
        "wine_products": MetadataValue.int(sum(1 for p in product_records if p['product_category'] == 'WINE')),
        "beer_products": MetadataValue.int(sum(1 for p in product_records if p['product_category'] == 'BEER')),
        "spirits_products": MetadataValue.int(sum(1 for p in product_records if p['product_category'] == 'SPIRITS'))
    })

    return {
        'dimension_name': 'products',
        'records': product_records,
        'primary_key': 'product_id',
        'record_count': len(product_records),
        'partition_date': date_str,
        'generation_timestamp': datetime.now().isoformat()
    }


# Helper functions

def _is_us_holiday(dt: date) -> bool:
    """Basic US holiday detection."""
    # New Year's Day
    if dt.month == 1 and dt.day == 1:
        return True
    # Independence Day
    if dt.month == 7 and dt.day == 4:
        return True
    # Christmas
    if dt.month == 12 and dt.day == 25:
        return True
    # Add more holidays as needed
    return False


def _infer_product_category(class_type_code: str) -> str:
    """Infer product category from class_type_code using keyword matching."""
    if not class_type_code:
        return 'OTHER'

    code_lower = class_type_code.lower()

    if any(kw in code_lower for kw in ['wine', 'champagne', 'port', 'sherry',
            'vermouth', 'sake', 'mead', 'cider', 'perry', 'sparkling']):
        return 'WINE'

    if any(kw in code_lower for kw in ['tequila', 'whiskey', 'whisky', 'vodka',
            'rum', 'gin', 'brandy', 'cognac', 'spirits', 'liqueur', 'cordial',
            'mezcal', 'bourbon', 'scotch', 'rye', 'agave']):
        return 'SPIRITS'

    if any(kw in code_lower for kw in ['beer', 'ale', 'lager', 'malt', 'stout',
            'porter', 'pilsner', 'ipa']):
        return 'BEER'

    return 'OTHER'


def _get_season(month: int) -> str:
    """Get season from month."""
    if month in [12, 1, 2]:
        return 'Winter'
    elif month in [3, 4, 5]:
        return 'Spring'
    elif month in [6, 7, 8]:
        return 'Summer'
    else:
        return 'Fall'


def _create_company_key(business_name: str, mailing_address: Optional[str]) -> str:
    """Create unique company key for deduplication."""
    key_string = f"{business_name.strip().upper()}|{(mailing_address or '').strip().upper()}"
    return hashlib.md5(key_string.encode()).hexdigest()


def _create_company_id(business_name: str, mailing_address: Optional[str]) -> int:
    """Create numeric company ID for star schema."""
    key_string = f"{business_name.strip().upper()}|{(mailing_address or '').strip().upper()}"
    return int(hashlib.md5(key_string.encode()).hexdigest()[:8], 16)


def _create_product_key(brand_name: str, fanciful_name: Optional[str]) -> str:
    """Create unique product key for deduplication."""
    key_string = f"{brand_name.strip().upper()}|{(fanciful_name or '').strip().upper()}"
    return hashlib.md5(key_string.encode()).hexdigest()


def _create_product_id(brand_name: str, fanciful_name: Optional[str]) -> int:
    """Create numeric product ID for star schema."""
    key_string = f"{brand_name.strip().upper()}|{(fanciful_name or '').strip().upper()}"
    return int(hashlib.md5(key_string.encode()).hexdigest()[:8], 16)


def _calculate_company_quality_score(record: Dict[str, Any]) -> float:
    """Calculate data quality score for company record."""
    score_components = []

    # Business name quality
    business_name = record.get('applicant_business_name', '')
    if business_name:
        score_components.append(0.3)  # Has business name
        if len(business_name) > 5:
            score_components.append(0.1)  # Substantial name

    # Address quality
    address = record.get('applicant_mailing_address', '')
    if address:
        score_components.append(0.3)  # Has address
        if ',' in address:  # Structured address
            score_components.append(0.1)

    # Contact info quality
    if record.get('applicant_phone'):
        score_components.append(0.15)
    if record.get('applicant_email'):
        score_components.append(0.15)

    return min(sum(score_components), 1.0)


def _calculate_product_quality_score(record: Dict[str, Any]) -> float:
    """Calculate data quality score for product record."""
    score_components = []

    # Brand name quality
    if record.get('brand_name'):
        score_components.append(0.25)

    # Product classification
    if record.get('class_type_code'):
        score_components.append(0.2)
    if record.get('origin_code'):
        score_components.append(0.15)

    # Product details
    if record.get('product_description'):
        score_components.append(0.2)
    if record.get('alcohol_content'):
        score_components.append(0.1)
    if record.get('net_contents'):
        score_components.append(0.1)

    return min(sum(score_components), 1.0)


# Bad value patterns from HTML label parsing artifacts
_BAD_VALUES = {'(required)', 'required', '(optional)', 'optional', 'n/a', 'none'}


def _is_bad_value(value: Optional[str]) -> bool:
    """Check if a value is an HTML parsing artifact."""
    if not value:
        return True
    return value.strip().lower() in _BAD_VALUES


def _resolve_business_name(record: Dict[str, Any]) -> Optional[str]:
    """
    Resolve business name from available fields, filtering out HTML artifacts.

    Fallback chain:
    1. applicant_business_name (primary)
    2. cert_applicant_business_name (certificate data)
    3. applicant_name (person who signed)
    4. First segment of applicant_mailing_address (last resort - address often
       contains the company name as its first comma-separated element when the
       business name field has parsing artifacts like '(Required)')
    """
    # Try primary fields first
    for field in ('applicant_business_name', 'cert_applicant_business_name', 'applicant_name'):
        value = record.get(field)
        if value and not _is_bad_value(value):
            return value.strip()

    # Last resort: parse business name from the address field
    address = record.get('applicant_mailing_address', '')
    if address:
        first_segment = address.split(',')[0].strip()
        if first_segment and not _is_bad_value(first_segment):
            return first_segment

    return None