"""
Supabase Export Assets

Assets for exporting TTB data from S3 to Supabase tables for analytics and visualization.
"""
import hashlib
from typing import Dict, Any, List
from dagster import (
    asset,
    get_dagster_logger,
    AssetIn,
    Output,
    Config
)

from ciq_test_2.config.ttb_partitions import daily_partitions


class SupabaseExportConfig(Config):
    """Configuration for Supabase export operations."""
    batch_size: int = 1000
    enable_validation: bool = True


@asset(
    group_name="ttb_supabase_export",
    description="Export TTB reference data to Supabase tables",
    ins={"ttb_reference_data": AssetIn()},
    io_manager_key="supabase_io_manager"
)
def supabase_reference_data(context, config: SupabaseExportConfig, ttb_reference_data: Dict[str, Any]) -> Dict[str, Any]:
    """Export reference data to Supabase tables."""
    logger = get_dagster_logger()

    logger.info("Exporting TTB reference data to Supabase")

    # Transform reference data for Supabase schema
    export_data = {
        'product_class_types': [],
        'origin_codes': []
    }

    # Transform product class types
    if 'product_class_types' in ttb_reference_data:
        product_types = ttb_reference_data['product_class_types']
        if isinstance(product_types, dict):
            # Check for nested structure with 'by_code' or 'all_records'
            if 'by_code' in product_types:
                # Use by_code dictionary for code -> description mapping
                for code, description in product_types['by_code'].items():
                    export_data['product_class_types'].append({
                        'code': code,
                        'description': description
                    })
            elif 'all_records' in product_types:
                # Use all_records list
                for item in product_types['all_records']:
                    if isinstance(item, dict):
                        export_data['product_class_types'].append({
                            'code': item.get('code', ''),
                            'description': item.get('description', '')
                        })
            else:
                # Flat dictionary format
                for code, description in product_types.items():
                    if isinstance(description, str):
                        export_data['product_class_types'].append({
                            'code': code,
                            'description': description
                        })
        elif isinstance(product_types, list):
            # Handle list format
            for item in product_types:
                if isinstance(item, dict):
                    export_data['product_class_types'].append({
                        'code': item.get('code', ''),
                        'description': item.get('description', '')
                    })

    # Transform origin codes
    if 'origin_codes' in ttb_reference_data:
        origin_codes = ttb_reference_data['origin_codes']
        if isinstance(origin_codes, dict):
            # Check for nested structure with 'by_code' or 'all_records'
            if 'by_code' in origin_codes:
                # Use by_code dictionary for code -> description mapping
                for code, description in origin_codes['by_code'].items():
                    export_data['origin_codes'].append({
                        'code': code,
                        'description': description
                    })
            elif 'all_records' in origin_codes:
                # Use all_records list
                for item in origin_codes['all_records']:
                    if isinstance(item, dict):
                        export_data['origin_codes'].append({
                            'code': item.get('code', ''),
                            'description': item.get('description', '')
                        })
            else:
                # Flat dictionary format
                for code, description in origin_codes.items():
                    if isinstance(description, str):
                        export_data['origin_codes'].append({
                            'code': code,
                            'description': description
                        })
        elif isinstance(origin_codes, list):
            # Handle list format
            for item in origin_codes:
                if isinstance(item, dict):
                    export_data['origin_codes'].append({
                        'code': item.get('code', ''),
                        'description': item.get('description', '')
                    })

    logger.info(f"Transformed {len(export_data['product_class_types'])} product class types")
    logger.info(f"Transformed {len(export_data['origin_codes'])} origin codes")

    return export_data


@asset(
    group_name="ttb_supabase_export",
    description="Export date dimension to Supabase",
    ins={"dim_dates": AssetIn()},
    io_manager_key="supabase_io_manager"
)
def supabase_dim_dates(context, config: SupabaseExportConfig, dim_dates: Dict[str, Any]) -> Dict[str, Any]:
    """Export date dimension to Supabase."""
    logger = get_dagster_logger()

    logger.info("Exporting date dimension to Supabase")

    if 'records' not in dim_dates:
        logger.warning("No records found in date dimension")
        return {"records": []}

    records = dim_dates['records']
    logger.info(f"Exporting {len(records)} date dimension records")

    # Transform date records for Supabase
    transformed_records = []
    for record in records:
        transformed_record = {
            'date_id': record.get('date_id'),
            'date': record.get('date'),
            'year': record.get('year'),
            'quarter': record.get('quarter'),
            'month': record.get('month'),
            'day': record.get('day'),
            'day_of_week': record.get('day_of_week'),
            'day_of_year': record.get('day_of_year'),
            'week_of_year': record.get('week_of_year'),
            'fiscal_year': record.get('fiscal_year'),
            'fiscal_quarter': record.get('fiscal_quarter'),
            'is_weekend': record.get('is_weekend'),
            'is_holiday': record.get('is_holiday'),
            'month_name': record.get('month_name'),
            'day_name': record.get('day_name'),
            'quarter_name': record.get('quarter_name'),
            'season': record.get('season'),
            'days_from_epoch': record.get('days_from_epoch')
        }
        transformed_records.append(transformed_record)

    return {"records": transformed_records}


@asset(
    partitions_def=daily_partitions,
    group_name="ttb_supabase_export",
    description="Export company dimension to Supabase",
    ins={"dim_companies": AssetIn()},
    io_manager_key="supabase_io_manager"
)
def supabase_dim_companies(context, config: SupabaseExportConfig, dim_companies: Dict[str, Any]) -> Dict[str, Any]:
    """Export company dimension to Supabase."""
    logger = get_dagster_logger()

    logger.info(f"Exporting company dimension to Supabase for partition {context.partition_key}")

    if 'records' not in dim_companies:
        logger.warning("No records found in company dimension")
        return {"records": []}

    records = dim_companies['records']
    logger.info(f"Exporting {len(records)} company dimension records")

    # Transform company records for Supabase
    transformed_records = []
    for record in records:
        transformed_record = {
            'company_id': record.get('company_id'),
            'business_name': record.get('business_name'),
            'mailing_address': record.get('mailing_address'),
            'phone': record.get('phone'),
            'email': record.get('email'),
            'fax': record.get('fax'),
            'first_seen_date': record.get('first_seen_date'),
            'last_seen_date': record.get('last_seen_date'),
            'total_applications': record.get('total_applications'),
            'data_quality_score': record.get('data_quality_score'),
            'source_ttb_ids': record.get('source_ttb_ids', []),
            'partition_date': context.partition_key
        }
        transformed_records.append(transformed_record)

    return {"records": transformed_records}


@asset(
    partitions_def=daily_partitions,
    group_name="ttb_supabase_export",
    description="Export product dimension to Supabase",
    ins={"dim_products": AssetIn()},
    io_manager_key="supabase_io_manager"
)
def supabase_dim_products(context, config: SupabaseExportConfig, dim_products: Dict[str, Any]) -> Dict[str, Any]:
    """Export product dimension to Supabase."""
    logger = get_dagster_logger()

    logger.info(f"Exporting product dimension to Supabase for partition {context.partition_key}")

    if 'records' not in dim_products:
        logger.warning("No records found in product dimension")
        return {"records": []}

    records = dim_products['records']
    logger.info(f"Exporting {len(records)} product dimension records")

    # Transform product records for Supabase
    transformed_records = []
    for record in records:
        transformed_record = {
            'product_id': record.get('product_id'),
            'brand_name': record.get('brand_name'),
            'fanciful_name': record.get('fanciful_name'),
            'product_description': record.get('product_description'),
            'class_type_code': record.get('class_type_code'),
            'origin_code': record.get('origin_code'),
            'product_category': record.get('product_category'),
            'grape_varietals': record.get('grape_varietals'),
            'wine_appellation': record.get('wine_appellation'),
            'alcohol_content': record.get('alcohol_content'),
            'net_contents': record.get('net_contents'),
            'first_seen_date': record.get('first_seen_date'),
            'last_seen_date': record.get('last_seen_date'),
            'total_labels': record.get('total_labels'),
            'data_quality_score': record.get('data_quality_score'),
            'source_ttb_ids': record.get('source_ttb_ids', []),
            'partition_date': context.partition_key
        }
        transformed_records.append(transformed_record)

    return {"records": transformed_records}


@asset(
    partitions_def=daily_partitions,
    group_name="ttb_supabase_export",
    description="Export certificate facts to Supabase",
    ins={"fact_certificates": AssetIn()},
    io_manager_key="supabase_io_manager"
)
def supabase_fact_certificates(context, config: SupabaseExportConfig, fact_certificates: Dict[str, Any]) -> Dict[str, Any]:
    """Export certificate facts to Supabase."""
    logger = get_dagster_logger()

    logger.info(f"Exporting certificate facts to Supabase for partition {context.partition_key}")

    if 'records' not in fact_certificates:
        logger.warning("No records found in certificate facts")
        return {"records": []}

    records = fact_certificates['records']
    logger.info(f"Exporting {len(records)} certificate fact records")

    # Transform certificate fact records for Supabase (match table schema)
    # Note: fact_certificates has different fields than fact_products.
    # Map certificate-specific fields to the closest Supabase column.
    transformed_records = []
    for record in records:
        transformed_record = {
            'certificate_fact_id': record.get('certificate_fact_id'),
            'ttb_id': record.get('ttb_id'),
            'company_id': record.get('company_id'),
            'product_id': record.get('product_id'),
            'filing_date_id': record.get('application_date_id'),  # map application → filing
            'approval_date_id': record.get('approval_date_id'),
            'expiration_date_id': None,  # certificates don't have expiration
            'final_quality_score': record.get('final_quality_score'),
            'data_completeness_score': record.get('data_completeness_score'),
            'days_to_approval': None,  # not computed for certificates
            'status': record.get('certificate_status'),  # map certificate_status → status
            'serial_number': record.get('serial_number'),
            'vendor_code': None,  # certificates don't have vendor_code
            'receipt_method': record.get('receipt_method'),
            'filing_date': record.get('application_date'),  # map application → filing
            'approval_date': record.get('approval_date'),
            'expiration_date': None,
            'partition_date': record.get('partition_date'),
            'fact_creation_timestamp': record.get('fact_creation_timestamp'),
            'source_extraction_timestamp': record.get('source_extraction_timestamp'),
            'source_cleaning_timestamp': record.get('source_cleaning_timestamp'),
            'source_structuring_timestamp': record.get('source_structuring_timestamp')
        }
        transformed_records.append(transformed_record)

    return {"records": transformed_records}


@asset(
    partitions_def=daily_partitions,
    group_name="ttb_supabase_export",
    description="Export product facts to Supabase",
    ins={"fact_products": AssetIn()},
    io_manager_key="supabase_io_manager"
)
def supabase_fact_products(context, config: SupabaseExportConfig, fact_products: Dict[str, Any]) -> Dict[str, Any]:
    """Export product facts to Supabase."""
    logger = get_dagster_logger()

    logger.info(f"Exporting product facts to Supabase for partition {context.partition_key}")

    if 'records' not in fact_products:
        logger.warning("No records found in product facts")
        return {"records": []}

    records = fact_products['records']
    logger.info(f"Exporting {len(records)} product fact records")

    # Transform fact records for Supabase
    transformed_records = []
    for record in records:
        transformed_record = {
            'product_fact_id': record.get('product_fact_id'),
            'ttb_id': record.get('ttb_id'),
            'company_id': record.get('company_id'),
            'product_id': record.get('product_id'),
            'filing_date_id': record.get('filing_date_id'),
            'approval_date_id': record.get('approval_date_id'),
            'expiration_date_id': record.get('expiration_date_id'),
            'final_quality_score': record.get('final_quality_score'),
            'data_completeness_score': record.get('data_completeness_score'),
            'days_to_approval': record.get('days_to_approval'),
            'has_certificate_data': record.get('has_certificate_data'),
            'has_cola_detail_data': record.get('has_cola_detail_data'),
            'class_type_code': record.get('class_type_code'),
            'origin_code': record.get('origin_code'),
            'product_category': record.get('product_category'),
            'status': record.get('status'),
            'serial_number': record.get('serial_number'),
            'vendor_code': record.get('vendor_code'),
            'filing_date': record.get('filing_date'),
            'approval_date': record.get('approval_date'),
            'expiration_date': record.get('expiration_date'),
            'partition_date': record.get('partition_date'),
            'fact_creation_timestamp': record.get('fact_creation_timestamp'),
            'source_extraction_timestamp': record.get('source_extraction_timestamp'),
            'source_cleaning_timestamp': record.get('source_cleaning_timestamp'),
            'source_structuring_timestamp': record.get('source_structuring_timestamp')
        }
        transformed_records.append(transformed_record)

    return {"records": transformed_records}


def _create_cola_application_id(ttb_id: str) -> int:
    """Create unique COLA application ID from TTB ID."""
    key_string = f"cola_{ttb_id}"
    return int(hashlib.md5(key_string.encode()).hexdigest()[:8], 16)


def _clean_date_value(value):
    """Convert empty strings to None for date fields."""
    if value is None or value == '' or value == 'None':
        return None
    return value


def _calculate_days_to_approval(record: Dict[str, Any]) -> int:
    """Calculate days between filing/application date and approval date."""
    from datetime import datetime

    approval_date = record.get('approval_date')
    filing_date = record.get('filing_date') or record.get('application_date')

    if not approval_date or not filing_date:
        return None

    try:
        # Handle string dates
        if isinstance(approval_date, str):
            approval_dt = datetime.fromisoformat(approval_date.replace('Z', '+00:00'))
        else:
            approval_dt = approval_date

        if isinstance(filing_date, str):
            filing_dt = datetime.fromisoformat(filing_date.replace('Z', '+00:00'))
        else:
            filing_dt = filing_date

        return (approval_dt - filing_dt).days
    except Exception:
        return None


def _create_date_id(date_val) -> int:
    """Convert date to date_id format (YYYYMMDD)."""
    from datetime import datetime, date

    if not date_val:
        return None

    try:
        if isinstance(date_val, str):
            dt = datetime.fromisoformat(date_val.replace('Z', '+00:00'))
            return int(dt.strftime('%Y%m%d'))
        elif isinstance(date_val, datetime):
            return int(date_val.strftime('%Y%m%d'))
        elif isinstance(date_val, date):
            return int(date_val.strftime('%Y%m%d'))
    except Exception:
        pass

    return None


def _merge_ttb_records(cola_record: Dict[str, Any], cert_record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge cola-detail and certificate records, preferring cola-detail values.
    """
    if cola_record and cert_record:
        # Start with cola-detail as base (more complete)
        merged = cola_record.copy()
        # Fill missing/None fields from certificate
        for key, value in cert_record.items():
            if key not in merged or merged[key] is None or merged[key] == '':
                merged[key] = value
        merged['has_certificate'] = True
        merged['has_cola_detail'] = True
        return merged
    elif cola_record:
        merged = cola_record.copy()
        merged['has_cola_detail'] = True
        merged['has_certificate'] = False
        return merged
    elif cert_record:
        merged = cert_record.copy()
        merged['has_certificate'] = True
        merged['has_cola_detail'] = False
        return merged
    return {}


def _infer_source_of_product(origin_code: str, origin_codes_lookup: Dict[str, str]) -> str:
    """
    Infer source_of_product (DOMESTIC/IMPORTED) from origin_code.
    Uses ttb_reference_data for accurate lookup, falls back to keyword matching.
    """
    if not origin_code:
        return None

    origin_upper = origin_code.upper().strip()

    # First, check if origin_code exists in reference data
    if origin_codes_lookup:
        description = origin_codes_lookup.get(origin_upper, '').upper()
        # TTB reference data descriptions indicate country/region
        # US states/regions are DOMESTIC, foreign countries are IMPORTED
        if description:
            # Check for US indicators in description
            us_indicators = ['UNITED STATES', 'USA', 'U.S.', 'AMERICAN']
            if any(ind in description for ind in us_indicators):
                return 'DOMESTIC'
            # If it's a known foreign country, it's imported
            return 'IMPORTED'

    # Fallback: keyword-based inference
    us_origins = {'USA', 'UNITED STATES', 'US', 'AMERICAN', 'DOMESTIC',
                  'CALIFORNIA', 'KENTUCKY', 'TENNESSEE', 'OREGON', 'WASHINGTON',
                  'NEW YORK', 'TEXAS', 'FLORIDA', 'VIRGINIA', 'COLORADO'}

    if origin_upper in us_origins or 'USA' in origin_upper or 'AMERICAN' in origin_upper:
        return 'DOMESTIC'

    # If we have an origin code but it's not US, assume imported
    return 'IMPORTED'


def _infer_type_of_product(class_type_code: str, product_class_lookup: Dict[str, str]) -> str:
    """
    Infer type_of_product from class_type_code.
    Uses ttb_reference_data for accurate lookup, falls back to keyword matching.
    """
    if not class_type_code:
        return None

    class_upper = class_type_code.upper().strip()

    # First, check reference data for official classification
    if product_class_lookup:
        description = product_class_lookup.get(class_upper, '').upper()
        if description:
            # Check description for product type indicators
            if any(kw in description for kw in ['WINE', 'CHAMPAGNE', 'VERMOUTH', 'SAKE']):
                return 'WINE'
            if any(kw in description for kw in ['SPIRIT', 'WHISKEY', 'VODKA', 'RUM',
                    'GIN', 'BRANDY', 'TEQUILA', 'LIQUEUR', 'CORDIAL']):
                return 'DISTILLED SPIRITS'
            if any(kw in description for kw in ['BEER', 'ALE', 'MALT', 'LAGER']):
                return 'MALT BEVERAGE'

    # Fallback: keyword matching on the class_type_code itself
    class_lower = class_type_code.lower()

    if any(kw in class_lower for kw in ['wine', 'champagne', 'port', 'sherry',
            'vermouth', 'sake', 'mead', 'cider', 'perry', 'sparkling']):
        return 'WINE'

    if any(kw in class_lower for kw in ['tequila', 'whiskey', 'whisky', 'vodka',
            'rum', 'gin', 'brandy', 'cognac', 'spirits', 'liqueur', 'cordial',
            'mezcal', 'bourbon', 'scotch', 'rye', 'agave']):
        return 'DISTILLED SPIRITS'

    if any(kw in class_lower for kw in ['beer', 'ale', 'lager', 'malt', 'stout',
            'porter', 'pilsner', 'ipa']):
        return 'MALT BEVERAGE'

    return None


@asset(
    partitions_def=daily_partitions,
    group_name="ttb_supabase_export",
    description="Export complete COLA application data to Supabase with all fields",
    ins={
        "ttb_extracted_data": AssetIn(),
        "ttb_reference_data": AssetIn()
    },
    io_manager_key="supabase_io_manager"
)
def supabase_fact_cola_applications(context, config: SupabaseExportConfig, ttb_extracted_data: Dict[str, Any], ttb_reference_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Export COLA application data to Supabase fact_cola_applications table.

    This asset exports ALL fields extracted from HTML content with proper
    column separation (no JSONB nesting), including:
    - All product and classification details
    - Separate ct_code and or_code columns
    - Separate signature columns
    - Consolidated type_of_product and source_of_product as strings
    - Calculated days_to_approval
    - Receipt method tracking
    """
    logger = get_dagster_logger()

    logger.info(f"Exporting COLA applications to Supabase for partition {context.partition_key}")

    # Get extracted records from the input
    extracted_records = ttb_extracted_data.get('extracted_records', [])

    if not extracted_records:
        logger.warning("No extracted records found")
        return {"records": []}

    logger.info(f"Processing {len(extracted_records)} extracted records")

    # Extract lookup tables from reference data for inference
    product_class_types = ttb_reference_data.get('product_class_types', {})
    origin_codes = ttb_reference_data.get('origin_codes', {})

    # Create lookup dictionaries (code -> description)
    product_class_lookup = product_class_types.get('by_code', {}) if isinstance(product_class_types, dict) else {}
    origin_codes_lookup = origin_codes.get('by_code', {}) if isinstance(origin_codes, dict) else {}

    logger.info(f"Loaded {len(product_class_lookup)} product class types and {len(origin_codes_lookup)} origin codes for inference")

    # STEP 1: Group records by TTB ID
    records_by_ttb_id = {}
    for record in extracted_records:
        ttb_id = record.get('ttb_id', '')
        if not ttb_id:
            continue
        data_type = record.get('data_type', 'unknown')
        if ttb_id not in records_by_ttb_id:
            records_by_ttb_id[ttb_id] = {'cola-detail': None, 'certificate': None}
        if data_type == 'cola-detail':
            records_by_ttb_id[ttb_id]['cola-detail'] = record
        elif data_type == 'certificate':
            records_by_ttb_id[ttb_id]['certificate'] = record
        else:
            # Unknown data_type - use as cola-detail fallback if none exists
            if records_by_ttb_id[ttb_id]['cola-detail'] is None:
                records_by_ttb_id[ttb_id]['cola-detail'] = record

    # STEP 2: Merge paired records (prefer cola-detail over certificate)
    merged_records = []
    for ttb_id, record_pair in records_by_ttb_id.items():
        merged = _merge_ttb_records(record_pair['cola-detail'], record_pair['certificate'])
        if merged:
            merged_records.append(merged)

    logger.info(f"Merged {len(extracted_records)} records into {len(merged_records)} unique TTB IDs")

    # Transform merged records for Supabase (all fields as separate columns)
    transformed_records = []
    for record in merged_records:
        ttb_id = record.get('ttb_id', '')

        # Extract ct_code and or_code from ttb_codes if nested
        ttb_codes = record.get('ttb_codes', {}) or {}
        ct_code = record.get('ct_code') or ttb_codes.get('ct_code')
        or_code = record.get('or_code') or ttb_codes.get('or_code')

        # Extract signatures from nested structure if needed
        signatures = record.get('signatures', {}) or {}
        applicant_signature = record.get('applicant_signature') or signatures.get('applicant_signature')
        ttb_authorized_signature = record.get('ttb_authorized_signature') or signatures.get('authorized_signature_url')

        # Get type_of_product and source_of_product as strings
        type_of_product = record.get('type_of_product')
        source_of_product = record.get('source_of_product')

        # If source_of_product is still a dict (legacy), convert to string
        if isinstance(source_of_product, dict):
            if source_of_product.get('domestic'):
                source_of_product = 'DOMESTIC'
            elif source_of_product.get('imported'):
                source_of_product = 'IMPORTED'
            else:
                source_of_product = None

        # If type_of_product is still a dict (legacy), convert to string
        product_type = record.get('product_type', {}) or {}
        if isinstance(product_type, dict) and not type_of_product:
            if product_type.get('wine'):
                type_of_product = 'WINE'
            elif product_type.get('distilled_spirits'):
                type_of_product = 'DISTILLED SPIRITS'
            elif product_type.get('malt_beverage'):
                type_of_product = 'MALT BEVERAGE'

        # ENHANCED: Infer source_of_product from origin_code using reference data
        if not source_of_product:
            origin_code = record.get('origin_code')
            if origin_code:
                source_of_product = _infer_source_of_product(origin_code, origin_codes_lookup)

        # ENHANCED: Infer type_of_product from class_type_code using reference data
        if not type_of_product:
            class_type_code = record.get('class_type_code')
            if class_type_code:
                type_of_product = _infer_type_of_product(class_type_code, product_class_lookup)

        transformed_record = {
            # Primary identification
            'product_fact_id': _create_cola_application_id(ttb_id),
            'ttb_id': ttb_id,
            'serial_number': record.get('serial_number'),
            'vendor_code': record.get('vendor_code'),
            'rep_id_no': record.get('rep_id_no'),
            'receipt_method': record.get('receipt_method'),

            # Foreign keys (will be populated by lookup if available)
            'company_id': record.get('company_id'),
            'product_id': record.get('product_id'),
            'filing_date_id': _create_date_id(record.get('filing_date') or record.get('application_date')),
            'approval_date_id': _create_date_id(record.get('approval_date')),
            'expiration_date_id': _create_date_id(record.get('expiration_date')),

            # Product classification (separate columns, not JSONB)
            'class_type_code': record.get('class_type_code'),
            'origin_code': record.get('origin_code'),
            'ct_code': ct_code,
            'or_code': or_code,
            'product_category': record.get('product_category'),
            'type_of_product': type_of_product,
            'source_of_product': source_of_product,

            # Product details
            'brand_name': record.get('brand_name'),
            'fanciful_name': record.get('fanciful_name'),
            'formula': record.get('formula'),
            'grape_varietals': record.get('grape_varietals'),
            'wine_appellation': record.get('wine_appellation'),
            'wine_vintage': record.get('wine_vintage'),
            'total_bottle_capacity': record.get('total_bottle_capacity'),
            'for_sale_in': record.get('for_sale_in'),
            'type_of_application': record.get('type_of_application'),

            # Status and qualifications
            'status': record.get('status'),
            'qualifications': record.get('qualifications'),
            'additional_information': record.get('additional_information'),

            # Dates (clean empty strings to None)
            'filing_date': _clean_date_value(record.get('filing_date') or record.get('application_date')),
            'approval_date': _clean_date_value(record.get('approval_date')),
            'application_date': _clean_date_value(record.get('application_date')),
            'expiration_date': _clean_date_value(record.get('expiration_date')),
            'days_to_approval': _calculate_days_to_approval(record),

            # Signatures (separate columns, not JSONB)
            'applicant_signature': applicant_signature,
            # applicant_name: person who signed (Field 18 from certificate)
            # applicant_business_name: company name (from cola-detail or Field 8)
            # Filter out "(Required)" which can come from HTML label parsing errors
            'applicant_name': (
                record.get('applicant_name') if record.get('applicant_name') and record.get('applicant_name') != '(Required)'
                else (record.get('applicant_business_name') if record.get('applicant_business_name') and record.get('applicant_business_name') != '(Required)' else None)
            ),
            'ttb_authorized_signature': ttb_authorized_signature,

            # Permit reference
            'permit_number_fk': record.get('plant_registry_number'),
            'plant_registry_number': record.get('plant_registry_number'),

            # Quality metrics
            'final_quality_score': record.get('final_quality_score'),
            'data_completeness_score': record.get('data_completeness_score'),
            'has_certificate_data': record.get('has_certificate', False),
            'has_cola_detail_data': record.get('has_cola_detail', False),

            # Partition and timestamps
            'partition_date': record.get('partition_date') or context.partition_key,
            'fact_creation_timestamp': record.get('extraction_timestamp'),
            'source_extraction_timestamp': record.get('extraction_timestamp'),
            'source_cleaning_timestamp': record.get('cleaning_timestamp'),
            'source_structuring_timestamp': record.get('structuring_timestamp')
        }
        transformed_records.append(transformed_record)

    logger.info(f"Transformed {len(transformed_records)} COLA application records for Supabase")

    # Count by receipt method for logging
    receipt_counts = {}
    for r in transformed_records:
        rm = r.get('receipt_method')
        receipt_counts[rm] = receipt_counts.get(rm, 0) + 1
    logger.info(f"Receipt methods: {receipt_counts}")

    return {"records": transformed_records}


# Full Supabase export job
@asset(
    partitions_def=daily_partitions,
    group_name="ttb_supabase_export",
    description="Complete TTB data export to Supabase",
    ins={
        "supabase_reference_data": AssetIn(),
        "supabase_dim_dates": AssetIn(),
        "supabase_dim_companies": AssetIn(),
        "supabase_dim_products": AssetIn(),
        "supabase_fact_products": AssetIn(),
        "supabase_fact_certificates": AssetIn(),
        "supabase_fact_cola_applications": AssetIn()
    }
)
def ttb_supabase_export_complete(
    context,
    config: SupabaseExportConfig,
    supabase_reference_data: Dict[str, Any],
    supabase_dim_dates: Dict[str, Any],
    supabase_dim_companies: Dict[str, Any],
    supabase_dim_products: Dict[str, Any],
    supabase_fact_products: Dict[str, Any],
    supabase_fact_certificates: Dict[str, Any],
    supabase_fact_cola_applications: Dict[str, Any]
) -> Dict[str, Any]:
    """Summary asset indicating complete TTB export to Supabase."""
    logger = get_dagster_logger()

    export_summary = {
        'partition_date': context.partition_key,
        'reference_data': {
            'product_class_types': len(supabase_reference_data.get('product_class_types', [])),
            'origin_codes': len(supabase_reference_data.get('origin_codes', []))
        },
        'dimensions': {
            'dates': len(supabase_dim_dates.get('records', [])),
            'companies': len(supabase_dim_companies.get('records', [])),
            'products': len(supabase_dim_products.get('records', []))
        },
        'facts': {
            'products': len(supabase_fact_products.get('records', [])),
            'certificates': len(supabase_fact_certificates.get('records', [])),
            'cola_applications': len(supabase_fact_cola_applications.get('records', []))
        },
        'export_timestamp': context.run_id
    }

    logger.info(f"Complete TTB Supabase export summary: {export_summary}")

    return export_summary