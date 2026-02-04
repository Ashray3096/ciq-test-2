"""
TTB Reference Data Assets

This module contains assets for extracting and maintaining reference data from the TTB website,
including product class/type codes and origin codes used for validation and enrichment.
"""
import tempfile
from datetime import datetime
from typing import Dict, Any, List
import requests
import urllib3
from bs4 import BeautifulSoup

from dagster import (
    asset,
    Config,
    get_dagster_logger,
    AssetExecutionContext,
    MetadataValue
)

# Disable SSL warnings for requests with verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class TTBReferenceConfig(Config):
    """Configuration for TTB reference data extraction."""
    request_timeout: int = 30
    user_agent: str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    enable_caching: bool = True


@asset(
    group_name="ttb_reference",
    description="Product class and type codes from TTB website for validation",
    metadata={
        "data_type": "reference",
        "source": "ttbonline.gov",
        "format": "pickle"
    }
)
def ttb_product_class_types(
    context: AssetExecutionContext,
    config: TTBReferenceConfig
) -> Dict[str, Any]:
    """
    Extract product class/type codes from TTB website.

    These codes are used to validate and enrich product classification data.

    Returns:
        Dictionary containing product class/type lookup data
    """
    logger = get_dagster_logger()

    url = "https://ttbonline.gov/colasonline/lookupProductClassTypeCode.do?action=search&display=all"

    logger.info("Extracting product class/type codes from TTB website")

    try:
        # Setup session with proper headers
        session = requests.Session()
        session.headers.update({
            'User-Agent': config.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        })

        # Make request
        response = session.get(url, verify=False, timeout=config.request_timeout)
        response.raise_for_status()

        # Parse HTML
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract product class type codes
        records = parse_product_class_types(soup)

        # Create lookup dictionaries
        lookup_data = {
            'by_code': {record['code']: record['description'] for record in records},
            'by_description': {record['description']: record['code'] for record in records},
            'all_codes': [record['code'] for record in records],
            'all_records': records,
            'extraction_timestamp': datetime.now().isoformat(),
            'source_url': url,
            'total_records': len(records)
        }

        logger.info(f"Successfully extracted {len(records)} product class/type codes")

        # Add metadata
        context.add_output_metadata({
            "total_records": MetadataValue.int(len(records)),
            "source_url": MetadataValue.text(url),
            "extraction_timestamp": MetadataValue.text(lookup_data['extraction_timestamp']),
            "unique_codes": MetadataValue.int(len(lookup_data['by_code'])),
            "unique_descriptions": MetadataValue.int(len(lookup_data['by_description']))
        })

        return lookup_data

    except Exception as e:
        logger.error(f"Error extracting product class/type codes: {e}")
        # Return empty structure on error
        return {
            'by_code': {},
            'by_description': {},
            'all_codes': [],
            'all_records': [],
            'extraction_timestamp': datetime.now().isoformat(),
            'source_url': url,
            'total_records': 0,
            'error': str(e)
        }


@asset(
    group_name="ttb_reference",
    description="Origin codes from TTB website for geographic validation",
    metadata={
        "data_type": "reference",
        "source": "ttbonline.gov",
        "format": "pickle"
    }
)
def ttb_origin_codes(
    context: AssetExecutionContext,
    config: TTBReferenceConfig
) -> Dict[str, Any]:
    """
    Extract origin codes from TTB website.

    These codes are used to validate and enrich geographic origin data.

    Returns:
        Dictionary containing origin code lookup data
    """
    logger = get_dagster_logger()

    url = "https://ttbonline.gov/colasonline/lookupOriginCode.do?action=search&display=all"

    logger.info("Extracting origin codes from TTB website")

    try:
        # Setup session with proper headers
        session = requests.Session()
        session.headers.update({
            'User-Agent': config.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        })

        # Make request
        response = session.get(url, verify=False, timeout=config.request_timeout)
        response.raise_for_status()

        # Parse HTML
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract origin codes
        records = parse_origin_codes(soup)

        # Create lookup dictionaries
        lookup_data = {
            'by_code': {record['code']: record['description'] for record in records},
            'by_description': {record['description']: record['code'] for record in records},
            'all_codes': [record['code'] for record in records],
            'all_records': records,
            'extraction_timestamp': datetime.now().isoformat(),
            'source_url': url,
            'total_records': len(records)
        }

        logger.info(f"Successfully extracted {len(records)} origin codes")

        # Add metadata
        context.add_output_metadata({
            "total_records": MetadataValue.int(len(records)),
            "source_url": MetadataValue.text(url),
            "extraction_timestamp": MetadataValue.text(lookup_data['extraction_timestamp']),
            "unique_codes": MetadataValue.int(len(lookup_data['by_code'])),
            "unique_descriptions": MetadataValue.int(len(lookup_data['by_description']))
        })

        return lookup_data

    except Exception as e:
        logger.error(f"Error extracting origin codes: {e}")
        # Return empty structure on error
        return {
            'by_code': {},
            'by_description': {},
            'all_codes': [],
            'all_records': [],
            'extraction_timestamp': datetime.now().isoformat(),
            'source_url': url,
            'total_records': 0,
            'error': str(e)
        }


@asset(
    group_name="ttb_reference",
    description="Combined TTB reference data for validation and enrichment",
    metadata={
        "data_type": "reference",
        "source": "combined",
        "format": "pickle"
    }
)
def ttb_reference_data(
    context: AssetExecutionContext,
    ttb_product_class_types: Dict[str, Any],
    ttb_origin_codes: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Combine all TTB reference data into a single asset for easy access.

    Args:
        ttb_product_class_types: Product class/type codes lookup data
        ttb_origin_codes: Origin codes lookup data

    Returns:
        Combined reference data dictionary
    """
    logger = get_dagster_logger()

    combined_data = {
        'product_class_types': ttb_product_class_types,
        'origin_codes': ttb_origin_codes,
        'combined_timestamp': datetime.now().isoformat(),
        'data_sources': ['product_class_types', 'origin_codes']
    }

    # Calculate statistics
    total_product_codes = ttb_product_class_types.get('total_records', 0)
    total_origin_codes = ttb_origin_codes.get('total_records', 0)
    total_records = total_product_codes + total_origin_codes

    combined_data['statistics'] = {
        'total_product_class_types': total_product_codes,
        'total_origin_codes': total_origin_codes,
        'total_reference_records': total_records,
        'has_product_errors': 'error' in ttb_product_class_types,
        'has_origin_errors': 'error' in ttb_origin_codes
    }

    logger.info(f"Combined reference data: {total_product_codes} product codes + {total_origin_codes} origin codes = {total_records} total")

    # Add metadata
    context.add_output_metadata({
        "total_product_class_types": MetadataValue.int(total_product_codes),
        "total_origin_codes": MetadataValue.int(total_origin_codes),
        "total_reference_records": MetadataValue.int(total_records),
        "combined_timestamp": MetadataValue.text(combined_data['combined_timestamp']),
        "data_sources": MetadataValue.text(", ".join(combined_data['data_sources']))
    })

    return combined_data


def parse_product_class_types(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """Parse product class type codes from HTML soup."""
    records = []
    tables = soup.find_all('table')

    for table in tables:
        headers = table.find_all('th')
        if not headers:
            continue

        header_texts = [th.get_text(strip=True) for th in headers]
        if 'Class/Type Code' in header_texts and 'Description' in header_texts:
            rows = table.find_all('tr')
            for row in rows[1:]:  # Skip header
                cells = row.find_all('td')
                if len(cells) >= 2:
                    code = cells[0].get_text(strip=True)
                    description = cells[1].get_text(strip=True)

                    if code and description and len(code) <= 10:
                        records.append({
                            'code': code,
                            'description': description,
                            'type': 'product_class_type',
                            'extraction_timestamp': datetime.now().isoformat()
                        })
            break

    return records


def parse_origin_codes(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """Parse origin codes from HTML soup."""
    records = []
    tables = soup.find_all('table')

    for table in tables:
        headers = table.find_all('th')
        if not headers:
            continue

        header_texts = [th.get_text(strip=True) for th in headers]
        if 'Origin Code' in header_texts and 'Description' in header_texts:
            rows = table.find_all('tr')
            for row in rows[1:]:  # Skip header
                cells = row.find_all('td')
                if len(cells) >= 2:
                    code = cells[0].get_text(strip=True)
                    description = cells[1].get_text(strip=True)

                    if code and description and len(code) <= 10:
                        records.append({
                            'code': code,
                            'description': description,
                            'type': 'origin_code',
                            'extraction_timestamp': datetime.now().isoformat()
                        })
            break

    return records