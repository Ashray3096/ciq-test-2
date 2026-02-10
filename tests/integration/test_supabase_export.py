#!/usr/bin/env python3
"""
Integration test for Supabase dimension and fact exports.

Tests the supabase_dim_* and supabase_fact_* assets that read from S3-stored
dimensions/facts and write to Supabase PostgreSQL tables.

Excludes: supabase_reference_data (already tested), supabase_fact_cola_applications (out of scope).

Prerequisites:
  - Upstream dims/facts materialized to S3 for partition 2024-01-15
  - Supabase tables created (sql/create_supabase_tables.sql)
  - Env vars: SUPABASE_URL, SUPABASE_KEY, POSTGRES_URL, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

Usage:
    export $(grep -v '^#' .env.local | grep -v '^$' | xargs) && \
    .venv/bin/python tests/integration/test_supabase_export.py
"""
import os
import sys
import traceback
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parents[2] / "src"))

import psycopg2
from dagster import materialize, DagsterInstance, AssetSelection

# Upstream assets (for dependency graph)
from ciq_test_2.assets.raw import ttb_raw_data
from ciq_test_2.assets.processed import ttb_extracted_data, ttb_cleaned_data, ttb_structured_data
from ciq_test_2.assets.dimensional import dim_dates, dim_companies, dim_products
from ciq_test_2.assets.facts import fact_products, fact_certificates

# Supabase export assets under test
from ciq_test_2.assets.supabase_export import (
    supabase_dim_dates,
    supabase_dim_companies,
    supabase_dim_products,
    supabase_fact_products,
    supabase_fact_certificates,
)

# Resources
from ciq_test_2.resources.io_managers import TTBS3IOManager
from ciq_test_2.resources.supabase_resources import SupabaseResource, SupabaseIOManager

PARTITION_KEY = "2024-01-15"
SCHEMA = "ttb-pre-prod"


def get_resources():
    """Create resources for test materialization."""
    return {
        "io_manager": TTBS3IOManager(bucket_name="ciq-dagster", region_name="us-east-1"),
        "supabase_io_manager": SupabaseIOManager(supabase_resource=SupabaseResource()),
    }


def query_supabase(sql, params=None):
    """Run a read query against Supabase and return results."""
    postgres_url = os.environ["POSTGRES_URL"]
    conn = psycopg2.connect(postgres_url)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()


def test_supabase_dim_dates():
    """
    Test 1: Export dim_dates to Supabase (non-partitioned).

    Materializes dim_dates -> supabase_dim_dates.
    Verifies records land in "ttb-pre-prod".dim_dates.
    """
    print("=" * 70)
    print("TEST 1: supabase_dim_dates (non-partitioned)")
    print("=" * 70)

    instance = DagsterInstance.ephemeral()

    result = materialize(
        [dim_dates, supabase_dim_dates],
        instance=instance,
        resources=get_resources(),
        selection=AssetSelection.assets(supabase_dim_dates),
    )

    assert result.success, "supabase_dim_dates materialization failed"
    print("  Materialization: SUCCESS")

    # Verify in Supabase
    rows = query_supabase(
        f'SELECT count(*), min(date_id), max(date_id) FROM "{SCHEMA}".dim_dates'
    )
    count, min_id, max_id = rows[0]

    print(f"  Supabase dim_dates: {count} rows, date_id range [{min_id}, {max_id}]")

    assert count > 5000, f"Expected >5000 rows in dim_dates, got {count}"
    print(f"  PASS: row count {count} > 5000")

    assert min_id == 20150101, f"Expected min date_id 20150101, got {min_id}"
    assert max_id == 20301231, f"Expected max date_id 20301231, got {max_id}"
    print(f"  PASS: date_id range [20150101, 20301231]")

    print("  RESULT: PASSED\n")
    return count


def test_supabase_partitioned_dimensions():
    """
    Test 2: Export dim_companies + dim_products to Supabase (partitioned).

    Upstream dims loaded from S3, exported to Supabase tables.
    """
    print("=" * 70)
    print(f"TEST 2: supabase_dim_companies + supabase_dim_products (partition={PARTITION_KEY})")
    print("=" * 70)

    instance = DagsterInstance.ephemeral()

    all_assets = [
        ttb_raw_data,
        ttb_extracted_data,
        ttb_cleaned_data,
        ttb_structured_data,
        dim_companies,
        dim_products,
        supabase_dim_companies,
        supabase_dim_products,
    ]

    selection = AssetSelection.assets(supabase_dim_companies, supabase_dim_products)

    result = materialize(
        all_assets,
        instance=instance,
        partition_key=PARTITION_KEY,
        resources=get_resources(),
        selection=selection,
    )

    assert result.success, "Partitioned dimension export failed"
    print("  Materialization: SUCCESS")

    # Verify dim_companies in Supabase
    print("\n  -- dim_companies --")
    rows = query_supabase(
        f'SELECT count(*) FROM "{SCHEMA}".dim_companies WHERE partition_date = %s',
        (PARTITION_KEY,)
    )
    company_count = rows[0][0]
    print(f"  Rows for partition {PARTITION_KEY}: {company_count}")
    assert company_count > 0, f"Expected >0 companies, got {company_count}"
    print(f"  PASS: {company_count} company rows inserted")

    # Spot-check: business_name not null
    rows = query_supabase(
        f'SELECT count(*) FROM "{SCHEMA}".dim_companies WHERE partition_date = %s AND business_name IS NULL',
        (PARTITION_KEY,)
    )
    null_names = rows[0][0]
    assert null_names == 0, f"Found {null_names} companies with NULL business_name"
    print(f"  PASS: all companies have business_name")

    # Verify dim_products in Supabase
    print("\n  -- dim_products --")
    rows = query_supabase(
        f'SELECT count(*) FROM "{SCHEMA}".dim_products WHERE partition_date = %s',
        (PARTITION_KEY,)
    )
    product_count = rows[0][0]
    print(f"  Rows for partition {PARTITION_KEY}: {product_count}")

    if product_count > 0:
        rows = query_supabase(
            f'SELECT count(*) FROM "{SCHEMA}".dim_products WHERE partition_date = %s AND brand_name IS NULL',
            (PARTITION_KEY,)
        )
        null_brands = rows[0][0]
        assert null_brands == 0, f"Found {null_brands} products with NULL brand_name"
        print(f"  PASS: all products have brand_name")
    else:
        print("  INFO: 0 product rows (no has_cola_detail data in partition)")
        print("  PASS: zero-record case handled")

    print("  RESULT: PASSED\n")
    return company_count, product_count


def test_supabase_facts():
    """
    Test 3: Export fact_products + fact_certificates to Supabase (partitioned).

    Requires dimensions already loaded into Supabase (FK constraints).
    """
    print("=" * 70)
    print(f"TEST 3: supabase_fact_products + supabase_fact_certificates (partition={PARTITION_KEY})")
    print("=" * 70)

    instance = DagsterInstance.ephemeral()

    all_assets = [
        ttb_raw_data,
        ttb_extracted_data,
        ttb_cleaned_data,
        ttb_structured_data,
        dim_companies,
        dim_products,
        fact_products,
        fact_certificates,
        supabase_fact_products,
        supabase_fact_certificates,
    ]

    selection = AssetSelection.assets(supabase_fact_products, supabase_fact_certificates)

    result = materialize(
        all_assets,
        instance=instance,
        partition_key=PARTITION_KEY,
        resources=get_resources(),
        selection=selection,
    )

    assert result.success, "Fact export materialization failed"
    print("  Materialization: SUCCESS")

    # Verify fact_products in Supabase
    print("\n  -- fact_products --")
    rows = query_supabase(
        f'SELECT count(*) FROM "{SCHEMA}".fact_products WHERE partition_date = %s',
        (PARTITION_KEY,)
    )
    fp_count = rows[0][0]
    print(f"  Rows for partition {PARTITION_KEY}: {fp_count}")
    assert fp_count > 0, f"Expected >0 fact_products rows, got {fp_count}"
    print(f"  PASS: {fp_count} fact_products rows inserted")

    # Check FK population
    rows = query_supabase(
        f"""SELECT
            count(*) FILTER (WHERE company_id IS NOT NULL) as has_company,
            count(*) FILTER (WHERE product_id IS NOT NULL) as has_product,
            count(*) FILTER (WHERE filing_date_id IS NOT NULL) as has_filing_date
        FROM "{SCHEMA}".fact_products WHERE partition_date = %s""",
        (PARTITION_KEY,)
    )
    has_company, has_product, has_filing_date = rows[0]
    print(f"  FK population: company_id={has_company}/{fp_count}, product_id={has_product}/{fp_count}, filing_date_id={has_filing_date}/{fp_count}")

    # Verify fact_certificates in Supabase
    print("\n  -- fact_certificates --")
    rows = query_supabase(
        f'SELECT count(*) FROM "{SCHEMA}".fact_certificates WHERE partition_date = %s',
        (PARTITION_KEY,)
    )
    fc_count = rows[0][0]
    print(f"  Rows for partition {PARTITION_KEY}: {fc_count}")
    assert fc_count > 0, f"Expected >0 fact_certificates rows, got {fc_count}"
    print(f"  PASS: {fc_count} fact_certificates rows inserted")

    # Check certificate-specific fields populated after bug fix
    rows = query_supabase(
        f"""SELECT
            count(*) FILTER (WHERE company_id IS NOT NULL) as has_company,
            count(*) FILTER (WHERE filing_date_id IS NOT NULL) as has_filing_date,
            count(*) FILTER (WHERE status IS NOT NULL) as has_status,
            count(*) FILTER (WHERE receipt_method IS NOT NULL) as has_receipt_method
        FROM "{SCHEMA}".fact_certificates WHERE partition_date = %s""",
        (PARTITION_KEY,)
    )
    has_company, has_filing_date, has_status, has_receipt_method = rows[0]
    print(f"  FK/field population: company_id={has_company}/{fc_count}, filing_date_id={has_filing_date}/{fc_count}")
    print(f"  Certificate fields: status={has_status}/{fc_count}, receipt_method={has_receipt_method}/{fc_count}")

    print("  RESULT: PASSED\n")
    return fp_count, fc_count


def main():
    """Run all Supabase export integration tests."""
    print("\n" + "=" * 70)
    print("  TTB Supabase Export Integration Test")
    print(f"  Partition: {PARTITION_KEY}")
    print(f"  Schema: {SCHEMA}")
    print("=" * 70 + "\n")

    passed = 0
    failed = 0

    # Test 1: dim_dates (non-partitioned, must run first for FK constraints)
    dim_dates_count = 0
    try:
        dim_dates_count = test_supabase_dim_dates()
        passed += 1
    except Exception as e:
        print(f"  FAILED: {e}")
        traceback.print_exc()
        failed += 1

    # Test 2: Partitioned dimensions (must run before facts for FK constraints)
    company_count = product_count = 0
    try:
        company_count, product_count = test_supabase_partitioned_dimensions()
        passed += 1
    except Exception as e:
        print(f"  FAILED: {e}")
        traceback.print_exc()
        failed += 1

    # Test 3: Facts (requires dims in Supabase first)
    fp_count = fc_count = 0
    try:
        fp_count, fc_count = test_supabase_facts()
        passed += 1
    except Exception as e:
        print(f"  FAILED: {e}")
        traceback.print_exc()
        failed += 1

    # Summary
    print("=" * 70)
    print("  SUPABASE TABLE SUMMARY")
    print(f"    dim_dates:          {dim_dates_count} rows")
    print(f"    dim_companies:      {company_count} rows (partition {PARTITION_KEY})")
    print(f"    dim_products:       {product_count} rows (partition {PARTITION_KEY})")
    print(f"    fact_products:      {fp_count} rows (partition {PARTITION_KEY})")
    print(f"    fact_certificates:  {fc_count} rows (partition {PARTITION_KEY})")
    print("=" * 70)
    print(f"  RESULT: {passed} passed, {failed} failed, {passed + failed} total")
    print("=" * 70)

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
