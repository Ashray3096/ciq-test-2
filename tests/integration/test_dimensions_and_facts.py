#!/usr/bin/env python3
"""
Integration test for ttb_dimensions and ttb_facts asset groups.

Tests dimension and fact table materialization using existing upstream data
at s3://ciq-dagster/ttb-pre-prod/ttb_processed_data/partition_date=2024-01-15/.

Usage:
    python tests/integration/test_dimensions_and_facts.py
"""
import sys
import traceback
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parents[2] / "src"))

from dagster import materialize, DagsterInstance, AssetSelection

from ciq_test_2.assets.raw import ttb_raw_data
from ciq_test_2.assets.processed import ttb_extracted_data, ttb_cleaned_data, ttb_structured_data
from ciq_test_2.assets.dimensional import dim_dates, dim_companies, dim_products
from ciq_test_2.assets.facts import fact_products, fact_certificates
from ciq_test_2.resources.io_managers import TTBS3IOManager

PARTITION_KEY = "2024-01-15"
BUCKET_NAME = "ciq-dagster"
REGION_NAME = "us-east-1"

# Expected fields for each asset
DIM_DATES_FIELDS = [
    'date_id', 'date', 'year', 'quarter', 'month', 'day',
    'day_of_week', 'day_of_year', 'week_of_year',
    'fiscal_year', 'fiscal_quarter',
    'is_weekend', 'is_holiday',
    'month_name', 'day_name', 'quarter_name', 'season',
    'days_from_epoch'
]


def get_resources():
    """Create resources for test materialization."""
    return {
        "io_manager": TTBS3IOManager(bucket_name=BUCKET_NAME, region_name=REGION_NAME)
    }


def test_dim_dates():
    """
    Test 1: Materialize dim_dates (standalone, non-partitioned).

    dim_dates generates a full date dimension covering 2015-2030.
    No upstream dependencies or partition key needed.
    """
    print("=" * 70)
    print("TEST 1: dim_dates (standalone, non-partitioned)")
    print("=" * 70)

    instance = DagsterInstance.ephemeral()

    result = materialize(
        [dim_dates],
        instance=instance,
        resources=get_resources(),
    )

    assert result.success, "dim_dates materialization failed"

    output = result.output_for_node("dim_dates")
    records = output.get('records', [])
    record_count = output.get('record_count', 0)

    print(f"  Record count: {record_count}")
    print(f"  Primary key: {output.get('primary_key')}")

    # Validate record count (2015-2030 = 16 years ~ 5844 days)
    assert record_count > 5000, f"Expected >5000 date records, got {record_count}"
    print(f"  PASS: record count {record_count} > 5000")

    # Validate all expected fields are present
    if records:
        sample = records[0]
        missing_fields = [f for f in DIM_DATES_FIELDS if f not in sample]
        assert not missing_fields, f"Missing fields in dim_dates: {missing_fields}"
        print(f"  PASS: all {len(DIM_DATES_FIELDS)} expected fields present")

        # Validate date_id format (YYYYMMDD integer)
        date_id = sample['date_id']
        assert isinstance(date_id, int), f"date_id should be int, got {type(date_id)}"
        assert 20150101 <= date_id <= 20301231, f"date_id {date_id} out of expected range"
        print(f"  PASS: date_id format valid (sample: {date_id})")

    print(f"  Output location: s3://{BUCKET_NAME}/ttb-pre-prod/ttb_analytics/dim_dates.pickle")
    print("  RESULT: PASSED\n")
    return output


def test_partitioned_dimensions_and_facts():
    """
    Test 2: Materialize partitioned dimensions and facts for 2024-01-15.

    Uses existing ttb_structured_data from S3 as upstream input.
    Materializes: dim_companies, dim_products, fact_products, fact_certificates.
    """
    print("=" * 70)
    print(f"TEST 2: Partitioned dimensions and facts (partition={PARTITION_KEY})")
    print("=" * 70)

    instance = DagsterInstance.ephemeral()

    # Include the full asset graph so Dagster can resolve dependencies,
    # but use selection to only materialize the dimension/fact assets.
    # Upstream ttb_structured_data will be loaded from S3 by the IO manager.
    all_assets = [
        ttb_raw_data,
        ttb_extracted_data,
        ttb_cleaned_data,
        ttb_structured_data,
        dim_companies,
        dim_products,
        fact_products,
        fact_certificates,
    ]

    selection = AssetSelection.assets(
        dim_companies, dim_products, fact_products, fact_certificates
    )

    result = materialize(
        all_assets,
        instance=instance,
        partition_key=PARTITION_KEY,
        resources=get_resources(),
        selection=selection,
    )

    assert result.success, "Partitioned materialization failed"

    outputs = {}

    # --- dim_companies ---
    print("\n  -- dim_companies --")
    dc = result.output_for_node("dim_companies")
    outputs['dim_companies'] = dc
    dc_records = dc.get('records', [])
    print(f"  Unique companies: {dc.get('record_count', 0)}")

    assert len(dc_records) > 0, "dim_companies produced 0 records"
    print(f"  PASS: {len(dc_records)} company records found")

    sample_company = dc_records[0]
    for field in ['company_id', 'business_name', 'data_quality_score']:
        assert field in sample_company, f"Missing field '{field}' in dim_companies"
    print("  PASS: required fields present (company_id, business_name, data_quality_score)")

    # Quality scores should be 0-1
    scores = [c['data_quality_score'] for c in dc_records]
    assert all(0 <= s <= 1 for s in scores), "Company quality scores out of [0, 1] range"
    print(f"  PASS: quality scores in [0,1] range (avg={sum(scores)/len(scores):.3f})")

    # --- dim_products ---
    print("\n  -- dim_products --")
    dp = result.output_for_node("dim_products")
    outputs['dim_products'] = dp
    dp_records = dp.get('records', [])
    print(f"  Unique products: {dp.get('record_count', 0)}")

    # dim_products may be 0 if no records have has_cola_detail=True
    if dp_records:
        sample_product = dp_records[0]
        for field in ['product_id', 'brand_name', 'product_category']:
            assert field in sample_product, f"Missing field '{field}' in dim_products"
        print("  PASS: required fields present (product_id, brand_name, product_category)")

        p_scores = [p['data_quality_score'] for p in dp_records]
        assert all(0 <= s <= 1 for s in p_scores), "Product quality scores out of [0, 1] range"
        print(f"  PASS: quality scores in [0,1] range (avg={sum(p_scores)/len(p_scores):.3f})")
    else:
        print("  INFO: 0 product records (no has_cola_detail=True data in this partition)")
        print("  PASS: zero-record output handled correctly")

    # --- fact_products ---
    print("\n  -- fact_products --")
    fp = result.output_for_node("fact_products")
    outputs['fact_products'] = fp
    fp_records = fp.get('records', [])
    fp_stats = fp.get('statistics', {})
    print(f"  Fact records: {fp.get('record_count', 0)}")

    if fp_records:
        sample_fact = fp_records[0]
        # FK fields
        for field in ['company_id', 'product_id', 'filing_date_id', 'approval_date_id', 'expiration_date_id']:
            assert field in sample_fact, f"Missing FK field '{field}' in fact_products"
        print("  PASS: FK fields present (company_id, product_id, date IDs)")

        # Measure fields
        for field in ['final_quality_score', 'data_completeness_score', 'days_to_approval']:
            assert field in sample_fact, f"Missing measure '{field}' in fact_products"
        print("  PASS: measure fields present (quality_score, completeness, days_to_approval)")

        # FK integrity check (warnings, not failures)
        missing_company = fp_stats.get('missing_company_keys', 0)
        missing_product = fp_stats.get('missing_product_keys', 0)
        total = len(fp_records)
        company_orphan_rate = missing_company / total if total else 0
        product_orphan_rate = missing_product / total if total else 0
        print(f"  INFO: company FK orphan rate: {company_orphan_rate:.1%} ({missing_company}/{total})")
        print(f"  INFO: product FK orphan rate: {product_orphan_rate:.1%} ({missing_product}/{total})")
    else:
        print("  INFO: 0 fact_products records (expected if no has_cola_detail data)")
        print("  PASS: zero-record output handled correctly")

    # --- fact_certificates ---
    print("\n  -- fact_certificates --")
    fc = result.output_for_node("fact_certificates")
    outputs['fact_certificates'] = fc
    fc_records = fc.get('records', [])
    fc_stats = fc.get('statistics', {})
    print(f"  Fact records: {fc.get('record_count', 0)}")

    if fc_records:
        sample_cert = fc_records[0]
        # FK fields
        for field in ['company_id', 'application_date_id', 'approval_date_id']:
            assert field in sample_cert, f"Missing FK field '{field}' in fact_certificates"
        print("  PASS: FK fields present (company_id, date IDs)")

        # Certificate-specific fields
        for field in ['is_approved', 'certificate_status']:
            assert field in sample_cert, f"Missing field '{field}' in fact_certificates"
        print("  PASS: certificate fields present (is_approved, certificate_status)")

        # FK integrity check
        missing_company = fc_stats.get('missing_company_keys', 0)
        total = len(fc_records)
        company_orphan_rate = missing_company / total if total else 0
        print(f"  INFO: company FK orphan rate: {company_orphan_rate:.1%} ({missing_company}/{total})")
    else:
        print("  INFO: 0 fact_certificates records (expected if no has_certificate data)")
        print("  PASS: zero-record output handled correctly")

    print("\n  RESULT: PASSED\n")
    return outputs


def test_edge_cases(outputs):
    """
    Test 3: Validate filtering and deduplication edge cases.

    Uses outputs from test_partitioned_dimensions_and_facts.
    """
    print("=" * 70)
    print("TEST 3: Edge cases (filtering / deduplication)")
    print("=" * 70)

    dc_records = outputs['dim_companies'].get('records', [])
    dp_records = outputs['dim_products'].get('records', [])

    dc_count = len(dc_records)
    dp_count = len(dp_records)
    fp_records = outputs['fact_products'].get('records', [])
    fp_count = len(fp_records)

    # dim_products (deduplicated) should be <= fact_products (per-record)
    # since facts are created per source record while products are deduplicated
    print(f"  dim_companies: {dc_count}, dim_products: {dp_count}, fact_products: {fp_count}")
    assert dp_count <= fp_count or (dp_count == 0 and fp_count == 0), (
        f"dim_products ({dp_count}) should be <= fact_products ({fp_count}) since products are deduplicated"
    )
    print(f"  PASS: dim_products ({dp_count}) <= fact_products ({fp_count}) (dedup reduces unique products)")

    # Company deduplication: total_applications > 1 means dedup is working
    if dc_records:
        multi_app_companies = [c for c in dc_records if c.get('total_applications', 0) > 1]
        print(f"  Companies with multiple applications (dedup working): {len(multi_app_companies)}")
        if multi_app_companies:
            max_apps = max(c['total_applications'] for c in multi_app_companies)
            print(f"  Max applications for a single company: {max_apps}")
            print("  PASS: deduplication is active")
        else:
            print("  INFO: no multi-application companies in this partition (dedup may still be correct)")

    # Zero-record handling for dim_products
    if dp_count == 0:
        print("  INFO: dim_products is empty (no cola-detail records) - this is valid")
        print("  PASS: zero-record dimension handled correctly")
    else:
        # Verify all products have brand_name (required field)
        products_with_brand = [p for p in dp_records if p.get('brand_name')]
        assert len(products_with_brand) == dp_count, "Some products missing brand_name"
        print(f"  PASS: all {dp_count} products have brand_name")

    print("  RESULT: PASSED\n")


def main():
    """Run all integration tests."""
    print("\n" + "=" * 70)
    print("  TTB Dimensions & Facts Integration Test")
    print(f"  Partition: {PARTITION_KEY}")
    print(f"  S3 Bucket: {BUCKET_NAME}")
    print("=" * 70 + "\n")

    passed = 0
    failed = 0

    # Test 1: dim_dates (standalone)
    try:
        test_dim_dates()
        passed += 1
    except Exception as e:
        print(f"  FAILED: {e}")
        traceback.print_exc()
        failed += 1

    # Test 2: Partitioned dimensions and facts
    outputs = None
    try:
        outputs = test_partitioned_dimensions_and_facts()
        passed += 1
    except Exception as e:
        print(f"  FAILED: {e}")
        traceback.print_exc()
        failed += 1

    # Test 3: Edge cases (requires outputs from test 2)
    if outputs:
        try:
            test_edge_cases(outputs)
            passed += 1
        except Exception as e:
            print(f"  FAILED: {e}")
            traceback.print_exc()
            failed += 1
    else:
        print("SKIPPED: test_edge_cases (test 2 did not produce outputs)")
        failed += 1

    # Summary
    print("=" * 70)
    print(f"  SUMMARY: {passed} passed, {failed} failed, {passed + failed} total")
    print("=" * 70)

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
