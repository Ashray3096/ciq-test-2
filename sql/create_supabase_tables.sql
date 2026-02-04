-- TTB Pipeline Supabase Tables
-- Run this SQL in your Supabase SQL editor to create the required tables
-- All tables are created in the "ttb-pre-prod" schema

-- Reference Tables
CREATE TABLE IF NOT EXISTS "ttb-pre-prod".ttb_product_class_types (
    code VARCHAR PRIMARY KEY,
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS "ttb-pre-prod".ttb_origin_codes (
    code VARCHAR PRIMARY KEY,
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Date Dimension Table (5,844 records, static)
CREATE TABLE IF NOT EXISTS "ttb-pre-prod".dim_dates (
    date_id INTEGER PRIMARY KEY,
    date DATE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    day INTEGER,
    day_of_week INTEGER,
    day_of_year INTEGER,
    week_of_year INTEGER,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    month_name VARCHAR(20),
    day_name VARCHAR(20),
    quarter_name VARCHAR(5),
    season VARCHAR(10),
    days_from_epoch INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Company Dimension Table (partitioned by date)
CREATE TABLE IF NOT EXISTS "ttb-pre-prod".dim_companies (
    company_id BIGINT PRIMARY KEY,
    business_name TEXT,
    mailing_address TEXT,
    phone VARCHAR(50),
    email VARCHAR(255),
    fax VARCHAR(50),
    first_seen_date DATE,
    last_seen_date DATE,
    total_applications INTEGER,
    data_quality_score DECIMAL(5,3),
    source_ttb_ids TEXT[], -- PostgreSQL array
    partition_date DATE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Product Dimension Table (partitioned by date)
CREATE TABLE IF NOT EXISTS "ttb-pre-prod".dim_products (
    product_id BIGINT PRIMARY KEY,
    brand_name TEXT,
    fanciful_name TEXT,
    product_description TEXT,
    class_type_code VARCHAR(100),
    origin_code VARCHAR(100),
    product_category VARCHAR(50),
    grape_varietals TEXT,
    wine_appellation TEXT,
    alcohol_content DECIMAL(5,2),
    net_contents VARCHAR(100),
    first_seen_date DATE,
    last_seen_date DATE,
    total_labels INTEGER,
    data_quality_score DECIMAL(5,3),
    source_ttb_ids TEXT[],
    partition_date DATE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Product Facts Table (main analytical table)
CREATE TABLE IF NOT EXISTS "ttb-pre-prod".fact_products (
    product_fact_id BIGINT PRIMARY KEY,
    ttb_id VARCHAR(50) NOT NULL,
    company_id BIGINT REFERENCES "ttb-pre-prod".dim_companies(company_id),
    product_id BIGINT REFERENCES "ttb-pre-prod".dim_products(product_id),
    filing_date_id INTEGER REFERENCES "ttb-pre-prod".dim_dates(date_id),
    approval_date_id INTEGER REFERENCES "ttb-pre-prod".dim_dates(date_id),
    expiration_date_id INTEGER REFERENCES "ttb-pre-prod".dim_dates(date_id),
    final_quality_score DECIMAL(5,3),
    data_completeness_score DECIMAL(5,3),
    days_to_approval INTEGER,
    has_certificate_data BOOLEAN,
    has_cola_detail_data BOOLEAN,
    class_type_code VARCHAR(100),
    origin_code VARCHAR(100),
    product_category VARCHAR(50),
    status VARCHAR(50),
    serial_number VARCHAR(50),
    vendor_code VARCHAR(50),
    filing_date DATE,
    approval_date DATE,
    expiration_date DATE,
    partition_date DATE,
    fact_creation_timestamp TIMESTAMPTZ,
    source_extraction_timestamp TIMESTAMPTZ,
    source_cleaning_timestamp TIMESTAMPTZ,
    source_structuring_timestamp TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Certificate Facts Table
CREATE TABLE IF NOT EXISTS "ttb-pre-prod".fact_certificates (
    certificate_fact_id BIGINT PRIMARY KEY,
    ttb_id VARCHAR(50) NOT NULL,
    company_id BIGINT REFERENCES "ttb-pre-prod".dim_companies(company_id),
    product_id BIGINT REFERENCES "ttb-pre-prod".dim_products(product_id),
    filing_date_id INTEGER REFERENCES "ttb-pre-prod".dim_dates(date_id),
    approval_date_id INTEGER REFERENCES "ttb-pre-prod".dim_dates(date_id),
    expiration_date_id INTEGER REFERENCES "ttb-pre-prod".dim_dates(date_id),
    final_quality_score DECIMAL(5,3),
    data_completeness_score DECIMAL(5,3),
    days_to_approval INTEGER,
    status VARCHAR(50),
    serial_number VARCHAR(50),
    vendor_code VARCHAR(50),
    receipt_method INTEGER,  -- 0=hand-delivered, 1=e-filed, 2=mailed, 3=overnight
    filing_date DATE,
    approval_date DATE,
    expiration_date DATE,
    partition_date DATE,
    fact_creation_timestamp TIMESTAMPTZ,
    source_extraction_timestamp TIMESTAMPTZ,
    source_cleaning_timestamp TIMESTAMPTZ,
    source_structuring_timestamp TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Performance Indexes
CREATE INDEX IF NOT EXISTS idx_fact_products_company_id ON "ttb-pre-prod".fact_products(company_id);
CREATE INDEX IF NOT EXISTS idx_fact_products_product_id ON "ttb-pre-prod".fact_products(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_products_approval_date ON "ttb-pre-prod".fact_products(approval_date);
CREATE INDEX IF NOT EXISTS idx_fact_products_partition_date ON "ttb-pre-prod".fact_products(partition_date);
CREATE INDEX IF NOT EXISTS idx_fact_products_ttb_id ON "ttb-pre-prod".fact_products(ttb_id);

CREATE INDEX IF NOT EXISTS idx_dim_companies_partition_date ON "ttb-pre-prod".dim_companies(partition_date);
CREATE INDEX IF NOT EXISTS idx_dim_companies_business_name ON "ttb-pre-prod".dim_companies(business_name);

CREATE INDEX IF NOT EXISTS idx_dim_products_partition_date ON "ttb-pre-prod".dim_products(partition_date);
CREATE INDEX IF NOT EXISTS idx_dim_products_brand_name ON "ttb-pre-prod".dim_products(brand_name);
CREATE INDEX IF NOT EXISTS idx_dim_products_class_type ON "ttb-pre-prod".dim_products(class_type_code);
CREATE INDEX IF NOT EXISTS idx_dim_products_origin ON "ttb-pre-prod".dim_products(origin_code);

-- Foreign Key Indexes
CREATE INDEX IF NOT EXISTS idx_fact_certificates_company_id ON "ttb-pre-prod".fact_certificates(company_id);
CREATE INDEX IF NOT EXISTS idx_fact_certificates_product_id ON "ttb-pre-prod".fact_certificates(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_certificates_approval_date ON "ttb-pre-prod".fact_certificates(approval_date);
CREATE INDEX IF NOT EXISTS idx_fact_certificates_partition_date ON "ttb-pre-prod".fact_certificates(partition_date);
CREATE INDEX IF NOT EXISTS idx_fact_certificates_receipt_method ON "ttb-pre-prod".fact_certificates(receipt_method);

-- Fact COLA Applications Table (complete extraction from TTB HTML)
CREATE TABLE IF NOT EXISTS "ttb-pre-prod".fact_cola_applications (
    -- Primary identification
    product_fact_id BIGINT PRIMARY KEY,
    ttb_id TEXT NOT NULL,
    serial_number TEXT,
    vendor_code TEXT,
    rep_id_no TEXT,
    receipt_method INTEGER,  -- 0=hand-delivered, 1=e-filed, 2=mailed, 3=overnight

    -- Foreign keys
    company_id BIGINT REFERENCES "ttb-pre-prod".dim_companies(company_id),
    product_id BIGINT REFERENCES "ttb-pre-prod".dim_products(product_id),
    filing_date_id INTEGER,
    approval_date_id INTEGER,
    expiration_date_id INTEGER,

    -- Product classification
    class_type_code TEXT,
    origin_code TEXT,
    ct_code TEXT,
    or_code TEXT,
    product_category TEXT,
    type_of_product TEXT,      -- 'WINE', 'DISTILLED SPIRITS', 'MALT BEVERAGE'
    source_of_product TEXT,    -- 'DOMESTIC', 'IMPORTED'

    -- Product details
    brand_name TEXT,
    fanciful_name TEXT,
    formula TEXT,
    grape_varietals TEXT,
    wine_appellation TEXT,
    wine_vintage TEXT,
    total_bottle_capacity TEXT,
    for_sale_in TEXT,
    type_of_application TEXT,

    -- Status and qualifications
    status TEXT,
    qualifications TEXT,
    additional_information TEXT,

    -- Dates
    filing_date DATE,
    approval_date DATE,
    application_date DATE,
    expiration_date DATE,
    days_to_approval INTEGER,

    -- Signatures
    applicant_signature TEXT,
    applicant_name TEXT,
    ttb_authorized_signature TEXT,

    -- Permit reference
    permit_number_fk TEXT,
    plant_registry_number TEXT,

    -- Quality metrics
    final_quality_score DOUBLE PRECISION,
    data_completeness_score DOUBLE PRECISION,
    has_certificate_data BOOLEAN,
    has_cola_detail_data BOOLEAN,

    -- Partition and timestamps
    partition_date DATE,
    fact_creation_timestamp TIMESTAMPTZ,
    source_extraction_timestamp TIMESTAMPTZ,
    source_cleaning_timestamp TIMESTAMPTZ,
    source_structuring_timestamp TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(ttb_id)
);

-- Indexes for fact_cola_applications
CREATE INDEX IF NOT EXISTS idx_fact_cola_ttb_id ON "ttb-pre-prod".fact_cola_applications(ttb_id);
CREATE INDEX IF NOT EXISTS idx_fact_cola_company_id ON "ttb-pre-prod".fact_cola_applications(company_id);
CREATE INDEX IF NOT EXISTS idx_fact_cola_product_id ON "ttb-pre-prod".fact_cola_applications(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_cola_approval_date ON "ttb-pre-prod".fact_cola_applications(approval_date);
CREATE INDEX IF NOT EXISTS idx_fact_cola_partition_date ON "ttb-pre-prod".fact_cola_applications(partition_date);
CREATE INDEX IF NOT EXISTS idx_fact_cola_class_type ON "ttb-pre-prod".fact_cola_applications(class_type_code);
CREATE INDEX IF NOT EXISTS idx_fact_cola_origin ON "ttb-pre-prod".fact_cola_applications(origin_code);
CREATE INDEX IF NOT EXISTS idx_fact_cola_receipt_method ON "ttb-pre-prod".fact_cola_applications(receipt_method);
CREATE INDEX IF NOT EXISTS idx_fact_cola_brand_name ON "ttb-pre-prod".fact_cola_applications(brand_name);

-- Comments for documentation
COMMENT ON TABLE "ttb-pre-prod".ttb_product_class_types IS 'TTB reference data for product class and type codes';
COMMENT ON TABLE "ttb-pre-prod".ttb_origin_codes IS 'TTB reference data for origin codes (countries/regions)';
COMMENT ON TABLE "ttb-pre-prod".dim_dates IS 'Date dimension table covering 2015-2030 for TTB analytics';
COMMENT ON TABLE "ttb-pre-prod".dim_companies IS 'Company dimension with deduplication and data quality scores';
COMMENT ON TABLE "ttb-pre-prod".dim_products IS 'Product dimension with brand, category, and quality information';
COMMENT ON TABLE "ttb-pre-prod".fact_products IS 'Main fact table for TTB product applications and approvals';
COMMENT ON TABLE "ttb-pre-prod".fact_certificates IS 'Fact table for TTB certificate data with receipt method tracking';
COMMENT ON TABLE "ttb-pre-prod".fact_cola_applications IS 'Complete COLA application data with all fields extracted from TTB HTML';
COMMENT ON COLUMN "ttb-pre-prod".fact_certificates.receipt_method IS 'Receipt method: 0=hand-delivered, 1=e-filed, 2=mailed, 3=overnight';
COMMENT ON COLUMN "ttb-pre-prod".fact_cola_applications.receipt_method IS 'Receipt method: 0=hand-delivered, 1=e-filed, 2=mailed, 3=overnight';
COMMENT ON COLUMN "ttb-pre-prod".fact_cola_applications.type_of_product IS 'Product type: WINE, DISTILLED SPIRITS, or MALT BEVERAGE';
COMMENT ON COLUMN "ttb-pre-prod".fact_cola_applications.source_of_product IS 'Product source: DOMESTIC or IMPORTED';

-- Enable Row Level Security (optional - for multi-tenant scenarios)
-- ALTER TABLE "ttb-pre-prod".ttb_product_class_types ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE "ttb-pre-prod".ttb_origin_codes ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE "ttb-pre-prod".dim_dates ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE "ttb-pre-prod".dim_companies ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE "ttb-pre-prod".dim_products ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE "ttb-pre-prod".fact_products ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE "ttb-pre-prod".fact_certificates ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE "ttb-pre-prod".fact_cola_applications ENABLE ROW LEVEL SECURITY;
