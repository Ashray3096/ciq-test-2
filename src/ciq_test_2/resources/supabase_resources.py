"""
Supabase Resources for TTB Pipeline

Provides Supabase client and IO manager for writing TTB data to Supabase tables.
"""
import os
from typing import Dict, List, Any, Optional
import json
from contextlib import contextmanager

from dagster import (
    ConfigurableResource,
    ConfigurableIOManager,
    InitResourceContext,
    InputContext,
    OutputContext,
    get_dagster_logger,
    EnvVar
)
from supabase import create_client, Client
import psycopg2
import psycopg2.extras
from psycopg2 import sql


class SupabaseResource(ConfigurableResource):
    """Supabase client resource for TTB pipeline."""

    url: str = EnvVar("SUPABASE_URL")
    key: str = EnvVar("SUPABASE_KEY")
    postgres_url: str = EnvVar("POSTGRES_URL")

    def get_client(self) -> Client:
        """Get Supabase client."""
        return create_client(self.url, self.key)

    @contextmanager
    def get_postgres_connection(self):
        """Get direct PostgreSQL connection for bulk operations."""
        conn = None
        try:
            conn = psycopg2.connect(self.postgres_url)
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                conn.close()


class SupabaseIOManager(ConfigurableIOManager):
    """IO Manager for writing TTB data to Supabase tables."""

    supabase_resource: SupabaseResource
    schema_name: str = "ttb-pre-prod"  # Default schema for TTB tables

    def handle_output(self, context: OutputContext, obj: Dict[str, Any]):
        """Write TTB data to appropriate Supabase table."""
        logger = get_dagster_logger()

        # Determine table name and data structure based on asset name
        asset_name = context.asset_key.path[-1]
        table_name = self._get_table_name(asset_name)

        logger.info(f"Writing {asset_name} data to Supabase table: {table_name}")

        try:
            if 'reference' in asset_name:
                self._write_reference_data(context, obj, logger)
            elif asset_name.startswith('supabase_dim_') or asset_name.startswith('dim_'):
                self._write_dimension_data(context, obj, table_name, logger)
            elif asset_name.startswith('supabase_fact_') or asset_name.startswith('fact_'):
                self._write_fact_data(context, obj, table_name, logger)
            else:
                logger.warning(f"Unknown asset type for Supabase export: {asset_name}")

        except Exception as e:
            logger.error(f"Failed to write {asset_name} to Supabase: {str(e)}")
            raise

    def load_input(self, context: InputContext) -> Dict[str, Any]:
        """Supabase IO Manager is write-only for this pipeline."""
        raise NotImplementedError("Supabase IO Manager is write-only")

    def _get_table_name(self, asset_name: str) -> str:
        """Map asset names to Supabase table names."""
        table_mapping = {
            'supabase_reference_data': ['ttb_product_class_types', 'ttb_origin_codes'],
            'supabase_dim_dates': 'dim_dates',
            'supabase_dim_companies': 'dim_companies',
            'supabase_dim_products': 'dim_products',
            'supabase_fact_products': 'fact_products',
            'supabase_fact_certificates': 'fact_certificates',
            'supabase_fact_cola_applications': 'fact_cola_applications',
            # Fallback mappings
            'ttb_reference_data': ['ttb_product_class_types', 'ttb_origin_codes'],
            'dim_dates': 'dim_dates',
            'dim_companies': 'dim_companies',
            'dim_products': 'dim_products',
            'fact_products': 'fact_products',
            'fact_certificates': 'fact_certificates',
            'fact_cola_applications': 'fact_cola_applications'
        }
        return table_mapping.get(asset_name, asset_name)

    def _get_full_table_name(self, table_name: str) -> str:
        """Get fully qualified table name with schema prefix."""
        return f'"{self.schema_name}".{table_name}'

    def _write_reference_data(self, context: OutputContext, obj: Dict[str, Any], logger):
        """Write reference data to product_class_types and origin_codes tables."""
        # Use direct PostgreSQL connection for schema-prefixed tables
        with self.supabase_resource.get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                # Write product class types
                if 'product_class_types' in obj:
                    logger.info(f"Writing {len(obj['product_class_types'])} product class types")
                    full_table = self._get_full_table_name('ttb_product_class_types')
                    for item in obj['product_class_types']:
                        try:
                            cursor.execute(f"""
                                INSERT INTO {full_table} (code, description)
                                VALUES (%s, %s)
                                ON CONFLICT (code)
                                DO UPDATE SET description = EXCLUDED.description
                            """, (item.get('code', ''), item.get('description', '')))
                        except Exception as e:
                            logger.warning(f"Failed to upsert product class type {item}: {e}")

                # Write origin codes
                if 'origin_codes' in obj:
                    logger.info(f"Writing {len(obj['origin_codes'])} origin codes")
                    full_table = self._get_full_table_name('ttb_origin_codes')
                    for item in obj['origin_codes']:
                        try:
                            cursor.execute(f"""
                                INSERT INTO {full_table} (code, description)
                                VALUES (%s, %s)
                                ON CONFLICT (code)
                                DO UPDATE SET description = EXCLUDED.description
                            """, (item.get('code', ''), item.get('description', '')))
                        except Exception as e:
                            logger.warning(f"Failed to upsert origin code {item}: {e}")

                conn.commit()

    def _write_dimension_data(self, context: OutputContext, obj: Dict[str, Any], table_name: str, logger):
        """Write dimension data using bulk upsert."""
        if 'records' not in obj:
            logger.warning(f"No records found in dimension data for {table_name}")
            return

        records = obj['records']
        logger.info(f"Writing {len(records)} records to {table_name}")

        # Use direct PostgreSQL connection for bulk operations
        with self.supabase_resource.get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                self._bulk_upsert_dimension(cursor, table_name, records, logger)
                conn.commit()

    def _write_fact_data(self, context: OutputContext, obj: Dict[str, Any], table_name: str, logger):
        """Write fact data using bulk upsert."""
        if 'records' not in obj:
            logger.warning(f"No records found in fact data for {table_name}")
            return

        records = obj['records']
        logger.info(f"Writing {len(records)} records to {table_name}")

        # Use direct PostgreSQL connection for bulk operations
        with self.supabase_resource.get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                self._bulk_upsert_fact(cursor, table_name, records, logger)
                conn.commit()

    def _bulk_upsert_dimension(self, cursor, table_name: str, records: List[Dict], logger):
        """Perform bulk upsert for dimension tables."""
        if not records:
            return

        # Get fully qualified table name with schema
        full_table_name = self._get_full_table_name(table_name)

        # Get field names from first record
        fields = list(records[0].keys())

        # Create the upsert query
        placeholders = ', '.join(['%s'] * len(fields))
        field_names = ', '.join(fields)

        # Handle different primary keys
        primary_key = self._get_primary_key(table_name)

        update_clause = ', '.join([
            f"{field} = EXCLUDED.{field}"
            for field in fields
            if field != primary_key
        ])

        query = f"""
            INSERT INTO {full_table_name} ({field_names})
            VALUES ({placeholders})
            ON CONFLICT ({primary_key})
            DO UPDATE SET {update_clause}
        """

        # Prepare data tuples
        data_tuples = []
        for record in records:
            # Convert arrays to PostgreSQL array format
            tuple_data = []
            for field in fields:
                value = record.get(field)
                if isinstance(value, list):
                    # Convert Python list to PostgreSQL array
                    value = value if value else None
                tuple_data.append(value)
            data_tuples.append(tuple(tuple_data))

        try:
            cursor.executemany(query, data_tuples)
            logger.info(f"Successfully upserted {len(data_tuples)} records to {full_table_name}")
        except Exception as e:
            logger.error(f"Error in bulk upsert for {full_table_name}: {e}")
            raise

    def _bulk_upsert_fact(self, cursor, table_name: str, records: List[Dict], logger):
        """Perform bulk upsert for fact tables."""
        if not records:
            return

        # Get fully qualified table name with schema
        full_table_name = self._get_full_table_name(table_name)

        # Get field names from first record
        fields = list(records[0].keys())

        # Create the upsert query
        placeholders = ', '.join(['%s'] * len(fields))
        field_names = ', '.join(fields)

        primary_key = self._get_primary_key(table_name)

        update_clause = ', '.join([
            f"{field} = EXCLUDED.{field}"
            for field in fields
            if field != primary_key
        ])

        query = f"""
            INSERT INTO {full_table_name} ({field_names})
            VALUES ({placeholders})
            ON CONFLICT ({primary_key})
            DO UPDATE SET {update_clause}
        """

        # Prepare data tuples
        data_tuples = []
        for record in records:
            tuple_data = [record.get(field) for field in fields]
            data_tuples.append(tuple(tuple_data))

        try:
            cursor.executemany(query, data_tuples)
            logger.info(f"Successfully upserted {len(data_tuples)} records to {full_table_name}")
        except Exception as e:
            logger.error(f"Error in bulk upsert for {full_table_name}: {e}")
            raise

    def _get_primary_key(self, table_name: str) -> str:
        """Get primary key field name for each table."""
        primary_keys = {
            'dim_dates': 'date_id',
            'dim_companies': 'company_id',
            'dim_products': 'product_id',
            'fact_products': 'product_fact_id',
            'fact_certificates': 'certificate_fact_id',
            'fact_cola_applications': 'product_fact_id'
        }
        return primary_keys.get(table_name, 'id')