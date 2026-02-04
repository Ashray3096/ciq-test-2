"""
TTB Supabase Loader

Utility for querying Supabase to find max TTB ID sequences for resume functionality.
This enables incremental extraction by starting from where the previous extraction left off.
"""
from typing import Dict, Optional
from dagster import get_dagster_logger


class TTBSupabaseLoader:
    """
    Query Supabase to find max sequences for resume functionality.

    This class queries the ciq.fact_cola_applications table to find the maximum
    TTB ID sequence number already extracted for each partition date and receipt method.
    """

    def __init__(self, url: str, key: str, schema: str = "ciq"):
        """
        Initialize the Supabase loader.

        Args:
            url: Supabase project URL
            key: Supabase API key
            schema: Database schema (default: 'ciq')
        """
        from supabase import create_client

        self.client = create_client(url, key)
        self.schema = schema
        self._cache: Dict[str, Dict[int, int]] = {}
        self._logger = get_dagster_logger()

    def get_max_sequence_per_receipt_method(self, partition_date: str) -> Dict[int, int]:
        """
        Get max sequence for each receipt method for a partition date.

        Parses TTB IDs to extract receipt method and sequence number.
        TTB ID format: YYJJJRRRSSSSS (14 digits)
          - YY: Year (2 digits)
          - JJJ: Julian day (3 digits)
          - RRR: Receipt method (3 digits): 000, 001, 002, 003
          - SSSSSS: Sequence (6 digits)

        Args:
            partition_date: Date string in YYYY-MM-DD format

        Returns:
            Dict mapping receipt_method -> max_sequence
            e.g., {0: 0, 1: 582, 2: 0, 3: 0}
        """
        if partition_date in self._cache:
            self._logger.debug(f"Cache hit for partition_date={partition_date}")
            return self._cache[partition_date]

        self._logger.info(f"Querying Supabase for max sequences on {partition_date}")

        try:
            result = self.client.schema(self.schema).table('fact_cola_applications') \
                .select('ttb_id') \
                .eq('partition_date', partition_date) \
                .execute()

            # Initialize all receipt methods to 0
            max_seqs: Dict[int, int] = {0: 0, 1: 0, 2: 0, 3: 0}

            for row in result.data:
                ttb_id = row.get('ttb_id', '')
                if ttb_id and len(ttb_id) == 14:
                    try:
                        # Extract receipt method (digits 5-7, 0-indexed)
                        receipt_method = int(ttb_id[5:8])
                        # Extract sequence (last 6 digits)
                        sequence = int(ttb_id[8:14])

                        if receipt_method in max_seqs:
                            if sequence > max_seqs[receipt_method]:
                                max_seqs[receipt_method] = sequence
                    except (ValueError, IndexError) as e:
                        self._logger.warning(f"Failed to parse TTB ID '{ttb_id}': {e}")

            self._cache[partition_date] = max_seqs

            # Log summary
            non_zero = {k: v for k, v in max_seqs.items() if v > 0}
            if non_zero:
                self._logger.info(f"Found max sequences for {partition_date}: {non_zero}")
            else:
                self._logger.info(f"No existing data found in Supabase for {partition_date}")

            return max_seqs

        except Exception as e:
            self._logger.error(f"Error querying Supabase for {partition_date}: {e}")
            # Return zeros on error to allow fresh extraction
            return {0: 0, 1: 0, 2: 0, 3: 0}

    def get_max_sequence(self, partition_date: str, receipt_method: int) -> int:
        """
        Get max sequence for a specific receipt method.

        Args:
            partition_date: Date string in YYYY-MM-DD format
            receipt_method: Receipt method code (0, 1, 2, or 3)

        Returns:
            Max sequence number, or 0 if no data found
        """
        max_seqs = self.get_max_sequence_per_receipt_method(partition_date)
        return max_seqs.get(receipt_method, 0)

    def clear_cache(self) -> None:
        """Clear the internal cache."""
        self._cache.clear()
        self._logger.debug("Cleared Supabase loader cache")
