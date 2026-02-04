"""
TTB ID utilities for parsing, validation, and generation.

TTB ID Structure (14 digits): YYJJJRRRSSSSS
- YY: Calendar year (last 2 digits)
- JJJ: Julian date (001-366)
- RRR: Receipt method (001=e-filed, 002/003=mailed/overnight, 000=hand delivered)
- SSSSSS: Sequential number (000001-999999, resets daily per receipt method)
"""
import time
from datetime import datetime, date, timedelta
from typing import NamedTuple, Iterator, Optional, List, Set, Dict, Any
from dataclasses import dataclass, field


class TTBIDComponents(NamedTuple):
    """Parsed components of a TTB ID."""
    year: int
    julian_day: int
    receipt_method: int
    sequence: int
    full_id: str


@dataclass
class TTBIDRange:
    """Represents a range of TTB IDs for processing."""
    start_date: date
    end_date: date
    receipt_methods: List[int]
    max_sequence: int = 999999


class TTBIDUtils:
    """Utilities for working with TTB IDs."""

    # Receipt method codes
    RECEIPT_METHODS = {
        1: "e-filed",
        2: "mailed",
        3: "overnight",
        0: "hand delivered"
    }

    @staticmethod
    def parse_ttb_id(ttb_id: str) -> TTBIDComponents:
        """
        Parse a TTB ID into its components.

        Args:
            ttb_id: 14-digit TTB ID string

        Returns:
            TTBIDComponents with parsed values

        Raises:
            ValueError: If TTB ID format is invalid
        """
        if not ttb_id or len(ttb_id) != 14 or not ttb_id.isdigit():
            raise ValueError(f"Invalid TTB ID format: {ttb_id}")

        year_digits = int(ttb_id[:2])
        julian_day = int(ttb_id[2:5])
        receipt_method = int(ttb_id[5:8])
        sequence = int(ttb_id[8:14])

        # Convert 2-digit year to 4-digit year
        # Assume years 00-30 are 2000s, 31-99 are 1900s
        if year_digits <= 30:
            year = 2000 + year_digits
        else:
            year = 1900 + year_digits

        # Validate components
        if not (1 <= julian_day <= 366):
            raise ValueError(f"Invalid Julian day: {julian_day}")

        if receipt_method not in [0, 1, 2, 3]:
            raise ValueError(f"Invalid receipt method: {receipt_method}")

        if not (1 <= sequence <= 999999):
            raise ValueError(f"Invalid sequence number: {sequence}")

        return TTBIDComponents(
            year=year,
            julian_day=julian_day,
            receipt_method=receipt_method,
            sequence=sequence,
            full_id=ttb_id
        )

    @staticmethod
    def build_ttb_id(year: int, julian_day: int, receipt_method: int, sequence: int) -> str:
        """
        Build a TTB ID from components.

        Args:
            year: 4-digit year
            julian_day: Julian day (1-366)
            receipt_method: Receipt method code (0, 1, 2, 3)
            sequence: Sequence number (1-999999)

        Returns:
            14-digit TTB ID string
        """
        # Convert to 2-digit year
        year_2digit = year % 100

        return f"{year_2digit:02d}{julian_day:03d}{receipt_method:03d}{sequence:06d}"

    @staticmethod
    def date_to_julian(date_obj: date) -> int:
        """Convert a date to Julian day of year."""
        return date_obj.timetuple().tm_yday

    @staticmethod
    def julian_to_date(year: int, julian_day: int) -> date:
        """Convert year and Julian day to date object."""
        return datetime.strptime(f"{year}-{julian_day}", "%Y-%j").date()

    @staticmethod
    def generate_ttb_ids_for_date(
        target_date: date,
        receipt_methods: List[int] = None,
        start_sequence: int = 1,
        max_sequence: int = 999999
    ) -> Iterator[str]:
        """
        Generate TTB IDs for a specific date.

        Args:
            target_date: Date to generate IDs for
            receipt_methods: List of receipt methods to include (default: [1, 2, 3])
            start_sequence: Starting sequence number
            max_sequence: Maximum sequence number to generate

        Yields:
            TTB ID strings
        """
        if receipt_methods is None:
            receipt_methods = [1, 2, 3, 0]  # All receipt methods including hand delivered

        julian_day = TTBIDUtils.date_to_julian(target_date)

        for receipt_method in receipt_methods:
            for sequence in range(start_sequence, max_sequence + 1):
                yield TTBIDUtils.build_ttb_id(
                    year=target_date.year,
                    julian_day=julian_day,
                    receipt_method=receipt_method,
                    sequence=sequence
                )

    @staticmethod
    def generate_date_range(start_date: date, end_date: date) -> Iterator[date]:
        """Generate dates between start and end date (inclusive)."""
        current = start_date
        while current <= end_date:
            yield current
            # Move to next day
            current = datetime.fromordinal(current.toordinal() + 1).date()

    @staticmethod
    def generate_date_range_backward(start_date: date, end_date: date) -> Iterator[date]:
        """Generate dates between start and end date (inclusive) in reverse order."""
        current = start_date
        while current >= end_date:
            yield current
            # Move to previous day
            current = datetime.fromordinal(current.toordinal() - 1).date()

    @staticmethod
    def get_partition_key(ttb_id: str) -> str:
        """
        Generate a partition key for a TTB ID.
        Format: YYYY-JJJ-RRR (year-julian_day-receipt_method)
        """
        components = TTBIDUtils.parse_ttb_id(ttb_id)
        return f"{components.year}-{components.julian_day:03d}-{components.receipt_method:03d}"

    @staticmethod
    def rate_limit_sleep():
        """Sleep for rate limiting (0.5 seconds)."""
        time.sleep(0.5)


@dataclass
class SequenceGap:
    """Represents a detected gap in TTB sequences."""
    start_sequence: int
    end_sequence: int
    gap_size: int = field(init=False)

    def __post_init__(self):
        self.gap_size = self.end_sequence - self.start_sequence - 1


class TTBSequenceTracker:
    """
    Enhanced tracker with gap detection and completeness monitoring.

    Tracks consecutive failures to detect end of sequence, while also
    tracking all successful/failed sequences for gap detection.
    """

    def __init__(
        self,
        max_consecutive_failures: int = 500,
        gap_probe_intervals: List[int] = None,
        enable_gap_detection: bool = True
    ):
        self.max_consecutive_failures = max_consecutive_failures
        self.gap_probe_intervals = gap_probe_intervals or [50, 100, 500, 1000]
        self.enable_gap_detection = enable_gap_detection

        # Basic tracking state
        self.consecutive_failures = 0
        self.total_processed = 0
        self.total_success = 0
        self.total_failures = 0

        # Sequence tracking for gap detection
        self.successful_sequences: Set[int] = set()
        self.failed_sequences: Set[int] = set()
        self.detected_gaps: List[SequenceGap] = []

        # Range tracking
        self.min_sequence_found: Optional[int] = None
        self.max_sequence_found: Optional[int] = None
        self.last_successful_sequence: Optional[int] = None

        # Probe state for gap detection
        self.probe_results: Dict[int, bool] = {}

    def record_success(self, sequence: int = None):
        """
        Record a successful request with optional sequence tracking.

        Args:
            sequence: The sequence number that was successful
        """
        self.consecutive_failures = 0
        self.total_processed += 1
        self.total_success += 1

        if sequence is not None:
            self.successful_sequences.add(sequence)
            self.last_successful_sequence = sequence

            # Update range tracking
            if self.min_sequence_found is None or sequence < self.min_sequence_found:
                self.min_sequence_found = sequence
            if self.max_sequence_found is None or sequence > self.max_sequence_found:
                self.max_sequence_found = sequence

    def record_failure(self, sequence: int = None):
        """
        Record a failed request with optional sequence tracking.

        Args:
            sequence: The sequence number that failed
        """
        self.consecutive_failures += 1
        self.total_processed += 1
        self.total_failures += 1

        if sequence is not None:
            self.failed_sequences.add(sequence)

    def should_stop(self) -> bool:
        """Check if we should stop processing due to consecutive failures."""
        return self.consecutive_failures >= self.max_consecutive_failures

    def should_probe_ahead(self) -> bool:
        """Determine if we should probe ahead for more sequences."""
        if not self.enable_gap_detection:
            return False
        return self.consecutive_failures >= self.max_consecutive_failures

    def get_probe_sequences(self, current_sequence: int, max_sequence: int) -> List[int]:
        """
        Get sequences to probe ahead for gap detection.

        Args:
            current_sequence: Current sequence position
            max_sequence: Maximum sequence to check

        Returns:
            List of sequences to probe
        """
        probes = []
        for interval in self.gap_probe_intervals:
            probe_seq = current_sequence + interval
            if probe_seq <= max_sequence and probe_seq not in self.probe_results:
                probes.append(probe_seq)
        return probes

    def record_probe_result(self, sequence: int, found: bool):
        """
        Record result of a gap probe.

        Args:
            sequence: Sequence that was probed
            found: True if data was found at this sequence
        """
        self.probe_results[sequence] = found
        if found:
            self.successful_sequences.add(sequence)
            # Update max if this probe found data beyond current max
            if self.max_sequence_found is None or sequence > self.max_sequence_found:
                self.max_sequence_found = sequence

    def detect_gaps(self) -> List[SequenceGap]:
        """
        Analyze successful sequences to detect gaps.

        Returns:
            List of detected gaps
        """
        if not self.successful_sequences:
            return []

        sorted_successful = sorted(self.successful_sequences)
        gaps = []

        for i in range(len(sorted_successful) - 1):
            current = sorted_successful[i]
            next_seq = sorted_successful[i + 1]

            if next_seq - current > 1:
                # There's a gap
                gap = SequenceGap(
                    start_sequence=current,
                    end_sequence=next_seq
                )
                gaps.append(gap)

        self.detected_gaps = gaps
        return gaps

    def get_completeness_report(self) -> Dict[str, Any]:
        """
        Generate a completeness report for this tracking session.

        Returns:
            Dictionary with completeness metrics
        """
        gaps = self.detect_gaps()
        total_expected = (
            self.max_sequence_found - self.min_sequence_found + 1
            if self.min_sequence_found is not None and self.max_sequence_found is not None
            else 0
        )
        total_found = len(self.successful_sequences)

        return {
            "min_sequence": self.min_sequence_found,
            "max_sequence": self.max_sequence_found,
            "total_expected": total_expected,
            "total_found": total_found,
            "total_failed": len(self.failed_sequences),
            "completeness_ratio": total_found / total_expected if total_expected > 0 else 0,
            "gaps_detected": len(gaps),
            "total_missing_in_gaps": sum(g.gap_size for g in gaps),
            "gap_details": [
                {
                    "start": g.start_sequence,
                    "end": g.end_sequence,
                    "size": g.gap_size
                } for g in gaps
            ],
            "probe_results": self.probe_results
        }

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive processing statistics."""
        return {
            "total_processed": self.total_processed,
            "total_success": self.total_success,
            "total_failures": self.total_failures,
            "consecutive_failures": self.consecutive_failures,
            "success_rate": self.total_success / max(1, self.total_processed),
            "sequences_found": len(self.successful_sequences),
            "sequence_range": (self.min_sequence_found, self.max_sequence_found),
            "gaps_detected": len(self.detected_gaps)
        }


class TTBBackfillManager:
    """
    Enhanced backfill manager with sequence completeness checking.

    Manages backward-looking backfill logic with data existence checking
    and sequence completeness verification.
    """

    def __init__(
        self,
        s3_client,
        bucket_name: str,
        stop_after_consecutive_found: int = 3,
        completeness_threshold: float = 0.95
    ):
        """
        Initialize backfill manager.

        Args:
            s3_client: S3 client for checking data existence
            bucket_name: S3 bucket name
            stop_after_consecutive_found: Stop backfill after this many consecutive days with existing data
            completeness_threshold: Minimum completeness ratio before triggering backfill
        """
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.stop_after_consecutive_found = stop_after_consecutive_found
        self.completeness_threshold = completeness_threshold
        self.consecutive_found_days = 0

        # Track incomplete partitions for targeted backfill
        self.incomplete_partitions: List[Dict[str, Any]] = []

    def check_partition_exists(self, target_date: date, receipt_method: int) -> bool:
        """
        Check if data already exists for a specific date and receipt method.

        Args:
            target_date: Date to check
            receipt_method: Receipt method code

        Returns:
            True if any data exists for this partition
        """
        try:
            # Check S3 prefix for this partition (using ttb-pre-prod path)
            s3_prefix = f"ttb-pre-prod/ttb_raw_data/partition_date={target_date.isoformat()}/"

            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=s3_prefix,
                MaxKeys=1  # Just check if any objects exist
            )

            return response.get('KeyCount', 0) > 0

        except Exception:
            # If we can't check, assume it doesn't exist
            return False

    def check_partition_completeness(
        self,
        target_date: date,
        receipt_method: int
    ) -> Dict[str, Any]:
        """
        Check completeness of a partition beyond just existence.

        Returns detailed completeness metrics.

        Args:
            target_date: Date to check
            receipt_method: Receipt method code

        Returns:
            Dictionary with completeness metrics
        """
        result = {
            "exists": False,
            "record_count": 0,
            "sequence_range": None,
            "gaps_detected": [],
            "completeness_ratio": 0.0,
            "needs_backfill": False
        }

        try:
            # Check if partition exists first
            if not self.check_partition_exists(target_date, receipt_method):
                result["needs_backfill"] = True
                return result

            result["exists"] = True

            # For detailed completeness checking, we would need to read the actual data
            # This is a placeholder for the full implementation
            # In production, you would load the pickle file and analyze sequences

            # Mark as complete if exists (basic check)
            result["completeness_ratio"] = 1.0
            result["needs_backfill"] = False

        except Exception as e:
            result["error"] = str(e)
            result["needs_backfill"] = True

        return result

    def should_stop_backfill(self, target_date: date, receipt_methods: List[int]) -> bool:
        """
        Check if we should stop the backfill process.

        Args:
            target_date: Current date being processed
            receipt_methods: List of receipt methods to check

        Returns:
            True if backfill should stop (enough consecutive existing data found)
        """
        # Check if all receipt methods for this date have existing data
        all_methods_exist = True
        for receipt_method in receipt_methods:
            if not self.check_partition_exists(target_date, receipt_method):
                all_methods_exist = False
                break

        if all_methods_exist:
            self.consecutive_found_days += 1
            return self.consecutive_found_days >= self.stop_after_consecutive_found
        else:
            # Reset counter if we find missing data
            self.consecutive_found_days = 0
            return False

    def identify_backfill_targets(
        self,
        start_date: date,
        end_date: date,
        receipt_methods: List[int]
    ) -> List[Dict[str, Any]]:
        """
        Scan date range and identify partitions needing backfill.

        Args:
            start_date: Start of date range to scan
            end_date: End of date range to scan
            receipt_methods: List of receipt methods to check

        Returns:
            List of backfill targets with priority scores
        """
        targets = []

        current_date = start_date
        while current_date <= end_date:
            for receipt_method in receipt_methods:
                completeness = self.check_partition_completeness(current_date, receipt_method)

                if completeness["needs_backfill"]:
                    priority = self._calculate_backfill_priority(completeness)
                    targets.append({
                        "date": current_date.isoformat(),
                        "receipt_method": receipt_method,
                        "completeness": completeness,
                        "priority": priority
                    })

            current_date += timedelta(days=1)

        # Sort by priority (higher = more urgent)
        targets.sort(key=lambda x: x["priority"], reverse=True)

        return targets

    def _calculate_backfill_priority(self, completeness: Dict[str, Any]) -> float:
        """
        Calculate backfill priority score.

        Args:
            completeness: Completeness check result

        Returns:
            Priority score (higher = more urgent)
        """
        priority = 0.0

        if not completeness["exists"]:
            priority += 100  # Missing entirely
        else:
            # Priority based on gaps
            gap_size = sum(g.get("size", 0) for g in completeness.get("gaps_detected", []))
            priority += gap_size * 2

            # Priority based on completeness ratio
            priority += (1 - completeness.get("completeness_ratio", 0)) * 50

        return priority

    def get_backfill_date_range(self, max_days_back: int = 365) -> Iterator[date]:
        """
        Generate dates for backward backfill starting from day-before-yesterday.

        Args:
            max_days_back: Maximum days to go back (safety limit)

        Yields:
            Dates in reverse chronological order
        """
        today = datetime.now().date()
        start_date = today - timedelta(days=2)  # Day before yesterday
        end_date = today - timedelta(days=max_days_back)  # Safety limit

        # Don't go back before 2015-01-01
        earliest_date = date(2015, 1, 1)
        if end_date < earliest_date:
            end_date = earliest_date

        return TTBIDUtils.generate_date_range_backward(start_date, end_date)