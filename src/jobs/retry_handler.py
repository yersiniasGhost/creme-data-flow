"""
Retry handler for failed MongoDB writes.

This module provides functionality to:
1. Read failed write logs
2. Parse and validate the data
3. Retry writing to MongoDB
4. Archive successfully retried writes
"""

import re
from typing import List, Tuple, Optional
from datetime import datetime
from pathlib import Path
from utils.logger import LogManager
from utils.envvars import EnvVars
from jobs.mongo_injection import MongoInjection
from jobs.data_validator import LineProtocolValidator


class FailedWriteEntry:
    """Represents a single failed write entry"""

    def __init__(self, timestamp: datetime, errors: List[str], data: List[str]):
        self.timestamp = timestamp
        self.errors = errors
        self.data = data

    def __repr__(self):
        return f"FailedWriteEntry(timestamp={self.timestamp}, errors={len(self.errors)}, data_lines={len(self.data)})"


class RetryHandler:
    """
    Handles retry logic for failed MongoDB writes.
    """

    def __init__(self, log_path: Optional[str] = None):
        self.logger = LogManager().get_logger("RetryHandler")
        self.log_path = log_path or EnvVars()._getenv(
            "FAILED_WRITES_LOG_PATH",
            "/var/log/raptor/failed_writes.log"
        )
        self.archive_path = self.log_path.replace(".log", "_archived.log")
        self.mongo_injection = None
        self.validator = LineProtocolValidator()

    def _initialize_mongo(self):
        """Lazy initialization of MongoDB injection"""
        if not self.mongo_injection:
            try:
                self.mongo_injection = MongoInjection()
                self.logger.info("MongoInjection initialized for retry")
            except Exception as e:
                self.logger.error(f"Failed to initialize MongoInjection: {e}")
                raise

    def parse_failed_writes_log(self) -> List[FailedWriteEntry]:
        """
        Parse the failed writes log file into structured entries.

        Returns:
            List of FailedWriteEntry objects
        """
        log_file = Path(self.log_path)

        if not log_file.exists():
            self.logger.info(f"No failed writes log found at {self.log_path}")
            return []

        entries = []

        try:
            with open(log_file, 'r') as f:
                content = f.read()

            # Split by entry delimiter
            raw_entries = content.split("=" * 50)

            for raw_entry in raw_entries:
                if not raw_entry.strip():
                    continue

                entry = self._parse_single_entry(raw_entry)
                if entry:
                    entries.append(entry)

            self.logger.info(f"Parsed {len(entries)} failed write entries from log")

        except Exception as e:
            self.logger.error(f"Error parsing failed writes log: {e}")

        return entries

    def _parse_single_entry(self, raw_entry: str) -> Optional[FailedWriteEntry]:
        """
        Parse a single failed write entry.

        Args:
            raw_entry: Raw text of the entry

        Returns:
            FailedWriteEntry or None if parsing fails
        """
        try:
            lines = raw_entry.strip().split('\n')

            # Extract timestamp
            timestamp = None
            timestamp_match = re.search(r'Failed Write at (.+?) ===', raw_entry)
            if timestamp_match:
                timestamp_str = timestamp_match.group(1)
                try:
                    timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
                except ValueError:
                    timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")

            # Extract errors
            errors = []
            errors_match = re.search(r'Errors: \[(.*?)\]', raw_entry, re.DOTALL)
            if errors_match:
                errors_str = errors_match.group(1)
                # Parse the list of errors
                errors = [e.strip().strip("'\"") for e in errors_str.split(',') if e.strip()]

            # Extract data lines
            data = []
            in_data_section = False
            for line in lines:
                if line.startswith("Data:"):
                    in_data_section = True
                    continue
                if in_data_section and line.strip() and not line.startswith("==="):
                    data.append(line.strip())

            if timestamp and data:
                return FailedWriteEntry(timestamp=timestamp, errors=errors, data=data)

        except Exception as e:
            self.logger.error(f"Failed to parse entry: {e}")

        return None

    def retry_failed_writes(self, max_retries: int = 3, dry_run: bool = False) -> Tuple[int, int]:
        """
        Retry all failed writes from the log.

        Args:
            max_retries: Maximum number of retry attempts per entry
            dry_run: If True, validate but don't actually write

        Returns:
            Tuple of (successful_count, failed_count)
        """
        self.logger.info("Starting retry of failed writes")

        if not dry_run:
            self._initialize_mongo()

        entries = self.parse_failed_writes_log()

        if not entries:
            self.logger.info("No failed writes to retry")
            return 0, 0

        successful = 0
        failed = 0
        successfully_retried_entries = []

        for entry in entries:
            self.logger.info(f"Retrying entry from {entry.timestamp} with {len(entry.data)} lines")

            # Validate data first
            validation_result = self.validator.validate_line_protocol_batch(entry.data)

            if not validation_result.is_valid:
                self.logger.warning(
                    f"Skipping entry - validation failed: {validation_result.errors}"
                )
                failed += 1
                continue

            if validation_result.warnings:
                self.logger.warning(f"Validation warnings: {validation_result.warnings}")

            # Attempt retry
            if dry_run:
                self.logger.info(f"[DRY RUN] Would retry {len(entry.data)} lines")
                successful += 1
                continue

            retry_success = False
            for attempt in range(max_retries):
                try:
                    self.logger.debug(f"Retry attempt {attempt + 1}/{max_retries}")
                    self.mongo_injection.write(entry.data)
                    self.logger.info(f"Successfully retried entry from {entry.timestamp}")
                    successful += 1
                    retry_success = True
                    successfully_retried_entries.append(entry)
                    break
                except Exception as e:
                    self.logger.error(f"Retry attempt {attempt + 1} failed: {e}")
                    if attempt == max_retries - 1:
                        failed += 1

            if not retry_success:
                self.logger.error(f"Failed to retry entry after {max_retries} attempts")

        # Archive successfully retried entries
        if successfully_retried_entries and not dry_run:
            self._archive_successful_retries(successfully_retried_entries)

        self.logger.info(f"Retry complete: {successful} successful, {failed} failed")
        return successful, failed

    def _archive_successful_retries(self, entries: List[FailedWriteEntry]):
        """
        Archive successfully retried entries and remove from main log.

        Args:
            entries: List of successfully retried entries
        """
        try:
            # Append to archive
            with open(self.archive_path, 'a') as f:
                f.write(f"\n=== Archived at {datetime.now()} ===\n")
                for entry in entries:
                    f.write(f"Original failure: {entry.timestamp}\n")
                    f.write(f"Errors: {entry.errors}\n")
                    f.write(f"Data lines: {len(entry.data)}\n")
                    f.write("-" * 30 + "\n")

            # Rebuild main log without successfully retried entries
            all_entries = self.parse_failed_writes_log()
            remaining_entries = [e for e in all_entries if e not in entries]

            with open(self.log_path, 'w') as f:
                for entry in remaining_entries:
                    f.write(f"\n=== Failed Write at {entry.timestamp} ===\n")
                    f.write(f"Errors: {entry.errors}\n")
                    f.write(f"Data:\n")
                    for line in entry.data:
                        f.write(f"{line}\n")
                    f.write("=" * 50 + "\n")

            self.logger.info(
                f"Archived {len(entries)} entries, {len(remaining_entries)} remaining in log"
            )

        except Exception as e:
            self.logger.error(f"Failed to archive retried entries: {e}")

    def get_failed_writes_summary(self) -> dict:
        """
        Get summary statistics about failed writes.

        Returns:
            Dictionary with summary statistics
        """
        entries = self.parse_failed_writes_log()

        if not entries:
            return {
                "total_entries": 0,
                "oldest_failure": None,
                "newest_failure": None,
                "total_lines": 0
            }

        timestamps = [e.timestamp for e in entries]
        total_lines = sum(len(e.data) for e in entries)

        return {
            "total_entries": len(entries),
            "oldest_failure": min(timestamps),
            "newest_failure": max(timestamps),
            "total_lines": total_lines,
            "entries": entries
        }


def main():
    """Command-line interface for retry handler"""
    import argparse

    parser = argparse.ArgumentParser(description="Retry failed MongoDB writes")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate without actually writing"
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Show summary of failed writes"
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum retry attempts per entry"
    )

    args = parser.parse_args()

    handler = RetryHandler()

    if args.summary:
        summary = handler.get_failed_writes_summary()
        print("\n=== Failed Writes Summary ===")
        print(f"Total entries: {summary['total_entries']}")
        print(f"Total lines: {summary['total_lines']}")
        print(f"Oldest failure: {summary['oldest_failure']}")
        print(f"Newest failure: {summary['newest_failure']}")
        print("=" * 30)
    else:
        successful, failed = handler.retry_failed_writes(
            max_retries=args.max_retries,
            dry_run=args.dry_run
        )
        print(f"\nRetry Results:")
        print(f"  Successful: {successful}")
        print(f"  Failed: {failed}")


if __name__ == "__main__":
    main()
