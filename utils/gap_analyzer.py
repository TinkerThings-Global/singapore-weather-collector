import polars as pl
from datetime import datetime, date, time, timedelta
from typing import List
from models import TimeRange, Config
from .logger import Logger

logger = Logger(name="gap_analyzer", filename="weather_data.log")


class DataGapAnalyzer:
    """Analyzes weather data files for gaps and completeness"""

    def __init__(self, config: Config):
        self.config = config

    def detect_gaps(self, df: pl.DataFrame, target_date: date) -> List[TimeRange]:
        """Detect gaps in timestamp data for a specific date"""
        if df.height == 0:
            # Empty file - entire day is a gap
            return [
                TimeRange(
                    start=datetime.combine(target_date, time(0, 0)),
                    end=datetime.combine(target_date, time(23, 59)),
                )
            ]

        # Parse timestamps as naive datetime (already in Singapore local time)
        df_sorted = df.with_columns(
            pl.col("timestamp").str.to_datetime()
        ).sort("timestamp")

        # Filter timestamps to only include the target date
        target_start = datetime.combine(target_date, time(0, 0))
        target_end = datetime.combine(target_date, time(23, 59, 59))

        df_filtered = df_sorted.filter(
            (pl.col("timestamp") >= target_start) & (pl.col("timestamp") <= target_end)
        )

        timestamps = df_filtered.get_column("timestamp").to_list()

        # Safety check after processing
        if not timestamps:
            return [
                TimeRange(
                    start=datetime.combine(target_date, time(0, 0)),
                    end=datetime.combine(target_date, time(23, 59)),
                )
            ]

        gaps = []

        # Expected start and end times (always timezone-naive)
        expected_start = datetime.combine(target_date, time(0, 0))
        expected_end = datetime.combine(target_date, time(23, 59))

        # Check if data starts after midnight
        if timestamps[0] > expected_start:
            gaps.append(TimeRange(expected_start, timestamps[0] - timedelta(minutes=1)))

        # Check for gaps between timestamps (> 1 minute)
        for i in range(len(timestamps) - 1):
            current = timestamps[i]
            next_ts = timestamps[i + 1]
            gap_duration = next_ts - current

            if gap_duration > timedelta(minutes=1):
                gaps.append(
                    TimeRange(
                        current + timedelta(minutes=1), next_ts - timedelta(minutes=1)
                    )
                )

        # Check if data ends before 23:59
        if timestamps[-1] < expected_end:
            gaps.append(TimeRange(timestamps[-1] + timedelta(minutes=1), expected_end))

        return gaps

    def is_complete(self, target_date: date, data_type: str) -> bool:
        """
        Check if data file is complete for a given date

        Args:
            target_date: Date to check
            data_type: Type of data (e.g., 'temp', 'rh')
        """

        file_path = (
            self.config.DATA_DIR
            / f"{data_type}_{self.config.STATION_ID}_{target_date.isoformat()}.csv"
        )

        if not file_path.exists():
            return False

        try:
            df = pl.read_csv(file_path)
            gaps = self.detect_gaps(df, target_date)
            return len(gaps) == 0
        except pl.ComputeError as e:
            logger.error(f"CSV parsing error for {file_path}: {e}")
            return False
        except Exception as e:
            logger.error(f"Error checking completeness for {file_path}: {e}")
            return False

    def get_incomplete_dates(self, exclude_today: bool = True) -> List[date]:
        """
        Get list of dates with incomplete data

        Args:
            exclude_today: If True, don't check today's data (may be in progress)
        """
        incomplete_dates = set()

        end_date = date.today()
        if exclude_today:
            end_date -= timedelta(days=1)

        date_range = [
            self.config.FIRST_DATE + timedelta(days=i)
            for i in range((end_date - self.config.FIRST_DATE).days + 1)
        ]

        for target_date in date_range:
            for data_type in self.config.API_SOURCES.keys():
                if not self.is_complete(target_date, data_type):
                    incomplete_dates.add(target_date)
                    break  # No need to check other data types for this date

        return sorted(list(incomplete_dates))

    def get_completeness_summary(self, target_date: date) -> dict:
        """Get detailed completeness summary for a specific date"""
        summary = {
            "date": target_date.isoformat(),
            "data_types": {},
            "overall_complete": True,
        }

        for data_type in self.config.API_SOURCES.keys():
            file_path = (
                self.config.DATA_DIR
                / f"{data_type}_{self.config.STATION_ID}_{target_date.isoformat()}.csv"
            )

            if not file_path.exists():
                summary["data_types"][data_type] = {
                    "file_exists": False,
                    "complete": False,
                    "gaps": 1440,  # Entire day missing
                }
                summary["overall_complete"] = False
                continue

            try:
                df = pl.read_csv(file_path)
                gaps = self.detect_gaps(df, target_date)
                total_gap_minutes = sum(
                    int((gap.end - gap.start).total_seconds() / 60) + 1 for gap in gaps
                )

                summary["data_types"][data_type] = {
                    "file_exists": True,
                    "complete": len(gaps) == 0,
                    "gaps": len(gaps),
                    "missing_minutes": total_gap_minutes,
                    "completeness_pct": round(
                        (1440 - total_gap_minutes) / 1440 * 100, 2
                    ),
                }

                if len(gaps) > 0:
                    summary["overall_complete"] = False

            except Exception as e:
                logger.error(f"Error analyzing {file_path}: {e}")
                summary["data_types"][data_type] = {
                    "file_exists": True,
                    "complete": False,
                    "error": str(e),
                }
                summary["overall_complete"] = False

        return summary
