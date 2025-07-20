from .logger import Logger
import asyncio
from datetime import datetime, date, time, timedelta
from typing import List, Optional
from models import Config, TimeRange
import httpx
import polars as pl
from .progress_tracker import ProgressTracker
from .gap_analyzer import DataGapAnalyzer
from .http_client import RetryableHTTPClient
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path


logger = Logger(name="data_collector", filename="weather_data.log")


def process_and_save_worker(
    raw_data: List[dict],
    target_date_str: str,
    data_type: str,
    station_id: str,
    data_dir: Path,
    value_col_name: str,
) -> dict:
    """Worker function for processing data in a separate process"""
    try:
        if not raw_data:
            return {"success": False, "error": "No data to process", "records": 0}

        # Convert to DataFrame and process
        df = pl.from_records(raw_data, strict=False).explode("data").unnest("data")

        # Filter and check timestamp dtype
        filtered_df = df.filter(pl.col("stationId") == station_id)
        
        # Handle timestamp conversion based on actual dtype
        if filtered_df.select("timestamp").dtypes[0] == pl.String:
            processed_df = filtered_df.with_columns(
                pl.col("timestamp")
                .str.slice(0, 19)
                .str.to_datetime("%Y-%m-%dT%H:%M:%S")
            )
        else:
            processed_df = filtered_df.with_columns(
                pl.col("timestamp").dt.replace_time_zone(None)
            )
        
        
        # Continue processing
        processed_df = (
            processed_df
            .rename({"value": value_col_name})
            .select("timestamp", value_col_name)
            .sort("timestamp")
            .unique(subset=["timestamp"], keep="last")
        )
        

        file_path = data_dir / f"{data_type}_{station_id}_{target_date_str}.csv"

        if file_path.exists():
            # Merge with existing data - ensure timestamps are naive
            existing = pl.read_csv(file_path, try_parse_dates=True)
            if existing.select("timestamp").dtypes[0] == pl.String:
                existing = existing.with_columns(
                    pl.col("timestamp").str.to_datetime().dt.replace_time_zone(None)
                )
            else:
                existing = existing.with_columns(
                    pl.col("timestamp").dt.replace_time_zone(None)
                )
            merged = (
                pl.concat([existing, processed_df])
                .unique(subset=["timestamp"], keep="last")
                .sort("timestamp")
            )
            merged.write_csv(file_path)
            total_records = merged.height
        else:
            processed_df.write_csv(file_path)
            total_records = processed_df.height

        return {
            "success": True,
            "error": None,
            "records": total_records,
            "new_records": processed_df.height,
        }

    except Exception as e:
        return {"success": False, "error": str(e), "records": 0}


def analyze_file_worker(
    file_path: Path, target_date_str: str, config_dict: dict
) -> dict:
    """Worker function for analyzing file gaps in a separate process"""
    try:
        from models import Config
        from utils.gap_analyzer import DataGapAnalyzer
        from datetime import date

        target_date = date.fromisoformat(target_date_str)

        # Reconstruct config from dict
        config = Config(
            DATA_DIR=Path(config_dict["DATA_DIR"]),
            STATION_ID=config_dict["STATION_ID"],
            FIRST_DATE=date.fromisoformat(config_dict["FIRST_DATE"]),
            API_SOURCES=config_dict["API_SOURCES"],
            API_VALUE_COLS=config_dict["API_VALUE_COLS"],
        )

        analyzer = DataGapAnalyzer(config)

        if not file_path.exists():
            return {
                "success": True,
                "timestamp_gaps": [
                    [f"{target_date}T00:00:00", f"{target_date}T23:59:00"]
                ],
                "status": "incomplete",
                "total_records": 0,
            }

        df = pl.read_csv(file_path)
        gaps = analyzer.detect_gaps(df, target_date)

        # Convert gaps to string format
        timestamp_gaps = []
        for gap in gaps:
            start_str = gap.start.strftime("%Y-%m-%dT%H:%M:%S")
            end_str = gap.end.strftime("%Y-%m-%dT%H:%M:%S")
            timestamp_gaps.append([start_str, end_str])

        status = "complete" if len(gaps) == 0 else "incomplete"

        return {
            "success": True,
            "timestamp_gaps": timestamp_gaps if timestamp_gaps else None,
            "status": status,
            "total_records": df.height,
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "timestamp_gaps": [
                [f"{target_date_str}T00:00:00", f"{target_date_str}T23:59:00"]
            ],
            "status": "incomplete",
            "total_records": 0,
        }


class WeatherDataCollector:
    """Main weather data collection orchestrator with multiprocessing support"""

    def __init__(self, config: Config):
        self.config = config
        self.progress_tracker = ProgressTracker(config.PROGRESS_FILE)
        self.gap_analyzer = DataGapAnalyzer(config)
        self.http_client = RetryableHTTPClient(config)
        self.executor = None
        self.request_semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_REQUESTS)

        # Ensure data directory exists
        self.config.DATA_DIR.mkdir(exist_ok=True)

    def _config_to_dict(self) -> dict:
        """Convert config to dict for multiprocessing"""
        return {
            "DATA_DIR": str(self.config.DATA_DIR),
            "STATION_ID": self.config.STATION_ID,
            "FIRST_DATE": self.config.FIRST_DATE.isoformat(),
            "API_SOURCES": self.config.API_SOURCES,
            "API_VALUE_COLS": self.config.API_VALUE_COLS,
        }

    async def fetch_all_pages_for_range(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        start_time: datetime,
        end_time: datetime,
        target_date: date,
        data_type: str,
        is_gap_filling: bool = False,
    ) -> List[dict]:
        """Fetch all pages for a specific time range"""
        # Use semaphore to limit concurrent pagination operations
        async with self.request_semaphore:
            all_rows = []

            # Determine query parameters based on context
            # For gaps that span multiple hours or start from beginning of day, use date-only query
            gap_duration_hours = (end_time - start_time).total_seconds() / 3600
            starts_from_midnight = start_time.time() == time(0, 0)
            
            use_date_only = not is_gap_filling or gap_duration_hours >= 6 or starts_from_midnight
            
            if use_date_only:
                # Full day query or large gap - use date-only for efficiency
                params = {"date": target_date.isoformat()}
            else:
                # Small gap filling - use specific timestamp
                timestamp_str = start_time.strftime("%Y-%m-%dT%H:%M:%S")
                params = {"date": timestamp_str}

            has_next_page = True
            page_count = 0
            max_pages = 100  # Safety limit to prevent infinite pagination

            while has_next_page and page_count < max_pages:
                page_count += 1
                try:
                    data = await self.http_client.get_with_retry(client, base_url, params)
                    if not data:
                        break

                    readings = data.get("data", {}).get("readings", [])
                    if readings:
                        if use_date_only:
                            # No time filtering for date-only queries - we want all data for the day
                            all_rows.extend(readings)
                        else:
                            # Filter readings to only include those in our target range for timestamp-specific queries
                            filtered_readings = []
                            for reading in readings:
                                reading_time = datetime.fromisoformat(reading["timestamp"][:19])
                                if start_time <= reading_time <= end_time:
                                    filtered_readings.append(reading)

                            all_rows.extend(filtered_readings)

                    # Check for next page
                    if pagination_token := data.get("data", {}).get("paginationToken"):
                        params["paginationToken"] = pagination_token
                    else:
                        has_next_page = False

                except Exception as e:
                    logger.error(
                        f"Error fetching data for {data_type} on {target_date}: {e}"
                    )
                    # Update progress with error
                    self.progress_tracker.update_state(
                        target_date,
                        data_type,
                        last_error=str(e),
                        last_updated=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    )
                    break

            if page_count >= max_pages:
                logger.warning(f"Hit pagination limit ({max_pages} pages) for {data_type} on {target_date}")
                
            logger.info(f"Fetched {len(all_rows)} records for {data_type} on {target_date}")
            return all_rows

    async def process_and_save_data(
        self, raw_data: List[dict], target_date: date, data_type: str
    ):
        """Process raw API data and save to CSV using multiprocessing"""
        if not raw_data:
            return

        try:
            value_col_name = self.config.API_VALUE_COLS[data_type]

            # Run CPU-intensive processing in separate process
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                self.executor,
                process_and_save_worker,
                raw_data,
                target_date.isoformat(),
                data_type,
                self.config.STATION_ID,
                self.config.DATA_DIR,
                value_col_name,
            )

            if result["success"]:
                logger.info(
                    f"Processed {result['new_records']} new records for {data_type} on {target_date}"
                )
                # Update progress tracker with new data analysis
                await self._update_progress_after_save(target_date, data_type)
            else:
                logger.error(
                    f"Error processing {data_type} data for {target_date}: {result['error']}"
                )
                self.progress_tracker.update_state(
                    target_date,
                    data_type,
                    last_error=result["error"],
                    last_updated=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                )

        except Exception as e:
            logger.error(f"Error processing {data_type} data for {target_date}: {e}")
            self.progress_tracker.update_state(
                target_date,
                data_type,
                last_error=str(e),
                last_updated=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            )

    async def _update_progress_after_save(self, target_date: date, data_type: str):
        """Update progress tracker with current file analysis using multiprocessing"""
        file_path = (
            self.config.DATA_DIR
            / f"{data_type}_{self.config.STATION_ID}_{target_date.isoformat()}.csv"
        )

        try:
            # Run file analysis in separate process
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                self.executor,
                analyze_file_worker,
                file_path,
                target_date.isoformat(),
                self._config_to_dict(),
            )

            if result["success"]:
                self.progress_tracker.update_state(
                    target_date,
                    data_type,
                    timestamp_gaps=result["timestamp_gaps"],
                    status=result["status"],
                    total_records=result["total_records"],
                    expected_records=1440,
                    last_updated=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    last_error=None,  # Clear any previous errors
                )
            else:
                logger.error(
                    f"Error analyzing {data_type} file for {target_date}: {result.get('error')}"
                )
                self.progress_tracker.update_state(
                    target_date,
                    data_type,
                    timestamp_gaps=result["timestamp_gaps"],
                    status=result["status"],
                    total_records=result["total_records"],
                    expected_records=1440,
                    last_updated=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    last_error=result.get("error"),
                )
        except Exception as e:
            logger.error(
                f"Error updating progress for {data_type} on {target_date}: {e}"
            )
            self.progress_tracker.update_state(
                target_date,
                data_type,
                last_error=str(e),
                last_updated=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            )

    async def add_null_records(self, time_range: TimeRange, target_date: date, data_type: str):
        """Add null records for genuinely missing API data during gap-filling"""
        # Generate timestamps for the gap
        missing_timestamps = []
        current_time = time_range.start
        while current_time <= time_range.end:
            missing_timestamps.append(current_time)
            current_time += timedelta(minutes=1)
        
        if not missing_timestamps:
            return
        
        # Create DataFrame with null values
        value_col_name = self.config.API_VALUE_COLS[data_type]
        null_df = pl.DataFrame({
            "timestamp": missing_timestamps,
            value_col_name: [None] * len(missing_timestamps)
        })
        
        # Save to CSV file
        file_path = self.config.DATA_DIR / f"{data_type}_{self.config.STATION_ID}_{target_date.isoformat()}.csv"
        
        if file_path.exists():
            # Merge with existing data
            existing = pl.read_csv(file_path, try_parse_dates=True)
            if existing.select("timestamp").dtypes[0] == pl.String:
                existing = existing.with_columns(
                    pl.col("timestamp").str.to_datetime().dt.replace_time_zone(None)
                )
            else:
                existing = existing.with_columns(
                    pl.col("timestamp").dt.replace_time_zone(None)
                )
            merged = (
                pl.concat([existing, null_df])
                .unique(subset=["timestamp"], keep="last")
                .sort("timestamp")
            )
            merged.write_csv(file_path)
            total_records = merged.height
        else:
            null_df.write_csv(file_path)
            total_records = null_df.height
        
        logger.info(f"Added {len(missing_timestamps)} null records for {data_type} on {target_date}")

    async def collect_data_for_range(
        self,
        client: httpx.AsyncClient,
        target_date: date,
        data_type: str,
        time_range: TimeRange,
        is_gap_filling: bool = False,
    ):
        """Collect data for a specific time range"""
        base_url = self.config.API_SOURCES[data_type]

        try:
            raw_data = await self.fetch_all_pages_for_range(
                client,
                base_url,
                time_range.start,
                time_range.end,
                target_date,
                data_type,
                is_gap_filling,
            )

            if raw_data:
                await self.process_and_save_data(raw_data, target_date, data_type)
            elif is_gap_filling:
                # API returned no data for gap-filling - add null records
                await self.add_null_records(time_range, target_date, data_type)

        except Exception as e:
            logger.error(
                f"Error collecting {data_type} data for {target_date} {time_range}: {e}"
            )
            self.progress_tracker.update_state(
                target_date,
                data_type,
                last_error=str(e),
                last_updated=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            )

    async def backfill_gaps(self, target_date: date, data_type: str):
        """Identify and fill gaps in data for a specific date/data_type"""
        file_path = (
            self.config.DATA_DIR
            / f"{data_type}_{self.config.STATION_ID}_{target_date.isoformat()}.csv"
        )

        if file_path.exists():
            try:
                df = pl.read_csv(file_path)
                gaps = self.gap_analyzer.detect_gaps(df, target_date)
            except Exception as e:
                logger.error(f"Error reading {file_path}: {e}")
                gaps = [
                    TimeRange(
                        datetime.combine(target_date, time(0, 0)),
                        datetime.combine(target_date, time(23, 59)),
                    )
                ]
        else:
            # File doesn't exist - collect entire day
            gaps = [
                TimeRange(
                    datetime.combine(target_date, time(0, 0)),
                    datetime.combine(target_date, time(23, 59)),
                )
            ]

        if gaps:
            logger.info(
                f"Found {len(gaps)} gaps for {data_type} on {target_date}: {[str(g) for g in gaps]}"
            )

            async with httpx.AsyncClient() as client:
                # Process all gaps in parallel
                gap_tasks = [
                    self.collect_data_for_range(
                        client, target_date, data_type, gap, is_gap_filling=True
                    )
                    for gap in gaps
                ]
                await asyncio.gather(*gap_tasks, return_exceptions=True)

    async def run_collection_pass(self, pass_name: str, target_dates: List[date]):
        """Run a single collection pass"""
        logger.info(f"Starting {pass_name} for {len(target_dates)} dates")

        semaphore = asyncio.Semaphore(self.config.MAX_CONCURRENT_DAYS)

        async def process_date(target_date: date):
            async with semaphore:
                # Process all data types in parallel for this date
                async def process_data_type(data_type: str):
                    if not self.gap_analyzer.is_complete(target_date, data_type):
                        await self.backfill_gaps(target_date, data_type)

                        # Update progress after collection attempt
                        await self._update_progress_after_save(target_date, data_type)

                        # Check if complete (quick check, don't wait for progress update)
                        await asyncio.sleep(0.01)  # Brief pause to let progress update start
                        if self.gap_analyzer.is_complete(target_date, data_type):
                            logger.info(
                                f"✅ {data_type} data complete for {target_date}"
                            )
                        else:
                            logger.warning(
                                f"⚠️  {data_type} data still incomplete for {target_date}"
                            )

                # Run all data types in parallel
                data_type_tasks = [
                    process_data_type(data_type) 
                    for data_type in self.config.API_SOURCES.keys()
                ]
                await asyncio.gather(*data_type_tasks, return_exceptions=True)

        # Process all dates concurrently
        tasks = [process_date(target_date) for target_date in target_dates]
        await asyncio.gather(*tasks, return_exceptions=True)

        logger.info(f"Completed {pass_name}")

    async def run_multi_pass_collection(self):
        """Run multi-pass data collection strategy with multiprocessing"""
        logger.info(
            "🚀 Starting multi-pass weather data collection with multiprocessing"
        )

        # Initialize ProcessPoolExecutor
        with ProcessPoolExecutor() as executor:
            self.executor = executor

            # Get all dates that need collection
            incomplete_dates = self.gap_analyzer.get_incomplete_dates()

            if not incomplete_dates:
                logger.info("✅ All data is already complete!")
                return

            logger.info(f"Found {len(incomplete_dates)} dates needing collection")

            # Pass 1: Initial collection
            await self.run_collection_pass(
                "Pass 1: Initial Collection", incomplete_dates
            )

            # Pass 2: Gap filling
            still_incomplete = self.gap_analyzer.get_incomplete_dates()
            if still_incomplete:
                logger.info(f"Pass 2 needed for {len(still_incomplete)} dates")
                await self.run_collection_pass("Pass 2: Gap Filling", still_incomplete)

            # Pass 3: Final validation
            final_incomplete = self.gap_analyzer.get_incomplete_dates()
            if final_incomplete:
                logger.warning(
                    f"⚠️  {len(final_incomplete)} dates remain incomplete after all passes"
                )
                for target_date in final_incomplete:
                    for data_type in self.config.API_SOURCES.keys():
                        if not self.gap_analyzer.is_complete(target_date, data_type):
                            file_path = (
                                self.config.DATA_DIR
                                / f"{data_type}_{self.config.STATION_ID}_{target_date.isoformat()}.csv"
                            )
                            if file_path.exists():
                                df = pl.read_csv(file_path)
                                gaps = self.gap_analyzer.detect_gaps(df, target_date)
                                logger.warning(
                                    f"  {data_type} {target_date}: {len(gaps)} gaps - {[str(g) for g in gaps]}"
                                )
            else:
                logger.info("🎉 All data collection complete!")

        # Clean up executor reference
        self.executor = None
