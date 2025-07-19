from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict


@dataclass
class Config:
    """Configuration for weather data collection"""

    DATA_DIR: Path = Path("weather_data_improved")
    PROGRESS_FILE: Path = Path("fetch_progress.json")
    STATION_ID: str = "S50"
    FIRST_DATE: date = date(2025, 6, 30)
    API_SOURCES: Dict[str, str] = None
    API_VALUE_COLS: Dict[str, str] = None
    MAX_CONCURRENT_DAYS: int = 4
    MAX_CONCURRENT_REQUESTS: int = 5
    MAX_RETRIES: int = 5
    BASE_RETRY_DELAY: float = 1.0
    MAX_RETRY_DELAY: float = 60.0

    def __post_init__(self):
        if self.API_SOURCES is None:
            self.API_SOURCES = {
                "temp": "https://api-open.data.gov.sg/v2/real-time/api/air-temperature",
                "rh": "https://api-open.data.gov.sg/v2/real-time/api/relative-humidity",
            }
        if self.API_VALUE_COLS is None:
            self.API_VALUE_COLS = {"temp": "temperature", "rh": "relative_humidity"}
