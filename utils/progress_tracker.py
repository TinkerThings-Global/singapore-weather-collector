from .logger import Logger
from models import ProgressState
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Dict, Optional
import json

logger = Logger(name="ProgressTracker", filename="weather_data.log")


class ProgressTracker:
    """Manages persistent progress tracking across script runs"""

    def __init__(self, progress_file: Path):
        self.progress_file = progress_file
        self.progress: Dict[str, ProgressState] = {}
        self.load_progress()

    def _get_key(self, date: date, data_type: str) -> str:
        return f"{date.isoformat()}_{data_type}"

    def load_progress(self):
        """Load progress from JSON file"""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, "r") as f:
                    data = json.load(f)
                    self.progress = {
                        key: ProgressState(**value) for key, value in data.items()
                    }
                logger.info(f"Loaded progress for {len(self.progress)} tasks")
            except Exception as e:
                logger.warning(f"Could not load progress file: {e}")
                self.progress = {}

    def save_progress(self):
        """Save progress to JSON file"""
        try:
            data = {key: asdict(state) for key, state in self.progress.items()}
            with open(self.progress_file, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save progress: {e}")

    def update_state(self, date: date, data_type: str, **kwargs):
        """Update progress state for a date/data_type"""
        key = self._get_key(date, data_type)
        if key not in self.progress:
            self.progress[key] = ProgressState(
                date=date.isoformat(), data_type=data_type
            )

        for field, value in kwargs.items():
            if hasattr(self.progress[key], field):
                setattr(self.progress[key], field, value)

        self.save_progress()

    def get_state(self, date: date, data_type: str) -> Optional[ProgressState]:
        """Get progress state for a date/data_type"""
        key = self._get_key(date, data_type)
        return self.progress.get(key)

    def mark_complete(self, date: date, data_type: str):
        """Mark a date/data_type as complete"""
        self.update_state(date, data_type, status="complete")

    def reset_state(self, date: date, data_type: str):
        """Reset progress state for a date/data_type"""
        key = self._get_key(date, data_type)
        if key in self.progress:
            del self.progress[key]
        self.save_progress()
