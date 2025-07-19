from dataclasses import dataclass
from datetime import datetime


@dataclass
class TimeRange:
    """Represents a time range for data collection"""

    start: datetime
    end: datetime

    def __str__(self):
        # Handle timezone-aware datetime objects
        start_str = self.start.strftime("%H:%M")
        end_str = self.end.strftime("%H:%M")
        return f"{start_str} - {end_str}"
