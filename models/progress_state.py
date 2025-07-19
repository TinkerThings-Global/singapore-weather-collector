from dataclasses import dataclass
from typing import Optional


@dataclass
class ProgressState:
    """Tracks data completeness for a specific date/data_type combination"""

    date: str
    data_type: str
    timestamp_gaps: Optional[list[list[str]]] = None
    status: str = "incomplete"
    last_error: Optional[str] = None
    total_records: int = 0
    expected_records: int = 1440
    last_updated: Optional[str] = None
