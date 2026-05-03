from dataclasses import dataclass
from datetime import datetime

@dataclass
class BoxTriggerEventData:
    """Model for BoxTrigger event data."""
    status: str
    message: str
    path: str
    files_sensed: list[tuple[str, str]] | None
    newer_than: datetime | str | None = None
    file_pattern: str = ""
