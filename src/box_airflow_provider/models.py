from datetime import datetime

from pydantic import BaseModel

class BoxTriggerEventData(BaseModel):
    """Pydantic model for BoxTrigger event data."""
    status: str
    message: str
    files_sensed: list[tuple[str, str]] | None
    newer_than: datetime | str | None = None
    path: str
    file_pattern: str = ""
