
import re
from datetime import date


_PARTITION_PATTERN = re.compile(
    r"(?:^|/)year=(\d{4})/month=(\d{1,2})/day=(\d{1,2})(?:/|$)"
)


def extract_object_date(object_key: str) -> date | None:
    """Extract the date from year=YYYY/month=MM/day=DD in an S3 key."""
    match = _PARTITION_PATTERN.search(object_key)

    if not match:
        return None

    year, month, day = map(int, match.groups())

    try:
        return date(year, month, day)
    except ValueError:
        return None


def select_objects(
    objects: list[dict],
    processed: dict[str, str],
    start_date: str | None = None,
    end_date: str | None = None,
    ignore_checkpoint: bool = False,
) -> list[dict]:
    """Select S3 objects by optional date range and checkpoint status."""
    if bool(start_date) != bool(end_date):
        raise ValueError("start_date and end_date must be provided together")

    if ignore_checkpoint and not start_date:
        raise ValueError(
            "ignore_checkpoint requires an explicit date range"
        )

    start = date.fromisoformat(start_date) if start_date else None
    end = date.fromisoformat(end_date) if end_date else None

    if start and end and start > end:
        raise ValueError("start_date must be on or before end_date")

    selected = []

    for obj in objects:
        key = obj["object_key"]
        etag = obj["etag"]

        if start:
            object_date = extract_object_date(key)

            if object_date is None:
                continue

            if not start <= object_date <= end:
                continue

        if not ignore_checkpoint and processed.get(key) == etag:
            continue

        selected.append(obj)

    return selected
