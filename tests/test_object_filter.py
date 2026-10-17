
import pytest

from etl.object_filter import extract_object_date, select_objects


def test_extract_object_date():
    key = (
        "raw/binance/trades/year=2026/month=10/"
        "day=14/trades_001.jsonl"
    )
    assert str(extract_object_date(key)) == "2026-10-14"


def test_extract_object_date_invalid_key():
    assert extract_object_date(
        "raw/binance/trades/file.jsonl"
    ) is None


def test_select_only_objects_in_date_range():
    objects = [
        {
            "object_key": (
                "raw/binance/trades/year=2026/month=10/"
                "day=14/a.jsonl"
            ),
            "etag": "a",
        },
        {
            "object_key": (
                "raw/binance/trades/year=2026/month=10/"
                "day=15/b.jsonl"
            ),
            "etag": "b",
        },
    ]

    result = select_objects(
        objects=objects,
        processed={},
        start_date="2026-10-14",
        end_date="2026-10-14",
        ignore_checkpoint=True,
    )

    assert len(result) == 1
    assert result[0]["etag"] == "a"


def test_incremental_skips_unchanged_checkpointed_files():
    obj = {
        "object_key": (
            "raw/binance/trades/year=2026/month=10/"
            "day=14/a.jsonl"
        ),
        "etag": "same-etag",
    }

    result = select_objects(
        objects=[obj],
        processed={obj["object_key"]: "same-etag"},
    )

    assert result == []


def test_backfill_reselects_checkpointed_files():
    obj = {
        "object_key": (
            "raw/binance/trades/year=2026/month=10/"
            "day=14/a.jsonl"
        ),
        "etag": "same-etag",
    }

    result = select_objects(
        objects=[obj],
        processed={obj["object_key"]: "same-etag"},
        start_date="2026-10-14",
        end_date="2026-10-14",
        ignore_checkpoint=True,
    )

    assert result == [obj]


def test_rejects_invalid_date_range():
    with pytest.raises(ValueError):
        select_objects(
            objects=[],
            processed={},
            start_date="2026-10-15",
            end_date="2026-10-14",
        )


def test_cannot_ignore_checkpoint_without_date_range():
    with pytest.raises(ValueError):
        select_objects(
            objects=[],
            processed={},
            ignore_checkpoint=True,
        )
