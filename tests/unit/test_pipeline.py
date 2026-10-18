from unittest.mock import Mock, call

from etl.pipeline import ETLPipeline


def test_pipeline_runs_steps_in_order():
    events = []

    raw_data = object()
    transformed_data = object()

    reader = Mock()
    transformer = Mock()
    writer = Mock()

    reader.read.side_effect = lambda: (
        events.append("read") or raw_data
    )

    transformer.transform.side_effect = lambda data: (
        events.append("transform") or transformed_data
    )

    writer.save.side_effect = lambda data: (
        events.append("save")
    )

    pipeline = ETLPipeline(
        reader=reader,
        transformer=transformer,
        writer=writer,
    )

    pipeline.run()

    assert events == ["read", "transform", "save"]

    reader.read.assert_called_once_with()
    transformer.transform.assert_called_once_with(raw_data)
    writer.save.assert_called_once_with(transformed_data)