#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from airbyte_cdk.models import SyncMode
from pytest import fixture
from source_convex.source import ConvexStream


@fixture
def patch_incremental_base_class(mocker):
    # Mock abstract methods to enable instantiating abstract class
    mocker.patch.object(ConvexStream, "path", "v0/example_endpoint")
    mocker.patch.object(ConvexStream, "primary_key", "test_primary_key")
    mocker.patch.object(ConvexStream, "__abstractmethods__", set())


def test_cursor_field(patch_incremental_base_class):
    stream = ConvexStream("murky-swan-635", "accesskey", "messages", None)
    expected_cursor_field = "_ts"
    assert stream.cursor_field == expected_cursor_field


def test_get_updated_state(patch_incremental_base_class):
    stream = ConvexStream("murky-swan-635", "accesskey", "messages", None)
    # TODO: replace this with your input parameters
    inputs = {"current_stream_state": None, "latest_record": None}
    # TODO: replace this with your expected updated stream state
    expected_state = {}
    assert stream.get_updated_state(**inputs) == expected_state


def test_stream_slices(patch_incremental_base_class):
    stream = ConvexStream("murky-swan-635", "accesskey", "messages", None)
    inputs = {"sync_mode": SyncMode.incremental, "cursor_field": [], "stream_state": {}}
    expected_stream_slice = [None]
    assert stream.stream_slices(**inputs) == expected_stream_slice


def test_supports_incremental(patch_incremental_base_class, mocker):
    mocker.patch.object(ConvexStream, "cursor_field", "dummy_field")
    stream = ConvexStream("murky-swan-635", "accesskey", "messages", None)
    assert stream.supports_incremental


def test_source_defined_cursor(patch_incremental_base_class):
    stream = ConvexStream("murky-swan-635", "accesskey", "messages", None)
    assert stream.source_defined_cursor


def test_stream_checkpoint_interval(patch_incremental_base_class):
    stream = ConvexStream("murky-swan-635", "accesskey", "messages", None)
    expected_checkpoint_interval = 128
    assert stream.state_checkpoint_interval == expected_checkpoint_interval
