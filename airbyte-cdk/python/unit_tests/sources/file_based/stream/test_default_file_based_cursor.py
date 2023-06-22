#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest
from airbyte_cdk.sources.file_based.remote_file import RemoteFile
from airbyte_cdk.sources.file_based.stream.cursor.default_file_based_cursor import DATE_TIME_FORMAT, DefaultFileBasedCursor
from freezegun import freeze_time


@pytest.mark.parametrize(
    "files_to_add, expected_start_time, expected_state_dict",
    [
        pytest.param([
            RemoteFile(uri="a.csv",
                       last_modified=datetime.strptime("2021-01-01T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"),
                       file_type="csv"),
            RemoteFile(uri="b.csv",
                       last_modified=datetime.strptime("2021-01-02T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"),
                       file_type="csv"),
            RemoteFile(uri="c.csv",
                       last_modified=datetime.strptime("2020-12-31T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"),
                       file_type="csv")

        ],
            [datetime(2021, 1, 1),
             datetime(2021, 1, 1),
             datetime(2020, 12, 31)],
            {"history": {
                "a.csv": "2021-01-01T00:00:00.000000Z",
                "b.csv": "2021-01-02T00:00:00.000000Z",
                "c.csv": "2020-12-31T00:00:00.000000Z",
            }, },
            id="test_file_start_time_is_earliest_time_in_history"),
        pytest.param([
            RemoteFile(uri="a.csv",
                       last_modified=datetime.strptime("2021-01-01T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"),
                       file_type="csv"),
            RemoteFile(uri="b.csv",
                       last_modified=datetime.strptime("2021-01-02T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"),
                       file_type="csv"),
            RemoteFile(uri="c.csv",
                       last_modified=datetime.strptime("2021-01-03T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"),
                       file_type="csv"),
            RemoteFile(uri="d.csv",
                       last_modified=datetime.strptime("2021-01-04T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"),
                       file_type="csv"),

        ],
            [datetime(2021, 1, 1),
             datetime(2021, 1, 1),
             datetime(2021, 1, 1),
             datetime(2021, 1, 2)],
            {"history": {
                "b.csv": "2021-01-02T00:00:00.000000Z",
                "c.csv": "2021-01-03T00:00:00.000000Z",
                "d.csv": "2021-01-04T00:00:00.000000Z",
            }, },
            id="test_earliest_file_is_removed_from_history_if_history_is_full"),
        pytest.param([
            RemoteFile(uri="a.csv",
                       last_modified=datetime.strptime("2021-01-01T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"),
                       file_type="csv"),
            RemoteFile(uri="file_with_same_timestamp_as_b.csv",
                       last_modified=datetime.strptime("2021-01-02T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"),
                       file_type="csv"),
            RemoteFile(uri="b.csv",
                       last_modified=datetime.strptime("2021-01-02T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"),
                       file_type="csv"),
            RemoteFile(uri="c.csv",
                       last_modified=datetime.strptime("2021-01-03T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"),
                       file_type="csv"),
            RemoteFile(uri="d.csv",
                       last_modified=datetime.strptime("2021-01-04T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"),
                       file_type="csv"),

        ],
            [datetime(2021, 1, 1),
             datetime(2021, 1, 1),
             datetime(2021, 1, 1),
             datetime(2021, 1, 2),
             datetime(2021, 1, 2),
             ],
            {"history": {
                "file_with_same_timestamp_as_b.csv": "2021-01-02T00:00:00.000000Z",
                "c.csv": "2021-01-03T00:00:00.000000Z",
                "d.csv": "2021-01-04T00:00:00.000000Z",
            }, },
            id="test_files_are_sorted_by_timestamp_and_by_name"),
    ],
)
def test_add_file(files_to_add, expected_start_time, expected_state_dict):
    logger = MagicMock()
    cursor = DefaultFileBasedCursor(3, timedelta(days=3), logger)
    assert cursor._compute_start_time() == datetime.min

    for index, f in enumerate(files_to_add):
        cursor.add_file(f)
        assert expected_start_time[index] == cursor._compute_start_time()
    assert expected_state_dict == cursor.get_state()


@pytest.mark.parametrize("files, expected_files_to_sync, max_history_size, history_is_partial", [
    pytest.param([
        RemoteFile(uri="a.csv",
                   last_modified=datetime.strptime("2021-01-01T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"),
                   file_type="csv"),
        RemoteFile(uri="b.csv",
                   last_modified=datetime.strptime("2021-01-02T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"),
                   file_type="csv"),
        RemoteFile(uri="c.csv",
                   last_modified=datetime.strptime("2020-12-31T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"),
                   file_type="csv")
    ],
        [
            RemoteFile(uri="a.csv",
                       last_modified=datetime.strptime("2021-01-01T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"),
                       file_type="csv"),
            RemoteFile(uri="b.csv",
                       last_modified=datetime.strptime("2021-01-02T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"),
                       file_type="csv"),
            RemoteFile(uri="c.csv",
                       last_modified=datetime.strptime("2020-12-31T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"),
                       file_type="csv")
        ], 3, True, id="test_all_files_should_be_synced"),
    pytest.param([
        RemoteFile(uri="a.csv",
                   last_modified=datetime.strptime("2021-01-01T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"),
                   file_type="csv"),
        RemoteFile(uri="b.csv",
                   last_modified=datetime.strptime("2021-01-02T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"),
                   file_type="csv"),
        RemoteFile(uri="c.csv",
                   last_modified=datetime.strptime("2020-12-31T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"),
                   file_type="csv")
    ], [
        RemoteFile(uri="a.csv",
                   last_modified=datetime.strptime("2021-01-01T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"),
                   file_type="csv"),
        RemoteFile(uri="b.csv",
                   last_modified=datetime.strptime("2021-01-02T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"),
                   file_type="csv"),
        RemoteFile(uri="c.csv",
                   last_modified=datetime.strptime("2020-12-31T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"),
                   file_type="csv")

    ], 2, True, id="test_sync_more_files_than_history_size"),
])
def test_get_files_to_sync(files, expected_files_to_sync, max_history_size, history_is_partial):
    logger = MagicMock()
    cursor = DefaultFileBasedCursor(max_history_size, timedelta(days=3), logger)

    files_to_sync = list(cursor.get_files_to_sync(files))
    for f in files_to_sync:
        cursor.add_file(f)

    assert files_to_sync == expected_files_to_sync
    assert cursor.is_history_partial() == history_is_partial


@freeze_time("2023-06-16T00:00:00Z")
def test_only_recent_files_are_synced_if_history_is_full():
    logger = MagicMock()
    cursor = DefaultFileBasedCursor(2, timedelta(days=3), logger)

    files_in_history = [
        RemoteFile(uri="b1.csv", last_modified=datetime(2021, 1, 2), file_type="csv"),
        RemoteFile(uri="b2.csv", last_modified=datetime(2021, 1, 3), file_type="csv"),
    ]

    state = {
        "history": {
            f.uri: f.last_modified.strftime(DATE_TIME_FORMAT) for f in files_in_history
        },
    }
    cursor.set_initial_state(state)

    files = [
        RemoteFile(uri="a.csv", last_modified=datetime(2021, 1, 1), file_type="csv"),
        RemoteFile(uri="c.csv", last_modified=datetime(2021, 1, 2), file_type="csv"),
        RemoteFile(uri="d.csv", last_modified=datetime(2021, 1, 4), file_type="csv"),
    ]

    expected_files_to_sync = [
        RemoteFile(uri="c.csv", last_modified=datetime(2021, 1, 2), file_type="csv"),
        RemoteFile(uri="d.csv", last_modified=datetime(2021, 1, 4), file_type="csv"),
    ]

    files_to_sync = list(cursor.get_files_to_sync(files))
    assert files_to_sync == expected_files_to_sync
    logger.warning.assert_called_once()


@pytest.mark.parametrize("modified_at_delta, should_sync_file", [
    pytest.param(timedelta(days=-1), False, id="test_modified_at_is_earlier"),
    pytest.param(timedelta(days=0), False, id="test_modified_at_is_equal"),
    pytest.param(timedelta(days=1), True, id="test_modified_at_is_more_recent"),
])
def test_sync_file_already_present_in_history(modified_at_delta, should_sync_file):
    logger = MagicMock()
    cursor = DefaultFileBasedCursor(2, timedelta(days=3), logger)
    original_modified_at = datetime(2021, 1, 2)
    filename = "a.csv"
    files_in_history = [
        RemoteFile(uri=filename, last_modified=original_modified_at, file_type="csv"),
    ]

    state = {
        "history": {
            f.uri: f.last_modified.strftime(DATE_TIME_FORMAT) for f in files_in_history
        },
    }
    cursor.set_initial_state(state)

    files = [
        RemoteFile(uri=filename, last_modified=original_modified_at + modified_at_delta, file_type="csv"),
    ]

    files_to_sync = list(cursor.get_files_to_sync(files))
    assert bool(files_to_sync) == should_sync_file


@freeze_time("2023-06-06T00:00:00Z")
@pytest.mark.parametrize(
    "file_name, last_modified, earliest_dt_in_history, should_sync_file", [
        pytest.param("a.csv", datetime(2023, 6, 3), datetime(2023, 6, 6), True, id="test_last_modified_is_equal_to_time_buffer"),
        pytest.param("a.csv", datetime(2023, 6, 3), datetime(2023, 6, 3), False, id="test_last_modified_is_equal_to_earliest_dt_in_history_and_lexicographically_smaller"),
        pytest.param("c.csv", datetime(2023, 6, 3), datetime(2023, 6, 3), True, id="test_last_modified_is_equal_to_earliest_dt_in_history_and_lexicographically_greater"),
    ]
)
def test_should_sync_file(file_name, last_modified, earliest_dt_in_history, should_sync_file):
    logger = MagicMock()
    cursor = DefaultFileBasedCursor(1, timedelta(days=3), logger)

    cursor.add_file(RemoteFile(uri="b.csv", last_modified=earliest_dt_in_history, file_type="csv"))
    cursor._start_time = cursor._compute_start_time()

    assert bool(list(cursor.get_files_to_sync([RemoteFile(uri=file_name, last_modified=last_modified, file_type="csv")]))) == should_sync_file
