#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

import datetime
import time
from abc import ABC
from http import HTTPStatus
from unittest.mock import MagicMock

import pytest
import requests
from airbyte_cdk.models import SyncMode
from source_okta.source import (
    CustomRoles,
    GroupMembers,
    GroupRoleAssignments,
    Groups,
    IncrementalOktaStream,
    Logs,
    OktaStream,
    UserRoleAssignments,
    Users,
)


@pytest.fixture
def patch_base_class(mocker):
    """
    Base patcher for used streams
    """
    mocker.patch.object(OktaStream, "path", "v0/example_endpoint")
    mocker.patch.object(OktaStream, "primary_key", "test_primary_key")
    mocker.patch.object(OktaStream, "__abstractmethods__", set())
    mocker.patch.object(IncrementalOktaStream, "path", "v0/example_endpoint")
    mocker.patch.object(IncrementalOktaStream, "primary_key", "test_primary_key")
    mocker.patch.object(IncrementalOktaStream, "__abstractmethods__", set())


class TestStatusCodes:
    @pytest.mark.parametrize(
        ("http_status", "should_retry"),
        [
            (HTTPStatus.OK, False),
            (HTTPStatus.BAD_REQUEST, False),
            (HTTPStatus.TOO_MANY_REQUESTS, True),
            (HTTPStatus.INTERNAL_SERVER_ERROR, True),
        ],
    )
    def test_should_retry(self, patch_base_class, http_status, should_retry, url_base):
        response_mock = MagicMock()
        response_mock.status_code = http_status
        stream = OktaStream(url_base=url_base)
        assert stream.should_retry(response_mock) == should_retry


class TestOktaStream:
    def test_okta_stream_request_params(self, patch_base_class, url_base):
        stream = OktaStream(url_base=url_base)
        inputs = {"stream_slice": None, "stream_state": None, "next_page_token": None}
        expected_params = {"limit": 200}
        assert stream.request_params(**inputs) == expected_params

    def test_okta_stream_parse_response(self, patch_base_class, requests_mock, url_base, api_url):
        stream = OktaStream(url_base=url_base)
        requests_mock.get(f"{api_url}", json=[{"a": 123}, {"b": "xx"}])
        resp = requests.get(f"{api_url}")
        inputs = {"response": resp, "stream_state": MagicMock()}
        expected_parsed_object = [{"a": 123}, {"b": "xx"}]
        assert list(stream.parse_response(**inputs)) == expected_parsed_object

    def test_okta_stream_backoff_time(self, patch_base_class, url_base):
        response_mock = requests.Response()
        stream = OktaStream(url_base=url_base)
        expected_backoff_time = None
        assert stream.backoff_time(response_mock) == expected_backoff_time

    def test_okta_stream_incremental_request_params(self, patch_base_class, url_base):
        stream = IncrementalOktaStream(url_base=url_base)
        inputs = {"stream_slice": None, "stream_state": None, "next_page_token": None}
        expected_params = {"limit": 200}
        assert stream.request_params(**inputs) == expected_params

    def test_incremental_okta_stream_parse_response(self, patch_base_class, requests_mock, url_base, api_url):
        stream = IncrementalOktaStream(url_base=url_base)
        requests_mock.get(f"{api_url}", json=[{"a": 123}, {"b": "xx"}])
        resp = requests.get(f"{api_url}")
        inputs = {"response": resp, "stream_state": MagicMock()}
        expected_parsed_object = [{"a": 123}, {"b": "xx"}]
        assert list(stream.parse_response(**inputs)) == expected_parsed_object

    def test_incremental_okta_stream_backoff_time(self, patch_base_class, url_base):
        response_mock = MagicMock()
        stream = IncrementalOktaStream(url_base=url_base)
        expected_backoff_time = None
        assert stream.backoff_time(response_mock) == expected_backoff_time

    def test_okta_stream_incremental_backoff_time_empty(self, patch_base_class, url_base):
        stream = IncrementalOktaStream(url_base=url_base)
        response = MagicMock(requests.Response)
        response.status_code = 200
        expected_params = None
        inputs = {"response": response}
        assert stream.backoff_time(**inputs) == expected_params

    def test_okta_stream_incremental_back_off_now(self, patch_base_class, url_base):
        stream = IncrementalOktaStream(url_base=url_base)
        response = MagicMock(requests.Response)
        response.status_code = requests.codes.TOO_MANY_REQUESTS
        response.headers = {"x-rate-limit-reset": int(time.time())}
        expected_params = (0, 2)
        inputs = {"response": response}
        get_backoff_time = stream.backoff_time(**inputs)
        assert expected_params[0] <= get_backoff_time <= expected_params[1]

    def test_okta_stream_incremental_get_updated_state(self, patch_base_class, latest_record_instance, url_base):
        class TestIncrementalOktaStream(IncrementalOktaStream, ABC):
            def __init__(self, url_base: str, *args, **kwargs):
                super().__init__(url_base, *args, **kwargs)
                self._cursor_field = None

            @property
            def cursor_field(self) -> str:
                return self._cursor_field

        stream = TestIncrementalOktaStream(url_base=url_base)
        stream._cursor_field = "lastUpdated"

        current_stream_state = {"lastUpdated": "2021-04-21T21:03:55.000Z"}
        update_state = stream.get_updated_state(current_stream_state=current_stream_state, latest_record=latest_record_instance)
        expected_result = {"lastUpdated": "2022-07-18T07:58:11.000Z"}
        assert update_state == expected_result

    def test_okta_stream_http_method(self, patch_base_class, url_base):
        stream = OktaStream(url_base=url_base)
        expected_method = "GET"
        assert stream.http_method == expected_method


class TestNextPageToken:
    def test_next_page_token(self, patch_base_class, users_instance, url_base, api_url):
        stream = OktaStream(url_base=url_base)
        response = MagicMock(requests.Response)
        response.links = {"next": {"url": f"{api_url}?param1=test_value1&param2=test_value2"}}
        inputs = {"response": response}
        expected_token = {"param1": "test_value1", "param2": "test_value2"}
        result = stream.next_page_token(**inputs)
        assert result == expected_token

    def test_next_page_token_empty_params(self, patch_base_class, users_instance, url_base, api_url):
        stream = OktaStream(url_base=url_base)
        response = MagicMock(requests.Response)
        response.links = {"next": {"url": f"{api_url}"}}
        inputs = {"response": response}
        expected_token = {}
        result = stream.next_page_token(**inputs)
        assert result == expected_token

    def test_next_page_token_link_have_self_and_equal_next(self, patch_base_class, users_instance, url_base, api_url):
        stream = OktaStream(url_base=url_base)
        response = MagicMock(requests.Response)
        response.links = {"next": {"url": f"{api_url}"}, "self": {"url": f"{api_url}"}}
        inputs = {"response": response}
        expected_token = None
        result = stream.next_page_token(**inputs)
        assert result == expected_token


class TestStreamUsers:

    # Users
    def test_stream_users(self, requests_mock, patch_base_class, users_instance, url_base, api_url):
        stream = Users(url_base=url_base)
        record = users_instance
        requests_mock.get(f"{api_url}/users", json=[record])
        inputs = {"sync_mode": SyncMode.incremental}
        assert list(stream.read_records(**inputs)) == [record]

    def test_users_request_params_out_of_next_page_token(self, patch_base_class, url_base):
        stream = Users(url_base=url_base)
        inputs = {"stream_slice": None, "stream_state": None, "next_page_token": None}
        expected_params = {"limit": 200}
        assert stream.request_params(**inputs) == expected_params

    def test_users_source_request_params_have_next_cursor(self, patch_base_class, url_base):
        stream = Users(url_base=url_base)
        inputs = {"stream_slice": None, "stream_state": None, "next_page_token": {"next_cursor": "123"}}
        expected_params = {"limit": 200, "next_cursor": "123"}
        assert stream.request_params(**inputs) == expected_params

    def test_users_source_request_params_have_latest_entry(self, patch_base_class, url_base):
        stream = Users(url_base=url_base)
        inputs = {"stream_slice": None, "stream_state": {"lastUpdated": "some_date"}, "next_page_token": {"next_cursor": "123"}}
        expected_params = {"limit": 200, "next_cursor": "123", "filter": 'lastUpdated gt "some_date"'}
        assert stream.request_params(**inputs) == expected_params

    def test_users_source_parse_response(self, requests_mock, patch_base_class, users_instance, url_base, api_url):
        stream = Users(url_base=url_base)
        expected_params = users_instance
        requests_mock.get(f"{api_url}", json=[users_instance])
        assert list(stream.parse_response(response=requests.get(f"{api_url}"))) == [expected_params]


class TestStreamCustomRoles:
    def test_custom_roles(self, requests_mock, patch_base_class, custom_role_instance, url_base, api_url):
        stream = CustomRoles(url_base=url_base)
        record = {"roles": [custom_role_instance]}
        requests_mock.get(f"{api_url}/iam/roles?limit=200", json=record)
        inputs = {"sync_mode": SyncMode.incremental}
        assert list(stream.read_records(**inputs)) == record["roles"]

    def test_custom_roles_parse_response(self, requests_mock, patch_base_class, custom_role_instance, url_base, api_url):
        stream = CustomRoles(url_base=url_base)
        record = {"roles": [custom_role_instance]}
        expected_params = [custom_role_instance]
        requests_mock.get(f"{api_url}", json=record)
        r = requests.get(f"{api_url}")
        assert list(stream.parse_response(response=r)) == expected_params


class TestStreamGroups:
    def test_groups(self, requests_mock, patch_base_class, groups_instance, url_base, api_url):
        stream = Groups(url_base=url_base)
        record = [groups_instance]
        requests_mock.get(f"{api_url}/groups?limit=200", json=record)
        inputs = {"sync_mode": SyncMode.incremental}
        assert list(stream.read_records(**inputs)) == record

    def test_groups_parse_response(self, requests_mock, patch_base_class, groups_instance, url_base, api_url):
        stream = Groups(url_base=url_base)
        record = [groups_instance]
        expected_params = [groups_instance]
        requests_mock.get(f"{api_url}", json=record)
        r = requests.get(f"{api_url}")
        assert list(stream.parse_response(response=r)) == expected_params


class TestStreamGroupMembers:
    def test_group_members(self, requests_mock, patch_base_class, group_members_instance, url_base, api_url):
        stream = GroupMembers(url_base=url_base)
        group_id = "test_group_id"
        record = [group_members_instance]
        requests_mock.get(f"{api_url}/groups/{group_id}/users?limit=200", json=record)
        inputs = {"sync_mode": SyncMode.incremental, "stream_state": {}, "stream_slice": {"group_id": group_id}}
        assert list(stream.read_records(**inputs)) == record

    def test_group_members_parse_response(self, requests_mock, patch_base_class, group_members_instance, url_base, api_url):
        stream = GroupMembers(url_base=url_base)
        record = [group_members_instance]
        expected_params = [group_members_instance]
        requests_mock.get(f"{api_url}", json=record)
        r = requests.get(f"{api_url}")
        assert list(stream.parse_response(response=r)) == expected_params

    def test_group_members_request_params_with_latest_entry(self, patch_base_class, group_members_instance, url_base):
        stream = GroupMembers(url_base=url_base)
        inputs = {
            "stream_slice": {"group_id": "some_group"},
            "stream_state": {"id": "some_test_id"},
            "next_page_token": {"next_cursor": "123"},
        }
        expected_params = {"limit": 200, "next_cursor": "123", "after": "some_test_id"}
        assert stream.request_params(**inputs) == expected_params

    def test_group_members_slice_stream(self, requests_mock, patch_base_class, group_members_instance, groups_instance, url_base, api_url):
        stream = GroupMembers(url_base=url_base)
        requests_mock.get(f"{api_url}/groups?limit=200", json=[groups_instance])
        expected_params = [{"group_id": "test_group_id"}]
        assert list(stream.stream_slices()) == expected_params

    def test_group_member_request_get_update_state(self, latest_record_instance, url_base):
        stream = GroupMembers(url_base=url_base)
        stream._cursor_field = "id"
        current_stream_state = {"id": "test_user_group_id"}
        update_state = stream.get_updated_state(current_stream_state=current_stream_state, latest_record=latest_record_instance)
        expected_result = {"id": "test_user_group_id"}
        assert update_state == expected_result


class TestStreamGroupRoleAssignment:
    def test_group_role_assignments(self, requests_mock, patch_base_class, group_role_assignments_instance, url_base, api_url):
        stream = GroupRoleAssignments(url_base=url_base)
        record = [group_role_assignments_instance]
        group_id = "test_group_id"
        mock_address = f"{api_url}/groups/{group_id}/roles?limit=200"
        requests_mock.get(mock_address, json=record)
        inputs = {"sync_mode": SyncMode.full_refresh, "stream_state": {}, "stream_slice": {"group_id": group_id}}
        assert list(stream.read_records(**inputs)) == record

    def test_group_role_assignments_parse_response(
        self, requests_mock, patch_base_class, group_role_assignments_instance, url_base, api_url
    ):
        stream = GroupRoleAssignments(url_base=url_base)
        record = [group_role_assignments_instance]
        expected_params = [group_role_assignments_instance]
        requests_mock.get(f"{api_url}", json=record)
        r = requests.get(f"{api_url}")
        assert list(stream.parse_response(response=r)) == expected_params

    def test_group_role_assignments_slice_stream(
        self, requests_mock, patch_base_class, group_members_instance, groups_instance, url_base, api_url
    ):
        stream = GroupRoleAssignments(url_base=url_base)
        requests_mock.get(f"{api_url}/groups?limit=200", json=[groups_instance])
        expected_params = [{"group_id": "test_group_id"}]
        assert list(stream.stream_slices()) == expected_params


class TestStreamLogs:
    def test_logs(self, requests_mock, patch_base_class, logs_instance, url_base, api_url):
        stream = Logs(url_base=url_base)
        record = [logs_instance]
        requests_mock.get(f"{api_url}/logs?limit=200", json=record)
        inputs = {"sync_mode": SyncMode.incremental}
        assert list(stream.read_records(**inputs)) == record

    def test_logs_parse_response(self, requests_mock, patch_base_class, logs_instance, url_base, api_url):
        stream = Logs(url_base=url_base)
        record = [logs_instance]
        expected_params = [logs_instance]
        requests_mock.get(f"{api_url}/logs?limit=200", json=record)
        r = requests.get(f"{api_url}/logs?limit=200")
        assert list(stream.parse_response(response=r)) == expected_params

    def test_logs_request_params_for_since(self, patch_base_class, logs_instance, url_base):
        stream = Logs(url_base=url_base)
        inputs = {"stream_state": {"published": "2022-07-19T15:54:11.545Z"}, "stream_slice": None}
        expected_params = {"limit": 200, "since": "2022-07-19T15:54:11.545Z"}
        assert stream.request_params(**inputs) == expected_params

    def test_logs_request_params_for_until(self, patch_base_class, logs_instance, url_base):
        stream = Logs(url_base=url_base)
        testing_date = datetime.datetime.utcnow() + datetime.timedelta(days=10)
        inputs = {"stream_state": {"published": testing_date.isoformat()}, "stream_slice": None}
        expected_params = {"limit": 200, "since": testing_date.isoformat(), "until": testing_date.isoformat()}
        assert stream.request_params(**inputs) == expected_params


class TestStreamUserRoleAssignment:
    def test_user_role_assignments(self, requests_mock, patch_base_class, user_role_assignments_instance, url_base, api_url):
        stream = UserRoleAssignments(url_base=url_base)
        record = [user_role_assignments_instance]
        user_id = "test_user_id"
        mock_address = f"{api_url}/users/{user_id}/roles?limit=200"
        requests_mock.get(mock_address, json=record)
        inputs = {"sync_mode": SyncMode.full_refresh, "stream_state": {}, "stream_slice": {"user_id": user_id}}
        assert list(stream.read_records(**inputs)) == record

    def test_user_role_assignments_parse_response(self, requests_mock, patch_base_class, user_role_assignments_instance, url_base, api_url):
        stream = UserRoleAssignments(url_base=url_base)
        record = [user_role_assignments_instance]
        expected_params = [user_role_assignments_instance]
        requests_mock.get(f"{api_url}", json=record)
        r = requests.get(f"{api_url}")
        assert list(stream.parse_response(response=r)) == expected_params

    def test_user_role_assignments_slice_stream(
        self, requests_mock, patch_base_class, group_members_instance, users_instance, url_base, api_url
    ):
        stream = UserRoleAssignments(url_base=url_base)
        requests_mock.get(f"{api_url}/users?limit=200", json=[users_instance])
        expected_params = [{"user_id": "test_user_id"}]
        assert list(stream.stream_slices()) == expected_params
