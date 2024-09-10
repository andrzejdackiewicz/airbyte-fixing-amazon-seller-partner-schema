# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
from io import BytesIO
from pathlib import Path
from unittest import TestCase

import requests
import requests_mock
from airbyte_cdk.sources.declarative.extractors import ResponseToFileExtractor


class ResponseToFileExtractorTest(TestCase):
    def setUp(self) -> None:
        self._extractor = ResponseToFileExtractor()
        self._http_mocker = requests_mock.Mocker()
        self._http_mocker.__enter__()

    def tearDown(self) -> None:
        self._http_mocker.__exit__(None, None, None)

    def test_compressed_response(self) -> None:
        response = self._mock_streamed_response_from_file(self._compressed_response_path())
        extracted_records = list(self._extractor.extract_records(response))
        assert len(extracted_records) == 24

    def test_text_response(self) -> None:
        response = self._mock_streamed_response_from_file(self._decompressed_response_path())
        extracted_records = list(self._extractor.extract_records(response))
        assert len(extracted_records) == 24

    def test_text_response_with_null_bytes(self) -> None:
        csv_with_null_bytes = '"FIRST_\x00NAME","LAST_NAME"\n"a first n\x00ame","a last na\x00me"\n'
        response = self._mock_streamed_response(BytesIO(csv_with_null_bytes.encode("utf-8")))

        extracted_records = list(self._extractor.extract_records(response))

        assert extracted_records == [{"FIRST_NAME": "a first name", "LAST_NAME": "a last name"}]

    def _test_folder_path(self) -> Path:
        return Path(__file__).parent.resolve()

    def _compressed_response_path(self) -> Path:
        return self._test_folder_path() / "compressed_response"

    def _decompressed_response_path(self) -> Path:
        return self._test_folder_path() / "decompressed_response.csv"

    def _mock_streamed_response_from_file(self, path: Path) -> requests.Response:
        with path.open("rb") as f:
            return self._mock_streamed_response(f)  # type: ignore  # Could not find the right typing for file io

    def _mock_streamed_response(self, io: BytesIO) -> requests.Response:
        any_url = "https://anyurl.com"
        self._http_mocker.register_uri("GET", any_url, [{"body": io, "status_code": 200}])
        return requests.get(any_url)
