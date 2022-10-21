import json

from requests import Response

from source_alpha_vantage.object_dpath_extractor import ObjectDpathExtractor


def _create_response() -> Response:
    response_body = {
        "data": {
            "2022-01-01": {
                "id": "id1",
                "name": "name1",
            },
            "2022-01-02": {
                "id": "id2",
                "name": "name2",
            },
        }
    }

    response = Response()
    response._content = json.dumps(response_body).encode("utf-8")
    return response


def test_no_key_injection():
    extractor = ObjectDpathExtractor(
        field_pointer=["data"],
        config={},
        options={},
    )

    response = _create_response()
    records = extractor.extract_records(response)

    assert records == [
        {
            "id": "id1",
            "name": "name1",
        },
        {
            "id": "id2",
            "name": "name2",
        },
    ]


def test_key_injection():
    extractor = ObjectDpathExtractor(
        field_pointer=["data"],
        inject_key_as_field="date",
        config={},
        options={},
    )

    response = _create_response()
    records = extractor.extract_records(response)

    assert records == [
        {
            "date": "2022-01-01",
            "id": "id1",
            "name": "name1",
        },
        {
            "date": "2022-01-02",
            "id": "id2",
            "name": "name2",
        },
    ]


def test_key_injection_with_interpolation():
    extractor = ObjectDpathExtractor(
        field_pointer=["data"],
        inject_key_as_field="{{ config['key_field'] }}",
        config={"key_field": "date"},
        options={},
    )

    response = _create_response()
    records = extractor.extract_records(response)

    assert records == [
        {
            "date": "2022-01-01",
            "id": "id1",
            "name": "name1",
        },
        {
            "date": "2022-01-02",
            "id": "id2",
            "name": "name2",
        },
    ]
