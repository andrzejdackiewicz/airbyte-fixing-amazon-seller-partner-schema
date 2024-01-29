# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

from pathlib import Path

import pytest
from airbyte_protocol.models import ConnectorSpecification
from connector_acceptance_test import conftest
from connector_acceptance_test.tests.test_core import TestConnectorDocumentation as _TestConnectorDocumentation


@pytest.mark.parametrize(
    "connector_spec, docs_path, should_fail",
    (
            # SUCCESS: required field from spec exists in Prerequisites section
            (
                    {"required": ["start_date"], "properties": {"start_date": {"title": "Start Date"}}},
                    "data/docs/incorrect_not_all_structure.md",
                    False
            ),
            # FAIL: required field from spec does not exist in Prerequisites section
            (
                    {"required": ["access_token"], "properties": {"access_token": {"title": "Access Token"}}},
                    "data/docs/incorrect_not_all_structure.md",
                    True
            )
    )
)
def test_documentation_prerequisites_section(connector_spec, docs_path, should_fail):
    t = _TestConnectorDocumentation()
    docs_path = Path(__file__).parent / docs_path
    with open(docs_path, "r") as f:
        documentation = f.read().rstrip()

    if should_fail is True:
        with pytest.raises(AssertionError):
            t.test_prerequisites_content(ConnectorSpecification(connectionSpecification=connector_spec), documentation, docs_path)
    else:
        t.test_prerequisites_content(ConnectorSpecification(connectionSpecification=connector_spec), documentation, docs_path)


@pytest.mark.parametrize(
    "metadata, docs_path, should_fail",
    (
            # FAIL: Docs does not have required headers from standard template
            (
                    {"data": {"name": "GitHub"}},
                    "data/docs/incorrect_not_all_structure.md",
                    True,
            ),
            # SUCCESS: Docs follow standard template
            (
                    {"data": {"name": "GitHub"}},
                    "data/docs/correct.md",
                    False,
            ),
    )
)
def test_docs_structure_is_correct(mocker, metadata, docs_path, should_fail):
    mocker.patch.object(conftest.pytest, "fail")

    t = _TestConnectorDocumentation()

    docs_path = Path(__file__).parent / docs_path
    with open(docs_path, "r") as f:
        documentation = f.read().rstrip()

    t.test_docs_structure(documentation, metadata)

    if should_fail is True:
        conftest.pytest.fail.assert_called_once()
    else:
        conftest.pytest.fail.assert_not_called()


@pytest.mark.parametrize(
    "metadata, docs_path, should_fail",
    (
            # FAIL: Prerequisites section does not follow standard template
            (
                    {"data": {"name": "GitHub"}},
                    "data/docs/incorrect_not_all_structure.md",
                    True,
            ),
            # SUCCESS: Section descriptions follow standard template
            (
                    {"data": {"name": "GitHub"}},
                    "data/docs/correct.md",
                    False,
            ),
            # SUCCESS: Section descriptions follow standard template
            (
                    {"data": {"name": "GitHub"}},
                    "data/docs/correct.md",
                    False,
            ),
            # SUCCESS: Section descriptions follow standard template
            (
                    {"data": {"name": "GitHub"}},
                    "data/docs/correct_all_description_exist.md",
                    False,
            ),
    )
)
def test_docs_description(mocker, metadata, docs_path, should_fail):
    mocker.patch.object(conftest.pytest, "fail")

    t = _TestConnectorDocumentation()

    docs_path = Path(__file__).parent / docs_path
    with open(docs_path, "r") as f:
        documentation = f.read().rstrip()

    if should_fail is True:
        with pytest.raises(AssertionError):
            t.test_docs_descriptions(docs_path, documentation, metadata)
    else:
        t.test_docs_descriptions(docs_path, documentation, metadata)


@pytest.mark.parametrize(
    ("docs_path", "should_fail"),
    (
        (
            "data/docs/correct_all_description_exist.md",
            False,
        ),
        (
            "data/docs/invalid_links.md",
            True,
        ),
        (
            "data/docs/correct.md",
            False,
        ),
    )
)
def test_docs_urls(docs_path, should_fail):
    t = _TestConnectorDocumentation()

    with open(docs_path, "r") as f:
        documentation = f.read().rstrip()

    if should_fail is True:
        with pytest.raises(AssertionError):
            t.test_validate_links(documentation)
    else:
        t.test_validate_links(documentation)
