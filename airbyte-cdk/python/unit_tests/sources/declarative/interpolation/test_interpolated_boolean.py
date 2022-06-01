#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

import pytest
from airbyte_cdk.sources.declarative.interpolation.interpolated_boolean import InterpolatedBoolean

config = {"parent": {"key_with_true": True}, "string_key": "compare_me"}


@pytest.mark.parametrize(
    "test_name, template, expected_result",
    [
        ("test_static_condition", True, True),
        ("test_interpolated_true_value", "{{ config['parent']['key_with_true'] }}", True),
        ("test_interpolated_true_comparison", "{{ config['string_key'] == \"compare_me\" }}", True),
        ("test_interpolated_false_condition", "{{ config['string_key'] == \"witness_me\" }}", False),
        ("test_path_has_value_returns_true", "{{ config['string_key'] }}", True),
        ("test_missing_key_defaults_to_false", "{{ path_to_nowhere }}", False),
    ],
)
def test_interpolated_boolean(test_name, template, expected_result):
    interpolated_bool = InterpolatedBoolean(template)
    assert interpolated_bool.eval(config) == expected_result
