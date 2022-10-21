#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import inflection
import pytest
from source_acceptance_test.config import Config


@pytest.mark.usefixtures("inputs")
class BaseTest:
    @classmethod
    def config_key(cls):
        """Name of the test in configuration file, used to override test inputs,"""
        class_name = cls.__name__
        if class_name.startswith("Test"):
            class_name = class_name[len("Test") :]
        return inflection.underscore(class_name)

    MANDATORY_FOR_TEST_MODES = Config.TestMode.strict
