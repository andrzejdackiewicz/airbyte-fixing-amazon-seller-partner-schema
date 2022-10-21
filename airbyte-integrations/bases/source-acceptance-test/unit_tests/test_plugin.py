#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
import pytest
from source_acceptance_test import config, plugin

STRICT_TEST_MODE = config.Config.TestMode.strict
MEDIUM_TEST_MODE = config.Config.TestMode.medium
LIGHT_TEST_MODE = config.Config.TestMode.light

PARAMETRIZE_ACTION = plugin.TestAction.PARAMETRIZE
SKIP_ACTION = plugin.TestAction.SKIP
FAIL_ACTION = plugin.TestAction.FAIL


class MyTestClass:
    def dumb_test_function(self):
        assert 2 > 1


@pytest.mark.parametrize(
    "TestClass, test_class_mandatory_for_test_modes, global_test_mode, test_configuration, expected_action, expected_reason",
    [
        pytest.param(
            MyTestClass,
            (STRICT_TEST_MODE),
            STRICT_TEST_MODE,
            None,
            FAIL_ACTION,
            "MyTestClass.dumb_test_function failed: it was not configured but must be according to the current strict test mode.",
            id="Discovered test is mandatory in strict mode, we're in strict mode, it was not configured: FAIL",
        ),
        pytest.param(
            MyTestClass,
            (STRICT_TEST_MODE),
            LIGHT_TEST_MODE,
            None,
            SKIP_ACTION,
            "Skipping MyTestClass.dumb_test_function: not found in the config.",
            id="Discovered test is mandatory in strict mode, we are in light mode, it is not configured: SKIP",
        ),
        pytest.param(
            MyTestClass,
            set(),
            STRICT_TEST_MODE,
            None,
            SKIP_ACTION,
            "Skipping MyTestClass.dumb_test_function: not found in the config.",
            id="Discovered test is not mandatory in any mode, it was not configured: SKIP",
        ),
        pytest.param(
            MyTestClass,
            (STRICT_TEST_MODE),
            STRICT_TEST_MODE,
            config.GenericTestConfig(bypass_reason="A good reason."),
            SKIP_ACTION,
            "Skipping MyTestClass.dumb_test_function: A good reason.",
            id="Discovered test is mandatory in strict mode, a bypass reason was provided: SKIP",
        ),
        pytest.param(
            MyTestClass,
            (STRICT_TEST_MODE),
            LIGHT_TEST_MODE,
            config.GenericTestConfig(bypass_reason="A good reason."),
            SKIP_ACTION,
            "Skipping MyTestClass.dumb_test_function: A good reason.",
            id="Discovered test is mandatory in strict mode, we are in light mode, a bypass reason was provided: SKIP (with bypass reason shown)",
        ),
        pytest.param(
            MyTestClass,
            (MEDIUM_TEST_MODE, STRICT_TEST_MODE),
            LIGHT_TEST_MODE,
            None,
            SKIP_ACTION,
            "Skipping MyTestClass.dumb_test_function: not found in the config.",
            id="Discovered test is mandatory in medium and strict mode, we're in light mode, it was not configured: SKIP",
        ),
        pytest.param(
            MyTestClass,
            (STRICT_TEST_MODE),
            STRICT_TEST_MODE,
            config.GenericTestConfig(tests=[config.SpecTestConfig()]),
            PARAMETRIZE_ACTION,
            "Parametrize MyTestClass.dumb_test_function: tests are configured.",
            id="[Strict mode] Discovered test is configured: PARAMETRIZE",
        ),
        pytest.param(
            MyTestClass,
            (STRICT_TEST_MODE),
            LIGHT_TEST_MODE,
            config.GenericTestConfig(tests=[config.SpecTestConfig()]),
            PARAMETRIZE_ACTION,
            "Parametrize MyTestClass.dumb_test_function: tests are configured.",
            id="[Light mode] Discovered test is configured: PARAMETRIZE",
        ),
        pytest.param(
            MyTestClass,
            (STRICT_TEST_MODE),
            MEDIUM_TEST_MODE,
            config.GenericTestConfig(tests=[config.SpecTestConfig()]),
            PARAMETRIZE_ACTION,
            "Parametrize MyTestClass.dumb_test_function: tests are configured.",
            id="[Medium mode] Discovered test is configured: PARAMETRIZE",
        ),
        pytest.param(
            MyTestClass,
            set(),
            LIGHT_TEST_MODE,
            config.GenericTestConfig(tests=[config.SpecTestConfig()]),
            PARAMETRIZE_ACTION,
            "Parametrize MyTestClass.dumb_test_function: tests are configured.",
            id="[Light mode] Discovered test is configured, test is not mandatory in any mode: PARAMETRIZE",
        ),
    ],
)
def test_parametrize_skip_or_fail(
    TestClass, test_class_mandatory_for_test_modes, global_test_mode, test_configuration, expected_action, expected_reason
):
    TestClass.MANDATORY_FOR_TEST_MODES = test_class_mandatory_for_test_modes
    test_action, reason = plugin.parametrize_skip_or_fail(TestClass, TestClass.dumb_test_function, global_test_mode, test_configuration)
    assert (test_action, reason) == (expected_action, expected_reason)
