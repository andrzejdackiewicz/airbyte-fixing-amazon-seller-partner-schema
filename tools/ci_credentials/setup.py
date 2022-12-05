#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from setuptools import find_packages, setup

MAIN_REQUIREMENTS = ["requests", "ci_common_utils", "click~=8.1.3"]

TEST_REQUIREMENTS = ["requests-mock"]

setup(
    version="1.0.0",
    name="ci_credentials",
    description="Load and extract CI secrets for test suites",
    author="Airbyte",
    author_email="contact@airbyte.io",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    python_requires=">=3.9",
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },
    entry_points={
        "console_scripts": [
            "ci_credentials = ci_credentials.main:ci_credentials",
        ],
    },
)
