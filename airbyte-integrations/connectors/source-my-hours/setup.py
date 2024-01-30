#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from setuptools import find_packages, setup

MAIN_REQUIREMENTS = [
    "airbyte-cdk",
]

TEST_REQUIREMENTS = [
    "requests-mock~=1.9.3",
    "pytest~=6.1",
    "pytest-mock~=3.6.1",
    "responses~=0.16.0",
]

setup(
    entry_points={
        "console_scripts": [
            "source-my-hours=source_my_hours.run:run",
        ],
    },
    name="source_my_hours",
    description="Source implementation for My Hours.",
    author="Wisse Jelgersma",
    author_email="wisse@vrowl.nl",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={"": ["*.json", "schemas/*.json", "schemas/shared/*.json", "schemas/shared/*/*.json"]},
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },
)
