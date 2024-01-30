#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from setuptools import find_packages, setup

MAIN_REQUIREMENTS = ["airbyte-cdk", "backoff", "requests", "pandas"]

TEST_REQUIREMENTS = [
    "pytest-mock~=3.6.1",
    "pytest~=6.1",
    "requests-mock",
]

setup(
    entry_points={
        "console_scripts": [
            "source-sendgrid=source_sendgrid.run:run",
        ],
    },
    name="source_sendgrid",
    description="Source implementation for Sendgrid.",
    author="Airbyte",
    author_email="contact@airbyte.io",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={"": ["*.json", "schemas/*.json", "schemas/shared/*.json", "schemas/shared/*/*.json"]},
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },
)
