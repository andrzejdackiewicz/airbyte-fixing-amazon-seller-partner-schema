#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from setuptools import find_packages, setup

MAIN_REQUIREMENTS = [
    "airbyte-cdk~=0.1",
]

TEST_REQUIREMENTS = [
    "pytest~=6.2",
    "requests-mock~=1.9.3",
    "pytest-mock~=3.6.1",
]

setup(
    entry_points={
        "console_scripts": [
            "source-yahoo-finance-price=source_yahoo_finance_price.run:run",
        ],
    },
    name="source_yahoo_finance_price",
    description="Source implementation for Yahoo Finance Price.",
    author="Airbyte",
    author_email="contact@airbyte.io",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={"": ["*.json", "*.yaml", "schemas/*.json", "schemas/shared/*.json"]},
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },
)
