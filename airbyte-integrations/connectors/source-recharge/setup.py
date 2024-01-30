#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from setuptools import find_packages, setup

MAIN_REQUIREMENTS = [
    "airbyte-cdk~=0.1",
]

TEST_REQUIREMENTS = [
    "pytest-mock~=3.6.1",
    "pytest~=6.1",
    "requests-mock",
]

setup(
    entry_points={
        "console_scripts": [
            "source-recharge=source_recharge.run:run",
        ],
    },
    name="source_recharge",
    description="Source implementation for Recharge.",
    author="Airbyte",
    author_email="contact@airbyte.io",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={"": ["*.json", "schemas/*.json", "schemas/shared/*.json", "schemas/shared/*/*.json"]},
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },
)
