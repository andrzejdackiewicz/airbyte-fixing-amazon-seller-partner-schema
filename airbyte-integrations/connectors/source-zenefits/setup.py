#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from setuptools import find_packages, setup

MAIN_REQUIREMENTS = [
    "airbyte-cdk",
]

TEST_REQUIREMENTS = ["requests-mock~=1.9.3", "pytest~=6.2", "pytest-mock~=3.6.1"]

setup(
    entry_points={
        "console_scripts": [
            "source-zenefits=source_zenefits.run:run",
        ],
    },
    name="source_zenefits",
    description="Source implementation for Zenefits.",
    author="Airbyte",
    author_email="contact@airbyte.io",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={"": ["*.json", "*.yaml", "schemas/*.json", "schemas/shared/*.json"]},
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },
)
