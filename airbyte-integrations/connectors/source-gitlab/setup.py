#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from setuptools import find_packages, setup

MAIN_REQUIREMENTS = ["airbyte-cdk", "vcrpy==4.1.1"]

TEST_REQUIREMENTS = ["requests-mock~=1.9.3", "pytest~=6.1", "requests_mock", "pytest-mock"]

setup(
    entry_points={
        "console_scripts": [
            "source-gitlab=source_gitlab.run:run",
        ],
    },
    name="source_gitlab",
    description="Source implementation for Gitlab.",
    author="Airbyte",
    author_email="contact@airbyte.io",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={"": ["*.json", "schemas/*.json", "schemas/shared/*.json"]},
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },
)
