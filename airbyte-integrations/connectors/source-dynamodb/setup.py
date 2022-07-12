#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from setuptools import find_packages, setup

MAIN_REQUIREMENTS = ["airbyte-cdk~=0.1.56", "boto3>=1.24.25"]

TEST_REQUIREMENTS = [
    "pytest~=6.1",
    "source-acceptance-test",
    "boto3-stubs[dynamodb]",
]

setup(
    name="source_dynamodb",
    description="Source implementation for Dynamodb.",
    author="Airbyte",
    author_email="contact@airbyte.io",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={"": ["*.json", "*.yaml"]},
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },
)
