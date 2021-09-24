#
# Copyright (c) 2020 Airbyte, Inc., all rights reserved.
#


from setuptools import find_packages, setup

MAIN_REQUIREMENTS = [
    "airbyte-cdk==0.1.3",
    "pendulum==1.2.0",
    "requests==2.25.1",
]

TEST_REQUIREMENTS = ["pytest==6.1.2"]


setup(
    name="source_iterable",
    description="Source implementation for Iterable.",
    author="Airbyte",
    author_email="contact@airbyte.io",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS + TEST_REQUIREMENTS,
    package_data={"": ["*.json", "schemas/*.json"]},
)
