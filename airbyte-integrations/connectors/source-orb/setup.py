#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from setuptools import find_packages, setup

MAIN_REQUIREMENTS = ["airbyte-cdk~=0.1", "pendulum==2.1.2"]

TEST_REQUIREMENTS = ["pytest~=6.1", "pytest-mock~=3.6.1", "connector-acceptance-test", "responses~=0.13.3", "pendulum==2.1.2"]

setup(
    name="source_orb",
    description="Source implementation for Orb.",
    author="Airbyte",
    author_email="contact@airbyte.io",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={"": ["*.json", "schemas/*.json", "schemas/shared/*.json"]},
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },
)
