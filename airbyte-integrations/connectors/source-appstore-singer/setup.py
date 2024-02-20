#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from setuptools import find_packages, setup

MAIN_REQUIREMENTS = [
    "airbyte-cdk==0.1.6",
    "appstoreconnect==0.9.0",
    "asn1crypto==1.4.0",
    "certifi==2021.10.8",
    "cffi==1.15.0",
    "cryptography==2.6.1",
    "pendulum==2.1.2",
    "pyjwt==1.6.4",
    "tap-appstore @ https://github.com/airbytehq/tap-appstore/tarball/v0.3.0",
]

TEST_REQUIREMENTS = [
    "requests-mock~=1.9.3",
    "pytest~=6.1",
    "pytest-mock~=3.6.1",
]

setup(
    entry_points={
        "console_scripts": [
            "source-appstore-singer=source_appstore_singer.run:run",
        ],
    },
    name="source_appstore_singer",
    description="Source implementation for Appstore, built on the Singer tap implementation.",
    author="Airbyte",
    author_email="contact@airbyte.io",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={
        "": [
            # Include yaml files in the package (if any)
            "*.yml",
            "*.yaml",
            # Include all json files in the package, up to 4 levels deep
            "*.json",
            "*/*.json",
            "*/*/*.json",
            "*/*/*/*.json",
            "*/*/*/*/*.json",
        ]
    },
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },
)
