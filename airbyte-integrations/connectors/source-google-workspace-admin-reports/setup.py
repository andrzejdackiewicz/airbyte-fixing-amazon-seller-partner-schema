#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from setuptools import find_packages, setup

MAIN_REQUIREMENTS = [
    "airbyte-cdk~=0.1",
    "google-api-python-client==2.0.2",
    "google-auth-httplib2==0.1.0",
    "google-auth-oauthlib==0.4.3",
    "backoff==1.10.0",
    "pendulum==2.1.2",
]

TEST_REQUIREMENTS = [
    "requests-mock~=1.9.3",
    "pytest~=6.1",
    "pytest-mock~=3.6.1",
]

setup(
    entry_points={
        "console_scripts": [
            "source-google-workspace-admin-reports=source_google_workspace_admin_reports.run:run",
        ],
    },
    name="source_google_workspace_admin_reports",
    description="Source implementation for Google Workspace Admin Reports.",
    author="Airbyte",
    author_email="contact@airbyte.io",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={"": ["*.json", "schemas/*.json", "schemas/shared/*.json"]},
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },
)
