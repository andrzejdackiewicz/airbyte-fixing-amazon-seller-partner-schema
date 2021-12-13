#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


import setuptools

MAIN_REQUIREMENTS = [
    "airbyte-cdk~=0.1.25",
    "docker~=4.4",
    "PyYAML~=5.4",
    "icdiff~=1.9",
    "inflection~=0.5",
    "pdbpp~=0.10",
    "pydantic~=1.6",
    "pytest~=6.1",
    "pytest-sugar~=0.9",
    "pytest-timeout~=1.4",
    "pprintpp~=0.4",
    "dpath~=2.0.1",
    "jsonschema~=3.2.0",
    "jsonref==0.2",
    "flake8",
]

setuptools.setup(
    name="source-acceptance-test",
    description="Contains acceptance tests for source connectors.",
    author="Airbyte",
    author_email="contact@airbyte.io",
    url="https://github.com/airbytehq/airbyte",
    packages=setuptools.find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    entry_points={
        'console_scripts': [
            'airbyte_tests=source_acceptance_test.utils.airbyte_entrypoint:main',
            'airbyte_py_tests=source_acceptance_test.utils.airbyte_py_entrypoint:main'
        ],
    }

)
