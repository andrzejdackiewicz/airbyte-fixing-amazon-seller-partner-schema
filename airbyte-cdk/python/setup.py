#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import pathlib

from setuptools import find_packages, setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

setup(
    name="airbyte-cdk",
    # The version of the airbyte-cdk package is used at runtime to validate manifests. That validation must be
    # updated if our semver format changes such as using release candidate versions.
    version="0.33.2",
    description="A framework for writing Airbyte Connectors.",
    long_description=README,
    long_description_content_type="text/markdown",
    author="Airbyte",
    author_email="contact@airbyte.io",
    license="MIT",
    url="https://github.com/airbytehq/airbyte",
    classifiers=[
        # This information is used when browsing on PyPi.
        # Dev Status
        "Development Status :: 3 - Alpha",
        # Project Audience
        "Intended Audience :: Developers",
        "Topic :: Scientific/Engineering",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        # Python Version Support
        "Programming Language :: Python :: 3.8",
    ],
    keywords="airbyte connector-development-kit cdk",
    project_urls={
        "Documentation": "https://docs.airbyte.io/",
        "Source": "https://github.com/airbytehq/airbyte",
        "Tracker": "https://github.com/airbytehq/airbyte/issues",
    },
    packages=find_packages(exclude=("unit_tests",)),
    package_data={"airbyte_cdk": ["py.typed", "sources/declarative/declarative_component_schema.yaml"]},
    install_requires=[
        "airbyte-protocol-models==1.0.0",
        "backoff",
        "dpath~=2.0.1",
        "isodate~=0.6.1",
        "jsonschema~=3.2.0",
        "jsonref~=0.2",
        "pendulum",
        "genson==1.2.2",
        "pydantic~=1.9.2",
        "python-dateutil",
        "PyYAML~=5.4",
        "requests",
        "requests_cache",
        "Deprecated~=1.2",
        "Jinja2~=3.1.2",
        "cachetools",
    ],
    python_requires=">=3.8",
    extras_require={
        "dev": [
            "freezegun",
            "MyPy~=0.812",
            "pytest",
            "pytest-cov",
            "pytest-mock",
            "requests-mock",
            "pytest-httpserver",
        ],
        "sphinx-docs": [
            "Sphinx~=4.2",
            "sphinx-rtd-theme~=1.0",
        ],
    },
)
