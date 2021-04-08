"""
MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

from base_python.catalog_helpers import CatalogHelper
from base_python.client import BaseClient
from base_python.integration import AirbyteSpec, Destination, Integration, Source
from base_python.logger import AirbyteLogger
from base_python.sdk.abstract_source import AbstractSource

# Separate the SDK imports so they can be moved somewhere else more easily
from base_python.sdk.streams.auth.core import HttpAuthenticator
from base_python.sdk.streams.auth.token import TokenAuthenticator
from base_python.sdk.streams.core import Stream
from base_python.sdk.streams.http import HttpStream
from base_python.source import BaseSource

# Must be the last one because the way we load the connector module creates a circular
# dependency and models might not have been loaded yet
from base_python.entrypoint import AirbyteEntrypoint  # noqa isort:skip

__all__ = [
    "AirbyteLogger",
    "AirbyteSpec",
    "AbstractSource",
    "BaseClient",
    "BaseSource",
    "CatalogHelper",
    "Destination",
    "HttpAuthenticator",
    "HttpStream",
    "Integration",
    "Source",
    "Stream",
    "TokenAuthenticator",
]
