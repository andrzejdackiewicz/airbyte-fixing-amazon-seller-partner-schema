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

import backoff
from apiclient import discovery, errors
from requests.status_codes import codes as status_codes


def error_handler(error):
    return error.resp.status != status_codes.TOO_MANY_REQUESTS


class GoogleSheetsClient:
    @staticmethod
    @backoff.on_exception(backoff.expo, errors.HttpError, max_time=60, giveup=error_handler)
    def get(client: discovery.Resource, **kwargs):
        return client.get(**kwargs).execute()

    @staticmethod
    @backoff.on_exception(backoff.expo, errors.HttpError, max_time=60, giveup=error_handler)
    def create(client: discovery.Resource, **kwargs):
        return client.create(**kwargs).execute()

    @staticmethod
    @backoff.on_exception(backoff.expo, errors.HttpError, max_time=60, giveup=error_handler)
    def get_values(client: discovery.Resource, **kwargs):
        return client.values().batchGet(**kwargs).execute()

    @staticmethod
    @backoff.on_exception(backoff.expo, errors.HttpError, max_time=60, giveup=error_handler)
    def update_values(client: discovery.Resource, **kwargs):
        return client.values().batchUpdate(**kwargs).execute()
