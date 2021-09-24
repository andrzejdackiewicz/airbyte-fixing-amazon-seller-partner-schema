#
# Copyright (c) 2020 Airbyte, Inc., all rights reserved.
#


from typing import Any, Iterator, Mapping

from airbyte_cdk.logger import AirbyteLogger
from boto3 import session as boto3session
from botocore import UNSIGNED
from botocore.config import Config

from .s3file import S3File
from .source_files_abstract.stream import IncrementalFileStream


class IncrementalFileStreamS3(IncrementalFileStream):
    @property
    def storagefile_class(self) -> type:
        return S3File

    @staticmethod
    def _list_bucket(provider: Mapping[str, Any], accept_key=lambda k: True) -> Iterator[str]:
        """
        Wrapper for boto3's list_objects_v2 so we can handle pagination, filter by lambda func and operate with or without credentials

        :param provider: provider specific mapping as described in spec.json
        :param accept_key: lambda function to allow filtering return keys, e.g. lambda k: not k.endswith('/'), defaults to lambda k: True
        :yield: key (name) of each object
        """
        if S3File.use_aws_account(provider):
            session = boto3session.Session(
                aws_access_key_id=provider["aws_access_key_id"], aws_secret_access_key=provider["aws_secret_access_key"]
            )
            client = session.client("s3")
        else:
            session = boto3session.Session()
            client = session.client("s3", config=Config(signature_version=UNSIGNED))

        ctoken = None
        while True:
            # list_objects_v2 doesn't like a None value for ContinuationToken
            # so we don't set it if we don't have one.
            if ctoken:
                kwargs = dict(Bucket=provider["bucket"], Prefix=provider.get("path_prefix", ""), ContinuationToken=ctoken)
            else:
                kwargs = dict(Bucket=provider["bucket"], Prefix=provider.get("path_prefix", ""))
            response = client.list_objects_v2(**kwargs)
            try:
                content = response["Contents"]
            except KeyError:
                pass
            else:
                for c in content:
                    key = c["Key"]
                    if accept_key(key):
                        yield key
            ctoken = response.get("NextContinuationToken", None)
            if not ctoken:
                break

    @staticmethod
    def filepath_iterator(logger: AirbyteLogger, provider: dict) -> Iterator[str]:
        """
        See _list_bucket() for logic of interacting with S3

        :param logger: instance of AirbyteLogger to use as this is a staticmethod
        :param provider: S3 provider mapping as described in spec.json
        :yield: url filepath to use in S3File()
        """
        prefix = provider.get("path_prefix")
        if prefix is None:
            prefix = ""

        msg = f"Iterating S3 bucket '{provider['bucket']}'"
        logger.info(msg + f" with prefix: '{prefix}' " if prefix != "" else msg)

        for blob in IncrementalFileStreamS3._list_bucket(
            provider=provider, accept_key=lambda k: not k.endswith("/")  # filter out 'folders', we just want actual blobs
        ):
            yield blob
