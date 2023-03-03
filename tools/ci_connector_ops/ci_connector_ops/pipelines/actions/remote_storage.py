#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from pathlib import Path

from dagger import Client, File, Secret


async def upload_to_s3(dagger_client: Client, file_to_upload_path: Path, key: str, bucket: str) -> int:
    file_to_upload: File = dagger_client.host().directory(".", include=[str(file_to_upload_path)]).file(str(file_to_upload_path))
    aws_access_key_id: Secret = dagger_client.host().env_variable("AWS_ACCESS_KEY_ID").secret()
    aws_secret_access_key: Secret = dagger_client.host().env_variable("AWS_SECRET_ACCESS_KEY").secret()
    aws_region: Secret = dagger_client.host().env_variable("AWS_REGION").secret()
    return await (
        dagger_client.container()
        .from_("amazon/aws-cli:latest")
        .with_file(str(file_to_upload_path), file_to_upload)
        .with_secret_variable("AWS_ACCESS_KEY_ID", aws_access_key_id)
        .with_secret_variable("AWS_SECRET_ACCESS_KEY", aws_secret_access_key)
        .with_secret_variable("AWS_REGION", aws_region)
        .with_exec(["s3", "cp", str(file_to_upload_path), f"s3://{bucket}/{key}"])
        .exit_code()
    )
