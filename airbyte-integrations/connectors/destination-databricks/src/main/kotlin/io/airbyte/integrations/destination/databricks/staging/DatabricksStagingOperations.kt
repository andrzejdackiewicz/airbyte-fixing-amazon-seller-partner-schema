package io.airbyte.integrations.destination.databricks.staging

import com.databricks.sdk.WorkspaceClient
import io.airbyte.integrations.base.destination.typing_deduping.StreamId
import java.io.InputStream

class DatabricksStagingOperations(val workspaceClient: WorkspaceClient) {

    fun create(streamId: StreamId): Result<Unit> {
        TODO("Not yet implemented")
    }

    fun upload(streamId: StreamId, contents: InputStream): Result<String> {
        TODO("Not yet implemented")
    }

    fun delete(streamId: StreamId): Result<Unit> {
        TODO("Not yet implemented")
    }


}
