/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.task

import io.airbyte.cdk.write.DestinationWrite
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Singleton

interface TeardownTask : Task

/**
 * Wraps @[DestinationWrite.teardown] and stops the task launcher.
 *
 * TODO: Report teardown-complete and let the task launcher decide what to do next.
 */
class DefaultTeardownTask(
    private val destination: DestinationWrite,
    private val taskLauncher: DestinationTaskLauncher
) : TeardownTask {
    val log = KotlinLogging.logger {}

    override suspend fun execute() {
        destination.teardown()
        taskLauncher.handleTeardownComplete()
    }
}

interface TeardownTaskFactory {
    fun make(taskLauncher: DestinationTaskLauncher): TeardownTask
}

@Singleton
@Secondary
class DefaultTeardownTaskFactory(
    private val destination: DestinationWrite,
) : TeardownTaskFactory {
    override fun make(taskLauncher: DestinationTaskLauncher): TeardownTask {
        return DefaultTeardownTask(destination, taskLauncher)
    }
}
