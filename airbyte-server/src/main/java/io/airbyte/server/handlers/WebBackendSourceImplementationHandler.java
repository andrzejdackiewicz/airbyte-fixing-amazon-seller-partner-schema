/*
 * MIT License
 *
 * Copyright (c) 2020 Airbyte
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.airbyte.server.handlers;

import io.airbyte.api.model.CheckConnectionRead;
import io.airbyte.api.model.SourceImplementationCreate;
import io.airbyte.api.model.SourceImplementationIdRequestBody;
import io.airbyte.api.model.SourceImplementationRead;
import io.airbyte.commons.json.JsonValidationException;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.server.errors.KnownException;

import java.io.IOException;

public class WebBackendSourceImplementationHandler {

    private final SourceImplementationsHandler sourceImplementationsHandler;

    private final SchedulerHandler schedulerHandler;

    public WebBackendSourceImplementationHandler(
            final SourceImplementationsHandler sourceImplementationsHandler,
            final SchedulerHandler schedulerHandler) {
        this.sourceImplementationsHandler = sourceImplementationsHandler;
        this.schedulerHandler = schedulerHandler;
    }

    public SourceImplementationRead webBackendCreateSourceImplementationAndCheck(
            SourceImplementationCreate sourceImplementationCreate)
            throws ConfigNotFoundException, IOException, JsonValidationException {
        SourceImplementationRead sourceImplementation = sourceImplementationsHandler
                .createSourceImplementation(sourceImplementationCreate);

        final SourceImplementationIdRequestBody sourceImplementationIdRequestBody = new SourceImplementationIdRequestBody()
                .sourceImplementationId(sourceImplementation.getSourceImplementationId());

        CheckConnectionRead checkConnectionRead = null;

        boolean syncFailed;
        try {
            checkConnectionRead = schedulerHandler
                    .checkSourceImplementationConnection(sourceImplementationIdRequestBody);
        } finally {
            syncFailed = checkConnectionRead == null
                    || CheckConnectionRead.StatusEnum.FAILURE == checkConnectionRead.getStatus();
            if (syncFailed) {
                sourceImplementationsHandler.deleteSourceImplementation(sourceImplementationIdRequestBody);
            }
        }

        if (syncFailed) {
            throw new KnownException(400, "Unable to connect to source");
        }

        return sourceImplementation;
    }

}
