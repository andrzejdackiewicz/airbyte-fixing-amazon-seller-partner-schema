package io.airbyte.integrations.source.teradata.client.exception;

public class Error5xxException extends BaseException {

    public Error5xxException(int statusCode, String body, String reason) {
        super(statusCode, body, reason);
    }

    public Error5xxException(int statusCode, String body) {
        super(statusCode, body);
    }

}
