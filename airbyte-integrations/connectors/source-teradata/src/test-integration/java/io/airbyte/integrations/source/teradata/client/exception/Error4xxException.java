package io.airbyte.integrations.source.teradata.client.exception;

public class Error4xxException extends BaseException {

    public Error4xxException(int statusCode, String body, String reason) {
        super(statusCode, body, reason);
    }

    public Error4xxException(int statusCode, String body) {
        super(statusCode, body);
    }

}
