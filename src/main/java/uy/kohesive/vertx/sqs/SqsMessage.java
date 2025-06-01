package uy.kohesive.vertx.sqs;

import io.vertx.core.json.JsonObject;

/**
 * Represents an SQS message with receipt handle and message data.
 */
public record SqsMessage(String receipt, JsonObject message) {
    
    public SqsMessage {
        if (receipt == null) {
            throw new IllegalArgumentException("Receipt cannot be null");
        }
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }
    }
}
