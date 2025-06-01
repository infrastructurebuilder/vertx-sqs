package uy.kohesive.vertx.sqs;

import org.slf4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * Base interface for SQS verticles.
 */
public interface SqsVerticle {
    
    long DEFAULT_TIMEOUT = 5 * 60 * 1000L; // 5 minutes
    
    Logger getLog();
    
    SqsClient getClient();
    
    AwsCredentialsProvider getCredentialsProvider();
    
    void setCredentialsProvider(AwsCredentialsProvider credentialsProvider);
    
    /**
     * Helper method to delete a message from SQS queue.
     */
    default void deleteMessage(String queueUrl, String receipt) {
        getClient().deleteMessage(queueUrl, receipt, result -> {
            if (result.failed()) {
                getLog().warn("Unable to acknowledge message deletion with receipt = " + receipt);
            }
        });
    }
}
