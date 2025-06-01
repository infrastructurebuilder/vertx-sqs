package uy.kohesive.vertx.sqs;

import io.vertx.core.*;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import uy.kohesive.vertx.sqs.impl.SqsClientImpl;

/**
 * A verticle that polls SQS queue and forwards messages to the event bus.
 */
public class SqsQueueConsumerVerticle extends AbstractVerticle implements SqsVerticle {
    
    private static final Logger log = LoggerFactory.getLogger(SqsQueueConsumerVerticle.class);
    
    private AwsCredentialsProvider credentialsProvider;
    private SqsClient client;
    private long timerId = -1;
    
    public SqsQueueConsumerVerticle() {
    }
    
    public SqsQueueConsumerVerticle(AwsCredentialsProvider credentialsProvider) {
        this.credentialsProvider = credentialsProvider;
    }
    
    @Override
    public Logger getLog() {
        return log;
    }
    
    @Override
    public SqsClient getClient() {
        return client;
    }
    
    @Override
    public AwsCredentialsProvider getCredentialsProvider() {
        return credentialsProvider;
    }
    
    @Override
    public void setCredentialsProvider(AwsCredentialsProvider credentialsProvider) {
        this.credentialsProvider = credentialsProvider;
    }
    
    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        client = new SqsClientImpl(vertx, config(), credentialsProvider);
        
        String queueUrl = config().getString("queueUrl");
        String address = config().getString("address");
        Integer maxMessages = config().getInteger("messagesPerPoll", 1);
        Long timeout = config().getLong("timeout", DEFAULT_TIMEOUT);
        Long pollingInterval = config().getLong("pollingInterval");
        
        client.start().onComplete(result -> {
            if (result.succeeded()) {
                subscribe(pollingInterval, queueUrl, address, maxMessages, timeout);
                startPromise.complete();
            } else {
                startPromise.fail(result.cause());
            }
        });
    }
    
    private void subscribe(Long pollingInterval, String queueUrl, String address, Integer maxMessages, Long timeout) {
        timerId = vertx.setPeriodic(pollingInterval, timerId -> {
            client.receiveMessages(queueUrl, maxMessages).onComplete(result -> {
                if (result.succeeded()) {
                    log.debug("Polled {} messages", result.result().size());
                    result.result().forEach(message -> {
                        String receipt = message.getString("receiptHandle");
                        
                        vertx.eventBus().request(address, message, 
                            new DeliveryOptions().setSendTimeout(timeout))
                            .onComplete(ar -> {
                                if (ar.succeeded()) {
                                    deleteMessage(queueUrl, receipt);
                                } else {
                                    log.warn("Message with receipt {} was failed to process by the consumer", receipt);
                                }
                            });
                    });
                } else {
                    log.error("Unable to poll messages from " + queueUrl, result.cause());
                }
            });
        });
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) throws Exception {
        if (timerId != -1) {
            vertx.cancelTimer(timerId);
        }
        
        if (client != null) {
            client.stop().onComplete(result -> {
                if (result.succeeded()) {
                    stopPromise.complete();
                } else {
                    stopPromise.fail(result.cause());
                }
            });
        } else {
            stopPromise.complete();
        }
    }
}
