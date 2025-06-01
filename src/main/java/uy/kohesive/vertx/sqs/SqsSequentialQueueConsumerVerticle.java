package uy.kohesive.vertx.sqs;

import io.vertx.core.*;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import uy.kohesive.vertx.sqs.impl.SqsClientImpl;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A verticle that polls SQS queue sequentially with controlled concurrency.
 */
public class SqsSequentialQueueConsumerVerticle extends AbstractVerticle implements SqsVerticle {
    
    private static final Logger log = LoggerFactory.getLogger(SqsSequentialQueueConsumerVerticle.class);
    
    private AwsCredentialsProvider credentialsProvider;
    private SqsClient client;
    private ExecutorService pollingPool;
    private ExecutorService routingPool;
    
    public SqsSequentialQueueConsumerVerticle() {
    }
    
    public SqsSequentialQueueConsumerVerticle(AwsCredentialsProvider credentialsProvider) {
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
        Integer workersCount = config().getInteger("workersCount");
        Long timeout = config().getLong("timeout", DEFAULT_TIMEOUT);
        Integer bufferSize = config().getInteger("bufferSize", workersCount * 10);
        final Integer finalBufferSize = bufferSize > 10 ? 10 : bufferSize;
        Long pollingInterval = config().getLong("pollingInterval", 1000L);
        
        routingPool = Executors.newFixedThreadPool(workersCount);
        pollingPool = Executors.newSingleThreadExecutor();
        
        client.start().onComplete(result -> {
            if (result.succeeded()) {
                subscribe(queueUrl, address, workersCount, timeout, finalBufferSize, pollingInterval);
                startPromise.complete();
            } else {
                startPromise.fail(result.cause());
            }
        });
    }
    
    private void subscribe(String queueUrl, String address, Integer workersCount, Long timeout, Integer bufferSize, Long pollingInterval) {
        BlockingQueue<SqsMessage> buffer = new LinkedBlockingQueue<>();
        
        pollingPool.execute(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                if (buffer.isEmpty()) {
                    CountDownLatch latch = new CountDownLatch(1);
                    AtomicBoolean emptyQueue = new AtomicBoolean(false);
                    
                    client.receiveMessages(queueUrl, bufferSize).onComplete(result -> {
                        try {
                            if (result.succeeded()) {
                                var messages = result.result();
                                if (messages.isEmpty()) {
                                    emptyQueue.set(true);
                                } else {
                                    messages.forEach(jsonMessage -> {
                                        SqsMessage sqsMessage = new SqsMessage(
                                            jsonMessage.getString("receiptHandle"),
                                            jsonMessage
                                        );
                                        buffer.offer(sqsMessage);
                                    });
                                }
                            } else {
                                log.error("Can't poll messages from " + queueUrl, result.cause());
                            }
                        } finally {
                            latch.countDown();
                        }
                    });
                    
                    try {
                        latch.await();
                        
                        if (emptyQueue.get()) {
                            Thread.sleep(5000);
                        } else {
                            Thread.sleep(pollingInterval);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                } else {
                    try {
                        Thread.sleep(pollingInterval);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        });
        
        // Start routing workers
        Runnable routingTask = () -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    SqsMessage sqsMessage = buffer.take();
                    CountDownLatch latch = new CountDownLatch(1);
                    
                    vertx.eventBus().request(address, sqsMessage.message(), 
                        new DeliveryOptions().setSendTimeout(timeout))
                        .onComplete(ar -> {
                            if (ar.succeeded()) {
                                deleteMessage(queueUrl, sqsMessage.receipt());
                            } else {
                                log.warn("Message with receipt {} was failed to process by the consumer", sqsMessage.receipt());
                            }
                            latch.countDown();
                        });
                    
                    try {
                        latch.await(100 + timeout, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        };
        
        for (int i = 0; i < workersCount; i++) {
            routingPool.execute(routingTask);
        }
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) throws Exception {
        if (routingPool != null) {
            routingPool.shutdown();
        }
        if (pollingPool != null) {
            pollingPool.shutdown();
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
