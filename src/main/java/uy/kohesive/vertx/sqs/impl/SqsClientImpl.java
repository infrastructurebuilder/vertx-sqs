package uy.kohesive.vertx.sqs.impl;

import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.*;
import uy.kohesive.vertx.sqs.SqsClient;

import java.net.URI;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class SqsClientImpl implements SqsClient {
    
    private static final Logger log = LoggerFactory.getLogger(SqsClientImpl.class);
    
    private final Vertx vertx;
    private final JsonObject config;
    private final AwsCredentialsProvider credentialsProvider;
    
    private SqsAsyncClient client;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    
    public SqsClientImpl(Vertx vertx, JsonObject config, AwsCredentialsProvider credentialsProvider) {
        this.vertx = vertx;
        this.config = config;
        this.credentialsProvider = credentialsProvider;
    }
    
    private AwsCredentialsProvider getCredentialsProvider() {
        if (credentialsProvider != null) {
            return credentialsProvider;
        }
        
        if (config.getString("accessKey") != null) {
            String accessKey = config.getString("accessKey");
            String secretKey = config.getString("secretKey");
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
        } else {
            try {
                return DefaultCredentialsProvider.create();
            } catch (Exception t) {
                throw new RuntimeException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (~/.aws/credentials), and is in valid format."
                );
            }
        }
    }
    
    @Override
    public void start(Handler<AsyncResult<Void>> resultHandler) {
        log.info("Starting SQS client");
        
        vertx.executeBlocking(() -> {
            try {
                var builder = SqsAsyncClient.builder()
                    .credentialsProvider(getCredentialsProvider());
                
                String region = config.getString("region");
                if (region != null) {
                    builder.region(Region.of(region));
                }
                
                if (config.getString("host") != null && config.getInteger("port") != null) {
                    String host = config.getString("host");
                    Integer port = config.getInteger("port");
                    builder.endpointOverride(URI.create("http://" + host + ":" + port));
                }
                
                client = builder.build();
                initialized.set(true);
                
                return null;
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        }, false).onComplete(ar -> resultHandler.handle(ar.mapEmpty()));
    }
    
    @Override
    public Future<Void> start() {
        Promise<Void> promise = Promise.promise();
        start(ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }
    
    @Override
    public void stop(Handler<AsyncResult<Void>> resultHandler) {
        if (client != null) {
            client.close();
        }
        resultHandler.handle(Future.succeededFuture());
    }
    
    @Override
    public Future<Void> stop() {
        Promise<Void> promise = Promise.promise();
        stop(ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }
    
    private void withClient(java.util.function.Consumer<SqsAsyncClient> handler) {
        if (initialized.get()) {
            handler.accept(client);
        } else {
            throw new IllegalStateException("SQS client wasn't initialized");
        }
    }
    
    @Override
    public void sendMessage(String queueUrl, String messageBody, Handler<AsyncResult<String>> resultHandler) {
        sendMessage(queueUrl, messageBody, null, null, resultHandler);
    }
    
    @Override
    public Future<String> sendMessage(String queueUrl, String messageBody) {
        Promise<String> promise = Promise.promise();
        sendMessage(queueUrl, messageBody, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }
    
    @Override
    public void sendMessage(String queueUrl, String messageBody, JsonObject attributes, Handler<AsyncResult<String>> resultHandler) {
        sendMessage(queueUrl, messageBody, attributes, null, resultHandler);
    }
    
    @Override
    public Future<String> sendMessage(String queueUrl, String messageBody, JsonObject attributes) {
        Promise<String> promise = Promise.promise();
        sendMessage(queueUrl, messageBody, attributes, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }
    
    @Override
    public void sendMessage(String queueUrl, String messageBody, JsonObject attributes, Integer delaySeconds, Handler<AsyncResult<String>> resultHandler) {
        withClient(client -> {
            var requestBuilder = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody);
            
            if (delaySeconds != null) {
                requestBuilder.delaySeconds(delaySeconds);
            }
            
            if (attributes != null) {
                Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
                attributes.forEach(entry -> {
                    JsonObject attrValue = entry.getValue() instanceof JsonObject ? (JsonObject) entry.getValue() : null;
                    if (attrValue != null) {
                        String dataType = attrValue.getString("dataType");
                        String stringData = attrValue.getString("stringData");
                        byte[] binaryData = attrValue.getBinary("binaryData");
                        
                        var attrBuilder = MessageAttributeValue.builder().dataType(dataType);
                        if (binaryData != null) {
                            attrBuilder.binaryValue(SdkBytes.fromByteArray(binaryData));
                        }
                        if (stringData != null) {
                            attrBuilder.stringValue(stringData);
                        }
                        messageAttributes.put(entry.getKey(), attrBuilder.build());
                    }
                });
                requestBuilder.messageAttributes(messageAttributes);
            }
            
            handleAsyncResult(client.sendMessage(requestBuilder.build()), 
                SendMessageResponse::messageId, resultHandler);
        });
    }
    
    @Override
    public Future<String> sendMessage(String queueUrl, String messageBody, JsonObject attributes, Integer delaySeconds) {
        Promise<String> promise = Promise.promise();
        sendMessage(queueUrl, messageBody, attributes, delaySeconds, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }
    
    @Override
    public void createQueue(String name, Map<String, String> attributes, Handler<AsyncResult<String>> resultHandler) {
        withClient(client -> {
            // Convert string map to QueueAttributeName map for AWS SDK v2
            Map<QueueAttributeName, String> awsAttributes = new HashMap<>();
            if (attributes != null) {
                attributes.forEach((key, value) -> {
                    try {
                        QueueAttributeName attrName = QueueAttributeName.fromValue(key);
                        awsAttributes.put(attrName, value);
                    } catch (Exception e) {
                        // If the attribute name is not recognized, skip it
                        log.warn("Unrecognized queue attribute: {}", key);
                    }
                });
            }
            
            var request = CreateQueueRequest.builder()
                .queueName(name)
                .attributes(awsAttributes)
                .build();
            
            handleAsyncResult(client.createQueue(request), 
                CreateQueueResponse::queueUrl, resultHandler);
        });
    }
    
    @Override
    public Future<String> createQueue(String name, Map<String, String> attributes) {
        Promise<String> promise = Promise.promise();
        createQueue(name, attributes, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }
    
    @Override
    public void listQueues(String namePrefix, Handler<AsyncResult<List<String>>> resultHandler) {
        withClient(client -> {
            var requestBuilder = ListQueuesRequest.builder();
            if (namePrefix != null) {
                requestBuilder.queueNamePrefix(namePrefix);
            }
            
            handleAsyncResult(client.listQueues(requestBuilder.build()), 
                ListQueuesResponse::queueUrls, resultHandler);
        });
    }
    
    @Override
    public Future<List<String>> listQueues(String namePrefix) {
        Promise<List<String>> promise = Promise.promise();
        listQueues(namePrefix, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }
    
    @Override
    public void receiveMessage(String queueUrl, Handler<AsyncResult<List<JsonObject>>> resultHandler) {
        receiveMessages(queueUrl, 1, resultHandler);
    }
    
    @Override
    public Future<List<JsonObject>> receiveMessage(String queueUrl) {
        Promise<List<JsonObject>> promise = Promise.promise();
        receiveMessage(queueUrl, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }
    
    @Override
    public void receiveMessages(String queueUrl, Integer maxMessages, Handler<AsyncResult<List<JsonObject>>> resultHandler) {
        withClient(client -> {
            var request = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(maxMessages)
                .messageAttributeNames("All")
                .build();
            
            handleAsyncResult(client.receiveMessage(request), 
                response -> response.messages().stream()
                    .map(this::messageToJsonObject)
                    .collect(Collectors.toList()), 
                resultHandler);
        });
    }
    
    @Override
    public Future<List<JsonObject>> receiveMessages(String queueUrl, Integer maxMessages) {
        Promise<List<JsonObject>> promise = Promise.promise();
        receiveMessages(queueUrl, maxMessages, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }
    
    @Override
    public void deleteQueue(String queueUrl, Handler<AsyncResult<Void>> resultHandler) {
        withClient(client -> {
            var request = DeleteQueueRequest.builder()
                .queueUrl(queueUrl)
                .build();
            
            handleVoidAsyncResult(client.deleteQueue(request), resultHandler);
        });
    }
    
    @Override
    public Future<Void> deleteQueue(String queueUrl) {
        Promise<Void> promise = Promise.promise();
        deleteQueue(queueUrl, ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }
    
    @Override
    public void deleteMessage(String queueUrl, String receiptHandle, Handler<AsyncResult<Void>> resultHandler) {
        withClient(client -> {
            var request = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(receiptHandle)
                .build();
            
            handleVoidAsyncResult(client.deleteMessage(request), resultHandler);
        });
    }
    
    @Override
    public Future<Void> deleteMessage(String queueUrl, String receiptHandle) {
        Promise<Void> promise = Promise.promise();
        deleteMessage(queueUrl, receiptHandle, ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }
    
    @Override
    public void setQueueAttributes(String queueUrl, Map<String, String> attributes, Handler<AsyncResult<Void>> resultHandler) {
        withClient(client -> {
            // Convert string map to QueueAttributeName map for AWS SDK v2
            Map<QueueAttributeName, String> awsAttributes = new HashMap<>();
            if (attributes != null) {
                attributes.forEach((key, value) -> {
                    try {
                        QueueAttributeName attrName = QueueAttributeName.fromValue(key);
                        awsAttributes.put(attrName, value);
                    } catch (Exception e) {
                        // If the attribute name is not recognized, skip it
                        log.warn("Unrecognized queue attribute: {}", key);
                    }
                });
            }
            
            var request = SetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributes(awsAttributes)
                .build();
            
            handleVoidAsyncResult(client.setQueueAttributes(request), resultHandler);
        });
    }
    
    @Override
    public Future<Void> setQueueAttributes(String queueUrl, Map<String, String> attributes) {
        Promise<Void> promise = Promise.promise();
        setQueueAttributes(queueUrl, attributes, ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }
    
    @Override
    public void changeMessageVisibility(String queueUrl, String receiptHandle, Integer visibilityTimeout, Handler<AsyncResult<Void>> resultHandler) {
        withClient(client -> {
            var request = ChangeMessageVisibilityRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(receiptHandle)
                .visibilityTimeout(visibilityTimeout)
                .build();
            
            handleVoidAsyncResult(client.changeMessageVisibility(request), resultHandler);
        });
    }
    
    @Override
    public Future<Void> changeMessageVisibility(String queueUrl, String receiptHandle, Integer visibilityTimeout) {
        Promise<Void> promise = Promise.promise();
        changeMessageVisibility(queueUrl, receiptHandle, visibilityTimeout, ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }
    
    @Override
    public void getQueueUrl(String queueName, String queueOwnerAWSAccountId, Handler<AsyncResult<String>> resultHandler) {
        withClient(client -> {
            var requestBuilder = GetQueueUrlRequest.builder()
                .queueName(queueName);
            
            if (queueOwnerAWSAccountId != null) {
                requestBuilder.queueOwnerAWSAccountId(queueOwnerAWSAccountId);
            }
            
            handleAsyncResult(client.getQueueUrl(requestBuilder.build()), 
                GetQueueUrlResponse::queueUrl, resultHandler);
        });
    }
    
    @Override
    public Future<String> getQueueUrl(String queueName, String queueOwnerAWSAccountId) {
        Promise<String> promise = Promise.promise();
        getQueueUrl(queueName, queueOwnerAWSAccountId, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }
    
    @Override
    public void addPermissionAsync(String queueUrl, String label, List<String> aWSAccountIds, List<String> actions, Handler<AsyncResult<Void>> resultHandler) {
        withClient(client -> {
            var request = AddPermissionRequest.builder()
                .queueUrl(queueUrl)
                .label(label)
                .awsAccountIds(aWSAccountIds)
                .actions(actions)
                .build();
            
            handleVoidAsyncResult(client.addPermission(request), resultHandler);
        });
    }
    
    @Override
    public Future<Void> addPermissionAsync(String queueUrl, String label, List<String> aWSAccountIds, List<String> actions) {
        Promise<Void> promise = Promise.promise();
        addPermissionAsync(queueUrl, label, aWSAccountIds, actions, ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }
    
    @Override
    public void removePermission(String queueUrl, String label, Handler<AsyncResult<Void>> resultHandler) {
        withClient(client -> {
            var request = RemovePermissionRequest.builder()
                .queueUrl(queueUrl)
                .label(label)
                .build();
            
            handleVoidAsyncResult(client.removePermission(request), resultHandler);
        });
    }
    
    @Override
    public Future<Void> removePermission(String queueUrl, String label) {
        Promise<Void> promise = Promise.promise();
        removePermission(queueUrl, label, ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }
    
    @Override
    public void getQueueAttributes(String queueUrl, List<String> attributeNames, Handler<AsyncResult<JsonObject>> resultHandler) {
        withClient(client -> {
            // Convert string list to QueueAttributeName list for AWS SDK v2
            List<QueueAttributeName> awsAttributeNames = new ArrayList<>();
            if (attributeNames != null) {
                for (String attrName : attributeNames) {
                    try {
                        awsAttributeNames.add(QueueAttributeName.fromValue(attrName));
                    } catch (Exception e) {
                        // If the attribute name is not recognized, skip it
                        log.warn("Unrecognized queue attribute: {}", attrName);
                    }
                }
            }
            
            var request = GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributeNames(awsAttributeNames)
                .build();
            
            handleAsyncResult(client.getQueueAttributes(request), 
                response -> {
                    Map<String, Object> stringMap = new HashMap<>();
                    response.attributes().forEach((key, value) -> stringMap.put(key.toString(), value));
                    return new JsonObject(stringMap);
                }, resultHandler);
        });
    }
    
    @Override
    public Future<JsonObject> getQueueAttributes(String queueUrl, List<String> attributeNames) {
        Promise<JsonObject> promise = Promise.promise();
        getQueueAttributes(queueUrl, attributeNames, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }
    
    @Override
    public void listDeadLetterSourceQueues(String queueUrl, Handler<AsyncResult<List<String>>> resultHandler) {
        withClient(client -> {
            var request = ListDeadLetterSourceQueuesRequest.builder()
                .queueUrl(queueUrl)
                .build();
            
            handleAsyncResult(client.listDeadLetterSourceQueues(request), 
                ListDeadLetterSourceQueuesResponse::queueUrls, resultHandler);
        });
    }
    
    @Override
    public Future<List<String>> listDeadLetterSourceQueues(String queueUrl) {
        Promise<List<String>> promise = Promise.promise();
        listDeadLetterSourceQueues(queueUrl, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }
    
    private JsonObject messageToJsonObject(Message message) {
        JsonObject result = new JsonObject()
            .put("id", message.messageId())
            .put("body", message.body())
            .put("bodyMd5", message.md5OfBody())
            .put("receiptHandle", message.receiptHandle());
        
        // Convert MessageSystemAttributeName map to String map
        if (message.attributes() != null) {
            Map<String, Object> stringAttributes = new HashMap<>();
            message.attributes().forEach((key, value) -> stringAttributes.put(key.toString(), value));
            result.put("attributes", new JsonObject(stringAttributes));
        }
        
        if (message.messageAttributes() != null && !message.messageAttributes().isEmpty()) {
            JsonObject messageAttributes = new JsonObject();
            message.messageAttributes().forEach((key, value) -> {
                JsonObject attr = new JsonObject()
                    .put("dataType", value.dataType());
                
                if (value.binaryValue() != null) {
                    attr.put("binaryData", value.binaryValue().asByteArray());
                } else {
                    attr.put("stringData", value.stringValue());
                }
                
                messageAttributes.put(key, attr);
            });
            result.put("messageAttributes", messageAttributes);
        }
        
        return result;
    }
    
    private <T, R> void handleAsyncResult(CompletableFuture<T> future, 
                                         java.util.function.Function<T, R> mapper, 
                                         Handler<AsyncResult<R>> resultHandler) {
        future.whenComplete((result, throwable) -> {
            if (throwable != null) {
                resultHandler.handle(Future.failedFuture(throwable));
            } else {
                try {
                    R mappedResult = mapper.apply(result);
                    resultHandler.handle(Future.succeededFuture(mappedResult));
                } catch (Exception e) {
                    resultHandler.handle(Future.failedFuture(e));
                }
            }
        });
    }
    
    private <T> void handleVoidAsyncResult(CompletableFuture<T> future, 
                                         Handler<AsyncResult<Void>> resultHandler) {
        future.whenComplete((result, throwable) -> {
            if (throwable != null) {
                resultHandler.handle(Future.failedFuture(throwable));
            } else {
                resultHandler.handle(Future.succeededFuture());
            }
        });
    }
}
