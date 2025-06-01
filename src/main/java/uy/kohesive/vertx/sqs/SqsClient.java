package uy.kohesive.vertx.sqs;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import uy.kohesive.vertx.sqs.impl.SqsClientImpl;

import java.util.List;
import java.util.Map;

public interface SqsClient {

    static SqsClient create(Vertx vertx, JsonObject config) {
        return new SqsClientImpl(vertx, config, null);
    }

    /**
     * Async result is a queue's URL.
     */
    void createQueue(String name, Map<String, String> attributes, Handler<AsyncResult<String>> resultHandler);

    Future<String> createQueue(String name, Map<String, String> attributes);

    void deleteQueue(String queueUrl, Handler<AsyncResult<Void>> resultHandler);

    Future<Void> deleteQueue(String queueUrl);

    /**
     * Async result is a list of queues' URLs. 'namePrefix' is nullable.
     */
    void listQueues(String namePrefix, Handler<AsyncResult<List<String>>> resultHandler);

    Future<List<String>> listQueues(String namePrefix);

    /**
     * Async result is a message's Id.
     *
     * Message attributes JSON format:
     * <code>
     *   {
     *     "someLabel":{
     *       "dataType":"String",
     *       "stringData":"Hello World"
     *     },
     *     "anotherLabel":{
     *       "dataType":"Binary",
     *       "binaryData":"TWFuIGlzIGRpc3Rpbmd1"
     *     }
     *   }
     * </code>
     *
     */
    void sendMessage(String queueUrl, String messageBody, JsonObject attributes, Integer delaySeconds, Handler<AsyncResult<String>> resultHandler);

    Future<String> sendMessage(String queueUrl, String messageBody, JsonObject attributes, Integer delaySeconds);

    /**
     * Async result is a message's Id.
     */
    void sendMessage(String queueUrl, String messageBody, JsonObject attributes, Handler<AsyncResult<String>> resultHandler);

    Future<String> sendMessage(String queueUrl, String messageBody, JsonObject attributes);

    /**
     * Async result is a message's Id.
     */
    void sendMessage(String queueUrl, String messageBody, Handler<AsyncResult<String>> resultHandler);

    Future<String> sendMessage(String queueUrl, String messageBody);

    /**
     * Async result is a message JSON object.
     */
    void receiveMessage(String queueUrl, Handler<AsyncResult<List<JsonObject>>> resultHandler);

    Future<List<JsonObject>> receiveMessage(String queueUrl);

    void receiveMessages(String queueUrl, Integer maxMessages, Handler<AsyncResult<List<JsonObject>>> resultHandler);

    Future<List<JsonObject>> receiveMessages(String queueUrl, Integer maxMessages);

    void deleteMessage(String queueUrl, String receiptHandle, Handler<AsyncResult<Void>> resultHandler);

    Future<Void> deleteMessage(String queueUrl, String receiptHandle);

    void setQueueAttributes(String queueUrl, Map<String, String> attributes, Handler<AsyncResult<Void>> resultHandler);

    Future<Void> setQueueAttributes(String queueUrl, Map<String, String> attributes);

    void changeMessageVisibility(String queueUrl, String receiptHandle, Integer visibilityTimeout, Handler<AsyncResult<Void>> resultHandler);

    Future<Void> changeMessageVisibility(String queueUrl, String receiptHandle, Integer visibilityTimeout);

    /**
     * Async result is the queue's URL. 'queueOwnerAWSAccountId' is nullable.
     */
    void getQueueUrl(String queueName, String queueOwnerAWSAccountId, Handler<AsyncResult<String>> resultHandler);

    Future<String> getQueueUrl(String queueName, String queueOwnerAWSAccountId);

    void addPermissionAsync(String queueUrl, String label, List<String> aWSAccountIds, List<String> actions, Handler<AsyncResult<Void>> resultHandler);

    Future<Void> addPermissionAsync(String queueUrl, String label, List<String> aWSAccountIds, List<String> actions);

    void removePermission(String queueUrl, String label, Handler<AsyncResult<Void>> resultHandler);

    Future<Void> removePermission(String queueUrl, String label);

    /**
     * Async result is the attributes' keys/values map. 'attributeNames' is nullable.
     */
    void getQueueAttributes(String queueUrl, List<String> attributeNames, Handler<AsyncResult<JsonObject>> resultHandler);

    Future<JsonObject> getQueueAttributes(String queueUrl, List<String> attributeNames);

    void listDeadLetterSourceQueues(String queueUrl, Handler<AsyncResult<List<String>>> resultHandler);

    Future<List<String>> listDeadLetterSourceQueues(String queueUrl);

    void start(Handler<AsyncResult<Void>> resultHandler);

    Future<Void> start();

    void stop(Handler<AsyncResult<Void>> resultHandler);

    Future<Void> stop();

}
