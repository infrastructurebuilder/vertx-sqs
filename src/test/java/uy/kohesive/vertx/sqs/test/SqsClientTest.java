package uy.kohesive.vertx.sqs.test;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.elasticmq.rest.sqs.SQSRestServer;
import org.elasticmq.rest.sqs.SQSRestServerBuilder;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import uy.kohesive.vertx.sqs.SqsClient;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@ExtendWith(VertxExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
public class SqsClientTest {
    
    private static final int ELASTIC_MQ_PORT = 9324;
    private static final String ELASTIC_MQ_HOST = "localhost";
    
    private static Vertx vertx;
    private static SqsClient client;
    private static SQSRestServer sqsServer;
    
    private static String getQueueUrl(String queueName) {
        return "http://" + ELASTIC_MQ_HOST + ":" + ELASTIC_MQ_PORT + "/000000000000/" + queueName;
    }
    
    @BeforeAll
    static void before(VertxTestContext context) throws Exception {
        vertx = Vertx.vertx();
        sqsServer = SQSRestServerBuilder.withPort(ELASTIC_MQ_PORT).start();
        
        JsonObject config = new JsonObject()
            .put("host", ELASTIC_MQ_HOST)
            .put("port", ELASTIC_MQ_PORT)
            .put("accessKey", "someAccessKey")
            .put("secretKey", "someSecretKey")
            .put("region", "us-west-2");
        
        client = SqsClient.create(vertx, config);
        
        client.start().onComplete(context.succeeding(result -> {
            context.completeNow();
        }));
        
        context.awaitCompletion(10, TimeUnit.SECONDS);
    }
    
    @AfterAll
    static void after(VertxTestContext context) throws Exception {
        client.stop().onComplete(context.succeeding(result -> {
            vertx.close().onComplete(context.succeeding(closeResult -> {
                sqsServer.stopAndWait();
                context.completeNow();
            }));
        }));
        
        context.awaitCompletion(10, TimeUnit.SECONDS);
    }
    
    @Test
    void testCreateAndListQueue(VertxTestContext context) {
        Map<String, String> attributes = new HashMap<>();
        
        client.createQueue("testQueue", attributes).onComplete(context.succeeding(queueUrl -> {
            client.listQueues(null).onComplete(context.succeeding(queues -> {
                System.out.println(queues);
                context.verify(() -> {
                    Assertions.assertTrue(queues.contains(getQueueUrl("testQueue")));
                });
                context.completeNow();
            }));
        }));
    }
    
    @Test
    void testSendReceiveAndDelete(VertxTestContext context) {
        String queueName = getQueueUrl("testQueue");
        String messageBody = "Test message";
        
        String stringAttribute = "someString";
        byte[] binaryAttribute = stringAttribute.getBytes();
        
        JsonObject attributes = new JsonObject()
            .put("stringAttribute", new JsonObject()
                .put("dataType", "String")
                .put("stringData", stringAttribute))
            .put("binaryAttribute", new JsonObject()
                .put("dataType", "Binary")
                .put("binaryData", binaryAttribute));
        
        // Send
        client.sendMessage(queueName, messageBody, attributes).onComplete(context.succeeding(messageId -> {
            // Receive
            client.receiveMessage(queueName).onComplete(context.succeeding(messages -> {
                context.verify(() -> {
                    Assertions.assertFalse(messages.isEmpty());
                    
                    JsonObject theMessage = messages.stream()
                        .filter(msg -> messageBody.equals(msg.getString("body")))
                        .findFirst()
                        .orElse(null);
                    
                    Assertions.assertNotNull(theMessage);
                    
                    JsonObject messageAttributes = theMessage.getJsonObject("messageAttributes");
                    Assertions.assertNotNull(messageAttributes);
                    Assertions.assertEquals(stringAttribute, 
                        messageAttributes.getJsonObject("stringAttribute").getString("stringData"));
                    
                    byte[] receivedByteArray = messageAttributes.getJsonObject("binaryAttribute").getBinary("binaryData");
                    Assertions.assertTrue(Arrays.equals(binaryAttribute, receivedByteArray));
                });
                
                // Delete
                String receipt = messages.get(0).getString("receiptHandle");
                Assertions.assertNotNull(receipt);
                
                client.deleteMessage(queueName, receipt).onComplete(context.succeeding(deleteResult -> {
                    // Message must be deleted by now, let's check
                    client.receiveMessage(queueName).onComplete(context.succeeding(deletedMessages -> {
                        context.verify(() -> {
                            boolean messageExists = deletedMessages.stream()
                                .anyMatch(msg -> messageBody.equals(msg.getString("body")));
                            Assertions.assertFalse(messageExists);
                        });
                        context.completeNow();
                    }));
                }));
            }));
        }));
    }
}
