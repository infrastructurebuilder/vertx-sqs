package uy.kohesive.vertx.sqs.test;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.elasticmq.rest.sqs.SQSRestServer;
import org.elasticmq.rest.sqs.SQSRestServerBuilder;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import uy.kohesive.vertx.sqs.SqsClient;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@ExtendWith(VertxExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
public class SqsQueueConsumerVerticleTest {
    
    private static final int ELASTIC_MQ_PORT = 9324;
    private static final String ELASTIC_MQ_HOST = "localhost";
    
    private static Vertx vertx;
    private static SqsClient client;
    private static SQSRestServer sqsServer;
    
    private String deploymentId;
    
    private static String getQueueUrl(String queueName) {
        return "http://" + ELASTIC_MQ_HOST + ":" + ELASTIC_MQ_PORT + "/000000000000/" + queueName;
    }
    
    private static JsonObject getConfig() {
        return new JsonObject()
            // SQS client config
            .put("host", ELASTIC_MQ_HOST)
            .put("port", ELASTIC_MQ_PORT)
            .put("accessKey", "someAccessKey")
            .put("secretKey", "someSecretKey")
            .put("region", "us-west-2")
            // Consumer verticle config
            .put("pollingInterval", 1000)
            .put("queueUrl", getQueueUrl("testQueue"))
            .put("address", "sqs.queue.test");
    }
    
    @BeforeAll
    static void before(VertxTestContext context) throws Exception {
        vertx = Vertx.vertx();
        sqsServer = SQSRestServerBuilder.withPort(ELASTIC_MQ_PORT).start();
        
        System.out.println("Started SQS server");
        
        client = SqsClient.create(vertx, getConfig());
        
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
    
    @BeforeEach
    void beforeTest(VertxTestContext context) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("VisibilityTimeout", "1");
        
        client.createQueue("testQueue", attributes).onComplete(context.succeeding(queueUrl -> {
            context.verify(() -> {
                Assertions.assertEquals(queueUrl, getQueueUrl("testQueue"));
            });
            
            vertx.deployVerticle("uy.kohesive.vertx.sqs.SqsQueueConsumerVerticle", 
                new DeploymentOptions().setConfig(getConfig())).onComplete(context.succeeding(id -> {
                    deploymentId = id;
                    context.completeNow();
                }));
        }));
    }
    
    @AfterEach
    void afterTest(VertxTestContext context) {
        client.deleteQueue(getQueueUrl("testQueue")).onComplete(context.succeeding(result -> {
            context.completeNow();
        }));
    }
    
    @Test
    void testConsumeWithDeleteAcknowledge(VertxTestContext context) {
        testConsume(context, true);
    }
    
    @Test
    void testConsumeWithoutDeleteAcknowledge(VertxTestContext context) {
        testConsume(context, false);
    }
    
    private void testConsume(VertxTestContext context, boolean acknowledgeDelete) {
        CountDownLatch latch = new CountDownLatch(1);
        String testQueue = getQueueUrl("testQueue");
        String messageBody = "Test message body, acknowledged=" + acknowledgeDelete;
        
        client.sendMessage(testQueue, messageBody).onComplete(context.succeeding(messageId -> {
            var consumer = vertx.eventBus().consumer("sqs.queue.test", (Message<JsonObject> message) -> {
                if (acknowledgeDelete) {
                    message.reply(null); // delete the message
                }
                
                context.verify(() -> {
                    Assertions.assertEquals(messageBody, message.body().getString("body"));
                });
                latch.countDown();
            });
            
            try {
                latch.await(3, TimeUnit.SECONDS);
                
                // We wait longer than VisibilityTimeout, undeploy all the consumers and check if the queue is empty or not
                vertx.undeploy(deploymentId).onComplete(context.succeeding(undeployResult -> {
                    consumer.unregister();
                    
                    vertx.executeBlocking(() -> {
                        Thread.sleep(1500);
                        return null;
                    }, false).onComplete(context.succeeding(blockingResult -> {
                        client.receiveMessage(testQueue).onComplete(context.succeeding(messages -> {
                            System.out.println(messages.size() + " message(s) received: " + String.join(", ", 
                                messages.stream().map(Object::toString).toArray(String[]::new)));
                            
                            context.verify(() -> {
                                Assertions.assertEquals(acknowledgeDelete, messages.isEmpty());
                            });
                            context.completeNow();
                        }));
                    }));
                }));
            } catch (InterruptedException e) {
                context.failNow(e);
            }
        }));
    }
}
