[![Java](https://img.shields.io/badge/java-21-blue.svg)](https://openjdk.org/projects/jdk/21/) [![Maven Central](https://img.shields.io/maven-central/v/uy.kohesive.vertx/vertx-sqs.svg)](https://mvnrepository.com/artifact/uy.kohesive.sqs) [![CircleCI branch](https://img.shields.io/circleci/project/kohesive/vertx-sqs/master.svg)](https://circleci.com/gh/kohesive/vertx-sqs/tree/master) [![Issues](https://img.shields.io/github/issues/kohesive/vertx-sqs.svg)](https://github.com/kohesive/vertx-sqs/issues?q=is%3Aopen) [![DUB](https://img.shields.io/dub/l/vibe-d.svg)](https://github.com/kohesive/vertx-sqs/blob/master/LICENSE)

# Amazon SQS Client for Vert.x

This Vert.x client allows Amazon SQS access in two ways:

* As a @VertxGen service bridge to Amazon SQS Async Client methods  
* As an Amazon SQS queue consuming verticle

**Updated for Vert.x 5.x, Java 21, and AWS SDK v2**

### Maven

Add the following dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>uy.kohesive.vertx</groupId>
    <artifactId>vertx-sqs</artifactId>
    <version>1.0.0-BETA-02</version>
</dependency>
```

### Requirements

- Java 21 or higher
- Vert.x 5.x
- Maven 3.6+


## Service usage

Client must be configured with a region. It can also be configured with AWS credentials, otherwise a default `~/.aws/credentials` credentials file will be used:

```
JsonObject config = new JsonObject()
    .put("accessKey", "someAccessKey")
    .put("secretKey", "someSecretKey")
    .put("region", "us-west-2");
```

The client is initialized asynchronously:

```
SqsClient client = SqsClient.create(vertx, config);
client.start(result -> {
    if (result.succeeded()) {
        System.out.println("Client is initialized"); 
    }
});
```

Once the client is initialized, it can be used to access the Amazon SQS API in async manner:

```
client.sendMessage("MyQueue", "Hello World", result -> {
    if (result.succeeded()) {
        System.out.println("Message is sent");
    }
});
```

## Queue consuming verticle usage

An SQS queue consumer can be configured to poll the queue periodically and send the messages polled to a Vert.x event bus address. The verticle is deployed with a config containing AWS credentials (see above), region, SQS queue url, Vert.x address and a polling interval in millisecods:

```
JsonObject config = new JsonObject()
    .put("accessKey", "someAccessKey")
    .put("secretKey", "someSecretKey")
    .put("region", "us-west-2")
    .put("pollingInterval", 1000)
    .put("queueUrl", "https://sqs.us-west-2.amazonaws.com/1000/MyQueue")
    .put("address", "sqs.queue.MyQueue");
    
vertx.deployVerticle("uy.kohesive.vertx.sqs.SqsQueueConsumerVerticle", new DeploymentOptions().setConfig(config));    
```

When the verticle is successfully deployed, it starts polling the SQS and routing the messages to the Vert.x address configured.

To delete a message from the SQS queue, simply reply with `null` to that message in your Vert.x event bus consumer:

```
vertx.eventBus().consumer("sqs.queue.MyQueue", message -> {
    // Process the message
    // ...
    message.reply(null);
});
```

### Sequential consumer verticle

`SqsSequentialQueueConsumerVerticle` is used to limit the SQS messages consumption rate. It uses a thread pool of size configured by `workersCount` parameters to fetch messages from the queue, and waiting for message acknowledgment (see above) before fetching more messages. Messages are buffered, the buffer size is configured by `bufferSize` parameter.

## Message producer verticle usage

An SQS message producer verticle can be configured to route the event bus messages to an SQS queue. The verticle is deployed with a config containing AWS credentials (see above), region, SQS queue url, Vert.x address and a `local` flag, which specifies whether or not the vertcle should start a local message consumer (`false` by default):

```
JsonObject config = new JsonObject()
    .put("accessKey", "someAccessKey")
    .put("secretKey", "someSecretKey")
    .put("region", "us-west-2")
    .put("queueUrl", "https://sqs.us-west-2.amazonaws.com/1000/MyQueue")
    .put("address", "sqs.queue.MyQueue")
    .put("local", true);
    
vertx.deployVerticle("uy.kohesive.vertx.sqs.SqsQueueProducerVerticle", new DeploymentOptions().setConfig(config));    
```

When the verticle is successfully deployed, it starts routing the event-bus messages (message body is expected to be of String type) from the event-bus address configured to the SQS queue.

## AWS Credentials

All the verticles mentioned above can be constucted using a secondary constructor accepting `AWSCredentialsProvider` instance. In that case, no AWS credentials configuration via the `JsonObject` is needed, but cluster deployment becomes a problem, as we can't deploy the verticle using only its ID.
