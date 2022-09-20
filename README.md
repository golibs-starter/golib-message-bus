# Golib Message Bus

Message Bus solutions for Golang project. Kafka is now supported.

### Setup instruction

Base setup, see [GoLib Instruction](https://gitlab.com/golibs-starter/golib/-/blob/develop/README.md)

Both `go get` and `go mod` are supported.

```shell
go get gitlab.com/golibs-starter/golib-message-bus
```

### Usage

Using `fx.Option` to include dependencies for injection.

```go
package main

import (
    "gitlab.com/golibs-starter/golib-message-bus"
    "gitlab.com/golibs-starter/golib-message-bus/kafka/core"
    "go.uber.org/fx"
)

func main() {
    fx.New(
        // Required
        golibmsg.KafkaCommonOpt(),

        // When you want to create topics if it doesn't exist.
        golibmsg.KafkaAdminOpt(),

        // When you want to produce message to Kafka.
        golibmsg.KafkaProducerOpt(),

        // When you want to consume message from Kafka.
        golibmsg.KafkaConsumerOpt(),

        // When you want all consumers are ready before application started.
        // Helpful in the integration test, see example here:
        // https://gitlab.com/golibs-starter/golib-sample/-/blob/develop/src/worker/testing/handler/send_order_to_delivery_provider_test.go
        golibmsg.KafkaConsumerReadyWaitOpt(),

        // When you want to enable graceful shutdown.
        // This option will invoke producer/consumer close functions when OnStop is hooked.
        golibmsg.KafkaGracefulShutdownOpt(),

        // When you want to register a consumer.
        // Consumer has to implement core.ConsumerHandler
        golibmsg.ProvideConsumer(NewCustomConsumer),
    ).Run()
}

// CustomConsumer is implementation of core.ConsumerHandler
type CustomConsumer struct {
}

func NewCustomConsumer() core.ConsumerHandler {
    return &CustomConsumer{}
}

func (c CustomConsumer) HandlerFunc(message *core.ConsumerMessage) {
    // Will run when a message arrived
}

func (c CustomConsumer) Close() {
    // Will run when application stop
}
```

### Configuration

```yaml
app:
    kafka:
        # Configuration for KafkaCommonOpt()
        # This is global configuration for all admin/producer/consumer if it is not configured.
        # ========================================
        # ======== START GLOBAL CONFIG ===========
        # ========================================
        # Kafka brokers to connect to.
        # Separate with commas. By default, localhost:9092 is used.
        bootstrapServers: kafka1:9092,kafka2:9092

        # Whether to use TLS when connecting to the broker.
        # By default, unsecured connection is used (leave empty).
        securityProtocol: TLS

        # A user-provided string sent with every request to the brokers for logging, debugging, and auditing purposes.
        clientId: golib

        # TLS configuration when securityProtocol=TLS
        tls:
            # A file contains public key from a pair of files.
            # The file must contain PEM encoded data.
            certFileLocation: "config/certs/test.dev-cert.pem"

            # A file contains private key from a pair of files.
            # The file must contain PEM encoded data.
            keyFileLocation: "config/certs/test.dev-key.pem"

            # A file contains root certificate authorities
            # that clients use when verifying server certificates.
            caFileLocation: "config/certs/test.dev-ca.pem"

            # Controls whether a client verifies
            # the server's certificate chain and host name.
            insecureSkipVerify: false
        # ========================================
        # ========= END GLOBAL CONFIG ============
        # ========================================

        # Configuration for KafkaAdminOpt()
        # These fields which existing in global config
        # can be overridden as bellow.
        admin:
            bootstrapServers: kafka1:9092,kafka2:9092
            securityProtocol: TLS
            clientId: golib
            tls:
                certFileLocation: "config/certs/test.dev-cert.pem"
                keyFileLocation: "config/certs/test.dev-key.pem"
                caFileLocation: "config/certs/test.dev-ca.pem"
                insecureSkipVerify: false
            topics:
                -   name: c1.http-request # Topic name when auto create topics is enabled
                    partitions: 1 # The number of partitions when topic is created. Default: 1.
                    replicaFactor: 1 # The number of copies of a topic in a Kafka cluster. Default: 1
                    retention: 72h # The period of time the topic will retain old log segments before deleting or compacting them. Default 72h.
                -   name: c1.order.order-created
                    partitions: 1
                    replicaFactor: 1
                    retention: 72h

        # Configuration for KafkaProducerOpt()
        # These fields which existing in global config
        # can be overridden as bellow.
        producer:
            bootstrapServers: kafka1:9092,kafka2:9092
            securityProtocol: TLS
            clientId: golib
            tls:
                certFileLocation: "config/certs/test.dev-cert.pem"
                keyFileLocation: "config/certs/test.dev-key.pem"
                caFileLocation: "config/certs/test.dev-ca.pem"
                insecureSkipVerify: false
            flushMessages: 1
            flushFrequency: 1s
            eventMappings:
                RequestCompletedEvent:
                    topicName: c1.http-request # Defines the topic that event will be sent to.
                    transactional: false # Enable/disable transactional when sending event message.
                    disable: false # Enable/disable send event message
                OrderCreatedEvent:
                    topicName: c1.order.order-created
                    transactional: false
                    disable: true

        # Configuration for KafkaConsumerOpt()
        # These fields which existing in global config
        # can be overridden as bellow.
        consumer:
            bootstrapServers: kafka1:9092,kafka2:9092
            securityProtocol: TLS
            clientId: golib
            tls:
                certFileLocation: "config/certs/test.dev-cert.pem"
                keyFileLocation: "config/certs/test.dev-key.pem"
                caFileLocation: "config/certs/test.dev-ca.pem"
                insecureSkipVerify: false
            handlerMappings:
                PushRequestCompletedToElasticSearchHandler: # It has to equal to the struct name of consumer
                    topic: c1.http-request # The topic that consumed by consumer
                    groupId: c1.http-request.PushRequestCompletedEsHandler.local # The group that consumed by consumer
                    enable: true # Enable/disable consumer
                PushOrderToElasticSearchHandler:
                    topic: c1.order.order-created
                    groupId: c1.order.order-created.PushRequestCompletedEsHandler.local
                    enable: true
```
