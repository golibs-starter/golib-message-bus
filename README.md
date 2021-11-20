# Golib Message Bus

Message Bus solutions for Golang project. Kafka is now supported.

### Setup instruction

Base setup, see [GoLib Instruction](https://gitlab.id.vin/vincart/golib/-/blob/develop/README.md)

Both `go get` and `go mod` are supported.

```shell
go get gitlab.id.vin/vincart/golib-message-bus
```

### Usage

Using `fx.Option` to include dependencies for injection.

```go
package main

import (
    "gitlab.id.vin/vincart/golib-message-bus"
    "go.uber.org/fx"
)

func main()  {
    options := []fx.Option{
        // Required
        golibmsg.KafkaCommonOpt(),

        // When you want to create topics if it doesn't exist.
        golibmsg.KafkaAdminOpt(),

        // When you want to produce message to Kafka.
        golibmsg.KafkaProducerOpt(),

        // When you want to consume message from Kafka.
        golibmsg.KafkaConsumerOpt(),

        // When you want to register a consumer
        golibmsg.ProvideConsumer(consumer.NewCustomConsumer),
    }
}
```

### Configuration

```yaml
app:
  kafka: # Configuration for KafkaCommonOpt()
    bootstrapServers: kafka1:9092,kafka2:9092 # Kafka brokers to connect to. Separate with commas. By default, localhost:9092 is used.
    securityProtocol: TLS # Whether to use TLS when connecting to the broker. By default, unsecured connection is used (leave empty).
    clientId: vincart # A user-provided string sent with every request to the brokers for logging, debugging, and auditing purposes.
    tls:
      certFileLocation: "config/certs/test.dev-cert.pem" # A file contains public key from a pair of files. The file must contain PEM encoded data.
      keyFileLocation: "config/certs/test.dev-key.pem" # A file contains private key from a pair of files. The file must contain PEM encoded data.
      caFileLocation: "config/certs/test.dev-ca.pem" # A file contains root certificate authorities that clients use when verifying server certificates.
      insecureSkipVerify: false # Controls whether a client verifies the server's certificate chain and host name.
    admin:
      bootstrapServers: kafka1:9092,kafka2:9092
      securityProtocol: TLS
      clientId: vincart
      tls:
        certFileLocation: "config/certs/test.dev-cert.pem"
        keyFileLocation: "config/certs/test.dev-key.pem"
        caFileLocation: "config/certs/test.dev-ca.pem"
        insecureSkipVerify: false
    producer:
      bootstrapServers: kafka1:9092,kafka2:9092
      securityProtocol: TLS
      clientId: vincart
      tls:
        certFileLocation: "config/certs/test.dev-cert.pem"
        keyFileLocation: "config/certs/test.dev-key.pem"
        caFileLocation: "config/certs/test.dev-ca.pem"
        insecureSkipVerify: false
      flushMessages: 1
      flushFrequency: 1s
    consumer:
      bootstrapServers: kafka1:9092,kafka2:9092
      securityProtocol: TLS
      clientId: vincart
      tls:
        certFileLocation: "config/certs/test.dev-cert.pem"
        keyFileLocation: "config/certs/test.dev-key.pem"
        caFileLocation: "config/certs/test.dev-ca.pem"
        insecureSkipVerify: false

vinid:
  kafka:
    topics: # Configuration for KafkaAdminOpt()
      - name: c1.http-request # Topic name when auto create topics is enabled
        partitions: 1 # The number of partitions when topic is created. Default: 1.
        replicaFactor: 1 # The number of copies of a topic in a Kafka cluster. Default: 1
        retention: 72h # The period of time the topic will retain old log segments before deleting or compacting them. Default 72h.
      - name: c1.order.order-created
        partitions: 1
        replicaFactor: 1
        retention: 72h

    consumer: # Configuration for KafkaConsumerOpt()
      topics:
        PushRequestCompletedToElasticSearchHandler: # It has to equal to the struct name of consumer
          topic: c1.http-request # The topic that consumed by consumer
          groupId: c1.http-request.PushRequestCompletedEsHandler.local # The group that consumed by consumer
          enable: true # Enable/disable consumer
        PushOrderToElasticSearchHandler:
          topic: c1.order.order-created
          groupId: c1.order.order-created.PushRequestCompletedEsHandler.local
          enable: true

  messagebus:
    event:
      producer: # Configuration for KafkaProducerOpt()
        topicMappings:
          RequestCompletedEvent:
            topicName: c1.http-request # Defines the topic that event will be sent to.
            transactional: false # Enable/disable transactional when sending event message.
            disable: false # Enable/disable send event message
          OrderCreatedEvent:
            topicName: c1.order.order-created
            transactional: false
            disable: true
```
