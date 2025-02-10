# .NET load testing producer using the Producer and ProducerAsync method

This project contains 2 .NET applications that produce records to a topic on Confluent Cloud and prints batch information to the console.

This has been built to play around with the batching properties. In my example, I have deployed the following:
```
- Kafka Cluser: Standard Kafka Cluster in AWS
- Topic: raw.inventory
- Partitions: 6
- max.message.bytes: 7340032
```

The batching parameters are configurable in the `appsettings.json` configuration file, however I landed on the following properties:

```
"LingerMs": 500,
"BatchNumMessages": 4000,
"MessageMaxBytes": 6291456,
"BatchSize": 6291456
```

## Getting Started

FYI: To generate a class from an AVRO schema, run:

`avrogen -s User.avsc . --namespace "confluent.io.examples.serialization.avro:avro"`

### Prerequisites

.NET Core (>= 8.0) installed.

### Usage

Build either the `Produce` or `ProduceAsync` producer by running:

`cd dotnet-producerAsync OR dotnet-producer`

```shell
$ dotnet build
```
Run the `Produce` or `ProduceAsync` by running:

```shell
$ dotnet run
```
### App Configuration

Create a `appsettings.json` file in the root folder as below:

```json
{
    "Kafka": {
      "BootstrapServers": "<<bootstrap name>>:<<bootstrap port>>",
      "SaslUsername": "<<API Key>>",
      "SaslPassword": "<<API Secret>>",
      "LingerMs": 2000,
      "BatchNumMessages": 4000,
      "MessageMaxBytes": 6291456,
      "BatchSize": 6291456,
      "Debug": "msg"
    },
    "SchemaRegistry": {
      "Url": "https://<<schema registry url>>",
      "BasicAuthUserInfo": "<<API Key>>:<<API Secret>>"
    },
    "Producer": {
      "numOfMessages": 400000,
      "sizeOfPayload": 2048
    },
    "_comments": {
      "note1": "1048576 = 1MB",
      "note2": "2097152 = 2MB",
      "note3": "4194304 = 4MB",
      "note4": "6291456 = 6MB",
      "note5": "7340032 = 7MB"
    }
  }
```
