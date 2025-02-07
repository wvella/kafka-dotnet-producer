# .NET load testing producer using the Producer method

This project contains a .NET application that produces records to a topic on Confluent Cloud and prints the produced records to the console.

This has been built to play around with the batching properties. In my example, I have deployed a Standard Cluster in Confluent Cloud, with 1 Topic, 6 partitions, with a `max.message.bytes` of `7340032`.

The producer is configured with the following properties:

```
    "LingerMs": 500,
    "BatchNumMessages": 4000,
    "MessageMaxBytes": 6291456,
    "BatchSize": 6291456
```

## Getting Started

### Prerequisites

.NET Core (>= 8.0) installed.

### Usage

Build the producer by running:

```shell
$ dotnet build producer.csproj
```

Run the producer by running this from the src/ directory:

```shell
$ dotnet run
```

### App Configuration

Create a appsettings.json file in the root as below:

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
