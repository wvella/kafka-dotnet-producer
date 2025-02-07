// Copyright 2018 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using Confluent.Kafka.SyncOverAsync;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Json;
using System.IO;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace Confluent.Kafka.Examples.AvroSpecific
{
    class Producer
    {
        public static void Main(string[] args)
        {
            var stopwatchMain = Stopwatch.StartNew();
            const string topicName = "raw.inventory";

            // Build configuration
            var configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .Build();

            // Read Kafka configuration
            var kafkaConfig = configuration.GetSection("Kafka");
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = kafkaConfig["BootstrapServers"],
                SaslUsername = kafkaConfig["SaslUsername"],
                SaslPassword = kafkaConfig["SaslPassword"],
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                Acks = Acks.All,
                Debug = kafkaConfig["debug"],
                LingerMs = int.Parse(kafkaConfig["LingerMs"]),
                BatchNumMessages = int.Parse(kafkaConfig["BatchNumMessages"]),
                MessageMaxBytes = int.Parse(kafkaConfig["MessageMaxBytes"]),
                BatchSize = int.Parse(kafkaConfig["BatchSize"])
            };

            // Read Schema Registry configuration
            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = configuration["SchemaRegistry:Url"],
                BasicAuthUserInfo = configuration["SchemaRegistry:BasicAuthUserInfo"]
            };

            var avroSerializerConfig = new AvroSerializerConfig
            {
                // optional Avro serializer properties:
                BufferBytes = 100
            };

            Action<DeliveryReport<string, User>> handler = r =>
                Console.WriteLine(!r.Error.IsError
                    ? $"Delivered {r.Value.name} to partition: {r.Partition} offset: {r.Offset} with a status of: {r.Status}"
                    : $"Delivery Error: {r.Error.Reason}");

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))

            using (var p = new ProducerBuilder<string, User>(producerConfig)
            .SetValueSerializer(new AvroSerializer<User>(schemaRegistry, avroSerializerConfig).AsSyncOverAsync())
            .Build())
            {
                Console.WriteLine($"{p.Name} producing on {topicName}.");

                int i = 1;
                int numOfMessages = int.Parse(configuration["Producer:numOfMessages"]);
                int sizeOfPayload = int.Parse(configuration["Producer:sizeOfPayload"]);

                for (int j = 0; j < numOfMessages; j++)
                {
                    User user = new User { name = "user:" + j, favorite_color = new string('g', sizeOfPayload), favorite_number = ++i, hourly_rate = new Avro.AvroDecimal(67.99) };
                    try
                    {
                        // Example 1 - Produce - Still Async with a callback
                        p.Produce(topicName, new Message<string, User> { Key = "user-" + j, Value = user }, handler);
                    }
                    catch (ProduceException<string, User> e) when (e.Error.Code == ErrorCode.Local_QueueFull)
                    {
                        Console.WriteLine($"Local Delivery Queue full. Retrying... {e.Message}");
                        Console.WriteLine(j);
                        p.Poll(TimeSpan.FromMilliseconds(2000)); // https://support.confluent.io/hc/en-us/articles/360053146092-How-to-bypass-BufferError-Local-Queue-full-in-librdkafka-producers
                    }
                    catch (Exception e) // Catch all other exceptions
                    {
                        Console.WriteLine($"Delivery failed: {e.Message}");
                    }
                }
                // wait for up to 100 seconds for any inflight messages to be delivered.
                p.Flush(TimeSpan.FromSeconds(100));
            }
            stopwatchMain.Stop();
            Console.WriteLine($"Total execution time: {stopwatchMain.Elapsed}");
        }

    }
}
