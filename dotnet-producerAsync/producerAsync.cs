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
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Json;
using System.IO;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace wvella.avro
{
    class ProducerAsync
    {
        public static async Task Main(string[] args)
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

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))

            using (var producer =
                new ProducerBuilder<string, User>(producerConfig)
                    .SetValueSerializer(new AvroSerializer<User>(schemaRegistry, avroSerializerConfig))
                    .Build())
            {
                Console.WriteLine($"{producer.Name} producing on {topicName}.");

                int i = 1;
                int numOfMessages = int.Parse(configuration["Producer:numOfMessages"]);
                int sizeOfPayload = int.Parse(configuration["Producer:sizeOfPayload"]);
                List<Task<DeliveryResult<string, User>>> sendTasks = new List<Task<DeliveryResult<string, User>>>();
                for (int j = 0; j < numOfMessages; j++)
                {
                    User user = new User
                    {
                        name = "user:" + j,
                        favorite_color = new string('g', sizeOfPayload),
                        favorite_number = ++i,
                        hourly_rate = new Avro.AvroDecimal(67.99)
                    };

                    try
                    {
                        // Example 1 - ProduceAsync with an 'await' - Effectively making it Sync.
                        await produceMessagesAsync(producer, topicName, user, j);

                        // Example 2 - ProduceAsync with an 'TaskList' - Effectively making it Async and allows batching.
                        //produceMessagesAsyncBatch(producer, topicName, user, j, sendTasks);
                    }
                    catch (ProduceException<Null, string> e)
                    {
                        Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                    }
                }
                // Example 2 - ProduceAsync with an 'TaskList' - Effectively making it Async and allows batching.
                /* var results = await Task.WhenAll(sendTasks);
                foreach (var result in results)
                {
                    Console.WriteLine("--------------------------------------------------------------------------------------------------------------------------------------------------------");
                    Console.WriteLine($"Delivered '{result.Value.name}' to partition: '{result.Partition}' offset: '{result.Offset}'");
                    Console.WriteLine("--------------------------------------------------------------------------------------------------------------------------------------------------------");
                } */
            }
            stopwatchMain.Stop();
            Console.WriteLine($"Total execution time: {stopwatchMain.Elapsed}");
        }

        private static async Task produceMessagesAsync(IProducer<string, User> producer, string topicName, User user, int j)
        {
            var stopwatch = Stopwatch.StartNew();
            var dr = await producer.ProduceAsync(topicName, new Message<string, User> { Key = "user" + j, Value = user });
            Console.WriteLine("--------------------------------------------------------------------------------------------------------------------------------------------------------");
            Console.WriteLine($"Delivered '{dr.Value.name}' to partition: '{dr.Partition}' offset: '{dr.TopicPartitionOffset.Offset}' in {stopwatch.ElapsedMilliseconds} ms");
            Console.WriteLine("--------------------------------------------------------------------------------------------------------------------------------------------------------");
            stopwatch.Stop();
        }

        private static void produceMessagesAsyncBatch(IProducer<string, User> producer, string topicName, User user, int j, List<Task<DeliveryResult<string, User>>> sendTasks)
        {
            sendTasks.Add(producer.ProduceAsync(topicName, new Message<string, User> { Key = "user" + j, Value = user }));
        }
    }
}
