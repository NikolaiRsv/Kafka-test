using Confluent.Kafka;
using Newtonsoft.Json;
using System;

namespace KafkaApp
{
    public class MessageConsumer
    {
        private readonly IConsumer<Ignore, string> consumer;

        public MessageConsumer(ConsumerConfig config)
        {
            consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        }

        public void StartConsuming()
        {
            Console.WriteLine("Consuming messages. Enter 'stop' to stop consuming:");

            consumer.Subscribe("my-data");

            try
            {
                while (true)
                {
                    var consumeResult = consumer.Consume();
                    var personJson = consumeResult.Message.Value;

                    if (!string.IsNullOrEmpty(personJson))
                    {
                        var person = JsonConvert.DeserializeObject<Person>(personJson);

                        if (person != null)
                        {
                            Console.WriteLine($"Received message - Id: {person.Id}, Name: {person.Name}");
                        }
                    }

                    if (consumeResult.IsPartitionEOF)
                    {
                        Console.WriteLine($"Reached end of partition: {consumeResult.TopicPartitionOffset}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
            finally
            {
                consumer.Close();
                consumer.Dispose();
            }

            Console.WriteLine("Stopped consuming messages.");
        }

        public void StopConsuming()
        {
            consumer.Close();
        }
    }
}
