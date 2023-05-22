using Confluent.Kafka;
using Newtonsoft.Json;
using System;

namespace KafkaApp
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "your-group-id",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true
            };

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
            };

            var consumer = new Consumer(config);
            var producer = new Producer(producerConfig);

            Console.WriteLine("Enter 'consume' to start consuming messages, 'produce' to start producing messages, or 'exit' to quit:");

            string command;
            while ((command = Console.ReadLine()) != "exit")
            {
                if (command == "consume")
                {
                    consumer.StartConsuming();
                }
                else if (command == "produce")
                {
                    Console.WriteLine("Producing messages. Enter values for Id and Name or 'stop' to stop producing:");

                    string input;
                    while ((input = Console.ReadLine()) != "stop")
                    {
                        string[] values = input.Split(',');

                        if (values.Length == 2 && int.TryParse(values[0].Trim(), out int id))
                        {
                            var person = new Person { Id = id, Name = values[1].Trim() };
                            producer.ProduceMessage(person);
                        }
                        else
                        {
                            Console.WriteLine("Invalid input. Please enter Id and Name separated by a comma.");
                        }
                    }

                    Console.WriteLine("Stopped producing messages.");
                }
                else
                {
                    Console.WriteLine("Invalid command. Enter 'consume' to start consuming messages, 'produce' to start producing messages, or 'exit' to quit.");
                }
            }

            consumer.StopConsuming();
            producer.Dispose();
        }
    }
}
