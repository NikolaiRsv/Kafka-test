using System;

namespace YourNamespace
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var consumer = new Consumer("localhost:9092", "my-data"))
            using (var producer = new Producer("localhost:9092"))
            {
                Console.WriteLine("Enter 'consume' to start consuming messages, 'produce' to start producing messages, or 'exit' to quit:");

                string command;
                while ((command = Console.ReadLine()) != "exit")
                {
                    if (command == "consume")
                    {
                        Console.WriteLine("Consuming messages. Enter 'stop' to stop consuming:");
                        consumer.Start();
                        while (Console.ReadLine() != "stop") { }
                        consumer.Stop();
                        Console.WriteLine("Stopped consuming messages.");
                    }
                    else if (command == "produce")
                    {
                        Console.WriteLine("Producing messages. Enter messages to send or 'stop' to stop producing:");
                        string message;
                        while ((message = Console.ReadLine()) != "stop") producer.Produce(message);
                        Console.WriteLine("Stopped producing messages.");
                    }
                    else Console.WriteLine("Invalid command. Enter 'consume' to start consuming messages, 'produce' to start producing messages, or 'exit' to quit.");
                }
            }
        }
    }
}