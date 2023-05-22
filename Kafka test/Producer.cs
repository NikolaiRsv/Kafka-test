using Confluent.Kafka;
using Newtonsoft.Json;
using System;

namespace KafkaApp
{
    public class Producer : IDisposable
    {
        private readonly IProducer<Null, string> producer;

        public Producer(ProducerConfig config)
        {
            producer = new ProducerBuilder<Null, string>(config).Build();
        }

        public void ProduceMessage(Person person)
        {
            var personJson = JsonConvert.SerializeObject(person);
            var message = new Message<Null, string> { Value = personJson };

            var deliveryReport = producer.ProduceAsync("my-data", message).GetAwaiter().GetResult();
            Console.WriteLine($"Produced message - Id: {person.Id}, Name: {person.Name}, Topic: {deliveryReport.Topic}");
        }

        public void Dispose()
        {
            producer.Dispose();
        }
    }
}