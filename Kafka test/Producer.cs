using Confluent.Kafka;

namespace YourNamespace
{
    public class Producer : IDisposable
    {
        private IProducer<Null, string> producer;

        public Producer(string bootstrapServers)
        {
            producer = new ProducerBuilder<Null, string>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build();
        }

        public void Produce(string message) => producer.Produce("my-data", new Message<Null, string> { Value = message });

        public void Dispose() => producer.Dispose();
    }
}