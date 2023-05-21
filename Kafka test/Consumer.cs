using System;
using System.Threading;
using Confluent.Kafka;

namespace YourNamespace
{
    public class Consumer : IDisposable
    {
        private IConsumer<Ignore, string> consumer;
        private Thread consumingThread;
        private CancellationTokenSource cancellationTokenSource;

        public Consumer(string bootstrapServers, string groupId)
        {
            consumer = new ConsumerBuilder<Ignore, string>(new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true
            }).Build();
        }

        public void Start()
        {
            cancellationTokenSource = new CancellationTokenSource();
            consumingThread = new Thread(() =>
            {
                consumer.Subscribe("my-data");
                try { while (true) Console.WriteLine($"Received message: {consumer.Consume(cancellationTokenSource.Token).Message.Value}"); }
                catch (OperationCanceledException) { }
            });
            consumingThread.Start();
        }

        public void Stop()
        {
            cancellationTokenSource.Cancel();
            consumingThread.Join();
        }

        public void Dispose() => consumer.Close();
    }
}