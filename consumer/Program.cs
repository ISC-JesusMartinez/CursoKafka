using System;
using System.Threading;
using Confluent.Kafka;

namespace consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            using var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                ClientId = "consumer1",
                GroupId = "group1",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true, // default(true)
                EnableAutoOffsetStore = true, // default (true)
            };

            ConsumerBuilder<Ignore, string> consumerBuilder = new ConsumerBuilder<Ignore, string>(config);

            using (IConsumer<Ignore, string> consumer = consumerBuilder.Build())
            {
                consumer.Subscribe("test-topic");

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = consumer.Consume(cts.Token);

                            // Procesar el mensaje
                            Console.WriteLine($"Mensaje recibido Topic: {cr.Topic}. Partición: {cr.Partition.Value}. Key: {cr.Message.Key}. Valor: {cr.Message.Value}.");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine("Error occured: {0}", e.Error.Reason);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                }

                consumer.Close();
            }
        }
    }
}
