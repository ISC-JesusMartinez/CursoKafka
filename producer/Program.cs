using System;
using Confluent.Kafka;

namespace producer
{
    class Program
    {
        static void Main(string[] args)
        {
            string topicName = "test-topic";

            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                ClientId = "producer1",
            };

            ProducerBuilder<Null, string> producerBuilder = new ProducerBuilder<Null, string>(config);

            using (IProducer<Null, string> producer = producerBuilder.Build())
            {
                for (int i = 0; i < 100; i++)
                {
                    var message = new Message<Null, string>
                    {
                        Value = $"Mensaje {i}",
                    };

                    producer.Produce(topicName, message, (deliveryReport)=>
                    {
                        if (!deliveryReport.Error.IsError)
                        {
                            Console.WriteLine($"Mensaje entregado en {deliveryReport.TopicPartitionOffset} Mensaje: {deliveryReport.Message.Value}");
                        }
                        else
                        {
                            Console.WriteLine($"Error en entrega {deliveryReport.Error.Reason}");
                        }
                    });
                }

                producer.Flush();
            }
        }
    }
}
