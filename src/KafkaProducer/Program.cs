using System;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace KafkaProducer
{
    static class Program
    {
        public static async Task Main(string[] args)
        {
            var config = new ProducerConfig { 
                BootstrapServers = "edge01",
                //SaslUsername = "prod1",
                //SaslPassword = "prod1-secret",
                //SaslMechanism = SaslMechanism.ScramSha256,
                //SecurityProtocol = SecurityProtocol.SaslSsl,
                //EnableSslCertificateVerification = false,
                //Debug = "broker,topic,msg",
                //Acks = Acks.None,
                //CompressionType = CompressionType.None
            };

            // If serializers are not specified, default serializers from
            // `Confluent.Kafka.Serializers` will be automatically used where
            // available. Note: by default strings are encoded as UTF8.
            using (var p = new ProducerBuilder<string, string>(config).Build())
            {
                while (true)
                {
                    var dr = await p.ProduceAsync("test01", new Message<string, string> { Key = "a0001", Value = $"{DateTime.Now}" });
                    Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                    await Task.Delay(1000);
                }
            }
        }
    }
}