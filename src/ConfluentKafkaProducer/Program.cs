using System;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace KafkaProducer
{
    static class Program
    {
        public static async Task Main(string[] args)
        {
            var config = new ProducerConfig { 
                BootstrapServers = "edge01"
                //SaslUsername = "prod1",
                //SaslPassword = "prod1-secret",
                //SaslMechanism = SaslMechanism.ScramSha256,
                //SecurityProtocol = SecurityProtocol.SaslSsl,
                //EnableSslCertificateVerification = false,
                //Debug = "broker,topic,msg",
                //Acks = Acks.None,
                //CompressionType = CompressionType.None
            };

            var topic = "test01";
            var url = "https://baconipsum.com/api/?type=meat-and-filler&paras=100&format=text";
            var client = new HttpClient();

            var random = new Random();

            using (var p = new ProducerBuilder<string, string>(config).Build())
            {
                while (true)
                {
                    var word = random.Next(1, 10).ToString();
                    var dr = await p.ProduceAsync(topic, new Message<string, string> { Key = $"{DateTime.UtcNow.ToString("u")}", Value = $"{word}" });
                    Console.WriteLine($"Delivered '{word}' to '{dr.TopicPartitionOffset.Offset}'");
                    await Task.Delay(100);
                    //var text = await client.GetStringAsync(url);
                    //foreach (var line in text.Split())
                    //{
                    //    foreach (var word in line.Split(' ').Where(xx => !string.IsNullOrWhiteSpace(xx)))
                    //    {
                    //        //var dr = await p.ProduceAsync(topic, new Message<string, string> { Key = $"{DateTime.UtcNow.ToString("u")}", Value = $"{word}" });
                    //        Console.WriteLine($"Delivered '{word}' to '{dr.TopicPartitionOffset.Offset}'");
                    //    }
                    //}
                    //Console.WriteLine("///////////////////////// RELOAD ////////////////////////////////");
                }
            }
        }
    }
}