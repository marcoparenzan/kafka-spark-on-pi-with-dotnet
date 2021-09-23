using Kafka.Public;
using System;
using System.Text;
using System.Threading.Tasks;

var logger = new Kafka.Public.Loggers.ConsoleLogger();

var cluster = new ClusterClient(new Configuration { Seeds = "edge01:9092" }, logger);

var random = new Random();

while (true)
{
    var word = random.Next(1, 10).ToString();
    var now = DateTime.UtcNow;
    cluster.Produce("test01", $"{now.ToString("u")}", $"{word}", now);
    Console.WriteLine(word);
    await Task.Delay(100);
}