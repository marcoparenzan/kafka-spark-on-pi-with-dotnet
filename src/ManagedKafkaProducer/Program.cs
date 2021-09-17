using Kafka.Public;
using System;
using System.Text;
using System.Threading.Tasks;

var logger = new Kafka.Public.Loggers.ConsoleLogger();

var cluster = new ClusterClient(new Configuration { Seeds = "edge01:9092" }, logger);

while (true)
{
    var msg = $"{DateTime.Now}";
    cluster.Produce("test01", Encoding.UTF8.GetBytes(msg));
    Console.WriteLine(msg);
    await Task.Delay(1000);
}