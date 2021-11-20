using Kafka.Public;
using System;
using System.Text;

var cluster = new ClusterClient(new Configuration { Seeds = "edge01.local:9092" }, new Kafka.Public.Loggers.ConsoleLogger());

cluster.MessageReceived += kafkaRecord => { Console.WriteLine($"{Encoding.UTF8.GetString((byte[])kafkaRecord.Value)}"); };
// OR (for consumer group usage)
cluster.Subscribe("some group", new[] { "test01" }, new ConsumerGroupConfiguration { AutoCommitEveryMs = 5000 });

Console.ReadLine();