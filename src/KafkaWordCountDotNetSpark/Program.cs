using System;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Streaming;
using static Microsoft.Spark.Sql.Functions;

namespace KafkaWordCountDotNetSpark
{
    class Program
    {
        static void Main(string[] args)
        {
            string bootstrapServers = "edge01.local:9092";
            string subscribeType = "subscribe";
            string topics = "test01";

            SparkSession spark = SparkSession
                .Builder()
                .AppName("KafkaWordCountDotNetSpark")
                .GetOrCreate();

            DataFrame lines = spark
                .ReadStream()
                .Format("kafka")
                .Option("kafka.bootstrap.servers", bootstrapServers)
                .Option(subscribeType, topics)
                .Load()
                .SelectExpr("CAST(value AS STRING)")
                ;

            DataFrame words = lines
                .Select(Explode(Split(lines["value"], " "))
                    .Alias("word"));
            DataFrame wordCounts = words.GroupBy("word").Count();

            StreamingQuery query = 
                wordCounts
                .WriteStream()
                .OutputMode("complete")
                .Format("console")
                .Start();

            query.AwaitTermination();
        }
    }
}