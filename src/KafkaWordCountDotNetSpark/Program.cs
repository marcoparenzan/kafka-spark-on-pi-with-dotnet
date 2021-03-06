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
            
            spark.SparkContext.SetLogLevel("OFF");

            DataFrame rows = spark
                .ReadStream()
                .Format("kafka")
                .Option("kafka.bootstrap.servers", bootstrapServers)
                .Option(subscribeType, topics)
                .Option("includeTimestamp", true)
                .Load()
                .SelectExpr("timestamp", "CAST(value AS STRING)")
                ;

            DataFrame watermarkingDF = rows
                .WithWatermark("timestamp", "1 minute")
            ;

            DataFrame windowedCounts = watermarkingDF
                .GroupBy(
                    rows["value"],
                    Window(rows["timestamp"], "30 seconds")
                )
                .Count()
                .OrderBy(Col("window.end").Desc())
            ;

            StreamingQuery query =
                windowedCounts
                .WriteStream()
                .Format("csv")
                //.OutputMode("complete")
                .OutputMode("append")
                .Option("path", "/home/pi/data")
                .Trigger(Trigger.ProcessingTime("30 seconds"))
                .Option("checkpointLocation", "/home/pi/data/checkpoint")
                .Start();

            query.AwaitTermination();
        }
    }
}
