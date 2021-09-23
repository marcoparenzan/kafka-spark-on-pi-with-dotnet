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
            string subscribeType = "subscribe"  ;
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
                // .Option("includeTimestamp", true)
                .Load()
                .SelectExpr("CAST(CAST(key AS STRING) as Timestamp)", "CAST(value AS STRING)")
                ;

            DataFrame windowedCounts = rows
                .WithWatermark("key", "5 seconds")
                .GroupBy(
                    Window(rows["key"], "30 seconds"),
                    rows["value"]
                )
                .Count()
                .OrderBy(Col("window.end").Desc())
            ;

            StreamingQuery query = 
                windowedCounts
                .WriteStream()
                .OutputMode("complete")
                .Option("truncate", "false")
                // .OutputMode("append") // Filesink only support Append mode.
                // .Format("csv") // supports these formats : csv, json, orc, parquet
                .Trigger(Trigger.ProcessingTime("30 seconds"))
                // .Option("checkpointLocation", "checkpoint")
                // .Option("path", "output/filesink_output")
                // .Option("header", true)
                .Format("console")
                .Start();

            query.AwaitTermination();
        }
    }
}