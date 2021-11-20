using System;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Streaming;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace KafkaRollerLineDotNetSpark
{
    class Program
    {
        static void Main(string[] args)
        {
            string bootstrapServers = "edge01.local:9092";
            string subscribeType = "subscribe";
            string topics = "test01";

            var schema = new StructType(new[] {
                new StructField("DeviceId", new StringType()),
                new StructField("LocalStamp", new DateType()),
                new StructField("Torque", new DoubleType()),
                new StructField("Power", new DoubleType()),
                new StructField("Pressure", new DoubleType()),
                new StructField("Temperature", new DoubleType()),
                new StructField("Velocity", new DoubleType()),
            });

            SparkSession spark = SparkSession
                .Builder()
                .AppName(nameof(KafkaRollerLineDotNetSpark))
                .GetOrCreate();
            
            spark.SparkContext.SetLogLevel("OFF");

            DataFrame dataFrame = spark
                .ReadStream()
                .Format("kafka")
                .Option("kafka.bootstrap.servers", bootstrapServers)
                .Option(subscribeType, topics)
                .Option("includeTimestamp", true)
                .Load()
                .SelectExpr("timestamp", "CAST(value AS STRING)")
                ;

            var dfJSON = dataFrame.WithColumn("jsonData", FromJson(dataFrame.Col("value"), schema.Json))
                .Select("jsonData.*");

            DataFrame watermarkingDF = dfJSON
                .WithWatermark("timestamp", "1 minute")
            ;

            DataFrame windowedCounts = watermarkingDF
                .GroupBy(
                    dfJSON["Torque"],
                    Window(dfJSON["timestamp"], "30 seconds")
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
