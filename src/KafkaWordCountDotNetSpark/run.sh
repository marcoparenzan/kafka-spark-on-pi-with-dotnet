spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 --master local microsoft-spark-2-4_2.11-2.0.0.jar dotnet KafkaWordCountDotNetSpark.dll