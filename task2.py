from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, from_unixtime
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType, TimestampType

def main():
    # 1. Створення SparkSession з підключенням пакету для Kafka
    
    spark = SparkSession.builder \
    .appName("IoT-Kafka-Spark-Streaming") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0") \
    .getOrCreate()

    
    spark.sparkContext.setLogLevel("WARN")

    # 2. Визначення схеми вхідних даних згідно з task1.py
    schema = StructType([
        StructField("sensor_id", IntegerType(), True),
        StructField("timestamp", LongType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True)
    ])

    # 3. Зчитування потоку даних з Kafka-топіку
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "yukh_building_sensors") \
        .option("startingOffsets", "latest") \
        .load()

    # 4. Десеріалізація JSON та конвертація timestamp у тип TimestampType
    parsed_stream = kafka_stream \
        .selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", from_unixtime(col("timestamp")).cast(TimestampType()))

    # 5. Застосування Watermark та Sliding Window:
    # - Watermark: 10 секунд (дозволяє запізнілі дані)
    # - Window duration: 1 хвилина (довжина вікна)
    # - Slide duration: 30 секунд (інтервал зсуву)
    windowed_stats = parsed_stream \
        .withWatermark("event_time", "10 seconds") \
        .groupBy(
            window(col("event_time"), "1 minute", "30 seconds"),
            col("sensor_id")  # Групуємо за конкретним датчиком (можна прибрати, якщо потрібна загальна середня)
        ) \
        .agg(
            avg("temperature").alias("avg_temperature"),
            avg("humidity").alias("avg_humidity")
        )

    # 6. Виведення результатів потоку в консоль
    query = windowed_stats \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()