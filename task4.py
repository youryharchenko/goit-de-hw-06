from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, from_unixtime, lit
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType, TimestampType

def main():
    # 1. Ініціалізація SparkSession з підтримкою Kafka коннектора
    spark = SparkSession.builder \
        .appName("IoT-Kafka-Spark-Alerts-Streaming") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # 2. Зчитування статичних умов алертів з файлу alerts_conditions.csv
    conditions_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("alerts_conditions.csv")
    
    print("📋 Завантажені правила алертів з файлу:")
    conditions_df.show(truncate=False)

    # 3. Визначення схеми вхідних даних з Kafka (task1.py)
    schema = StructType([
        StructField("sensor_id", IntegerType(), True),
        StructField("timestamp", LongType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True)
    ])

    # 4. Зчитування потоку даних з Kafka-топіку
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "yukh_building_sensors") \
        .option("startingOffsets", "latest") \
        .load()

    # 5. Десеріалізація та підготовка часової мітки
    parsed_stream = kafka_stream \
        .selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", from_unixtime(col("timestamp")).cast(TimestampType()))

    # 6. Обчислення середніх значень за допомогою Sliding Window (1 хв вікно, 30 сек зсув) + Watermark (10 сек)
    windowed_stats = parsed_stream \
        .withWatermark("event_time", "10 seconds") \
        .groupBy(
            window(col("event_time"), "1 minute", "30 seconds"),
            col("sensor_id")
        ) \
        .agg(
            avg("temperature").alias("avg_temperature"),
            avg("humidity").alias("avg_humidity")
        )

    # 7. CROSS JOIN стрімінгового DataFrame зі статичним DataFrame умов
    cross_joined_df = windowed_stats.crossJoin(conditions_df)

    # 8. ФІЛЬТРАЦІЯ: перевірка, чи потрапляють середні показники під критерії алертів
    # Умова враховує, що якщо мін/макс дорівнюють -999, то цей параметр не перевіряється.
    alerts_stream = cross_joined_df.filter(
        (
            (col("temperature_min") != -999) & 
            (col("avg_temperature") >= col("temperature_min")) & 
            (col("avg_temperature") <= col("temperature_max"))
        ) | (
            (col("humidity_min") != -999) & 
            (col("avg_humidity") >= col("humidity_min")) & 
            (col("avg_humidity") <= col("humidity_max"))
        )
    ).select(
        col("window"),
        col("sensor_id"),
        col("code").alias("alert_code"),
        col("message"),
        col("avg_temperature"),
        col("avg_humidity")
    )

    # 9. Виведення активованих алертів у консоль в реальному часі
    query = alerts_stream \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()