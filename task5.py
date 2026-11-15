from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, from_unixtime, struct, to_json, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType, TimestampType

def main():
    # 1. Ініціалізація SparkSession з підтримкою Kafka коннектора
    spark = SparkSession.builder \
        .appName("IoT-Kafka-Spark-Alerts-Producer") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # 2. Завантаження статичних умов алертів з файлу alerts_conditions.csv
    conditions_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("alerts_conditions.csv")

    # 3. Визначення схеми вхідних даних з Kafka (task1.py)
    schema = StructType([
        StructField("sensor_id", IntegerType(), True),
        StructField("timestamp", LongType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True)
    ])

    # 4. Зчитування потоку даних з вхідного Kafka-топіку
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

    # 8. ФІЛЬТРАЦІЯ: перевірка умов за файлом `alerts_conditions.csv`
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
    )

    # 9. Форматування полів відповідно до вимог та структури з прикладу зображення:
    # Очікувані поля: window (start, end), t_avg, h_avg, code, message, timestamp
    formatted_alerts = alerts_stream \
        .withColumn("t_avg", col("avg_temperature")) \
        .withColumn("h_avg", col("avg_humidity")) \
        .withColumn("code", col("code").cast("string")) \
        .withColumn("timestamp", current_timestamp().cast("string")) \
        .select(
            to_json(
                struct(
                    col("window"),
                    col("t_avg"),
                    col("h_avg"),
                    col("code"),
                    col("message"),
                    col("timestamp")
                )
            ).alias("value") # Spark Kafka коннектор очікує поле 'value' для тіла повідомлення
        )

    # 10. Запис результатів стріму у вихідний Kafka-топік (`yukh_alerts_output`)
    query = formatted_alerts \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "yukh_alerts_output") \
        .option("checkpointLocation", "/tmp/spark-kafka-checkpoint-alerts") \
        .outputMode("update") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()