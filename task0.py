from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic

def create_iot_topics():
    # Конфігурація підключення до Kafka 4 брокера
    admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})
    
    # Унікальний префікс для уникнення колізій імен
    prefix = "yukh_"
    
    # Визначаємо список топіків з конфігурацією партицій та реплікації
    new_topics = [
        NewTopic(f"{prefix}building_sensors", num_partitions=3, replication_factor=1),
        NewTopic(f"{prefix}temperature_alerts", num_partitions=2, replication_factor=1),
        NewTopic(f"{prefix}humidity_alerts", num_partitions=2, replication_factor=1)
    ]
    
    # Асинхронне створення топіків
    futures = admin_client.create_topics(new_topics)
    
    for topic_name, future in futures.items():
        try:
            future.result()  # Очікуємо на завершення операції створення
            print(f"✅ Топік '{topic_name}' успішно створено.")
        except Exception as e:
            print(f"⚠️ Не вдалося створити топік '{topic_name}': {e}")

if __name__ == '__main__':
    create_iot_topics()