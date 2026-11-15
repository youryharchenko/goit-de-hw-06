import json
import time
import random
from confluent_kafka import Producer

def delivery_report(err, msg):
    """Callback-функція для підтвердження доставки повідомлення."""
    if err is not None:
        print(f"❌ Помилка доставки: {err}")
    else:
        print(f"✅ [Датчик {msg.key().decode('utf-8')}] Надіслано у топік {msg.topic()} [партиція {msg.partition()}] @ offset {msg.offset()}")

def run_single_sensor():
    
    # Унікальний префікс для уникнення колізій імен
    prefix = "yukh_"

    # Конфігурація підключення до локального брокера Kafka 4 (KRaft)
    conf = {'bootstrap.servers': 'localhost:9092'}
    producer = Producer(conf)
    
    # Ідентифікатор датчика генерується ОДИН раз при запуску скрипту і залишається постійним
    sensor_id = random.randint(1000, 9999)
    topic_name = f"{prefix}building_sensors"
    
    print(f"🚀 Запуск симуляції датчика. Унікальний ID датчика: {sensor_id}")
    print(f"📡 Відправка даних у топік '{topic_name}' (натисніть Ctrl+C для зупинки)...")
    
    try:
        while True:
            # Генерація показників згідно з вимогами
            temperature = round(random.uniform(25.0, 45.0), 2)
            humidity = round(random.uniform(15.0, 85.0), 2)
            timestamp = int(time.time())  # Час отримання даних
            
            # Структура даних повідомлення
            sensor_data = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "temperature": temperature,
                "humidity": humidity
            }
            
            # Відправляємо в Kafka (використовуємо sensor_id як ключ для збереження порядку подій)
            producer.produce(
                topic=topic_name,
                key=str(sensor_id).encode('utf-8'),
                value=json.dumps(sensor_data).encode('utf-8'),
                callback=delivery_report
            )
            
            # Асинхронний виклик для обробки подій доставки
            producer.poll(0)
            
            # Інтервал між відправками нових даних (наприклад, кожні 60 секунд)
            time.sleep(60)
            
    except KeyboardInterrupt:
        print(f"\n🛑 Зупинка роботи датчика {sensor_id}...")
    finally:
        # Очищення буфера перед закриттям
        producer.flush()

if __name__ == '__main__':
    run_single_sensor()