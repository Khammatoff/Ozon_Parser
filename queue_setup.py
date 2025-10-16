import os
import pika
import logging
import time
import sys
from dotenv import load_dotenv  # Добавить эту строку

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def setup_queues():
    """Заполнение очереди RabbitMQ ID продавцов с повторными попытками подключения"""

    load_dotenv()
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'rabbitmq')
    rabbitmq_user = os.getenv('RABBITMQ_USER', 'admin')
    rabbitmq_pass = os.getenv('RABBITMQ_PASS', 'guest')

    start_id = int(os.getenv('START_SELLER_ID', 1))
    end_id = int(os.getenv('END_SELLER_ID', 30000))

    logging.info(f"🚀 Запуск заполнения очереди")
    logging.info(f"🎯 Диапазон ID продавцов: {start_id} - {end_id}")

    max_retries = 10
    for attempt in range(max_retries):
        try:
            credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=rabbitmq_host,
                    credentials=credentials,
                    heartbeat=600,
                    connection_attempts=3,
                    retry_delay=5
                )
            )

            channel = connection.channel()

            # УДАЛИТЬ существующую очередь и создать заново
            try:
                channel.queue_delete(queue='seller_ids')
                logging.info("🗑️ Удалена существующая очередь")
            except:
                pass

            # СОЗДАТЬ очередь с TTL
            channel.queue_declare(
                queue='seller_ids',
                durable=True,
                arguments={'x-message-ttl': 86400000}  # TTL 24 часа
            )
            logging.info("✅ Очередь 'seller_ids' создана")

            # Заполняем очередь
            added_count = 0
            batch_size = 1000

            for seller_id in range(start_id, end_id + 1):
                channel.basic_publish(
                    exchange='',
                    routing_key='seller_ids',
                    body=str(seller_id),
                    properties=pika.BasicProperties(
                        delivery_mode=2  # Сохранять сообщение на диск
                    )
                )
                added_count += 1

                if added_count % batch_size == 0:
                    logging.info(f"✅ Добавлено {added_count} ID в очередь")

            logging.info(f"🎉 Успешно добавлено {added_count} ID продавцов")
            connection.close()
            return True

        except Exception as e:
            logging.error(f"❌ Попытка {attempt + 1}/{max_retries} не удалась: {e}")
            if attempt < max_retries - 1:
                time.sleep(10)
            else:
                logging.error("❌ Все попытки провалились")
                return False


if __name__ == "__main__":
    success = setup_queues()
    if success:
        print("✅ Очередь успешно заполнена!")
        sys.exit(0)
    else:
        print("❌ Не удалось заполнить очередь")
        sys.exit(1)