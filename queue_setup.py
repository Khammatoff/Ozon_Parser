import pika
import os
import time
import logging
from dotenv import load_dotenv

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def setup_queue():
    """Заполнение очереди RabbitMQ ID продавцов с повторными попытками подключения"""

    # Загружаем переменные окружения
    load_dotenv()

    # Получаем настройки
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'rabbitmq')  # Имя сервиса в docker-compose
    rabbitmq_user = os.getenv('RABBITMQ_USER', 'admin')
    rabbitmq_pass = os.getenv('RABBITMQ_PASS', 'guest')
    total_sellers = int(os.environ['TOTAL_SELLERS'])

    logging.info(f"🚀 Запуск заполнения очереди")
    logging.info(f"🎯 Подключение к RabbitMQ: {rabbitmq_host}")
    logging.info(f"📊 Будет добавлено {total_sellers} ID продавцов")

    # Цикл с повторными попытками подключения
    while True:
        try:
            credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=rabbitmq_host,
                    credentials=credentials,
                    heartbeat=600,
                    connection_attempts=10,
                    retry_delay=5
                )
            )
            logging.info("✅ Подключение к RabbitMQ установлено")
            break  # Успешно подключились
        except Exception as e:
            logging.error(f"❌ Ошибка подключения к RabbitMQ: {e}")
            logging.info("⏳ Повторная попытка через 5 секунд...")
            time.sleep(5)

    try:
        channel = connection.channel()

        # Создаем очередь (durable=True — сохраняется при перезапуске)
        channel.queue_declare(queue='seller_ids', durable=True)
        logging.info("✅ Очередь 'seller_ids' объявлена")

        # Заполняем очередь ID продавцов
        added_count = 0
        for seller_id in range(1, total_sellers + 1):
            channel.basic_publish(
                exchange='',
                routing_key='seller_ids',
                body=str(seller_id),
                properties=pika.BasicProperties(
                    delivery_mode=2  # Сохранять сообщение на диск
                )
            )
            added_count += 1

            if added_count % 1000 == 0:
                logging.info(f"✅ Добавлено {added_count} ID в очередь")

        logging.info(f"✅ Успешно добавлено {added_count} ID продавцов в очередь 'seller_ids'")

    except Exception as e:
        logging.error(f"❌ Ошибка при работе с очередью: {e}")
        raise
    finally:
        try:
            connection.close()
            logging.info("🔌 Соединение с RabbitMQ закрыто")
        except:
            pass


def check_queue_status():
    """Проверка количества сообщений в очереди"""
    try:
        load_dotenv()
        credentials = pika.PlainCredentials(
            os.getenv('RABBITMQ_USER', 'admin'),
            os.getenv('RABBITMQ_PASS', 'guest')
        )
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=os.getenv('RABBITMQ_HOST', 'rabbitmq'),
                credentials=credentials
            )
        )
        channel = connection.channel()

        # Объявляем очередь (без passive=True, чтобы создалась при необходимости)
        queue_info = channel.queue_declare(queue='seller_ids', durable=True)
        message_count = queue_info.method.message_count

        logging.info(f"📊 Статус очереди: {message_count} сообщений ожидают обработки")
        connection.close()
        return message_count

    except Exception as e:
        logging.error(f"❌ Ошибка при проверке очереди: {e}")
        return 0


if __name__ == "__main__":
    print("🚀 Заполнение очереди RabbitMQ ID продавцов")
    print("=" * 50)

    # Проверяем текущий статус очереди
    current_count = check_queue_status()
    if current_count > 0:
        response = input(f"⚠️  В очереди уже есть {current_count} сообщений. Перезаписать? (y/N): ")
        if response.lower() != 'y':
            print("❌ Отменено пользователем")
            exit(0)

    # Заполняем очередь
    setup_queue()

    # Показываем финальный статус
    check_queue_status()

    print("\n🎉 Очередь успешно заполнена!")
    print("✅ Теперь запустите парсеры:")
    print("docker compose up --build --scale parser=3")