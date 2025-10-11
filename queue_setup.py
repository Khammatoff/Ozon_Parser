import pika
import os
from dotenv import load_dotenv
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def setup_queue():
    """Заполнение очереди RabbitMQ ID продавцов"""

    # Загружаем переменные из .env
    load_dotenv()

    # Получаем настройки из .env
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
    rabbitmq_user = os.getenv('RABBITMQ_USER', 'guest')
    rabbitmq_pass = os.getenv('RABBITMQ_PASS', 'guest')
    total_sellers = int(os.getenv('TOTAL_SELLERS', '30000'))

    logging.info(f"Подключение к RabbitMQ: {rabbitmq_host}")
    logging.info(f"Будет добавлено {total_sellers} продавцов")

    try:
        # Создаем credentials
        credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)

        # Подключаемся к RabbitMQ
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=rabbitmq_host,
                credentials=credentials,
                heartbeat=600
            )
        )
        channel = connection.channel()

        # Создаем durable очередь (переживет перезапуск RabbitMQ)
        channel.queue_declare(queue='seller_ids', durable=True)

        # Добавляем ID продавцов в очередь
        added_count = 0
        for seller_id in range(1, total_sellers + 1):
            channel.basic_publish(
                exchange='',
                routing_key='seller_ids',
                body=str(seller_id),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Сообщения сохраняются на диске
                )
            )
            added_count += 1

            # Логируем прогресс каждые 1000 записей
            if added_count % 1000 == 0:
                logging.info(f"Добавлено {added_count} ID в очередь")

        connection.close()

        logging.info(f"✅ Успешно добавлено {added_count} ID продавцов в очередь 'seller_ids'")
        logging.info(f"📊 Очередь готова к обработке {total_sellers} продавцов")

    except Exception as e:
        logging.error(f"❌ Ошибка при заполнении очереди: {str(e)}")
        raise


def check_queue_status():
    """Проверка статуса очереди"""
    try:
        load_dotenv()

        rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        rabbitmq_user = os.getenv('RABBITMQ_USER', 'guest')
        rabbitmq_pass = os.getenv('RABBITMQ_PASS', 'guest')

        credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=rabbitmq_host,
                credentials=credentials
            )
        )
        channel = connection.channel()

        # Получаем информацию об очереди
        queue_info = channel.queue_declare(queue='seller_ids', durable=True, passive=True)
        message_count = queue_info.method.message_count

        logging.info(f"📊 Статус очереди: {message_count} сообщений ожидают обработки")

        connection.close()
        return message_count

    except Exception as e:
        logging.error(f"Ошибка при проверке очереди: {str(e)}")
        return 0


if __name__ == "__main__":
    print("🚀 Заполнение очереди RabbitMQ ID продавцов")
    print("=" * 50)

    # Проверяем текущий статус
    current_count = check_queue_status()
    if current_count > 0:
        response = input(f"⚠️  В очереди уже есть {current_count} сообщений. Перезаписать? (y/N): ")
        if response.lower() != 'y':
            print("Отменено пользователем")
            exit(0)

    # Заполняем очередь
    setup_queue()

    # Показываем финальный статус
    check_queue_status()

    print("\n🎉 Очередь готова! Запускайте парсеры:")
    print("docker compose up --build -d --scale parser=3")