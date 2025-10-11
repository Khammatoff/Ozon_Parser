Быстрый старт
1. Клонирование и настройка
bash
git clone <repository-url>
cd ozon-parser
2. Настройка окружения
Создайте файл .env:

env
# RabbitMQ настройки
RABBITMQ_HOST=rabbitmq
RABBITMQ_USER=admin
RABBITMQ_PASS=your_secure_password

# Парсер настройки
TOTAL_SELLERS=30000
REQUEST_DELAY_MIN=3
REQUEST_DELAY_MAX=5
BOT_COUNT=3

# Прокси (для будущего использования)
PROXY_ENABLED=false
3. Запуск системы
bash
# Запуск всех сервисов
docker compose up --build -d --scale parser=3

# Проверка статуса
docker compose ps
4. Заполнение очереди
bash
# Заполнение очереди ID продавцов
docker compose exec parser python queue_setup.py
5. Мониторинг
bash
# Просмотр логов
docker compose logs -f parser

# Web интерфейс RabbitMQ
# http://localhost:15672 (admin/your_secure_password)
⚙️ Конфигурация
Переменные окружения
Переменная	По умолчанию	Описание
RABBITMQ_HOST	rabbitmq	Хост RabbitMQ
RABBITMQ_USER	admin	Пользователь RabbitMQ
RABBITMQ_PASS	password	Пароль RabbitMQ
TOTAL_SELLERS	30000	Общее количество продавцов
REQUEST_DELAY_MIN	3	Минимальная задержка между запросами
REQUEST_DELAY_MAX	5	Максимальная задержка между запросами
BOT_COUNT	3	Количество ботов-парсеров
Масштабирование
bash
# Запуск 5 ботов
docker compose up -d --scale parser=5

# Запуск 1 бота
docker compose up -d --scale parser=1
📊 Выходные данные
Данные сохраняются в CSV файл data/sellers.csv:

Поле	Описание
URL	URL страницы продавца
Название	Название продавца
HTML_модалки	HTML код модального окна
ОГРН	ОГРН юридического лица
ИНН	ИНН юридического лица
Юрлицо	Наименование юридического лица
Сайт	Веб-сайт продавца
Отзывы	Количество отзывов
Рейтинг	Рейтинг продавца
Срок_регистрации	Срок работы на Ozon
Кол-во_товаров	Количество товаров
Товары	Информация о товарах
🛠️ Разработка
Локальная разработка
bash
# Установка зависимостей
pip install -r requirements.txt

# Запуск RabbitMQ
docker compose up rabbitmq -d

# Запуск парсера локально
python parser.py

# Заполнение очереди
python queue_setup.py
Тестирование
bash
# Тестовый запуск с 10 продавцами
TOTAL_SELLERS=10 docker compose exec parser python queue_setup.py

# Просмотр логов конкретного бота
docker compose logs parser_1