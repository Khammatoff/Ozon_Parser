# Ozon Seller Parser

Парсер продавцов Ozon с использованием Selenium, Docker и RabbitMQ для распределенной обработки.

## 🎯 Цель

Собрать данные 30,000 продавцов Ozon путем перебора ID от 1 до 30,000.

## 📊 Данные для сбора

- URL продавца
- Название продавца
- HTML модального окна с юридической информацией
- ОГРН
- ИНН
- Юридическое лицо (ИП/ООО)
- Веб-сайт
- Количество отзывов
- Рейтинг
- Срок регистрации
- Количество товаров
- Список товаров

## 🏗️ Архитектура
┌─────────────────┐ ┌──────────────┐ ┌──────────────┐
│ Queue Setup │ -> │ RabbitMQ │ -> │ Parsers │
│ (однократно) │ │ (очередь) │ │ (многоПоточно)│
└─────────────────┘ └──────────────┘ └──────────────┘
│
↓
┌──────────────┐
│ CSV File │
│ (результаты)│
└──────────────┘

text

## 🚀 Быстрый старт

### 1. Клонирование и настройка

```bash
git clone <repository-url>
cd ozon-parser
2. Настройка окружения
Создайте файл .env:

env
RABBITMQ_USER=admin
RABBITMQ_PASS=admin123
TOTAL_SELLERS=10
USE_PROXIES=false
PROXY_ROTATION_COUNT=10
# PROXY_LIST=proxy1:port,proxy2:port,proxy3:port
3. Запуск тестового режима (10 продавцов)
bash
# Сборка и запуск
docker-compose up --build --scale parser=3
4. Запуск полного парсинга (30,000 продавцов)
bash
# Измените TOTAL_SELLERS в .env на 30000
docker-compose down
docker-compose up --build --scale parser=5
⚙️ Конфигурация
Переменные окружения (.env)
Переменная	Значение по умолчанию	Описание
RABBITMQ_USER	admin	Пользователь RabbitMQ
RABBITMQ_PASS	admin123	Пароль RabbitMQ
TOTAL_SELLERS	10	Количество продавцов для парсинга
USE_PROXIES	false	Использовать прокси для обхода блокировок
PROXY_ROTATION_COUNT	10	Смена прокси после N запросов
PROXY_LIST	-	Список прокси (через запятую)
Масштабирование парсеров
bash
# 3 парсера (рекомендуется для теста)
docker-compose up --scale parser=3

# 5 парсеров (для продакшена)
docker-compose up --scale parser=5

# 10 парсеров (максимальная скорость)
docker-compose up --scale parser=10
📁 Структура проекта
text
ozon-parser/
├── docker-compose.yml      # Docker Compose конфигурация
├── Dockerfile             # Образ для парсеров
├── requirements.txt       # Python зависимости
├── .env                  # Переменные окружения
├── parser.py             # Основной парсер
├── queue_setup.py        # Инициализатор очереди
├── data/                 # Директория для результатов
│   └── sellers.csv       # Файл с данными
├── logs/                 # Директория для логов
│   └── parser.log        # Логи парсера
└── README.md            # Документация
🔧 Особенности реализации
🛡️ Anti-Detection
Selenium Stealth режим

Случайные User-Agent

Уникальные Chrome профили для каждого контейнера

Случайные задержки между запросами (4-6 секунд)

Ротация прокси (если включено)

🔄 Отказоустойчивость
Автоматическое переподключение к RabbitMQ

Повторная обработка неудачных запросов

Сохранение прогресса после каждого продавца

Durable очереди RabbitMQ

📈 Масштабируемость
Распределенная обработка через RabbitMQ

Независимые контейнеры парсеров

Балансировка нагрузки между воркерами

🎪 Компоненты системы
Queue Setup
Заполняет очередь RabbitMQ ID продавцов

Запускается однократно при старте системы

Создает durable очередь для надежности

Parser
Многопоточные парсеры в Docker контейнерах

Автоматическое распределение задач через RabbitMQ

Индивидуальные Chrome профили для избежания конфликтов

RabbitMQ
Центральный message broker

Гарантированная доставка сообщений

Web интерфейс управления: http://localhost:15672

📊 Мониторинг
RabbitMQ Management
bash
# Откройте в браузере
http://localhost:15672
# Логин: admin
# Пароль: admin123
Логи в реальном времени
bash
# Просмотр логов всех сервисов
docker-compose logs -f

# Логи только парсеров
docker-compose logs -f parser

# Логи RabbitMQ
docker-compose logs -f rabbitmq
🛠️ Решение проблем
Распространенные ошибки
Конфликт user-data-dir

bash
# Решение: Очистите контейнеры и перезапустите
docker-compose down
docker-compose up --build
Блокировка Ozon

bash
# Решение: Включите прокси в .env
USE_PROXIES=true
PROXY_LIST=proxy1:port,proxy2:port
Проблемы с подключением к RabbitMQ

bash
# Решение: Проверьте healthcheck
docker-compose ps
Оптимизация производительности
bash
# Увеличьте количество парсеров
docker-compose up --scale parser=10

# Уменьшите задержки (осторожно!)
# В parser.py измените time.sleep(random.uniform(2, 4))
📈 Результаты
Данные сохраняются в data/sellers.csv после обработки каждого продавца. Формат CSV с кодировкой UTF-8 для поддержки кириллицы.

🔒 Безопасность
Изолированные Docker контейнеры

Индивидуальные Chrome сессии

Опциональное использование прокси

Логирование без чувствительных данных