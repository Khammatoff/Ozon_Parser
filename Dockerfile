FROM python:3.9-slim

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    libnss3 \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    libgbm-dev \
    libxss1 \
    libasound2 \
    libxrandr2 \
    fonts-liberation \
    gnupg \
    apt-transport-https \
    ca-certificates \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Добавление ключа Google Chrome
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-chrome.gpg

# Добавление репозитория Chrome
RUN echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list

# Установка Google Chrome
RUN apt-get update && apt-get install -y google-chrome-stable

# Установка Python зависимостей (включая webdriver-manager)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование кода
COPY . .

# Создание директории для логов
RUN mkdir -p logs

CMD ["python", "parser.py"]
