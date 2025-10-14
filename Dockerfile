FROM python:3.9-slim

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    wget \
    curl \
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

# === 🔥 ОЧИСТКА КЭША WEBDRIVER-MANAGER (важно!) ===
RUN rm -rf /root/.wdm

# Создание структуры папок для volume mounts
RUN mkdir -p /app/data /app/logs /app/screenshots
WORKDIR /app

# Установка Python зависимостей (включая webdriver-manager)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование кода
COPY . .

CMD ["python", "parser.py"]

# Копируем скрипты объединения
COPY merge_scripts /app/merge_scripts

# Устанавливаем pandas для объединения CSV
RUN pip install pandas