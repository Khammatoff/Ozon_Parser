import csv
import logging
import random
import time
import os
import sys
import tempfile
import shutil
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium_stealth import stealth
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType
import pika

# Создаём папки в контейнере (совпадают с volume mounts)
os.makedirs("/app/logs", exist_ok=True)
os.makedirs("/app/data", exist_ok=True)
os.makedirs("/app/screenshots", exist_ok=True)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/parser.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)


class OzonSellerParser:
    def __init__(self):
        self.instance_id = os.getenv('HOSTNAME', f"parser-{random.randint(1000, 9999)}")
        self.request_count = 0
        self.driver = None
        self.wait = None

        # Уникальная временная директория для Chrome
        self.chrome_temp_dir = tempfile.mkdtemp()
        logging.info(f"Инициализация парсера {self.instance_id}, профиль: {self.chrome_temp_dir}")

        try:
            self.setup_driver()
            self.wait = WebDriverWait(self.driver, 15)

            # Инициализация CSV
            self.data_dir = "/app/data"
            os.makedirs(self.data_dir, exist_ok=True)
            self.csv_file = f"{self.data_dir}/sellers_{self.instance_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            self.init_csv()
        except Exception as e:
            logging.error(f"❌ Ошибка инициализации парсера: {e}", exc_info=True)
            self.close()
            raise

    def setup_driver(self):
        """Настройка Chrome для работы в Docker с headless и stealth"""
        chrome_options = Options()

        # === Критические опции для Docker ===
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-setuid-sandbox")
        chrome_options.add_argument("--disable-software-rasterizer")
        chrome_options.add_argument("--disable-ipc-flooding-protection")
        chrome_options.add_argument("--disable-background-timer-throttling")
        chrome_options.add_argument("--disable-backgrounding-occluded-windows")
        chrome_options.add_argument("--disable-renderer-backgrounding")
        chrome_options.add_argument("--disable-features=VizDisplayCompositor")
        chrome_options.add_argument("--window-size=1920,1080")

        # === Улучшенные stealth опции ===
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-features=UserAgentClientHint")
        chrome_options.add_argument("--no-first-run")
        chrome_options.add_argument("--no-default-browser-check")
        chrome_options.add_argument("--disable-component-extensions-with-background-pages")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        # Уникальный профиль
        chrome_options.add_argument(f"--user-data-dir={self.chrome_temp_dir}")

        # Случайный User-Agent
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ]
        chrome_options.add_argument(f"--user-agent={random.choice(user_agents)}")

        # === Инициализация драйвера ===
        try:
            service = Service(ChromeDriverManager(chrome_type=ChromeType.GOOGLE).install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
        except Exception as e:
            logging.error(f"❌ Ошибка создания драйвера: {e}", exc_info=True)
            raise

        # Применение selenium-stealth
        try:
            stealth(
                self.driver,
                languages=["ru-RU", "ru", "en-US", "en"],
                vendor="Google Inc.",
                platform="Win32",
                webgl_vendor="Intel Inc.",
                renderer="Intel Iris OpenGL Engine",
                fix_hairline=True,
                run_on_insecure_origins=False
            )
        except Exception as e:
            logging.warning(f"⚠️ Ошибка применения selenium-stealth: {e}")

    def init_csv(self):
        """Инициализация CSV файла с заголовками"""
        headers = [
            'URL', 'Название', 'HTML_модалки', 'ОГРН', 'ИНН',
            'Юрлицо', 'Сайт', 'Отзывы', 'Рейтинг',
            'Срок_регистрации', 'Кол-во_товаров', 'Товары', 'Instance_ID'
        ]
        try:
            with open(self.csv_file, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
            logging.info(f"✅ Создан CSV файл: {self.csv_file}")
        except Exception as e:
            logging.error(f"❌ Ошибка создания CSV: {e}", exc_info=True)

    def save_to_csv(self, data):
        """Сохранение данных в CSV"""
        try:
            with open(self.csv_file, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow([
                    data.get('URL', ''),
                    data.get('Название', ''),
                    data.get('HTML_модалки', ''),
                    data.get('ОГРН', ''),
                    data.get('ИНН', ''),
                    data.get('Юрлицо', ''),
                    data.get('Сайт', ''),
                    data.get('Отзывы', ''),
                    data.get('Рейтинг', ''),
                    data.get('Срок_регистрации', ''),
                    data.get('Кол-во_товаров', ''),
                    data.get('Товары', ''),
                    self.instance_id
                ])
                # 🔥 КРИТИЧЕСКОЕ ИЗМЕНЕНИЕ: форсируем запись на диск
                f.flush()
                os.fsync(f.fileno())
            logging.info(f"✅ Данные сохранены в CSV: {self.csv_file}")
            return True
        except Exception as e:
            logging.error(f"❌ Ошибка сохранения в CSV: {e}", exc_info=True)
            return False

    def extract_basic_info(self):
        """Извлечение базовой информации о продавце"""
        try:
            data = {}

            # Ожидание загрузки страницы
            time.sleep(random.uniform(3, 5))

            # Попытка найти название продавца
            try:
                name_selectors = [
                    "h1",
                    ".seller-name",
                    "[data-widget='webSellerName']",
                    ".seller-info h1"
                ]
                for selector in name_selectors:
                    try:
                        name_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                        if name_element and name_element.text.strip():
                            data['Название'] = name_element.text.strip()
                            break
                    except:
                        continue
            except Exception as e:
                logging.warning(f"⚠️ Не удалось извлечь название: {e}")
                data['Название'] = ''

            return data
        except Exception as e:
            logging.error(f"❌ Ошибка извлечения базовой информации: {e}")
            return {}

    def click_and_get_legal_info(self):
        """Клик по кнопке юридической информации и извлечение данных"""
        try:
            # Поиск кнопки юридической информации
            legal_button_selectors = [
                "button[data-widget='webLegalInfo']",
                ".legal-info-button",
                "button:contains('Юридическая информация')",
                "a[href*='legal']"
            ]

            for selector in legal_button_selectors:
                try:
                    legal_button = self.wait.until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                    legal_button.click()
                    time.sleep(random.uniform(2, 4))

                    # Извлечение данных из модалки
                    modal_data = self.extract_modal_info()
                    return modal_data
                except:
                    continue

            return {}
        except Exception as e:
            logging.warning(f"⚠️ Не удалось получить юридическую информацию: {e}")
            return {}

    def extract_modal_info(self):
        """Извлечение информации из модального окна"""
        try:
            modal_data = {}

            # Поиск модального окна
            modal_selectors = [
                ".modal-content",
                "[role='dialog']",
                ".legal-info-modal"
            ]

            for selector in modal_selectors:
                try:
                    modal = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if modal:
                        modal_data['HTML_модалки'] = modal.get_attribute('outerHTML')

                        # Извлечение ОГРН/ИНН
                        text_content = modal.text.lower()
                        if 'огрн' in text_content:
                            modal_data['ОГРН'] = 'извлечено'
                        if 'инн' in text_content:
                            modal_data['ИНН'] = 'извлечено'

                        break
                except:
                    continue

            return modal_data
        except Exception as e:
            logging.warning(f"⚠️ Ошибка извлечения данных из модалки: {e}")
            return {}

    def parse_seller(self, seller_id):
        """Основной метод парсинга продавца"""
        url = f"https://www.ozon.ru/seller/{seller_id}"
        logging.info(f"🔍 Парсим продавца {seller_id}")

        seller_data = {'URL': url}

        try:
            self.driver.get(url)
            time.sleep(random.uniform(5, 8))  # Увеличенная задержка

            page_source = self.driver.page_source.lower()
            blocking_indicators = [
                "страница не найдена", "404", "доступ ограничен",
                "captcha", "bot", "automation", "доступ запрещен"
            ]

            if any(phrase in page_source for phrase in blocking_indicators):
                # Делаем скриншот при ошибке
                screenshot_path = f"/app/screenshots/error_{seller_id}_{int(time.time())}.png"
                self.driver.save_screenshot(screenshot_path)
                # 🔥 КРИТИЧЕСКОЕ ИЗМЕНЕНИЕ: пауза после скриншота
                time.sleep(2)
                logging.warning(f"❌ Продавец {seller_id} не найден или заблокирован. Скриншот: {screenshot_path}")
                return None

            seller_data.update(self.extract_basic_info())
            legal_info = self.click_and_get_legal_info()
            if legal_info:
                seller_data.update(legal_info)

            if self.save_to_csv(seller_data):
                logging.info(f"✅ Успешно обработан продавец {seller_id}")
                return seller_data
            else:
                logging.error(f"❌ Ошибка сохранения продавца {seller_id}")
                return None

        except Exception as e:
            # Сохраняем скриншот при критической ошибке
            screenshot_path = f"/app/screenshots/crash_{seller_id}_{int(time.time())}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                # 🔥 КРИТИЧЕСКОЕ ИЗМЕНЕНИЕ: пауза после скриншота
                time.sleep(2)
                logging.error(f"💥 Критическая ошибка при парсинге {seller_id}. Скриншот: {screenshot_path}",
                              exc_info=True)
            except:
                logging.error(f"💥 Ошибка при парсинге {seller_id}: {e}", exc_info=True)
            return None

    def close(self):
        """Корректное закрытие драйвера и очистка"""
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                logging.error(f"❌ Ошибка при закрытии драйвера: {e}")
            self.driver = None

        if self.chrome_temp_dir and os.path.exists(self.chrome_temp_dir):
            try:
                shutil.rmtree(self.chrome_temp_dir)
            except Exception as e:
                logging.warning(f"⚠️ Не удалось удалить временную директорию: {e}")


def callback(ch, method, properties, body):
    seller_id = body.decode()
    logging.info(f"🎯 Получен ID продавца: {seller_id}")

    parser = None
    try:
        parser = OzonSellerParser()
        result = parser.parse_seller(seller_id)
        if result:
            logging.info(f"✅ Успешно обработан продавец {seller_id}")
        else:
            logging.warning(f"⚠️ Не удалось обработать продавца {seller_id}")
        time.sleep(random.uniform(4, 5))
    except Exception as e:
        logging.error(f"❌ Критическая ошибка при обработке {seller_id}: {e}", exc_info=True)
    finally:
        if parser:
            parser.close()
    ch.basic_ack(delivery_tag=method.delivery_tag)


def start_consumer():
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=os.getenv('RABBITMQ_HOST', 'rabbitmq'),
                    credentials=pika.PlainCredentials(
                        os.getenv('RABBITMQ_USER', 'admin'),
                        os.getenv('RABBITMQ_PASS', 'guest')
                    ),
                    heartbeat=600
                )
            )
            channel = connection.channel()
            channel.queue_declare(queue='seller_ids', durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue='seller_ids', on_message_callback=callback)
            logging.info("🔄 Ожидаем сообщения из RabbitMQ...")
            channel.start_consuming()
        except Exception as e:
            logging.error(f"❌ Ошибка подключения к RabbitMQ: {e}", exc_info=True)
            time.sleep(10)


if __name__ == "__main__":
    start_consumer()