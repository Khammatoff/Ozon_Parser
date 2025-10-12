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
import pika

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/parser.log'),
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
            self.data_dir = "data"
            os.makedirs(self.data_dir, exist_ok=True)
            self.csv_file = f"{self.data_dir}/sellers_{self.instance_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            self.init_csv()
        except Exception as e:
            logging.error(f"❌ Ошибка инициализации парсера: {e}")
            self.close()
            raise

    def setup_driver(self):
        """Настройка Chrome для работы в Docker с headless и stealth"""
        chrome_options = Options()

        # === Критические опции для Docker ===
        chrome_options.add_argument("--headless=new")              # Современный headless
        chrome_options.add_argument("--no-sandbox")                # Обязательно для Docker
        chrome_options.add_argument("--disable-dev-shm-usage")     # Использует /tmp вместо /dev/shm
        chrome_options.add_argument("--disable-gpu")               # Не нужен в headless
        chrome_options.add_argument("--disable-extensions")        # Уменьшает нагрузку
        chrome_options.add_argument("--disable-setuid-sandbox")
        chrome_options.add_argument("--disable-software-rasterizer")
        chrome_options.add_argument("--disable-ipc-flooding-protection")
        chrome_options.add_argument("--disable-background-timer-throttling")
        chrome_options.add_argument("--disable-backgrounding-occluded-windows")
        chrome_options.add_argument("--disable-renderer-backgrounding")
        chrome_options.add_argument("--disable-features=VizDisplayCompositor")
        chrome_options.add_argument("--window-size=1920,1080")     # Размер окна

        # === Stealth: скрытие автоматизации ===
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
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
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
        except Exception as e:
            logging.error(f"❌ Ошибка создания драйвера: {e}")
            raise

        # === Применение selenium-stealth (всё, что ниже — удаляем, он делает сам) ===
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
            logging.info(f"Создан CSV файл: {self.csv_file}")
        except Exception as e:
            logging.error(f"❌ Ошибка создания CSV: {e}")


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
            logging.info(f"💾 Данные сохранены в CSV")
            return True
        except Exception as e:
            logging.error(f"❌ Ошибка сохранения в CSV: {e}")
            return False

    def parse_seller(self, seller_id):
        """Основной метод парсинга продавца"""
        url = f"https://www.ozon.ru/seller/{seller_id}"
        logging.info(f"🔍 Парсим продавца {seller_id}")

        seller_data = {'URL': url}

        try:
            self.driver.get(url)
            time.sleep(random.uniform(3, 5))

            page_source = self.driver.page_source.lower()
            if any(phrase in page_source for phrase in ["страница не найдена", "404", "доступ ограничен", "captcha"]):
                logging.warning(f"❌ Продавец {seller_id} не найден или заблокирован")
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
            logging.error(f"❌ Ошибка парсинга {seller_id}: {str(e)}")
            return None

    def extract_basic_info(self):
        info = {}
        try:
            title_selectors = ["h1", "[data-widget='title']", ".title"]
            for selector in title_selectors:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    info['Название'] = element.text.strip()
                    break
                except:
                    continue
            else:
                info['Название'] = ""
        except:
            info['Название'] = ""

        try:
            rating_els = self.driver.find_elements(By.CSS_SELECTOR, "[class*='rating'], .rating")
            info['Рейтинг'] = rating_els[0].text.strip() if rating_els else ""
        except:
            info['Рейтинг'] = ""

        try:
            product_els = self.driver.find_elements(By.XPATH, "//*[contains(text(), 'товар')]")
            for el in product_els:
                if 'товар' in el.text.lower():
                    info['Кол-во_товаров'] = el.text.strip()
                    break
            else:
                info['Кол-во_товаров'] = ""
        except:
            info['Кол-во_товаров'] = ""

        return info

    def click_and_get_legal_info(self):
        try:
            button = self.driver.find_element(By.CSS_SELECTOR, ".ag5_5_0-a")
            self.driver.execute_script("arguments[0].click();", button)
            time.sleep(3)

            modal = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".b65_4_8-a"))
            )

            html_modal = modal.get_attribute('outerHTML')
            legal_info = self.parse_legal_info(modal.text)
            legal_info['HTML_модалки'] = html_modal

            try:
                close_btn = modal.find_element(By.CSS_SELECTOR, "[class*='close'], button")
                self.driver.execute_script("arguments[0].click();", close_btn)
            except:
                pass

            return legal_info

        except Exception as e:
            logging.warning(f"Не удалось получить юридическую информацию: {e}")
            return {}

    def parse_legal_info(self, text):
        data = {}
        import re
        ip_match = re.search(r'(ИП\s+[А-Яа-яЁё\s\"]+)', text)
        ooo_match = re.search(r'(ООО\s+[А-Яа-яЁё\s\"]+)', text)
        if ip_match:
            data['Юрлицо'] = ip_match.group(1)
        elif ooo_match:
            data['Юрлицо'] = ooo_match.group(1)

        ogrn_match = re.search(r'ОГРН[:\s]*(\d{12,13})', text)
        if ogrn_match:
            data['ОГРН'] = ogrn_match.group(1)

        inn_match = re.search(r'ИНН[:\s]*(\d{10,12})', text)
        if inn_match:
            data['ИНН'] = inn_match.group(1)

        return data

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
        logging.error(f"❌ Критическая ошибка: {e}")
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
            logging.error(f"❌ Ошибка подключения к RabbitMQ: {e}")
            time.sleep(10)


if __name__ == "__main__":
    start_consumer()
