from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium_stealth import stealth
import pandas as pd
import random
import time
import logging
import os
import pika
import json
import sys
import re

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
        self.setup_driver()
        self.wait = WebDriverWait(self.driver, 15)
        self.data_dir = "data"
        os.makedirs(self.data_dir, exist_ok=True)

        # Инициализация CSV файла
        self.csv_file = f"{self.data_dir}/sellers.csv"
        if not os.path.exists(self.csv_file):
            df = pd.DataFrame(columns=[
                'URL', 'Название', 'HTML_модалки', 'ОГРН', 'ИНН',
                'Юрлицо', 'Сайт', 'Отзывы', 'Рейтинг',
                'Срок_регистрации', 'Кол-во_товаров', 'Товары'
            ])
            df.to_csv(self.csv_file, index=False, encoding='utf-8-sig')

    def setup_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument("--user-data-dir=/tmp/user-data")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

        # Автоматическая установка ChromeDriver через webdriver-manager
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

        # Настройка stealth режима
        stealth(self.driver,
                languages=["ru-RU", "ru"],
                vendor="Google Inc.",
                platform="Win32",
                webgl_vendor="Intel Inc.",
                renderer="Intel Iris OpenGL Engine",
                fix_hairline=True,
                )

    def parse_seller(self, seller_id):
        url = f"https://www.ozon.ru/seller/{seller_id}"
        logging.info(f"Парсим продавца {seller_id}")

        try:
            self.driver.get(url)
            time.sleep(random.uniform(3, 5))  # Случайная задержка

            # Проверка на 404
            if "страница не найдена" in self.driver.page_source.lower() or "404" in self.driver.title:
                logging.warning(f"Продавец {seller_id} не найден (404)")
                return None

            seller_data = {'URL': url}

            # Извлечение основной информации со страницы
            seller_data.update(self.extract_basic_info())

            # Клик по кнопке и извлечение юридической информации
            legal_info = self.click_and_get_legal_info()
            if legal_info:
                seller_data.update(legal_info)

            self.save_to_csv(seller_data)
            logging.info(f"Успешно сохранен продавец {seller_id}")
            return seller_data

        except Exception as e:
            logging.error(f"Ошибка при парсинге {seller_id}: {str(e)}")
            return None

    def extract_basic_info(self):
        """Извлечение основной информации со страницы продавца"""
        info = {}
        try:
            # Название продавца
            title_elem = self.driver.find_element(By.CSS_SELECTOR, "h1, [data-widget='title']")
            info['Название'] = title_elem.text.strip()
        except:
            info['Название'] = ""

        try:
            # Рейтинг
            rating_elem = self.driver.find_element(By.CSS_SELECTOR, "[class*='rating'], .rating")
            info['Рейтинг'] = rating_elem.text.strip()
        except:
            info['Рейтинг'] = ""

        try:
            # Количество товаров
            products_elem = self.driver.find_element(By.XPATH, "//*[contains(text(), 'товар')]")
            info['Кол-во_товаров'] = products_elem.text.strip()
        except:
            info['Кол-во_товаров'] = ""

        return info

    def click_and_get_legal_info(self):
        """Клик по кнопке и получение юридической информации из модального окна"""
        try:
            # Поиск и клик по кнопке с классом "ag5_5_0-a"
            button = self.wait.until(
                EC.element_to_be_clickable((By.CLASS_NAME, "ag5_5_0-a"))
            )
            self.driver.execute_script("arguments[0].click();", button)
            time.sleep(2)  # Ждем открытия модалки

            # Ожидание модального окна
            modal = self.wait.until(
                EC.presence_of_element_located((By.CLASS_NAME, "b65_4_8-a"))
            )

            # Извлечение HTML модалки
            html_modal = modal.get_attribute('outerHTML')

            # Парсинг всей информации из модального окна
            modal_data = self.parse_modal_info(modal)
            modal_data['HTML_модалки'] = html_modal

            # Закрытие модального окна
            close_btn = modal.find_element(By.CSS_SELECTOR, "button[class*='b65_4_8-b1']")
            self.driver.execute_script("arguments[0].click();", close_btn)

            return modal_data

        except Exception as e:
            logging.warning(f"Не удалось найти кнопку или модальное окно: {str(e)}")
            return {}

    def parse_modal_info(self, modal_element):
        """Парсинг всей информации из модального окна"""
        info = {}

        try:
            # Извлечение юридической информации (ИП/ООО + ОГРН)
            legal_text_elem = modal_element.find_element(By.CSS_SELECTOR, ".dq0_11 .tsBody400Small")
            legal_text = legal_text_elem.text.strip()

            # Парсинг юридической информации
            legal_data = self.parse_legal_text(legal_text)
            info.update(legal_data)

        except Exception as e:
            logging.warning(f"Не удалось извлечь юридическую информацию: {str(e)}")

        # Извлечение остальной информации из модалки
        info.update(self.extract_modal_stats(modal_element))

        return info

    def parse_legal_text(self, text):
        """Парсинг юридической информации из текста"""
        data = {}

        try:
            # Разделяем текст по строкам
            lines = text.split('\n')

            for line in lines:
                line = line.strip()

                # Поиск ИП/ООО
                if line.startswith('ИП ') or line.startswith('ООО '):
                    data['Юрлицо'] = line
                    # Извлекаем имя для названия
                    data['Название'] = line

                # Поиск ОГРН (12-13 цифр)
                ogrn_match = re.search(r'(\d{12,13})', line)
                if ogrn_match:
                    data['ОГРН'] = ogrn_match.group(1)

                # Поиск ИНН (10-12 цифр)
                inn_match = re.search(r'(\d{10,12})', line)
                if inn_match and 'ОГРН' not in data:
                    data['ИНН'] = inn_match.group(1)

        except Exception as e:
            logging.warning(f"Ошибка парсинга юридического текста: {str(e)}")

        return data

    def extract_modal_stats(self, modal_element):
        """Извлечение статистики из модального окна"""
        stats = {}

        try:
            # Заказы
            orders_elem = modal_element.find_element(By.XPATH,
                                                     ".//div[contains(text(), 'Заказов')]/following-sibling::div//div[@class='b5_4_4-b0']")
            stats['Заказы'] = orders_elem.text.strip()
        except:
            stats['Заказы'] = ""

        try:
            # Работает с Ozon
            works_elem = modal_element.find_element(By.XPATH,
                                                    ".//div[contains(text(), 'Работает с Ozon')]/following-sibling::div//div[@class='b5_4_4-b0']")
            stats['Срок_регистрации'] = works_elem.text.strip()
        except:
            stats['Срок_регистрации'] = ""

        try:
            # Рейтинг
            rating_elem = modal_element.find_element(By.XPATH,
                                                     ".//div[contains(text(), 'Средняя оценка')]/following-sibling::div//div[@class='b5_4_4-b0']")
            stats['Рейтинг'] = rating_elem.text.strip()
        except:
            stats['Рейтинг'] = ""

        try:
            # Отзывы
            reviews_elem = modal_element.find_element(By.XPATH,
                                                      ".//div[contains(text(), 'Количество отзывов')]/following-sibling::div//div[@class='b5_4_4-b0']")
            stats['Отзывы'] = reviews_elem.text.strip()
        except:
            stats['Отзывы'] = ""

        return stats

    def save_to_csv(self, data):
        """Сохранение данных в CSV"""
        df = pd.DataFrame([data])
        df.to_csv(self.csv_file, mode='a', header=False, index=False, encoding='utf-8-sig')
        logging.info(f"Данные сохранены в {self.csv_file}")

    def close(self):
        self.driver.quit()


def callback(ch, method, properties, body):
    seller_id = int(body.decode())
    parser = OzonSellerParser()

    try:
        parser.parse_seller(seller_id)
        # Случайная задержка 3-5 секунд между запросами
        time.sleep(random.uniform(3, 5))

    except Exception as e:
        logging.error(f"Ошибка при обработке продавца {seller_id}: {str(e)}")
    finally:
        parser.close()
    ch.basic_ack(delivery_tag=method.delivery_tag)


def start_consumer():
    try:
        rabbitmq_host = os.getenv('RABBITMQ_HOST', 'rabbitmq')
        rabbitmq_user = os.getenv('RABBITMQ_USER', 'guest')
        rabbitmq_pass = os.getenv('RABBITMQ_PASS', 'guest')

        credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=rabbitmq_host,
                credentials=credentials,  # ← ДОБАВИТЬ АУТЕНТИФИКАЦИЮ
                heartbeat=600
            )
        )
        channel = connection.channel()

        channel.queue_declare(queue='seller_ids', durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue='seller_ids', on_message_callback=callback)

        logging.info("Ожидаем сообщения из RabbitMQ...")
        channel.start_consuming()

    except Exception as e:
        logging.error(f"Ошибка подключения к RabbitMQ: {str(e)}")
        time.sleep(10)
        start_consumer()


if __name__ == "__main__":
    start_consumer()