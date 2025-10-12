import csv
import logging
import random
import time
import os
import sys
import tempfile
import shutil
import json
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

# Создаём папки в контейнере
os.makedirs("/app/logs", exist_ok=True)
os.makedirs("/app/data", exist_ok=True)
os.makedirs("/app/screenshots", exist_ok=True)
os.makedirs("/app/html", exist_ok=True)  # Для сохранения HTML

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
        self.current_proxy = None
        self.proxy_list = []
        self.proxy_rotation_count = int(os.getenv('PROXY_ROTATION_COUNT', 3))
        self.proxy_timeout = int(os.getenv('PROXY_ROTATION_TIMEOUT', 30))

        # Загружаем список прокси
        proxy_list_str = os.getenv('PROXY_LIST', '')
        if proxy_list_str:
            self.proxy_list = [p.strip() for p in proxy_list_str.split(',') if p.strip()]

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

    def rotate_proxy(self):
        """Ротация прокси и перезапуск драйвера"""
        if not self.proxy_list or len(self.proxy_list) <= 1:
            return False

        try:
            # Переходим к следующему прокси по кругу
            self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxy_list)
            new_proxy = self.proxy_list[self.current_proxy_index]

            old_proxy = self.current_proxy
            self.current_proxy = new_proxy
            self.requests_per_proxy = 0  # Сбрасываем счетчик

            logging.info(
                f"🔄 Ротируем прокси [{self.current_proxy_index + 1}/{len(self.proxy_list)}]: {old_proxy} -> {new_proxy}")

            # Перезапускаем драйвер с новым прокси
            if self.driver:
                self.driver.quit()
                time.sleep(2)

            self.setup_driver()
            self.wait = WebDriverWait(self.driver, 15)

            logging.info("✅ Прокси успешно сменен")
            return True

        except Exception as e:
            logging.error(f"❌ Ошибка ротации прокси: {e}")
            # Пробуем восстановить работу с текущим прокси
            try:
                if self.driver:
                    self.driver.quit()
                self.setup_driver()
                self.wait = WebDriverWait(self.driver, 15)
            except:
                pass
            return False

    def setup_driver(self):
        """Настройка Chrome для работы в Docker с headless, stealth и прокси"""
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

        # === Размер окна ===
        chrome_options.add_argument("--window-size=1366,768")

        # === УЛУЧШЕННАЯ НАСТРОЙКА ПРОКСИ ===
        use_proxies = os.getenv('USE_PROXIES', 'false').lower() == 'true'
        proxy_list_str = os.getenv('PROXY_LIST', '')

        if use_proxies and proxy_list_str:
            self.proxy_list = [p.strip() for p in proxy_list_str.split(',') if p.strip()]

            if self.proxy_list:
                # Инициализируем индекс прокси при первом запуске
                if not hasattr(self, 'current_proxy_index'):
                    self.current_proxy_index = 0
                    self.requests_per_proxy = 0

                # Берем текущий прокси по индексу (а не случайный)
                current_proxy = self.proxy_list[self.current_proxy_index]
                chrome_options.add_argument(f'--proxy-server={current_proxy}')
                self.current_proxy = current_proxy

                logging.info(
                    f"🔄 Используем прокси [{self.current_proxy_index + 1}/{len(self.proxy_list)}]: {current_proxy}")
            else:
                logging.warning("⚠️ PROXY_LIST пустой, работаем без прокси")
                self.current_proxy = None
                self.proxy_list = []
        else:
            logging.info("🔌 Прокси отключены в настройках")
            self.current_proxy = None
            self.proxy_list = []

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

        # Случайный User-Agent из обновленного списка
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/121.0.0.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15"
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
            'URL', 'Название', 'Рейтинг', 'Отзывы', 'Заказы',
            'Описание', 'Ссылка_на_магазин', 'Instance_ID',
            'ОГРН', 'ИНН', 'Название_юр_лица', 'Веб-сайт',
            'Кол-во_товаров_на_странице', 'Общее_кол-во_товаров', 'Срок_регистрации',
            'Html_путь', 'Товары_JSON'
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
                    data.get('Рейтинг', ''),
                    data.get('Отзывы', ''),
                    data.get('Заказы', ''),
                    data.get('Описание', ''),
                    data.get('Ссылка_на_магазин', ''),
                    self.instance_id,
                    data.get('ОГРН', ''),
                    data.get('ИНН', ''),
                    data.get('Название_юр_лица', ''),
                    data.get('Веб-сайт', ''),
                    data.get('Кол-во_товаров_на_странице', ''),
                    data.get('Общее_кол-во_товаров', ''),
                    data.get('Срок_регистрации', ''),
                    data.get('Html_путь', ''),
                    data.get('Товары_JSON', '')
                ])
                f.flush()
                os.fsync(f.fileno())
            logging.info(f"✅ Данные сохранены в CSV")
            return True
        except Exception as e:
            logging.error(f"❌ Ошибка сохранения в CSV: {e}", exc_info=True)
            return False

    def save_html_page(self, seller_id, prefix=""):
        """Сохранение HTML страницы для отладки"""
        try:
            html_path = f"/app/html/{prefix}{seller_id}_{int(time.time())}.html"
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(self.driver.page_source)
            return html_path
        except Exception as e:
            logging.warning(f"⚠️ Не удалось сохранить HTML: {e}")
            return ""

    def extract_products_from_main_page(self):
        try:
            logging.info("🛒 Начинаем парсинг товаров с главной страницы...")

            # Ожидаем появления контейнера с товарами - ИЩЕМ ТОЧНЫЙ СЕЛЕКТОР
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-widget='infiniteVirtualPagination']"))
            )

            # Прокрутка
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            scroll_attempts = 0
            while scroll_attempts < 3:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
                scroll_attempts += 1

            products = []

            # ⚡ ОСНОВНОЙ СЕЛЕКТОР ДЛЯ КАРТОЧЕК ТОВАРОВ ⚡
            # Ищем ВСЕ карточки товаров по классу и data-атрибуту
            product_cards = self.driver.find_elements(By.CSS_SELECTOR, "div.tile-root[data-index]")

            logging.info(f"📦 Найдено карточек товаров: {len(product_cards)}")

            for card in product_cards[:20]:  # Первые 20 товаров
                try:
                    product_data = {}

                    # 1. НАЗВАНИЕ ТОВАРА - ищем в ссылке или в span
                    try:
                        # Вариант 1: Ищем текст в ссылке
                        link_elem = card.find_element(By.CSS_SELECTOR, "a[href*='/product/']")
                        product_data['name'] = link_elem.get_attribute('textContent').strip()
                    except:
                        # Вариант 2: Ищем в span с текстом
                        try:
                            spans = card.find_elements(By.CSS_SELECTOR, "span")
                            for span in spans:
                                text = span.text.strip()
                                if text and len(text) > 10:  # Название обычно длинное
                                    product_data['name'] = text
                                    break
                        except:
                            product_data['name'] = ''

                    # 2. ЦЕНА ТОВАРА - ищем элементы с ценой
                    try:
                        # Ищем элементы содержащие "₽" или цифры с символами валют
                        price_selectors = [
                            "span[class*='price']",
                            "span[class*='money']",
                            "div[class*='price']",
                            "//span[contains(text(), '₽')]"
                        ]
                        for selector in price_selectors:
                            try:
                                if selector.startswith("//"):
                                    price_elems = card.find_elements(By.XPATH, selector)
                                else:
                                    price_elems = card.find_elements(By.CSS_SELECTOR, selector)

                                for elem in price_elems:
                                    text = elem.text.strip()
                                    if '₽' in text or any(char.isdigit() for char in text):
                                        product_data['price'] = text
                                        break
                                if product_data.get('price'):
                                    break
                            except:
                                continue
                    except:
                        product_data['price'] = ''

                    # 3. ССЫЛКА НА ТОВАР
                    try:
                        link_elem = card.find_element(By.CSS_SELECTOR, "a[href*='/product/']")
                        product_data['link'] = "https://www.ozon.ru" + link_elem.get_attribute('href')
                    except:
                        product_data['link'] = ''

                    # 4. РЕЙТИНГ ТОВАРА (если есть)
                    try:
                        rating_selectors = [
                            "[class*='rating']",
                            "[class*='star']",
                            "//span[contains(@class, 'rating')]"
                        ]
                        for selector in rating_selectors:
                            try:
                                if selector.startswith("//"):
                                    rating_elem = card.find_element(By.XPATH, selector)
                                else:
                                    rating_elem = card.find_element(By.CSS_SELECTOR, selector)

                                if rating_elem.text.strip():
                                    product_data['rating'] = rating_elem.text.strip()
                                    break
                            except:
                                continue
                    except:
                        product_data['rating'] = ''

                    # Добавляем только если есть название
                    if product_data.get('name'):
                        products.append(product_data)

                except Exception as e:
                    logging.debug(f"⚠️ Ошибка парсинга карточки товара: {e}")
                    continue

            logging.info(f"✅ Успешно спарсено товаров: {len(products)}")
            return products

        except Exception as e:
            logging.error(f"❌ Ошибка при парсинге товаров: {e}")
            return []

    def click_shop_button(self):
        """Клик по кнопке 'Магазин' с конкретными селекторами из HTML"""
        try:
            logging.info("🛍️ Ищем кнопку 'Магазин'...")

            # ⚡ СКРОЛЛИМ К ВЕРХУ ПЕРЕД ПОИСКОМ
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(2)

            # 🔥 ОСНОВНЫЕ СЕЛЕКТОРЫ ИЗ ВАШЕГО HTML:
            shop_selectors = [
                # Селекторы из вашего HTML
                "div[data-widget='sellertransparency'] a[href*='/seller/']",
                "div.m6h_19 a[href*='/seller/']",  # По классу из data-widget
                "div.hm7_19 a[href*='/seller/']",  # Внутренний контейнер
                "div.hm8_19 a[href*='/seller/']",
                "div.b5_4_4-a0",  # Кнопка с стилями из вашего HTML
                "div[style*='background: var'][style*='border-radius: 8px']",  # По стилям

                # Дополнительные селекторы
                "//a[contains(@href, '/seller/') and contains(text(), 'Магазин')]",
                "//button[contains(text(), 'Магазин')]",
                "//div[contains(@class, 'shop')]//a[contains(@href, '/seller/')]"
            ]

            for selector in shop_selectors:
                try:
                    if selector.startswith("//"):
                        elements = self.driver.find_elements(By.XPATH, selector)
                    else:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)

                    logging.info(f"🔎 Проверяем селектор: {selector}, найдено элементов: {len(elements)}")

                    for element in elements:
                        try:
                            if element.is_displayed() and element.is_enabled():
                                # Получаем URL перед кликом
                                shop_url = element.get_attribute('href')
                                element_text = element.text.strip()

                                logging.info(f"🎯 Нашли кнопку магазина: '{element_text}', URL: {shop_url}")

                                # Скроллим к элементу и кликаем
                                self.driver.execute_script(
                                    "arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", element)
                                time.sleep(2)

                                # Кликаем через JavaScript (надежнее)
                                self.driver.execute_script("arguments[0].click();", element)

                                # Ждем загрузки новой страницы
                                time.sleep(random.uniform(3, 5))
                                WebDriverWait(self.driver, 10).until(
                                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                                )

                                logging.info(f"✅ Успешно перешли в магазин")
                                return shop_url

                        except Exception as e:
                            logging.debug(f"⚠️ Элемент не кликабелен: {e}")
                            continue

                except Exception as e:
                    logging.debug(f"⚠️ Ошибка с селектором {selector}: {e}")
                    continue

            logging.warning("⚠️ Не удалось найти и кликнуть на кнопку 'Магазин'")

            # Сохраняем скриншот для отладки
            screenshot_path = f"/app/screenshots/no_shop_button_{int(time.time())}.png"
            self.driver.save_screenshot(screenshot_path)
            logging.info(f"📸 Сохранен скриншот для отладки: {screenshot_path}")

            return None

        except Exception as e:
            logging.error(f"❌ Ошибка при клике на кнопку магазина: {e}")
            return None

    def extract_legal_info_from_modal(self):
        """Извлечение юридической информации из модального окна 'О магазине'"""
        try:
            logging.info("⚖️ Ищем и открываем модальное окно с информацией о магазине...")

            legal_data = {}

            # 🔥 СЕЛЕКТОРЫ ДЛЯ ОТКРЫТИЯ МОДАЛЬНОГО ОКНА
            modal_button_selectors = [
                # Основные селекторы для открытия информации о магазине
                "button[data-widget*='webSeller']",
                "a[href*='#info']",
                "button[class*='info']",
                "div[class*='seller-info'] button",
                "//button[contains(., 'О магазине')]",
                "//a[contains(., 'О магазине')]",
                "//*[contains(., 'магазин') and contains(@class, 'button')]",
                "//*[@data-widget='webSellerName']//button",  # Кнопка рядом с названием магазина
                "//*[contains(@class, 'seller')]//button[contains(@class, 'info')]"
            ]

            # Пробуем найти и кликнуть на кнопку открытия модалки
            modal_opened = False
            for selector in modal_button_selectors:
                try:
                    if selector.startswith("//"):
                        elements = self.driver.find_elements(By.XPATH, selector)
                    else:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)

                    for element in elements:
                        if element.is_displayed():
                            logging.info(f"🎯 Нашли кнопку открытия модалки: {selector}")
                            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                            time.sleep(1)
                            self.driver.execute_script("arguments[0].click();", element)
                            time.sleep(3)
                            modal_opened = True
                            break
                    if modal_opened:
                        break
                except:
                    continue

            if not modal_opened:
                logging.warning("⚠️ Не удалось открыть модальное окно")
                return legal_data

            # ⚡ ИЗВЛЕКАЕМ ДАННЫЕ ИЗ МОДАЛЬНОГО ОКНА

            # 1. ОГРН (из текста внизу модалки)
            try:
                # Ищем блок с юридической информацией
                legal_text_selectors = [
                    "//div[contains(@class, 'bq03_0_2-a')]//span[contains(@class, 'tsBody400Small')]",
                    "//*[contains(@class, 'tsBody400Small') and contains(text(), '10477')]",
                    "//*[contains(text(), '1047796071839')]"
                ]

                for selector in legal_text_selectors:
                    try:
                        elements = self.driver.find_elements(By.XPATH, selector)
                        for element in elements:
                            text = element.text.strip()
                            # Ищем 13-значный номер (ОГРН)
                            import re
                            ogrn_match = re.search(r'\b\d{13}\b', text)
                            if ogrn_match:
                                legal_data['ОГРН'] = ogrn_match.group()
                                logging.info(f"✅ Найден ОГРН: {legal_data['ОГРН']}")
                                break

                            # Если нашли текст с ООО - извлекаем название компании
                            if 'ООО' in text or 'АО' in text or 'ИП' in text:
                                # Берем первую строку как название компании
                                company_name = text.split('\n')[0]
                                legal_data['Название_юр_лица'] = company_name
                                logging.info(f"✅ Название компании: {company_name}")

                    except:
                        continue
            except Exception as e:
                logging.warning(f"⚠️ Не удалось извлечь ОГРН: {e}")
                legal_data['ОГРН'] = ''

            # 2. РЕЙТИНГ (из модалки)
            try:
                rating_selectors = [
                    "//div[contains(@class, 'b5_4_4-b0') and contains(text(), 'из 5')]",
                    "//*[contains(text(), 'из 5')]",
                    "//*[contains(@title, 'из 5')]"
                ]
                for selector in rating_selectors:
                    try:
                        element = self.driver.find_element(By.XPATH, selector)
                        text = element.text.strip()
                        if 'из 5' in text:
                            legal_data['Рейтинг'] = text
                            break
                    except:
                        continue
            except:
                legal_data['Рейтинг'] = ''

            # 3. ОТЗЫВЫ
            try:
                reviews_selectors = [
                    "//*[contains(text(), 'отзыв')]/following-sibling::div//div[contains(@class, 'b5_4_4-b0')]",
                    "//div[contains(@class, 'b5_4_4-b0') and (contains(text(), 'K') or contains(text(), 'тыс') or contains(text(), 'отзыв'))]"
                ]
                for selector in reviews_selectors:
                    try:
                        element = self.driver.find_element(By.XPATH, selector)
                        text = element.text.strip()
                        # Проверяем, что это число (может содержать K, тыс и т.д.)
                        if any(char.isdigit() for char in text):
                            legal_data['Отзывы'] = text
                            break
                    except:
                        continue
            except:
                legal_data['Отзывы'] = ''

            # 4. ЗАКАЗЫ
            try:
                orders_selectors = [
                    "//*[contains(text(), 'Заказов')]/following-sibling::div//div[contains(@class, 'b5_4_4-b0')]",
                    "//div[contains(@class, 'b5_4_4-b0') and (contains(text(), 'M') or contains(text(), 'тыс') or contains(text(), 'заказ'))]"
                ]
                for selector in orders_selectors:
                    try:
                        element = self.driver.find_element(By.XPATH, selector)
                        text = element.text.strip()
                        if any(char.isdigit() for char in text):
                            legal_data['Заказы'] = text
                            break
                    except:
                        continue
            except:
                legal_data['Заказы'] = ''

            # 5. СРОК РЕГИСТРАЦИИ
            try:
                registration_selectors = [
                    "//*[contains(text(), 'Работает с Ozon')]/following-sibling::div//div[contains(@class, 'b5_4_4-b0')]",
                    "//*[contains(text(), 'Работает с')]/following-sibling::div//div[contains(@class, 'b5_4_4-b0')]",
                    "//*[contains(text(), 'На Ozon с')]",
                    "//*[contains(@title, 'лет') or contains(@title, 'год') or contains(@title, 'месяц')]",
                    "//div[contains(@class, 'b5_4_4-b0') and (contains(text(), 'лет') or contains(text(), 'год') or contains(text(), 'месяц'))]"
                ]

                for selector in registration_selectors:
                    try:
                        if selector.startswith("//"):
                            elements = self.driver.find_elements(By.XPATH, selector)
                        else:
                            elements = self.driver.find_elements(By.CSS_SELECTOR, selector)

                        for element in elements:
                            text = element.text.strip()
                            # Проверяем, содержит ли текст временные единицы
                            if any(word in text.lower() for word in ['лет', 'год', 'месяц', 'день']):
                                legal_data['Срок_регистрации'] = text
                                logging.info(f"✅ Найден срок регистрации: {text}")
                                break
                        if legal_data.get('Срок_регистрации'):
                            break
                    except:
                        continue

                # Если не нашли по селекторам, пробуем найти любой элемент с временными данными
                if not legal_data.get('Срок_регистрации'):
                    try:
                        all_elements = self.driver.find_elements(By.XPATH,
                                                                 "//*[contains(text(), 'Работает с Ozon')]//following::div[1]//div")
                        for element in all_elements:
                            text = element.text.strip()
                            if any(word in text.lower() for word in ['лет', 'год', 'месяц']):
                                legal_data['Срок_регистрации'] = text
                                break
                    except:
                        pass

            except Exception as e:
                logging.warning(f"⚠️ Не удалось извлечь срок регистрации: {e}")
                legal_data['Срок_регистрации'] = ''

            # 6. ВЕБ-САЙТ (пробуем найти)
            try:
                # Ищем ссылки которые могут быть сайтом компании
                all_links = self.driver.find_elements(By.TAG_NAME, "a")
                for link in all_links:
                    href = link.get_attribute('href') or ''
                    text = link.text.strip()
                    if ('http' in href and 'ozon.ru' not in href and
                            not href.startswith('javascript') and len(text) > 3):
                        legal_data['Веб-сайт'] = href
                        break
            except:
                legal_data['Веб-сайт'] = ''

            # 🔥 ЗАКРЫВАЕМ МОДАЛЬНОЕ ОКНО
            try:
                close_selectors = [
                    "button[class*='b65_4_8-b1']",  # Кнопка закрытия из вашего HTML
                    "button[aria-label*='Закрыть']",
                    "//button[contains(@class, 'b65_4_8-b1')]",
                    "//button[contains(., 'Понятно')]",
                    "button.b25_4_4-a0"  # Кнопка "Понятно" из вашего HTML
                ]

                for selector in close_selectors:
                    try:
                        if selector.startswith("//"):
                            close_btn = self.driver.find_element(By.XPATH, selector)
                        else:
                            close_btn = self.driver.find_element(By.CSS_SELECTOR, selector)

                        if close_btn.is_displayed():
                            self.driver.execute_script("arguments[0].click();", close_btn)
                            time.sleep(2)
                            logging.info("✅ Модальное окно закрыто")
                            break
                    except:
                        continue
            except Exception as e:
                logging.warning(f"⚠️ Не удалось закрыть модальное окно: {e}")

            logging.info(f"✅ Юридическая информация извлечена: {legal_data}")
            return legal_data

        except Exception as e:
            logging.error(f"❌ Ошибка при получении юридической информации: {e}")
            return {}

    def extract_shop_info(self):
        """Извлечение информации со страницы магазина"""
        try:
            data = {}

            # Ожидаем загрузки контента
            time.sleep(random.uniform(3, 5))

            # Название магазина
            try:
                name_selectors = [
                    "h1",
                    ".seller-name",
                    "[data-widget='webSellerName']",
                    ".shop-title",
                    "//h1[contains(@class, 'title')]"
                ]
                for selector in name_selectors:
                    try:
                        if selector.startswith("//"):
                            element = self.driver.find_element(By.XPATH, selector)
                        else:
                            element = self.driver.find_element(By.CSS_SELECTOR, selector)
                        if element.text.strip():
                            data['Название'] = element.text.strip()
                            break
                    except:
                        continue
            except:
                data['Название'] = ''

            # Рейтинг
            try:
                rating_selectors = [
                    "[data-widget*='rating']",
                    ".seller-rating",
                    ".rating-value",
                    "//*[contains(@class, 'rating')]"
                ]
                for selector in rating_selectors:
                    try:
                        if selector.startswith("//"):
                            element = self.driver.find_element(By.XPATH, selector)
                        else:
                            element = self.driver.find_element(By.CSS_SELECTOR, selector)
                        if element.text.strip():
                            data['Рейтинг'] = element.text.strip()
                            break
                    except:
                        continue
            except:
                data['Рейтинг'] = ''

            # Отзывы
            try:
                reviews_selectors = [
                    "[data-widget*='reviews']",
                    ".reviews-count",
                    "//*[contains(., 'отзыв')]"
                ]
                for selector in reviews_selectors:
                    try:
                        if selector.startswith("//"):
                            element = self.driver.find_element(By.XPATH, selector)
                        else:
                            element = self.driver.find_element(By.CSS_SELECTOR, selector)
                        if element.text.strip():
                            data['Отзывы'] = element.text.strip()
                            break
                    except:
                        continue
            except:
                data['Отзывы'] = ''

            # Заказы
            try:
                orders_selectors = [
                    "[data-widget*='orders']",
                    "//*[contains(., 'заказ')]"
                ]
                for selector in orders_selectors:
                    try:
                        if selector.startswith("//"):
                            element = self.driver.find_element(By.XPATH, selector)
                        else:
                            element = self.driver.find_element(By.CSS_SELECTOR, selector)
                        if element.text.strip():
                            data['Заказы'] = element.text.strip()
                            break
                    except:
                        continue
            except:
                data['Заказы'] = ''

            # Описание
            try:
                desc_selectors = [
                    ".seller-description",
                    "[data-widget*='description']",
                    "//*[contains(@class, 'description')]"
                ]
                for selector in desc_selectors:
                    try:
                        if selector.startswith("//"):
                            element = self.driver.find_element(By.XPATH, selector)
                        else:
                            element = self.driver.find_element(By.CSS_SELECTOR, selector)
                        if element.text.strip():
                            data['Описание'] = element.text.strip()[:500]
                            break
                    except:
                        continue
            except:
                data['Описание'] = ''

            # Срок регистрации
            try:
                registration_selectors = [
                    "//*[contains(., 'На Ozon с')]",
                    "//*[contains(., 'регистрация')]",
                    "//*[contains(., 'с ') and contains(., '20')]"
                ]
                for selector in registration_selectors:
                    try:
                        element = self.driver.find_element(By.XPATH, selector)
                        if element.text.strip():
                            data['Срок_регистрации'] = element.text.strip()
                            break
                    except:
                        continue
            except:
                data['Срок_регистрации'] = ''

            # Общее количество товаров
            try:
                total_products_selectors = [
                    "//*[contains(., 'товар') and contains(., 'шт')]",
                    "//*[contains(., 'Товары')]",
                    "[data-widget*='totalProducts']"
                ]
                for selector in total_products_selectors:
                    try:
                        if selector.startswith("//"):
                            element = self.driver.find_element(By.XPATH, selector)
                        else:
                            element = self.driver.find_element(By.CSS_SELECTOR, selector)
                        if element.text.strip():
                            data['Общее_кол-во_товаров'] = element.text.strip()
                            break
                    except:
                        continue
            except:
                data['Общее_кол-во_товаров'] = ''

            return data

        except Exception as e:
            logging.error(f"❌ Ошибка извлечения информации о магазине: {e}")
            return {}

    def parse_seller(self, seller_id):
        """Основной метод парсинга продавца с ротацией прокси"""
        url = f"https://www.ozon.ru/seller/{seller_id}"

        # 🔄 ПРОВЕРКА РОТАЦИИ ПЕРЕД НАЧАЛОМ ПАРСИНГА
        if self.proxy_list:
            self.requests_per_proxy += 1
            proxy_rotation_count = int(os.getenv('PROXY_ROTATION_COUNT', 5))

            if self.requests_per_proxy >= proxy_rotation_count:
                logging.info(f"🔄 Достигнут лимит запросов ({self.requests_per_proxy}) для прокси, ротируем...")
                self.rotate_proxy()

        # Логируем информацию о прокси
        proxy_info = f" [Прокси: {self.current_proxy}]" if self.current_proxy else " [Без прокси]"
        proxy_count_info = f" [Запросов на прокси: {self.requests_per_proxy}]" if self.proxy_list else ""
        logging.info(f"🔍 Парсим продавца {seller_id}{proxy_info}{proxy_count_info}")

        seller_data = {'URL': url}

        try:
            # === ЭТАП 1: Загружаем страницу продавца ===
            logging.info(f"🌐 Загружаем страницу продавца: {url}")
            self.driver.get(url)
            time.sleep(random.uniform(5, 8))

            # Проверяем на блокировку
            page_source = self.driver.page_source.lower()
            blocking_indicators = [
                "страница не найдена", "404", "доступ ограничен",
                "captcha", "bot", "automation", "доступ запрещен",
                "blocked", "cloudflare", "недоступен"
            ]

            if any(phrase in page_source for phrase in blocking_indicators):
                screenshot_path = f"/app/screenshots/error_{seller_id}_{int(time.time())}.png"
                self.driver.save_screenshot(screenshot_path)
                time.sleep(2)

                # 🔄 ПРИНУДИТЕЛЬНАЯ РОТАЦИЯ ПРИ БЛОКИРОВКЕ
                if ("доступ ограничен" in page_source or "blocked" in page_source) and self.proxy_list:
                    logging.warning(f"🚫 Обнаружена блокировка, принудительно ротируем прокси...")
                    self.rotate_proxy()

                if "доступ ограничен" in page_source or "blocked" in page_source:
                    logging.warning(f"🚫 Продавец {seller_id} заблокирован{proxy_info}. Скриншот: {screenshot_path}")
                elif "страница не найдена" in page_source or "404" in page_source:
                    logging.warning(f"❓ Продавец {seller_id} не существует{proxy_info}")
                else:
                    logging.warning(
                        f"⚠️ Неизвестная ошибка для продавца {seller_id}{proxy_info}. Скриншот: {screenshot_path}")
                return None

            # === ЭТАП 2: Сохраняем HTML главной страницы ===
            seller_data['Html_путь'] = self.save_html_page(seller_id, "main_")

            # === ЭТАП 3: Парсим товары с главной страницы ===
            logging.info("🛒 Парсим товары с главной страницы...")
            products = self.extract_products_from_main_page()
            seller_data['Кол-во_товаров_на_странице'] = len(products)
            seller_data['Товары_JSON'] = json.dumps(products, ensure_ascii=False)
            logging.info(f"✅ Спарсено товаров: {len(products)}")

            # === ЭТАП 4: Скроллим к верху для поиска кнопки "Магазин" ===
            logging.info("⬆️ Скроллим к верху для поиска кнопки 'Магазин'...")
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(3)

            # === ЭТАП 5: Ищем и кликаем кнопку "Магазин" ===
            logging.info("🛍️ Ищем кнопку 'Магазин'...")
            shop_url = self.click_shop_button()

            if not shop_url:
                logging.warning(f"⚠️ Не удалось найти кнопку 'Магазин' для продавца {seller_id}")
                # Сохраняем то, что успели собрать
                if products:
                    self.save_to_csv(seller_data)
                    logging.info(f"✅ Сохранены данные без информации о магазине")
                return seller_data

            seller_data['Ссылка_на_магазин'] = shop_url
            logging.info(f"✅ Успешно перешли в магазин: {shop_url}")

            # === ЭТАП 6: Сохраняем HTML страницы магазина ===
            seller_data['Html_путь'] += f"; {self.save_html_page(seller_id, 'shop_')}"

            # === ЭТАП 7: Парсим информацию о магазине ===
            logging.info("🏪 Парсим информацию о магазине...")
            shop_info = self.extract_shop_info()
            seller_data.update(shop_info)

            # === ЭТАП 8: Парсим юридическую информацию ===
            logging.info("⚖️ Парсим юридическую информацию...")
            legal_info = self.extract_legal_info_from_modal()
            seller_data.update(legal_info)

            # === ЭТАП 9: Сохраняем все данные ===
            if self.save_to_csv(seller_data):
                logging.info(f"✅ Успешно обработан продавец {seller_id}{proxy_info}")
                return seller_data
            else:
                logging.error(f"❌ Ошибка сохранения продавца {seller_id}{proxy_info}")
                return None

        except Exception as e:
            # Сохраняем скриншот при критической ошибке
            screenshot_path = f"/app/screenshots/crash_{seller_id}_{int(time.time())}.png"
            try:
                self.driver.save_screenshot(screenshot_path)
                time.sleep(2)
                logging.error(f"💥 Критическая ошибка при парсинге {seller_id}{proxy_info}. Скриншот: {screenshot_path}",
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