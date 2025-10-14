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
        """Инициализация CSV файла с заголовками по ТЗ"""
        headers = [
            'URL', 'название', 'Html', 'ОГРН', 'ИНН', 'Название юр лица',
            'Кол-во отзывов', 'рейтинг', 'Срок регистрации', 'Товары'
        ]
        try:
            with open(self.csv_file, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
            logging.info(f"✅ Создан CSV файл: {self.csv_file}")
        except Exception as e:
            logging.error(f"❌ Ошибка создания CSV: {e}", exc_info=True)

    def save_to_csv(self, data):
        """Сохранение данных в CSV по ТЗ"""
        try:
            with open(self.csv_file, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow([
                    data.get('URL', ''),
                    data.get('Название', ''),
                    data.get('Html_путь', ''),  # Переименовали
                    data.get('ОГРН', ''),
                    data.get('ИНН', ''),
                    data.get('Название_юр_лица', ''),
                    data.get('Отзывы', ''),  # Переименовали
                    data.get('Рейтинг', ''),  # Переименовали
                    data.get('Срок_регистрации', ''),  # Переименовали
                    data.get('Товары_JSON', '')  # Переименовали
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
        """Парсинг товаров с главной страницы без скроллинга"""
        try:
            logging.info("🛒 Начинаем парсинг товаров с главной страницы...")

            # Ждем появления контейнера с товарами
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "div[data-widget*='paginator'], div[data-widget*='tileGrid'], div.tile-root"))
                )
            except:
                logging.warning("⚠️ Не найден контейнер с товарами, возвращаем пустой список")
                return []

            products = []

            # ⚡ ОСНОВНЫЕ СЕЛЕКТОРЫ ДЛЯ КАРТОЧЕК ТОВАРОВ ⚡
            # Ищем карточки товаров по разным селекторам
            product_selectors = [
                "div.tile-root[data-index]",  # Основной селектор
                "div[data-widget*='tileGrid'] div.tile-root",  # Внутри tileGrid
                "#paginator div.tile-root",  # Внутри пагинатора
                "div[data-index]"  # Любой элемент с data-index
            ]

            product_cards = []
            for selector in product_selectors:
                try:
                    cards = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if cards:
                        product_cards = cards
                        logging.info(f"📦 Найдено карточек товаров по селектору '{selector}': {len(cards)}")
                        break
                except:
                    continue

            if not product_cards:
                logging.warning("⚠️ Не найдено карточек товаров")
                return []

            # Парсим только видимые карточки (первые 20 или все видимые)
            visible_cards = [card for card in product_cards if card.is_displayed()][:20]
            logging.info(f"🎯 Парсим видимых карточек: {len(visible_cards)}")

            for card in visible_cards:
                try:
                    product_data = {}

                    # 1. НАЗВАНИЕ ТОВАРА
                    try:
                        # Ищем название в основном месте
                        name_selectors = [
                            ".bq03_0_2-a span.tsBody500Medium",  # Основной селектор названия
                            "a[href*='/product/'] .bq03_0_2-a span",  # В ссылке
                            ".tsBody500Medium",  # По классу текста
                            "span[class*='tsBody500']"  # Любой span с текстом
                        ]

                        for name_selector in name_selectors:
                            try:
                                name_elem = card.find_element(By.CSS_SELECTOR, name_selector)
                                name_text = name_elem.text.strip()
                                if name_text and len(name_text) > 5:  # Название должно быть достаточно длинным
                                    product_data['name'] = name_text
                                    break
                            except:
                                continue

                        if not product_data.get('name'):
                            product_data['name'] = ''

                    except Exception as e:
                        logging.debug(f"⚠️ Ошибка поиска названия: {e}")
                        product_data['name'] = ''

                    # 2. ЦЕНА ТОВАРА
                    try:
                        # Основные селекторы цены
                        price_selectors = [
                            ".c35_3_8-a1.tsHeadline500Medium",  # Основная цена
                            "span[class*='tsHeadline500Medium']",  # Цена по классу
                            ".c35_3_8-a0 span",  # В контейнере цены
                            "//span[contains(text(), '₽')]"  # Любой элемент с символом рубля
                        ]

                        for price_selector in price_selectors:
                            try:
                                if price_selector.startswith("//"):
                                    price_elems = card.find_elements(By.XPATH, price_selector)
                                else:
                                    price_elems = card.find_elements(By.CSS_SELECTOR, price_selector)

                                for elem in price_elems:
                                    text = elem.text.strip()
                                    # Ищем цену с символом рубля или просто цифры
                                    if '₽' in text or (any(char.isdigit() for char in text) and len(text) <= 20):
                                        product_data['price'] = text
                                        break
                                if product_data.get('price'):
                                    break
                            except:
                                continue

                        if not product_data.get('price'):
                            product_data['price'] = ''

                    except Exception as e:
                        logging.debug(f"⚠️ Ошибка поиска цены: {e}")
                        product_data['price'] = ''

                    # 3. ССЫЛКА НА ТОВАР
                    try:
                        link_selectors = [
                            "a[href*='/product/']",  # Основная ссылка на товар
                            ".tile-clickable-element[href*='/product/']"  # Кликабельный элемент
                        ]

                        for link_selector in link_selectors:
                            try:
                                link_elem = card.find_element(By.CSS_SELECTOR, link_selector)
                                href = link_elem.get_attribute('href')
                                if href and '/product/' in href:
                                    product_data['link'] = href if href.startswith(
                                        'http') else f"https://www.ozon.ru{href}"
                                    break
                            except:
                                continue

                        if not product_data.get('link'):
                            product_data['link'] = ''

                    except Exception as e:
                        logging.debug(f"⚠️ Ошибка поиска ссылки: {e}")
                        product_data['link'] = ''

                    # 4. ФОТО ТОВАРА
                    try:
                        img_selectors = [
                            "img.i4s_24.b95_3_3-a",
                            "img[loading='eager']",
                            "img[src*='ozon.ru']",
                            "img.b95_3_3-a"
                        ]

                        for img_selector in img_selectors:
                            try:
                                img_elem = card.find_element(By.CSS_SELECTOR, img_selector)
                                img_src = img_elem.get_attribute('src')
                                if img_src and 'ozon.ru' in img_src:
                                    product_data['image'] = img_src
                                    break
                            except:
                                continue

                        if not product_data.get('image'):
                            product_data['image'] = ''

                    except Exception as e:
                        logging.debug(f"⚠️ Ошибка поиска фото: {e}")
                        product_data['image'] = ''

                    # 5. РЕЙТИНГ ТОВАРА
                    try:
                        rating_selectors = [
                            ".p6b3_0_2-a4 span[style*='color:var(--textPremium)']",  # Рейтинг
                            "span[style*='color:var(--textPremium)']",  # По цвету
                            "//span[contains(@style, 'textPremium')]"  # XPath по стилю
                        ]

                        for rating_selector in rating_selectors:
                            try:
                                if rating_selector.startswith("//"):
                                    rating_elems = card.find_elements(By.XPATH, rating_selector)
                                else:
                                    rating_elems = card.find_elements(By.CSS_SELECTOR, rating_selector)

                                for elem in rating_elems:
                                    text = elem.text.strip()
                                    # Проверяем, что это рейтинг (число с точкой или просто число)
                                    if text and ('.' in text or text.replace('.', '').isdigit()):
                                        product_data['rating'] = text
                                        break
                                if product_data.get('rating'):
                                    break
                            except:
                                continue

                        if not product_data.get('rating'):
                            product_data['rating'] = ''

                    except Exception as e:
                        logging.debug(f"⚠️ Ошибка поиска рейтинга: {e}")
                        product_data['rating'] = ''

                    # 6. КОЛИЧЕСТВО ОТЗЫВОВ
                    try:
                        reviews_selectors = [
                            ".p6b3_0_2-a4 span[style*='color:var(--textSecondary)']",  # Отзывы
                            "span[style*='color:var(--textSecondary)']",  # По цвету
                            "//span[contains(text(), 'отзыв')]"  # По тексту
                        ]

                        for reviews_selector in reviews_selectors:
                            try:
                                if reviews_selector.startswith("//"):
                                    reviews_elems = card.find_elements(By.XPATH, reviews_selector)
                                else:
                                    reviews_elems = card.find_elements(By.CSS_SELECTOR, reviews_selector)

                                for elem in reviews_elems:
                                    text = elem.text.strip()
                                    if 'отзыв' in text.lower():
                                        product_data['reviews_count'] = text
                                        break
                                if product_data.get('reviews_count'):
                                    break
                            except:
                                continue

                        if not product_data.get('reviews_count'):
                            product_data['reviews_count'] = ''

                    except Exception as e:
                        logging.debug(f"⚠️ Ошибка поиска отзывов: {e}")
                        product_data['reviews_count'] = ''

                    # Добавляем товар только если есть название
                    if product_data.get('name'):
                        products.append(product_data)
                        logging.debug(f"✅ Добавлен товар: {product_data['name'][:50]}...")

                except Exception as e:
                    logging.debug(f"⚠️ Ошибка парсинга карточки товара: {e}")
                    continue

            logging.info(f"✅ Успешно спарсено товаров: {len(products)}")
            return products

        except Exception as e:
            logging.error(f"❌ Ошибка при парсинге товаров: {str(e)}")
            return []

    def click_shop_button(self):
        """Клик по кнопке 'Магазин' с конкретными селекторами из HTML"""
        try:
            logging.info("🛍️ Ищем кнопку 'Магазин'...")

            # ОСНОВНЫЕ СЕЛЕКТОРЫ ИЗ ТВОЕГО HTML
            shop_selectors = [
                "div.b5_4_4-a0[title='Магазин']",  # По title
                "div.b5_4_4-b0[title='Магазин']",  # По title внутреннего элемента
                "//div[@title='Магазин' and contains(@class, 'b5_4_4-b0')]",  # XPath
                "div.b5_4_4-a0",  # Основной контейнер
                "//div[contains(@class, 'b5_4_4-b0') and text()='Магазин']"  # По тексту
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
                            if element.is_displayed():
                                logging.info(f"🎯 Нашли кнопку магазина: '{element.text}'")

                                # Пробуем разные способы клика
                                try:
                                    element.click()
                                except:
                                    self.driver.execute_script("arguments[0].click();", element)

                                time.sleep(3)
                                logging.info("✅ Успешно кликнули на кнопку 'Магазин'")
                                return True

                        except Exception as e:
                            logging.debug(f"⚠️ Элемент не кликабелен: {e}")
                            continue

                except Exception as e:
                    logging.debug(f"⚠️ Ошибка с селектором {selector}: {e}")
                    continue

            logging.warning("⚠️ Не удалось найти и кликнуть на кнопку 'Магазин'")
            return False

        except Exception as e:
            logging.error(f"❌ Ошибка при клике на кнопку магазина: {e}")
            return False

    def extract_legal_info_from_modal(self):
        """Извлечение юридической информации из модального окна 'О магазине'"""
        try:
            logging.info("⚖️ Ищем и открываем модальное окно с информацией о магазине...")

            legal_data = {}

            # Селекторы для открытия модального окна (дополненные)
            modal_button_selectors = [
                "//div[contains(@class, 'b5_4_4-a0')]//div[contains(text(), 'Магазин')]",
                "//div[contains(@class, 'b5_4_4-b0') and contains(text(), 'Магазин')]",
                "div.b5_4_4-a0[style*='background: var']"
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

            # Ждем загрузки модального окна
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'b65_4_8-a')]"))
            )

            # Извлекаем все данные из модального окна
            modal_data = self.extract_all_modal_data()
            legal_data.update(modal_data)

            # Закрываем модальное окно
            self.close_modal()

            logging.info(f"✅ Юридическая информация извлечена: {legal_data}")
            return legal_data

        except Exception as e:
            logging.error(f"❌ Ошибка при получении юридической информации: {e}")
            return {}

    def extract_all_modal_data(self):
        """Извлечение всех данных из модального окна"""
        data = {}

        try:
            # 1. Название магазина (в заголовке модалки)
            try:
                title_element = self.driver.find_element(By.XPATH, "//span[@class='tsHeadline600Medium']")
                data['Название'] = title_element.text.strip()
            except:
                data['Название'] = ''

            # 2. Извлекаем данные из таблицы (Заказы, Работает с Ozon, Рейтинг и т.д.)
            cell_selectors = [
                "//div[contains(@class, 'b35_3_10-a9')]//span",  # Названия параметров
            ]

            # Ищем все строки с данными
            rows = self.driver.find_elements(By.XPATH, "//div[contains(@class, 'b35_3_10-a')]")

            for row in rows:
                try:
                    # Получаем название параметра
                    param_name_element = row.find_element(By.XPATH, ".//div[contains(@class, 'b35_3_10-a9')]//span")
                    param_name = param_name_element.text.strip()

                    # Получаем значение параметра
                    value_element = row.find_element(By.XPATH, ".//div[contains(@class, 'b5_4_4-b0')]")
                    param_value = value_element.text.strip()

                    # Сохраняем в соответствующие поля
                    if 'Заказов' in param_name:
                        data['Заказы'] = param_value
                    elif 'Работает с Ozon' in param_name:
                        data['Срок_регистрации'] = param_value
                    elif 'Средняя оценка товаров' in param_name:
                        data['Рейтинг'] = param_value
                    elif 'Количество отзывов' in param_name:
                        data['Отзывы'] = param_value

                except Exception as e:
                    continue

            # 3. Юридическая информация (ОГРН, адрес и т.д.)
            try:
                legal_text_elements = self.driver.find_elements(
                    By.XPATH, "//div[contains(@class, 'bq03_0_2-a')]//span[contains(@class, 'tsBody400Small')]"
                )

                legal_text = ""
                for element in legal_text_elements:
                    legal_text += element.text.strip() + "\n"

                # Извлекаем ОГРН (13 цифр)
                import re
                ogrn_match = re.search(r'\b\d{13}\b', legal_text)
                if ogrn_match:
                    data['ОГРН'] = ogrn_match.group()

                # Извлекаем название юрлица (первая строка)
                lines = legal_text.split('\n')
                for line in lines:
                    if 'ООО' in line or 'АО' in line or 'ИП' in line:
                        data['Название_юр_лица'] = line.strip()
                        break

                data['Юридический_адрес'] = legal_text.strip()

            except Exception as e:
                logging.warning(f"⚠️ Не удалось извлечь юридическую информацию: {e}")

        except Exception as e:
            logging.error(f"❌ Ошибка извлечения данных из модалки: {e}")

        return data

    def close_modal(self):
        """Закрытие модального окна"""
        try:
            close_selectors = [
                "button.b25_4_4-a0",  # Кнопка "Понятно"
                "button.b65_4_8-b1",  # Кнопка закрытия
                "//button[contains(., 'Понятно')]",
                "//button[contains(@class, 'b65_4_8-b1')]"
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
                        return True
                except:
                    continue

            # Если не нашли кнопку, пробуем кликнуть вне модалки
            try:
                overlay = self.driver.find_element(By.CSS_SELECTOR, "div.b65_4_8-a0")
                self.driver.execute_script("arguments[0].click();", overlay)
                time.sleep(1)
            except:
                pass

        except Exception as e:
            logging.warning(f"⚠️ Не удалось закрыть модальное окно: {e}")

    def extract_shop_info(self):
        """Извлечение информации со страницы магазина"""
        try:
            data = {}

            # Ожидаем загрузки контента
            time.sleep(random.uniform(3, 5))

            # Название магазина
            try:
                name_selectors = [
                    # Основной селектор из твоего HTML
                    ".bq03_0_2-a.bq03_0_2-a4.bq03_0_2-a5.h5n_19 span.tsHeadline600Large",
                    ".tsHeadline600Large",  # Резервный селектор
                    "h1",
                    ".seller-name",
                    "[data-widget='webSellerName']",
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
                            logging.info(f"✅ Найдено название магазина: {data['Название']}")
                            break
                    except:
                        continue
            except Exception as e:
                logging.warning(f"⚠️ Не удалось извлечь название магазина: {e}")
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

    def save_screenshot(self, seller_id, prefix=""):
        """Сохранение скриншота для отладки"""
        try:
            screenshot_path = f"/app/screenshots/{prefix}{seller_id}_{int(time.time())}.png"
            self.driver.save_screenshot(screenshot_path)
            logging.info(f"📸 Сохранен скриншот: {screenshot_path}")
            return screenshot_path
        except Exception as e:
            logging.warning(f"⚠️ Не удалось сохранить скриншот: {e}")
            return ""

    def parse_seller(self, seller_id):
        """Основной метод парсинга продавца"""
        url = f"https://www.ozon.ru/seller/{seller_id}"

        seller_data = {'URL': url}
        html_paths = []  # Список для хранения путей к HTML файлам

        try:
            # Загружаем страницу
            self.driver.get(url)
            time.sleep(random.uniform(5, 8))

            # 🔥 СОХРАНЯЕМ HTML СРАЗУ ПОСЛЕ ЗАГРУЗКИ
            main_html_path = self.save_html_page(seller_id, "main_")
            html_paths.append(main_html_path)

            # 🔥 СОХРАНЯЕМ СКРИНШОТ
            self.save_screenshot(seller_id, "loaded_")

            # Парсим товары с главной страницы
            try:
                products = self.extract_products_from_main_page()
                seller_data['Кол-во_товаров_на_странице'] = len(products)
                seller_data['Товары_JSON'] = json.dumps(products, ensure_ascii=False)
            except Exception as e:
                logging.error(f"❌ Ошибка при парсинге товаров: {e}")
                # Сохраняем скриншот при ошибке парсинга товаров
                self.save_screenshot(seller_id, "products_error_")
                seller_data['Кол-во_товаров_на_странице'] = 0
                seller_data['Товары_JSON'] = '[]'

            # Пробуем открыть и спарсить модальное окно
            try:
                if self.click_shop_button():
                    # 🔥 СОХРАНЯЕМ HTML ПОСЛЕ КЛИКА НА МАГАЗИН
                    shop_html_path = self.save_html_page(seller_id, "shop_")
                    html_paths.append(shop_html_path)

                    legal_info = self.extract_legal_info_from_modal()
                    seller_data.update(legal_info)
                else:
                    logging.warning(f"⚠️ Не удалось открыть модалку для продавца {seller_id}")
                    # Сохраняем скриншот при неудачном клике
                    self.save_screenshot(seller_id, "shop_button_error_")
            except Exception as e:
                logging.error(f"❌ Ошибка при работе с модальным окном: {e}")
                self.save_screenshot(seller_id, "modal_error_")

            # 🔥 СОХРАНЯЕМ ВСЕ ПУТИ К HTML В ДАННЫЕ
            seller_data['Html_путь'] = "; ".join(html_paths)

            # Сохраняем данные
            self.save_to_csv(seller_data)
            logging.info(f"✅ Успешно обработан продавец {seller_id}")
            return seller_data

        except Exception as e:
            logging.error(f"❌ Критическая ошибка парсинга продавца {seller_id}: {e}")
            # 🔥 СОХРАНЯЕМ СКРИНШОТ И HTML ПРИ ЛЮБОЙ КРИТИЧЕСКОЙ ОШИБКЕ
            self.save_screenshot(seller_id, "critical_error_")
            error_html_path = self.save_html_page(seller_id, "error_")
            seller_data['Html_путь'] = error_html_path

            # Сохраняем то, что успели собрать
            try:
                self.save_to_csv(seller_data)
            except:
                pass

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