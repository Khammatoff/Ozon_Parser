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
from concurrent.futures import ThreadPoolExecutor
import threading

# ПЕРЕМЕСТИТЕ ВСЕ ИНИЦИАЛИЗАЦИЮ ПОСЛЕ ИМПОРТОВ
executor = ThreadPoolExecutor(max_workers=5)
lock = threading.Lock()

# Создаём папки в контейнере
os.makedirs("/app/logs", exist_ok=True)
os.makedirs("/app/data", exist_ok=True)
os.makedirs("/app/screenshots", exist_ok=True)
os.makedirs("/app/html", exist_ok=True)

# Настройка логирования ДО определения классов
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/parser.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

def parse_task(seller_id: str):
    parser = None
    try:
        parser = OzonSellerParser()
        result = parser.parse_seller(seller_id)
        if result:
            logging.info(f"✅ Успешно обработан продавец {seller_id}")
        else:
            logging.warning(f"⚠️ Не удалось обработать продавца {seller_id}")
        time.sleep(random.uniform(10, 20))
    except Exception as e:
        logging.error(f"❌ Критическая ошибка при обработке {seller_id}: {e}", exc_info=True)
    finally:
        if parser:
            parser.close()


class OzonSellerParser:
    def __init__(self):
        self.instance_id = os.getenv('HOSTNAME', f"parser-{random.randint(1000, 9999)}")
        self.request_count = 0
        self.driver = None
        self.wait = None
        self.current_proxy = None
        self.proxy_list = []
        self.proxy_rotation_count = 0 #int(os.getenv('PROXY_ROTATION_COUNT', 3))
        self.proxy_timeout = int(os.getenv('PROXY_ROTATION_TIMEOUT', 30))
        self.screenshot_counter = 0  # Счетчик скриншотов

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

    def take_screenshot(self, prefix=""):
        """Сохранение скриншота с автоматической нумерацией"""
        try:
            self.screenshot_counter += 1
            screenshot_path = f"/app/screenshots/{self.instance_id}_{self.screenshot_counter:03d}_{prefix}_{int(time.time())}.png"
            self.driver.save_screenshot(screenshot_path)
            logging.info(f"📸 Сохранен скриншот: {screenshot_path}")
            return screenshot_path
        except Exception as e:
            logging.warning(f"⚠️ Не удалось сохранить скриншот: {e}")
            return ""

    def create_proxy_auth_extension(self, proxy_host, proxy_port, proxy_username, proxy_password):
        """Создание расширения для аутентификации прокси"""
        try:
            manifest_json = """
            {
                "version": "1.0.0",
                "manifest_version": 2,
                "name": "Chrome Proxy",
                "permissions": [
                    "proxy",
                    "tabs",
                    "unlimitedStorage",
                    "storage",
                    "<all_urls>",
                    "webRequest",
                    "webRequestBlocking"
                ],
                "background": {
                    "scripts": ["background.js"]
                },
                "minimum_chrome_version":"22.0.0"
            }
            """

            background_js = """
            var config = {
                mode: "fixed_servers",
                rules: {
                    singleProxy: {
                        scheme: "http",
                        host: "%s",
                        port: parseInt(%s)
                    },
                    bypassList: ["localhost"]
                }
            };

            chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

            function callbackFn(details) {
                return {
                    authCredentials: {
                        username: "%s",
                        password: "%s"
                    }
                };
            }

            chrome.webRequest.onAuthRequired.addListener(
                callbackFn,
                {urls: ["<all_urls>"]},
                ['blocking']
            );
            """ % (proxy_host, proxy_port, proxy_username, proxy_password)

            import tempfile
            import zipfile
            import os

            proxy_dir = tempfile.mkdtemp()

            with open(os.path.join(proxy_dir, "manifest.json"), "w") as f:
                f.write(manifest_json)

            with open(os.path.join(proxy_dir, "background.js"), "w") as f:
                f.write(background_js)

            proxy_ext = os.path.join(proxy_dir, "proxy_auth.zip")

            with zipfile.ZipFile(proxy_ext, 'w') as zp:
                zp.write(os.path.join(proxy_dir, "manifest.json"), "manifest.json")
                zp.write(os.path.join(proxy_dir, "background.js"), "background.js")

            logging.info(f"✅ Создано расширение для прокси: {proxy_host}:{proxy_port}")
            return proxy_ext

        except Exception as e:
            logging.error(f"❌ Ошибка создания расширения прокси: {e}")
            return None

    def rotate_proxy(self):
        """Ротация прокси и перезапуск драйвера"""
        if not self.proxy_list or len(self.proxy_list) <= 1:
            logging.info("🔄 Нет доступных прокси для ротации")
            return False

        try:
            # Очищаем временные файлы расширений прокси
            try:
                import glob
                temp_dirs = glob.glob("/tmp/tmp*")
                for temp_dir in temp_dirs:
                    try:
                        if os.path.exists(temp_dir) and "proxy_auth" in temp_dir:
                            shutil.rmtree(temp_dir, ignore_errors=True)
                            logging.debug(f"🧹 Очищена временная директория: {temp_dir}")
                    except Exception as e:
                        logging.debug(f"⚠️ Не удалось очистить {temp_dir}: {e}")
            except Exception as e:
                logging.warning(f"⚠️ Ошибка при очистке временных файлов: {e}")

            # Переходим к следующему прокси по кругу
            if not hasattr(self, 'current_proxy_index'):
                self.current_proxy_index = 0

            old_proxy_index = self.current_proxy_index
            self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxy_list)
            new_proxy = self.proxy_list[self.current_proxy_index]

            old_proxy = self.current_proxy
            self.current_proxy = new_proxy
            self.requests_per_proxy = 0  # Сбрасываем счетчик

            logging.info(
                f"🔄 Ротируем прокси [{old_proxy_index + 1}→{self.current_proxy_index + 1}/{len(self.proxy_list)}]: {old_proxy} -> {new_proxy}")

            # Перезапускаем драйвер с новым прокси
            if self.driver:
                try:
                    self.driver.quit()
                    time.sleep(3)  # Увеличиваем задержку для полного закрытия
                except Exception as e:
                    logging.warning(f"⚠️ Ошибка при закрытии драйвера: {e}")

            # Переинициализируем драйвер с новым прокси
            try:
                self.setup_driver()
                self.wait = WebDriverWait(self.driver, 15)

                # Проверяем работоспособность нового прокси
                test_url = "https://httpbin.org/ip"
                self.driver.get(test_url)
                time.sleep(2)

                logging.info("✅ Прокси успешно сменен и протестирован")
                return True

            except Exception as e:
                logging.error(f"❌ Ошибка инициализации драйвера с новым прокси: {e}")
                # Пробуем вернуться к предыдущему прокси
                try:
                    self.current_proxy_index = old_proxy_index
                    self.current_proxy = old_proxy
                    if self.driver:
                        self.driver.quit()
                    self.setup_driver()
                    self.wait = WebDriverWait(self.driver, 15)
                    logging.info("🔄 Возврат к предыдущему прокси")
                except:
                    logging.error("❌ Критическая ошибка - не удалось восстановить драйвер")
                return False

        except Exception as e:
            logging.error(f"❌ Ошибка ротации прокси: {e}")
            # Пробуем восстановить работу с текущим прокси
            try:
                if self.driver:
                    self.driver.quit()
                self.setup_driver()
                self.wait = WebDriverWait(self.driver, 15)
                logging.info("🔄 Восстановление драйвера после ошибки ротации")
            except Exception as restore_error:
                logging.error(f"❌ Не удалось восстановить драйвер: {restore_error}")
            return False

    def setup_driver(self):
        """Настройка Chrome для работы в Docker"""
        from webdriver_manager.chrome import ChromeDriverManager
        from webdriver_manager.core.os_manager import ChromeType

        chrome_options = Options()

        # Критические опции для Docker
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-setuid-sandbox")
        chrome_options.add_argument("--window-size=1920,1080")

        # Улучшенные stealth опции
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-features=UserAgentClientHint")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        # User-Agent
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        ]
        chrome_options.add_argument(f"--user-agent={random.choice(user_agents)}")

        # Уникальный профиль
        chrome_options.add_argument(f"--user-data-dir={self.chrome_temp_dir}")

        try:
            # АВТОМАТИЧЕСКАЯ УСТАНОВКА ChromeDriver
            service = Service(ChromeDriverManager(chrome_type=ChromeType.GOOGLE).install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)

            # Применяем stealth
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
            logging.info("✅ Драйвер успешно инициализирован")

        except Exception as e:
            logging.error(f"❌ Ошибка создания драйвера: {e}", exc_info=True)
            raise

    def init_csv(self):
        """Инициализация CSV файла"""
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
        """Сохранение данных в CSV"""
        try:
            with open(self.csv_file, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow([
                    data.get('URL', ''),
                    data.get('Название', ''),
                    data.get('Html_путь', ''),
                    data.get('ОГРН', ''),
                    data.get('ИНН', ''),
                    data.get('Название_юр_лица', ''),
                    data.get('Отзывы', ''),
                    data.get('Рейтинг', ''),
                    data.get('Срок_регистрации', ''),
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
        """Сохранение HTML страницы"""
        try:
            html_path = f"/app/html/{prefix}{seller_id}_{int(time.time())}.html"
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(self.driver.page_source)
            return html_path
        except Exception as e:
            logging.warning(f"⚠️ Не удалось сохранить HTML: {e}")
            return ""

    def extract_products_from_main_page(self):
        """Парсинг товаров с главной страницы"""
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

    def click_shop_button(self) -> bool:
        """Клик по кнопке 'Магазин' на главной странице - УПРОЩЕННАЯ ВЕРСИЯ"""
        try:
            logging.info("🛍️ Ищем кнопку 'Магазин'...")
            self.take_screenshot("before_shop_button")

            # Основные селекторы для кнопки "Магазин"
            shop_selectors = [
                "//div[@title='Магазин']",
                "//div[contains(@class, 'b5_4_7-b0') and contains(text(), 'Магазин')]",
                "//div[contains(text(), 'Магазин') and contains(@class, 'b5_4_7')]",
            ]

            for selector in shop_selectors:
                try:
                    elements = self.driver.find_elements(By.XPATH, selector)
                    logging.info(f"🔍 Поиск по '{selector}': найдено {len(elements)}")

                    for el in elements:
                        if el.is_displayed() and el.is_enabled():
                            logging.info(f"🎯 Нашли кнопку: {selector}")

                            # Прокручиваем и кликаем
                            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", el)
                            time.sleep(1)

                            try:
                                el.click()
                            except:
                                self.driver.execute_script("arguments[0].click();", el)

                            # Ждем открытия модалки
                            time.sleep(3)
                            self.take_screenshot("after_shop_click")

                            # Проверяем, открылась ли модалка
                            if self.check_modal_opened():
                                logging.info("✅ Модальное окно успешно открыто")
                                return True
                            else:
                                logging.warning("⚠️ Кнопка нажата, но модалка не открылась")
                                return False

                except Exception as e:
                    logging.debug(f"⚠️ Ошибка с селектором {selector}: {e}")
                    continue

            logging.warning("❌ Не удалось найти кнопку 'Магазин'")
            return False

        except Exception as e:
            logging.error(f"❌ Ошибка клика по кнопке магазина: {e}")
            return False

    def check_modal_opened(self) -> bool:
        """Проверка, что модальное окно открылось"""
        try:
            modal_indicators = [
                "div[data-widget='modalLayout']",
                ".vue-portal-target",
                "//div[contains(text(), 'О магазине')]"
            ]

            for selector in modal_indicators:
                try:
                    if selector.startswith("//"):
                        elements = self.driver.find_elements(By.XPATH, selector)
                    else:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)

                    if elements and any(el.is_displayed() for el in elements):
                        logging.info(f"✅ Модальное окно обнаружено: {selector}")
                        return True
                except:
                    continue

            return False
        except Exception as e:
            logging.debug(f"⚠️ Ошибка проверки модалки: {e}")
            return False

    def extract_legal_info_from_modal(self):
        """Извлечение всей информации из модального окна - ОБЪЕДИНЕННЫЙ МЕТОД"""
        try:
            logging.info("⚖️ Извлекаем данные из модального окна...")

            data = {}

            # Ждем загрузки модального окна
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-widget='modalLayout']"))
                )
                logging.info("✅ Модальное окно загружено")
            except:
                logging.warning("⚠️ Не удалось дождаться загрузки модального окна")
                return data

            self.take_screenshot("modal_content")

            # 1. Парсим метрики магазина
            metrics_data = self.extract_metrics_from_modal()
            data.update(metrics_data)

            # 2. Парсим юридическую информацию
            legal_data = self.extract_legal_text_from_modal()
            data.update(legal_data)

            logging.info(f"✅ Все данные из модалки извлечены: {data}")
            return data

        except Exception as e:
            logging.error(f"❌ Ошибка при извлечении информации из модалки: {e}")
            return {}

    def extract_metrics_from_modal(self):
        """Извлечение метрик из модального окна"""
        data = {}

        try:
            # Ищем все строки с метриками
            metric_rows = self.driver.find_elements(
                By.CSS_SELECTOR,
                "div[data-widget='cellList'] .b35_3_13-a"
            )

            logging.info(f"📊 Найдено строк с метриками: {len(metric_rows)}")

            for row in metric_rows:
                try:
                    # Получаем название метрики
                    name_elem = row.find_elements(By.CSS_SELECTOR, ".b35_3_13-a9")
                    if not name_elem:
                        continue

                    metric_name = name_elem[0].text.strip()
                    if not metric_name:
                        continue

                    # Получаем значение метрики
                    value_elem = row.find_elements(By.CSS_SELECTOR, ".b5_4_7-b0")
                    value = value_elem[0].text.strip() if value_elem else ""
                    value_title = value_elem[0].get_attribute('title') if value_elem else ""
                    final_value = value or value_title

                    logging.info(f"📊 Метрика: '{metric_name}' = '{final_value}'")

                    # Сопоставляем с нашими полями
                    if any(word in metric_name.lower() for word in ['заказ', 'заказов']):
                        data['Заказы'] = final_value
                    elif any(word in metric_name.lower() for word in ['работает', 'ozon']):
                        data['Срок_регистрации'] = final_value
                    elif any(word in metric_name.lower() for word in ['оценк', 'рейтинг', 'средняя']):
                        data['Рейтинг'] = final_value
                    elif any(word in metric_name.lower() for word in ['отзыв', 'количество']):
                        data['Отзывы'] = final_value

                except Exception as e:
                    logging.debug(f"⚠️ Ошибка парсинга строки метрики: {e}")
                    continue

        except Exception as e:
            logging.warning(f"⚠️ Ошибка при парсинге метрик: {e}")

        return data

    def extract_legal_text_from_modal(self):
        """Извлечение юридической информации из модального окна"""
        data = {}

        try:
            # Ищем блок с юридической информацией
            legal_selectors = [
                "div[data-widget='textBlock'] .tsBody400Small",
                ".d0q_11 .tsBody400Small",
                "//span[@class='tsBody400Small']"
            ]

            legal_text = ""
            for selector in legal_selectors:
                try:
                    if selector.startswith("//"):
                        elements = self.driver.find_elements(By.XPATH, selector)
                    else:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)

                    for element in elements:
                        if element.is_displayed():
                            text = element.text.strip()
                            if text and len(text) > 10:
                                legal_text = text
                                logging.info(f"✅ Найден юридический текст: {text}")
                                break

                    if legal_text:
                        break

                except Exception as e:
                    logging.debug(f"⚠️ Ошибка с селектором {selector}: {e}")
                    continue

            # Парсим юридическую информацию
            if legal_text:
                lines = legal_text.split('\n')

                # Название юрлица (первая строка)
                if lines:
                    data['Название_юр_лица'] = lines[0].strip()
                    logging.info(f"✅ Название юрлица: {data['Название_юр_лица']}")

                # Ищем ОГРН и ИНН
                import re

                # ОГРН (13 цифр)
                ogrn_match = re.search(r'\b\d{13}\b', legal_text)
                if ogrn_match:
                    data['ОГРН'] = ogrn_match.group()
                else:
                    ogrn_alt = re.search(r'ОГРН\s*[:\-]?\s*(\d{13})', legal_text, re.IGNORECASE)
                    if ogrn_alt:
                        data['ОГРН'] = ogrn_alt.group(1)

                # ИНН (10 или 12 цифр)
                inn_match = re.search(r'\b\d{10,12}\b', legal_text)
                if inn_match:
                    data['ИНН'] = inn_match.group()
                else:
                    inn_alt = re.search(r'ИНН\s*[:\-]?\s*(\d{10,12})', legal_text, re.IGNORECASE)
                    if inn_alt:
                        data['ИНН'] = inn_alt.group(1)

                # Авто-определение если не нашли по шаблонам
                if not data.get('ОГРН') and not data.get('ИНН'):
                    numbers = re.findall(r'\b\d{10,13}\b', legal_text)
                    for num in numbers:
                        if len(num) == 13 and not data.get('ОГРН'):
                            data['ОГРН'] = num
                        elif len(num) in [10, 12] and not data.get('ИНН'):
                            data['ИНН'] = num

                logging.info(f"✅ Юридические данные: ОГРН={data.get('ОГРН')}, ИНН={data.get('ИНН')}")

        except Exception as e:
            logging.warning(f"⚠️ Не удалось извлечь юридическую информацию: {e}")

        return data

    def close_modal(self):
        """Закрытие модального окна - УПРОЩЕННАЯ ВЕРСИЯ"""
        try:
            logging.info("🔒 Закрываем модальное окно...")
            self.take_screenshot("before_modal_close")

            # Основные селекторы для закрытия
            close_selectors = [
                "//button[contains(., 'Понятно')]",
                "button[data-widget='modalClose']",
                ".b65_4_11-b1",  # Крестик
                "//button[contains(@class, 'b25_5_1-a0')]"
            ]

            for selector in close_selectors:
                try:
                    if selector.startswith("//"):
                        elements = self.driver.find_elements(By.XPATH, selector)
                    else:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)

                    for btn in elements:
                        if btn.is_displayed() and btn.is_enabled():
                            self.driver.execute_script("arguments[0].click();", btn)
                            time.sleep(2)
                            self.take_screenshot("after_modal_close")
                            logging.info("✅ Модальное окно закрыто")
                            return True
                except:
                    continue

            # Альтернатива: клик по overlay
            try:
                overlay = self.driver.find_element(By.CSS_SELECTOR, ".b65_4_11-a0")
                self.driver.execute_script("arguments[0].click();", overlay)
                time.sleep(1)
                logging.info("✅ Модальное окно закрыто через overlay")
                return True
            except:
                pass

            logging.warning("⚠️ Не удалось закрыть модальное окно")
            return False

        except Exception as e:
            logging.warning(f"⚠️ Ошибка при закрытии модального окна: {e}")
            return False

    def extract_shop_info(self):
        """Извлечение ТОЛЬКО названия магазина с главной страницы"""
        try:
            data = {}

            # ПОИСК НАЗВАНИЯ МАГАЗИНА
            try:
                main_page_name_selectors = [
                    "h1.seller-name",
                    ".seller-title",
                    "[data-widget='webSellerName']",
                    "//h1[contains(@class, 'seller')]",
                    "//div[contains(@class, 'seller-header')]//h1",
                    "//span[contains(@class, 'tsHeadline600Large')]",
                    ".bq03_0_2-a.bq03_0_2-a4.bq03_0_2-a5.h5n_19 span.tsHeadline600Large",
                    "//div[contains(@class, 'h5n_19')]//span"
                ]

                shop_name_found = False
                for selector in main_page_name_selectors:
                    try:
                        if selector.startswith("//"):
                            elements = self.driver.find_elements(By.XPATH, selector)
                        else:
                            elements = self.driver.find_elements(By.CSS_SELECTOR, selector)

                        for element in elements:
                            if element.is_displayed() and element.text.strip():
                                shop_name = element.text.strip()
                                if len(shop_name) > 2:
                                    data['Название'] = shop_name
                                    logging.info(f"✅ Название магазина найдено на главной странице: {data['Название']}")
                                    shop_name_found = True
                                    break

                        if shop_name_found:
                            break

                    except Exception as e:
                        continue

                if not shop_name_found:
                    logging.warning("⚠️ Не удалось извлечь название магазина с главной страницы")
                    data['Название'] = ''

            except Exception as e:
                logging.error(f"❌ Ошибка при извлечении названия магазина: {e}")
                data['Название'] = ''

            return data

        except Exception as e:
            logging.error(f"❌ Ошибка извлечения информации о магазине: {e}")
            return {'Название': ''}

    def parse_seller(self, seller_id):
        """Парсинг данных продавца - УЛУЧШЕННАЯ ВЕРСИЯ"""
        url = f"https://www.ozon.ru/seller/{seller_id}"
        seller_data = {'URL': url}
        html_paths = []
        max_attempts = 3
        attempt = 1

        while attempt <= max_attempts:
            try:
                logging.info(f"🚀 Попытка {attempt}/{max_attempts} для продавца {seller_id}")
                self.take_screenshot(f"start_attempt_{attempt}")

                # Загрузка страницы
                if not self.load_seller_page(url, attempt):
                    if self.retry_after_blocking(seller_id, attempt, max_attempts):
                        attempt += 1
                        continue
                    else:
                        break

                # Основной парсинг
                parsing_success = self.parse_seller_data(seller_id, seller_data, html_paths)

                if parsing_success:
                    # Сохраняем результаты
                    seller_data['Html_путь'] = "; ".join(html_paths)
                    if self.save_to_csv(seller_data):
                        logging.info(f"✅ Успешно обработан продавец {seller_id}")
                        return seller_data
                    else:
                        logging.error(f"❌ Ошибка сохранения данных для {seller_id}")
                        return None
                else:
                    logging.warning(f"⚠️ Неполные данные для {seller_id}, попытка {attempt}")

                # Если дошли сюда, пробуем снова
                if attempt < max_attempts:
                    if self.retry_after_error(seller_id, attempt):
                        attempt += 1
                        continue
                    else:
                        break
                else:
                    logging.error(f"❌ Все попытки провалились для {seller_id}")
                    break

            except Exception as e:
                logging.error(f"❌ Критическая ошибка на попытке {attempt}: {e}")
                if not self.handle_critical_error(seller_id, attempt, max_attempts):
                    break
                attempt += 1

        # Сохраняем то, что удалось собрать
        return self.finalize_parsing(seller_data, html_paths)

    def load_seller_page(self, url, attempt):
        """Загрузка страницы продавца"""
        try:
            self.driver.set_page_load_timeout(30)
            time.sleep(random.uniform(2, 4))

            logging.info(f"🌐 Загружаем страницу: {url}")
            self.driver.get(url)

            # Проверка блокировки
            if self.check_and_handle_blocking():
                logging.warning(f"🛑 Обнаружена блокировка на попытке {attempt}")
                return False

            time.sleep(random.uniform(2, 4))
            self.random_mouse_movements()
            return True

        except Exception as e:
            logging.error(f"❌ Ошибка загрузки страницы: {e}")
            return False

    def parse_seller_data(self, seller_id, seller_data, html_paths):
        """Основной парсинг данных продавца"""
        try:
            # Сохраняем основную HTML страницу
            main_html_path = self.save_html_page(seller_id, "main_")
            html_paths.append(main_html_path)
            self.take_screenshot("page_loaded")

            # 1. Название магазина
            if not self.parse_shop_name(seller_data):
                logging.warning("⚠️ Не удалось извлечь название магазина")

            # 2. Товары на главной странице
            if not self.parse_products(seller_data):
                logging.warning("⚠️ Не удалось извлечь товары")

            # 3. Юридическая информация из модального окна
            if not self.parse_legal_info(seller_id, seller_data, html_paths):
                logging.warning("⚠️ Не удалось извлечь юридическую информацию")

            logging.info(f"✅ Основные данные извлечены для {seller_id}")
            return True

        except Exception as e:
            logging.error(f"❌ Ошибка парсинга данных: {e}")
            return False

    def parse_shop_name(self, seller_data):
        """Парсинг названия магазина"""
        try:
            shop_name_data = self.extract_shop_info()
            seller_data.update(shop_name_data)

            name = seller_data.get('Название', '')
            if name:
                logging.info(f"✅ Название магазина: {name}")
                return True
            else:
                seller_data['Название'] = ''
                return False

        except Exception as e:
            logging.error(f"❌ Ошибка извлечения названия: {e}")
            seller_data['Название'] = ''
            return False

    def parse_products(self, seller_data):
        """Парсинг товаров"""
        try:
            products = self.extract_products_from_main_page()
            seller_data['Кол-во_товаров_на_странице'] = len(products)
            seller_data['Товары_JSON'] = json.dumps(products, ensure_ascii=False, indent=2)

            logging.info(f"✅ Спарсено товаров: {len(products)}")
            return len(products) > 0

        except Exception as e:
            logging.error(f"❌ Ошибка парсинга товаров: {e}")
            seller_data['Кол-во_товаров_на_странице'] = 0
            seller_data['Товары_JSON'] = '[]'
            return False

    def parse_legal_info(self, seller_id, seller_data, html_paths):
        """Парсинг юридической информации"""
        try:
            if not self.click_shop_button():
                logging.warning("⚠️ Не удалось открыть модалку")
                return False

            # Сохраняем HTML модального окна
            shop_html_path = self.save_html_page(seller_id, "shop_")
            html_paths.append(shop_html_path)

            # Извлекаем данные из модалки
            legal_info = self.extract_legal_info_from_modal()
            seller_data.update(legal_info)

            # Закрываем модалку
            self.close_modal()

            # Проверяем, что получили хоть какие-то данные
            legal_fields = ['ОГРН', 'ИНН', 'Название_юр_лица', 'Отзывы', 'Рейтинг', 'Срок_регистрации']
            extracted_fields = [field for field in legal_fields if seller_data.get(field)]

            logging.info(f"✅ Извлечены юридические данные: {len(extracted_fields)} полей")
            return len(extracted_fields) > 0

        except Exception as e:
            logging.error(f"❌ Ошибка работы с модальным окном: {e}")
            return False

    def retry_after_blocking(self, seller_id, attempt, max_attempts):
        """Обработка блокировки и повторная попытка"""
        if attempt < max_attempts:
            delay = random.uniform(20, 40)
            logging.info(f"⏳ Задержка {delay:.1f} сек перед повторной попыткой")
            time.sleep(delay)
            return True
        else:
            logging.error(f"❌ Все {max_attempts} попыток заблокированы для {seller_id}")
            return False

    def retry_after_error(self, seller_id, attempt):
        """Повторная попытка после ошибки"""
        delay = random.uniform(10, 20)
        logging.info(f"⏳ Повторная попытка через {delay:.1f} сек")
        time.sleep(delay)
        return True

    def handle_critical_error(self, seller_id, attempt, max_attempts):
        """Обработка критических ошибок"""
        self.take_screenshot(f"critical_error_attempt_{attempt}")

        if attempt < max_attempts:
            delay = random.uniform(15, 25)
            logging.info(f"⏳ Критическая ошибка, повтор через {delay:.1f} сек")
            time.sleep(delay)
            return True
        else:
            logging.error(f"❌ Критические ошибки на всех попытках для {seller_id}")
            return False

    def finalize_parsing(self, seller_data, html_paths):
        """Финальная обработка результатов"""
        try:
            # Добавляем пути к HTML если они есть
            if html_paths:
                seller_data['Html_путь'] = "; ".join(html_paths)

            # Сохраняем то, что есть
            if seller_data:
                self.save_to_csv(seller_data)
                logging.info("💾 Данные сохранены (частичные)")
                return seller_data
            else:
                logging.error("❌ Не удалось собрать никаких данных")
                return None

        except Exception as e:
            logging.error(f"❌ Ошибка финализации: {e}")
            return None

    def check_and_handle_blocking(self) -> bool:
        """Улучшенная проверка блокировок"""
        try:
            # Проверяем различные индикаторы блокировки
            blocking_indicators = [
                "//h1[contains(text(), 'Доступ ограничен')]",
                "//h1[contains(text(), 'Ой!')]",
                "//title[contains(text(), 'Доступ')]",
                "//div[contains(text(), 'проверку')]",
                "//div[contains(text(), 'безопасност')]",
                "iframe[src*='captcha']",
                "//input[@name='captcha']"
            ]

            for indicator in blocking_indicators:
                try:
                    elements = self.driver.find_elements(By.XPATH, indicator)
                    if elements:
                        logging.warning(f"🛑 Обнаружена блокировка: {indicator}")
                        self.take_screenshot("blocked")
                        return True
                except:
                    continue

            return False

        except Exception as e:
            logging.debug(f"⚠️ Ошибка проверки блокировки: {e}")
            return False

    def random_mouse_movements(self):
        """Случайные движения мышью для имитации человека"""
        try:
            # Получаем размеры окна
            window_size = self.driver.get_window_size()
            width = window_size['width']
            height = window_size['height']

            # Генерируем случайные координаты
            x1 = random.randint(100, width - 100)
            y1 = random.randint(100, height - 100)
            x2 = random.randint(100, width - 100)
            y2 = random.randint(100, height - 100)

            # Создаем ActionChains для плавного движения
            actions = webdriver.ActionChains(self.driver)

            # Перемещаем мышью по случайной траектории
            actions.move_by_offset(x1, y1).pause(random.uniform(0.1, 0.3))
            actions.move_by_offset(x2 - x1, y2 - y1).pause(random.uniform(0.1, 0.3))
            actions.perform()

            # Случайный скролл
            scroll_pixels = random.randint(200, 800)
            self.driver.execute_script(f"window.scrollBy(0, {scroll_pixels});")
            time.sleep(random.uniform(0.5, 1.5))

        except Exception as e:
            logging.debug(f"⚠️ Ошибка при движении мышью: {e}")

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

    def task_wrapper():
        # Добавляем случайную задержку перед началом обработки
        delay = random.uniform(1, 10)
        logging.info(f"⏳ Случайная задержка перед обработкой {seller_id}: {delay:.2f} сек")
        time.sleep(delay)

        parse_task(seller_id)
        with lock:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    executor.submit(task_wrapper)


def start_consumer():
    while True:
        try:
            connection_params = pika.ConnectionParameters(
                host=os.getenv('RABBITMQ_HOST', 'rabbitmq'),
                port=5672,
                credentials=pika.PlainCredentials(
                    os.getenv('RABBITMQ_USER', 'guest'),
                    os.getenv('RABBITMQ_PASS', 'guest')
                ),
                heartbeat=600,
                blocked_connection_timeout=300,
                connection_attempts=10,
                retry_delay=5
            )

            connection = pika.BlockingConnection(connection_params)
            channel = connection.channel()

            # ИСПРАВЛЕННОЕ ОБЪЯВЛЕНИЕ ОЧЕРЕДИ
            channel.queue_declare(
                queue='seller_ids',
                durable=True,
                passive=True  # Только проверяем существование, не создаем заново
            )

            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(
                queue='seller_ids',
                on_message_callback=callback,
                auto_ack=False
            )

            logging.info("🔄 Ожидаем сообщения из RabbitMQ...")
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            logging.error(f"❌ Ошибка подключения к RabbitMQ: {e}")
            time.sleep(10)
        except Exception as e:
            logging.error(f"❌ Неожиданная ошибка: {e}", exc_info=True)
            time.sleep(10)


if __name__ == "__main__":
    start_consumer()