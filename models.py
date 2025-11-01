import asyncio
import hashlib
import json
from dataclasses import dataclass
from typing import Optional, Set, Dict, List
from decimal import Decimal, ROUND_HALF_UP

import aiohttp
from loguru import logger

from core.base import BaseParser
from tools import get_num, is_english


@dataclass(frozen=True)
class Product:
    """Дата-класс для представления товара."""
    title: str
    short_description: str
    full_description: str
    code_digis: int
    article: str
    price: int
    posters: List[str]
    characteristics: Dict[str, str]
    specification: Dict[str, str]
    documentation: List[str]
    accessories: List[str]
    brand: str
    
    def as_dict(self) -> Dict[str, str]:
        return {
            "Название": self.title,
            "Короткое описание": self.short_description,
            "Полное описание": self.full_description,
            "Код Digis": self.code_digis,
            "Артикул": self.article,
            "Цена": self.price,
            "Изображения": self.posters,
            "Характеристики": self.characteristics,
            "Спецификации": self.specification,
            "Документация": self.documentation,
            "Аксессуары": self.accessories,
            "Бренд": self.brand
        }

    def as_flat_dict(self) -> Dict[str, str]:
        """
        Преобразует продукт в плоский словарь с русскими названиями полей.
        Все вложенные структуры преобразуются в строки.
        
        Returns:
            Плоский словарь с строковыми значениями
            
        Examples:
            >>> product.as_flat_dict()
            {
                "Название": "Смартфон Apple iPhone 15",
                "Код Digis": "12345",
                "Характеристики": "Цвет: черный|| Память: 128ГБ",
                ...
            }
        """
        return {
            "Название": self.title,
            "Короткое описание": self.short_description if self.short_description else "Нет",
            "Полное описание": self.full_description if self.full_description else "Нет",
            "Код Digis": str(self.code_digis),
            "Артикул": self.article,
            "Цена": self._format_price(self.price),
            "Изображение": '|| '.join(self.posters) ,
            "Спецификации": self._dict_to_string(self.specification) if self.specification else "Нет",
            "Документация": '|| '.join(self.documentation) if self.specification else "Нет",
            "Аксессуары": "|| ".join(self.accessories) if self.accessories else "Нет",
            "Бренд": self.brand
        }
        
    def _format_price(self, price: int) -> str:
        """
        Форматирует цену в читаемый вид с разделителями.
        
        Args:
            price: Цена в рублях
            
        Returns:
            Отформатированная строка цены
        """
        return f"{price:,}".replace(",", " ") + " ₽"
    
    def _dict_to_string(self, dictionary: Dict[str, str]) -> str:
        """
        Преобразует словарь в читаемую строку.
        
        Args:
            dictionary: Словарь для преобразования
            
        Returns:
            Строковое представление словаря
        """
        if not dictionary:
            return "Нет данных"
        
        return "; ".join([f"{key}: {value}" for key, value in dictionary.items()])
    
    def __hash__(self) -> int:
        """
        Эффективное вычисление хэша на основе JSON-сериализации.
        
        Returns:
            Уникальный хэш для идентификации продукта
        """
        # Создаем словарь с отсортированными данными для консистентности
        data_dict = {
            'title': self.title,
            'short_description': self.short_description,
            'code_digis': self.code_digis,
            'article': self.article,
            'price': self.price,
            'poster': self.posters,
            'brand': self.brand,
            'characteristics': self._sort_dict(self.characteristics),
            'specification': self._sort_dict(self.specification),
            'documentation': self._sort_dict(self.documentation),
            'accessories': sorted(self.accessories)
        }
        json_string = json.dumps(data_dict, sort_keys=True, ensure_ascii=False)
        
        sha256_hash = hashlib.sha256(json_string.encode('utf-8'))
        return int(sha256_hash.hexdigest()[:16], 16)  # Берем первые 16 символов
    
    def _sort_dict(self, dictionary: Dict[str, str]) -> Dict[str, str]:
        """Сортирует словарь по ключам для консистентного хэширования."""
        return {k: dictionary[k] for k in sorted(dictionary)} if dictionary else {}
    
    @property
    def fingerprint(self) -> str:
        """
        Возвращает отпечаток продукта для уникальной идентификации.
        
        Returns:
            SHA256 хэш продукта в виде hex-строки
        """
        return hashlib.sha256(str(hash(self)).encode('utf-8')).hexdigest()
    
    def __eq__(self, other: object) -> bool:
        """Проверка равенства по всем полям."""
        if not isinstance(other, Product):
            return NotImplemented
        
        return (self.title == other.title and 
                self.short_description == other.short_description and
                self.code_digis == other.code_digis and
                self.article == other.article and
                self.price == other.price and
                self.poster == other.poster and
                self.characteristics == other.characteristics and
                self.specification == other.specification and
                self.documentation == other.documentation and
                sorted(self.accessories) == sorted(other.accessories) and
                self.brand == other.brand)


class ProductGenerator(BaseParser):
    """
    Генератор продуктов для парсинга данных товаров.
    
    Attributes:
        SUPPLIERS_URL: URL для получения списка брендов
        EXCHANGE_RATE_URL: URL для получения курса валют
    """
    
    SUPPLIERS_URL: str = "https://digis.ru/distribution/suppliers/"
    EXCHANGE_RATE_URL: str = "https://cash.rbc.ru/cash/json/converter_currency_rate/"
    
    # Константы для обработки валют
    RUB_SYMBOL: str = 'р'
    USD_TO_RUB_RATE_PRECISION: int = 2
    
    def __init__(
        self,
        session: aiohttp.ClientSession,
        base_url: str,
        parse_engine: str = 'html.parser',
        *,
        max_workers: int = 5,
        sleep_time: int = 3
    ) -> None:
        """
        Инициализация генератора продуктов.
        
        Args:
            session: HTTP-сессия для запросов
            base_url: Базовый URL для парсинга
            parse_engine: Движок для парсинга HTML
            max_workers: Максимальное количество workers
            sleep_time: Время ожидания между запросами
        """
        super().__init__(session, base_url, parse_engine, max_workers=max_workers, sleep_time=sleep_time)
        self._rub_exchange_rate: Optional[Decimal] = None
        self._brands: Set[str] = set()
    
    async def update_brands(self) -> None:
        """Обновляет список доступных брендов с сайта поставщика."""
        try:
            soup = await self._fetch(self.SUPPLIERS_URL)
            if not soup:
                logger.warning("Не удалось получить данные о брендах")
                return
                
            brands = [
                img.get('title') for img in soup.select('ul.row img') 
                if img.get('title')
            ]
            self._brands.update(brands)
            logger.info(f"Обновлено {len(brands)} брендов")
            
        except Exception as e:
            logger.error(f"Ошибка при обновлении брендов: {e}")
            raise
    
    async def update_exchange_rate(self) -> None:
        """Обновляет курс доллара к рублю."""
        try:
            params = {
                'currency_from': 'USD',
                'currency_to': 'RUR',
                'source': 'cbrf',
                'sum': 1,
                'date': ''  # Текущая дата
            }
            
            async with self._session.get(self.EXCHANGE_RATE_URL, params=params) as response:
                response.raise_for_status()
                data = await response.json()
                
                if data.get('status') != 200:
                    logger.error(f'Неожиданный статус ответа: {data.get("status")}')
                    return
                
                rate = data.get('data', {}).get('rate1')
                if rate:
                    self._rub_exchange_rate = Decimal(str(rate)).quantize(
                        Decimal('0.01'), rounding=ROUND_HALF_UP
                    )
                    logger.info(f"Обновлен курс доллара: {self._rub_exchange_rate}")
                else:
                    logger.warning("Курс доллара не найден в ответе")
                    
        except aiohttp.ClientError as e:
            logger.error(f"Ошибка сети при получении курса валют: {e}")
        except Exception as e:
            logger.error(f"Неожиданная ошибка при обновлении курса: {e}")
            raise
    
    async def update(self) -> None:
        """Выполняет одновременное обновление брендов и курса валют."""
        await asyncio.gather(
            self.update_brands(),
            self.update_exchange_rate(),
            return_exceptions=True
        )
    
    def _safe_extract_price(self, price_str: str) -> int:
        """
        Безопасно извлекает цену из строки с конвертацией в рубли при необходимости.
        
        Args:
            price_str: Строка с ценой
            
        Returns:
            Цена в рублях в виде целого числа
            
        Raises:
            ValueError: Если цена не может быть обработана
        """
        try:
            cleaned_price = price_str.replace(' ', '')
            price_value = get_num(cleaned_price)
            
            if self.RUB_SYMBOL in cleaned_price.lower():
                return price_value
            
            if self._rub_exchange_rate is None:
                logger.warning("Курс доллара не установлен, используется цена как есть")
                return price_value
                
            return int((price_value * self._rub_exchange_rate).quantize(
                Decimal('1'), rounding=ROUND_HALF_UP
            ))
            
        except (ValueError, TypeError) as e:
            logger.error(f"Ошибка обработки цены '{price_str}': {e}")
            raise ValueError(f"Некорректный формат цены: {price_str}") from e
    
    def _find_brand(self, title: str) -> str:
        """
        Находит бренд в заголовке товара.
        
        Args:
            title: Заголовок товара
            
        Returns:
            Найденный бренд или первое английское слово из заголовка
        """
        title_lower = title.lower()
        
        # Поиск среди известных брендов
        for brand in self._brands:
            if brand.lower() in title_lower:
                return brand
        
        # Резервный поиск по английским словам
        for word in title_lower.split():
            if is_english(word) and len(word) > 2:  # Исключаем короткие слова
                return word.title()
        
        logger.warning(f"Бренд не обнаружен для: {title}")
        return "Unknown"
    
    def create_product(
        self,
        title: str,
        short_description: str,
        full_description: str,
        code_digis: int,
        article: str,
        price: str,
        poster: List[str],
        characteristics: Dict[str, str],
        specification: Dict[str, str],
        documentation: List[str],
        accessories: List[str],
    ) -> Product:
        """
        Создает объект Product на основе переданных данных.
        
        Args:
            title: Название товара
            short_description: Короткое описание
            full_description: Полное описание
            code_digis: Код Digis
            article: Артикул
            price: Цена (строка)
            poster: URL постера
            characteristics: Характеристики
            specification: Спецификация
            documentation: Документация
            accessories: Аксессуары
            
        Returns:
            Созданный объект Product
        """
        
        return Product(
            title=title.strip(),
            short_description=short_description.strip(),
            full_description=full_description.strip(),
            code_digis=code_digis,
            article=article.strip(),
            price=self._safe_extract_price(price),
            posters=poster,
            characteristics=characteristics or {},
            specification=specification or {},
            documentation=documentation or {},
            accessories=accessories or [],
            brand=self._find_brand(title)
        )