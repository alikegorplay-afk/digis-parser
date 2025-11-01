from abc import ABC, abstractmethod
from typing import Optional, Dict, List
from bs4 import BeautifulSoup
from loguru import logger

from core.base import BaseParser
from tools import get_integer

class DigisParser(BaseParser, ABC):
    """
    Абстрактный парсер для извлечения данных о продуктах с сайта Digis.
    
    Реализует шаблонный метод parse_product, который использует
    абстрактные методы для извлечения конкретных данных.
    
    Attributes:
        base_url: Базовый URL для парсинга
    """
    
    @abstractmethod
    def _extract_title(self, soup: BeautifulSoup) -> str:
        """
        Извлекает заголовок продукта.
        
        Args:
            soup: BeautifulSoup объект страницы продукта
            
        Returns:
            Заголовок продукта
            
        Raises:
            AttributeError: Если заголовок не найден
        """
        pass
    
    @abstractmethod
    def _extract_description(self, soup: BeautifulSoup) -> str:
        """
        Извлекает краткое описание продукта.
        
        Args:
            soup: BeautifulSoup объект страницы продукта
            
        Returns:
            Краткое описание продукта
        """
        
    @abstractmethod
    def _extract_full_description(self, soup: BeautifulSoup) -> str:
        """
        Извлекает краткое описание продукта.
        
        Args:
            soup: BeautifulSoup объект страницы продукта
            
        Returns:
            Краткое описание продукта
        """
    
    @abstractmethod
    def _extract_digis_code(self, soup: BeautifulSoup) -> int:
        """
        Извлекает код Digis продукта.
        
        Args:
            soup: BeautifulSoup объект страницы продукта
            
        Returns:
            Код Digis продукта
            
        Raises:
            ValueError: Если код не может быть преобразован в число
        """
        pass
    
    @abstractmethod
    def _extract_article(self, soup: BeautifulSoup) -> str:
        """
        Извлекает артикул продукта.
        
        Args:
            soup: BeautifulSoup объект страницы продукта
            
        Returns:
            Артикул продукта
        """
        pass
    
    @abstractmethod
    def _extract_price(self, soup: BeautifulSoup) -> str:
        """
        Извлекает цену продукта.
        
        Args:
            soup: BeautifulSoup объект страницы продукта
            
        Returns:
            Строка с ценой продукта
        """
        pass
    
    @abstractmethod
    def _extract_poster(self, soup: BeautifulSoup) -> str:
        """
        Извлекает URL постера продукта.
        
        Args:
            soup: BeautifulSoup объект страницы продукта
            
        Returns:
            URL изображения продукта
        """
        pass
    
    @abstractmethod
    def _extract_characteristics(self, soup: BeautifulSoup) -> Dict[str, str]:
        """
        Извлекает характеристики продукта.
        
        Args:
            soup: BeautifulSoup объект страницы продукта
            
        Returns:
            Словарь характеристик в формате {название: значение}
        """
        pass
    
    @abstractmethod
    def _extract_specification(self, soup: BeautifulSoup) -> Dict[str, str]:
        """
        Извлекает спецификации продукта.
        
        Args:
            soup: BeautifulSoup объект страницы продукта
            
        Returns:
            Словарь спецификаций в формате {название: значение}
        """
        pass
    
    @abstractmethod
    def _extract_documentation(self, soup: BeautifulSoup) -> Dict[str, str]:
        """
        Извлекает документацию продукта.
        
        Args:
            soup: BeautifulSoup объект страницы продукта
            
        Returns:
            Словарь документации в формате {название_документа: URL}
        """
        pass
    
    @abstractmethod
    def _extract_accessories(self, soup: BeautifulSoup) -> List[str]:
        """
        Извлекает список аксессуаров продукта.
        
        Args:
            soup: BeautifulSoup объект страницы продукта
            
        Returns:
            Список названий аксессуаров
        """
        pass
    
    async def parse_product(self, url: str) -> Optional[Dict[str, str]]:
        """
        Парсит данные продукта по указанному URL.
        
        Args:
            url: URL страницы продукта
            
        Returns:
            Словарь с данными продукта или None при ошибке
            
        Examples:
            >>> parser = ConcreteDigisParser(session, "https://example.com")
            >>> product_data = await parser.parse_product("https://digis.ru/product/123")
            >>> print(product_data['title'])
            "Смартфон Apple iPhone 15 Pro"
        """
        try:
            soup = await self._fetch(url)
            if not soup:
                logger.error(f"Не удалось загрузить страницу продукта: {url}")
                return None
            
            product_data = {
                'title': self._extract_title(soup),
                'short_description': self._extract_description(soup),
                'full_description': self._extract_full_description(soup),
                'code_digis': self._extract_digis_code(soup),
                'article': self._extract_article(soup),
                'price': self._extract_price(soup),
                'poster': self._extract_poster(soup),
                'characteristics': self._extract_characteristics(soup),
                'specification': self._extract_specification(soup),
                'documentation': self._extract_documentation(soup),
                'accessories': self._extract_accessories(soup),
            }
            
            logger.info(f"Успешно распарсен продукт: {product_data['title']}")
            return product_data
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге продукта {url}: {e}")
            return None

class ConcreteDigisParser(DigisParser):
    """
    Конкретная реализация парсера для сайта Digis.
    
    Реализует все абстрактные методы для извлечения данных
    со специфичной для Digis структурой HTML.
    """
    
    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Реализация извлечения заголовка для Digis."""
        logger.debug("Попытка извлечь title")
        try:
            title_element = soup.select_one('h1')
            logger.debug(f"Найден element: {title_element}")
            return title_element.get_text(strip=True) if title_element else "Неизвестный товар"
        except Exception as e:
            logger.warning(f"Ошибка извлечения заголовка: {e}")
            return "Неизвестный товар"
    
    def _extract_description(self, soup: BeautifulSoup) -> str:
        """Реализация извлечения описания для Digis."""
        logger.debug("Попытка извлечь description")
        try:
            desc_element = soup.select_one('div.prod-detail-head-desc')
            logger.debug("Описание найдено" if desc_element else "Описание не обнаружено")
            return desc_element.get_text(strip=True) if desc_element else ""
        except Exception as e:
            logger.warning(f"Ошибка извлечения описания: {e}")
            return ""
    
    def _extract_full_description(self, soup):
        """Реализация извлечения полного описания для Digis."""
        logger.debug("Попытка извлечь full_description")
        try:
            description = ' '.join(x.get_text(strip=True) for x in soup.select("#tab_description p"))
            return description
        except Exception as e:
            logger.warning(f"Ошибка извлечения полного описания: {e}")
            return ""
            
    
    def _extract_digis_code(self, soup: BeautifulSoup) -> int:
        """Реализация извлечения кода Digis."""
        logger.debug("Попытка извлечь артикул")
        try:
            result = self._extract_sku(soup).get('Код DIGIS', "")
            logger.debug(f"Удалось извлечь артикул: {result}" if result else "Не удалось извалечь артикул")
            return get_integer(result)
        except Exception as e:
            logger.warning(f"Ошибка извлечения кода Digis: {e}")
            return 0
    
    def _extract_article(self, soup: BeautifulSoup) -> str:
        """Реализация извлечения артикула."""
        logger.debug("Попытка извлечь артикул")
        try:
            result = self._extract_sku(soup).get('Артикул', "")
            logger.debug(f"Удалось извлечь артикул: {result}" if result else "Не удалось извалечь артикул")
            return result
        except Exception as e:
            logger.warning(f"Ошибка извлечения артикула: {e}")
            return ""
    
    def _extract_sku(self, soup: BeautifulSoup) -> dict[str, str]: 
        result = {}
        props = soup.select_one("div.prod-detail-box-buy-head .list-props")
        if not props:
            logger.warning("Не найден ни один обозначающий идентификатор")
            return result
        
        for li in props.find_all('li', recursive=False):
            values = li.get_text(strip=True).split(":")
            if len(values) != 2:
                logger.warning(f"Обнаруже неизваестный атрибут: {values}")
            
            key, value = values
            result[key] = value
        return result
        
    def _extract_price(self, soup: BeautifulSoup) -> str:
        """Реализация извлечения цены."""
        try:
            price = {}
            price_element = soup.select_one('div.price')
            if not price_element:
                logger.info("Не найдена цена")
                return "0"
            
            current_value = price_element.select_one('.val')
            current_currency = price_element.select_one('.currency')
            if not current_value or not current_currency:
                logger.warning("Остутсвует важный атрибут")
            else:
                price[current_currency.get_text(strip=True)] = current_value.get_text(strip=True)
                
            for li in price_element.select('li'):
                value = li.select_one('.val')
                currency = li.select_one('.currency')
                if not value or not currency:
                    logger.warning("Остутсвует атрибут")
                    continue
                price[currency.get_text(strip=True)] = value.get_text(strip=True)
            
            return (price.get('руб', "0") + ' руб') if 'руб' in price else price.get('USD', "0") + ' USD'
        
        except Exception as e:
            logger.warning(f"Ошибка извлечения цены: {e}")
            return "0"
    
    def _extract_poster(self, soup: BeautifulSoup) -> list[str]:
        """Реализация извлечения постера."""
        try:
            urls = []
            poster_element = soup.select('#prod-gallery .swiper-slide')
            for poster in poster_element:
                if not poster.a and not poster.img:
                    continue
                try:
                    urls.append(self._safe_extract_url(poster.a, 'href'))
                except Exception:
                    urls.append(self._safe_extract_url(poster.img, 'src'))
            
            if urls:
                return urls

            else:
                for poster in soup.select(".prod-detail-img img"):
                    urls.append(self._safe_extract_url(poster, 'src'))
                
                if not urls:
                    return ["https://digis.ru/bitrix_personal/templates/ia_pegas_digis/images/tmp/42_282.jpg"]
                
            return urls
                
        except Exception as e:
            logger.warning(f"Ошибка извлечения постера: {e}")
            return ["https://digis.ru/bitrix_personal/templates/ia_pegas_digis/images/tmp/42_282.jpg"]
    
    def _extract_characteristics(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Реализация извлечения характеристик."""
        try:
            characteristics = {}
            for tr in soup.select('#tab_features tr'):
                values = list(tr._all_strings(strip=True))
                if len(values) != 2:
                    logger.warning("Неподдерживаемый тип харектеристик")
                    continue
                key, value = values
                characteristics[key] = value
            return characteristics
        except Exception as e:
            logger.warning(f"Ошибка извлечения характеристик: {e}")
            return {}
    
    def _extract_specification(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Реализация извлечения спецификаций."""
        try:
            specification = {}
            spec_elements = soup.select('#tab_specification tr')
            for tr in spec_elements:
                values = list(tr._all_strings(strip=True))
                if len(values) != 2:
                    continue
                
                key, value = values
                specification[key] = value
            return specification
        except Exception as e:
            logger.warning(f"Ошибка извлечения спецификаций: {e}")
            return {}
    
    def _extract_documentation(self, soup: BeautifulSoup) -> List[str]:
        """Реализация извлечения документации."""
        try:
            documentation = []
            doc_elements = soup.select('#tab_documentation tr')
            for element in doc_elements:
                doc_url = element.select_one('.td-btn a')
                if not doc_url:
                    logger.warning("Пустое значение")
                    continue
                documentation.append(self._safe_extract_url(doc_url, 'href'))
            return documentation
        except Exception as e:
            logger.warning(f"Ошибка извлечения документации: {e}")
            return {}
    
    def _extract_accessories(self, soup: BeautifulSoup) -> List[str]:
        """Реализация извлечения аксессуаров."""
        try:
            accessories = []
            accessory_elements = soup.select('#tab_accessories tr')
            for element in accessory_elements:
                name = element.select_one('.col-body a')
                if name:
                    accessories.append(self._safe_extract_url(name, 'href'))
            return accessories
        except Exception as e:
            logger.warning(f"Ошибка извлечения аксессуаров: {e}")
            return []