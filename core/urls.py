import asyncio
import sys

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

from loguru import logger
from loguru._defaults import LOGURU_FORMAT
from .base import BaseParser

logger.remove()
logger.add(
    'logs.log',
    format = LOGURU_FORMAT.replace(".SSS", ''),
    level="INFO",
    rotation="10 MB"
)
logger.add(
    sys.stdout,
    format = LOGURU_FORMAT.replace(".SSS", ''),
    level="ERROR"
)
    
class DigisExractUrls(BaseParser):
    DISTRIBUTION_URL = "https://digis.ru/distribution"
    
    async def start_extract_urls(self):
        urls: set[str] = set()
        logger.info("Достаём URLS всех обьектов")
        urls_lvl1 = await self._extract_distribution()
        
        tasks = [asyncio.create_task(self._extrac_level2(url)) for url in urls_lvl1]
        for task in asyncio.as_completed(tasks):
            response_urls = await task
            urls = urls.union(response_urls)
            
        return urls
        
    async def _extract_distribution(self):
        logger.info("Обходим 1 уровень категорий")
        soup = await self._fetch(self.DISTRIBUTION_URL)
        if not soup:
            raise ValueError("Не удалось получить обязательный уровень")
        urls = []
        
        for lvl in soup.select("#main-rubrics .lvl-1"):
            url = lvl.select_one('.ttl')
            if not url:
                logger.warning("Ненайден url в lvl")
                continue
            
            urls.append(self._safe_extract_url(url, 'href'))
        return urls

    async def _extrac_level2(self, url: str) -> list[str]:
        logger.info("Обходим 2 уровень категорий")
        soup = await self._fetch(url)
        if not soup:
            logger.error(f"Не удалось получть данные для: {url}")
            return []
        
        canonical = soup.select_one('link[rel="canonical"]')
        
        box = soup.select('.rubric-list.row.flex.flex-wrap a')
        if not box:
            logger.warning(f"Не найден ни одна подкатегория!, URL: {canonical}")
            return []
        
        urls = [self._safe_extract_url(x, 'href') for x in box if self._safe_extract_url(x, 'href') is not None]
        logger.success(f"Была получено: {len(urls)} подкатегорий")
        
        return urls

class PaginationDigis(BaseParser):
    
    def _extract_page_urls(self, soup: BeautifulSoup) -> list[str]:
        urls = []
        tr_s = soup.select(".list-prods tbody tr")
        for tr in tr_s:
            if tr.a:
                urls.append(self._safe_extract_url(tr.a, 'href'))
        return urls
    
    async def start_parsing_catrgory(self, url: str):
        
        soup = await self._fetch(url)
        if not soup:
            logger.critical(f"Невозможно получть данные URL: {url}")
            return set()
        
        page_urls = set(self._extract_page_urls(soup))
        pages = soup.select_one('.pager-pages-list.line-items')
        
        if not pages:
            return page_urls
        max_page = max([int(page.get_text(strip=True)) for page in pages.children if page.get_text(strip=True).isdigit()])
        
        tasks = [asyncio.create_task(self._fetch(url, params = {'PAGEN_1': page})) for page in range(2, max_page + 1)]
        async for task in asyncio.as_completed(tasks):
            soup = await task
            if not soup:
                logger.warning("Не получилось получть данные")
            page_urls = page_urls.union(self._extract_page_urls(soup))
            
        logger.success(f"Получено: {len(page_urls)} URLS, и найдено: {max_page} страниц")
        return page_urls
        
class DigisManager(BaseParser):
    def __init__(self, session, base_url, parse_engine = 'html.parser', *, max_workers = 5, sleep_time = 3):
        super().__init__(session, base_url, parse_engine, max_workers=max_workers, sleep_time=sleep_time)
        self._urls_extracter = DigisExractUrls(session, base_url, parse_engine, max_workers=max_workers, sleep_time=sleep_time)
        self._pagination = PaginationDigis(session, base_url, parse_engine, max_workers=max_workers, sleep_time=sleep_time)
        
    async def extract_all_urls(self, save: bool = True):
        product_urls: set[str] = set()
        urls = await self._urls_extracter.start_extract_urls()
        
        tasks = [
            asyncio.create_task(self._pagination.start_parsing_catrgory(url)) 
            for url in urls
        ]
        for task in asyncio.as_completed(tasks):
            response = await task
            product_urls = product_urls.union(response)
            
        logger.success(f"Обнаружено: {len(product_urls)} продуктов")
        if save:
            self._save_as_excel(product_urls)
        return product_urls
    
    def _save_as_excel(self, products: set[str]):
        df = pd.DataFrame(list(products), columns=['URL'])
        df.to_excel('links.xlsx', index=False)
        
        
async def main():
    async with aiohttp.ClientSession() as session:
        api = DigisManager(session, "https://digis.ru", max_workers = 5, sleep_time=5)
        await api.extract_all_urls()

if __name__ == "__main__":
    asyncio.run(main())