import asyncio
import random

from urllib.parse import urljoin

import aiohttp
from loguru import logger
from bs4 import Tag, BeautifulSoup
from fake_headers import Headers


class BaseParser:
    def __init__(self, session: aiohttp.ClientSession, base_url: str, parse_engine: str = 'html.parser', *, max_workers: int = 5, sleep_time: int = 3):
        self._session = session
        self._base_url = base_url
        self.parse_engine = parse_engine
        self.sleep_time = sleep_time
        
        self.semaphore = asyncio.Semaphore(max_workers)
        self.headers = Headers(
            browser="chrome",
            os="win",
            headers=True
        )
    
    def _safe_extract_url(self, tag: Tag, attr: str):
        try:
            if url := tag.get(attr):
                return urljoin(self._base_url, url)
            else:
                logger.warning("Не удалось получить URL")
        except Exception as e:
            logger.error(f"Ошибка при вытаскивании URL: {e}")
    
    async def _fetch(self, url: str, *args, **kwargs) -> BeautifulSoup | None:
        async with self.semaphore:
            delay = self.sleep_time + random.uniform(-0.5, 1.0)
            await asyncio.sleep(max(2.0, delay))  # минимум 2 секунды
            
            for attempt in range(1, 4):
                try:
                    if attempt > 1:
                        wait_time = self.sleep_time * (2 ** (attempt - 1)) + random.uniform(1, 5)
                        logger.info(f"Ждем {wait_time:.1f} сек перед повторной попыткой")
                        await asyncio.sleep(wait_time)
                    
                    logger.info(f"Попытка #{attempt}: {url}")
                    async with self._session.get(
                        url, 
                        headers=self._get_headers(),
                        timeout=aiohttp.ClientTimeout(total=30),
                        *args, **kwargs
                    ) as response:
                        if response.status == 429:
                            logger.warning("🚨 Rate limit! Делаем длинную паузу")
                            await asyncio.sleep(60)
                            continue
                        
                        elif response.status == 403:
                            logger.error("💀 Полный бан! Останавливаемся")
                            return 
                        
                        response.raise_for_status()
                        logger.success(f"✅ Успешно: {url}")
                        return BeautifulSoup(await response.text(), self.parse_engine)
                        
                except aiohttp.ClientResponseError as e:
                    if 500 <= e.status < 600:
                        logger.warning(f"🔧 Серверная ошибка {e.status}, пробуем снова...")
                        continue
                    elif e.status == 404:
                        logger.warning(f"❌ 404: {url}")
                        return None
                    else:
                        logger.warning(f"⚠️ Код {e.status}: {url}")
                        continue
                        
                except asyncio.TimeoutError:
                    logger.warning(f"⏰ Таймаут попытка #{attempt}")
                    continue
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка: {type(e).__name__}")
                    continue
            
            logger.error(f"🚫 Все попытки исчерпаны для: {url}")
            return None

    def _get_headers(self):
        headers = self.headers.generate()
        headers['Referer'] = self._base_url
        headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        return headers
 