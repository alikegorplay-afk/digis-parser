import asyncio

from pathlib import Path
from typing import List, NoReturn, Optional

import aiohttp
import aiofiles
import aiocsv
import pandas as pd

from loguru import logger

from core.urls import DigisManager
from core.parser import ConcreteDigisParser
from models import ProductGenerator, Product

class DigisAPI:
    def __init__(
        self, 
        session: aiohttp.ClientSession,
        base_url: str,
        parse_engine: str = 'html.parser',
        *,
        max_workers: int = 5,
        sleep_time: int = 3
    ):
        self._digis_manager = DigisManager(session, base_url, parse_engine, max_workers=max_workers, sleep_time=sleep_time)
        self._product_parser = ConcreteDigisParser(session, base_url, parse_engine, max_workers=max_workers, sleep_time=sleep_time)
        self._generator = ProductGenerator(session, base_url, parse_engine, max_workers=max_workers, sleep_time=sleep_time) 
        self.max_worker = max_workers
    
    async def start_parsing(
        self, 
        fp_csv: str | Path, 
        safe_urls: bool,
        *,
        batch_size: int = 1000, 
        urls_path: str | Path | None = None
    ) -> NoReturn:
        """
        Запускает парсинг продуктов с ограничением одновременных запросов.
        
        Args:
            fp_csv: Путь к CSV файлу для сохранения
            safe_urls: Использовать безопасные URL
            max_concurrent: Максимальное количество одновременных запросов
            batch_size: Размер батча для обработки URL
            
        Raises:
            IOError: Ошибки записи в файл
            Exception: Критические ошибки парсинга
        """
        logger.info(f"Начало парсинга с {self.max_worker} одновременными запросами")
        
        try:
            # Инициализация данных
            await self._generator.update()
            if not urls_path:
                urls = len(await self._digis_manager.extract_all_urls(safe_urls))
            else:
                urls = []
                data = pd.read_excel(urls_path)
                for indx, line in data.iterrows():
                    urls.append(line["URL"])
            logger.info(f"Найдено {len(urls)} URL для парсинга")
            
            # Создание семафора для ограничения одновременных запросов
            semaphore = asyncio.Semaphore(self.max_worker)
            
            async with aiofiles.open(fp_csv, 'w', newline='', encoding='utf-8') as file:
                csv_writer = aiocsv.AsyncWriter(file)
                
                # Записываем заголовки
                headers = [
                    "Название", "Короткое описание", "Полное описание", "Код Digis", "Артикул", "Цена", "Изображении", "Спецификации", "Документации", "Аксессуары", "Бренд"
                ]
                await csv_writer.writerow(headers)
                
                # Обработка URL батчами для экономии памяти
                successful = 0
                failed = 0
                
                for batch_start in range(0, len(urls), batch_size):
                    batch_urls = urls[batch_start:batch_start + batch_size]
                    logger.info(f"Обработка батча {batch_start//batch_size + 1}/{(len(urls)-1)//batch_size + 1}")
                    
                    batch_results = await self._process_batch(
                        batch_urls, semaphore, csv_writer
                    )
                    successful += batch_results['successful']
                    failed += batch_results['failed']

                    if hasattr(asyncio, 'sleep'):
                        await asyncio.sleep(0.1)  # Даем event loop передохнуть
                
                logger.info(f"Парсинг завершен. Успешно: {successful}, Ошибок: {failed}")
                
        except Exception as e:
            logger.error(f"Критическая ошибка при парсинге: {e}")
            raise
    
    async def _process_batch(
        self,
        urls: List[str],
        semaphore: asyncio.Semaphore,
        csv_writer: aiocsv.AsyncWriter
    ) -> dict[str, int]:
        """
        Обрабатывает батч URL с ограничением одновременных запросов.
        
        Args:
            urls: Список URL для обработки
            semaphore: Семафор для ограничения concurrent запросов
            csv_writer: CSV writer для записи результатов
            
        Returns:
            Статистика обработки батча
        """
        tasks = []
        for url in urls:
            task = self._create_limited_task(url, semaphore)
            tasks.append(task)
        
        successful = 0
        failed = 0
        
        async for task in asyncio.as_completed(tasks):
            try:
                product_data = await task
                if product_data:
                    product = self._generator.create_product(**product_data)
                    await csv_writer.writerow(self._get_product_row(product))
                    successful += 1
                    
                    if successful % 50 == 0:
                        logger.info(f"Обработано продуктов: {successful}")
                        
            except Exception as e:
                failed += 1
                logger.warning(f"Ошибка обработки продукта: {e}")
        
        return {'successful': successful, 'failed': failed}
    
    async def _create_limited_task(self, url: str, semaphore: asyncio.Semaphore) -> Optional[dict]:
        """
        Создает задачу с ограничением через семафор.
        
        Args:
            url: URL для парсинга
            semaphore: Семафор для ограничения
            
        Returns:
            Данные продукта или None при ошибке
        """
        async with semaphore:
            try:
                return await self._product_parser.parse_product(url)
            except Exception as e:
                logger.warning(f"Ошибка парсинга {url}: {e}")
                return None
    
    def _get_product_row(self, product: 'Product', default: str = '-') -> List[str]:
        """
        Подготавливает строку продукта для CSV.
        
        Args:
            product: Объект продукта
            
        Returns:
            Список значений для записи в CSV
        #"Название", "Короткое описание", "Код Digis", "Артикул", "Цена", "Изображении", "Спецификации", "Документации", "Аксессуары", "Бренд"
        """
        flat_dict = product.as_flat_dict()
        return [
            flat_dict.get("Название", default),
            flat_dict.get("Короткое описание", default),
            flat_dict.get("Полное описание", default),
            flat_dict.get("Код Digis", default),
            flat_dict.get("Артикул", default),
            flat_dict.get("Цена", default),
            flat_dict.get("Изображение", default),
            flat_dict.get("Спецификации", default),
            flat_dict.get("Документация", default),
            flat_dict.get("Аксессуары", default),
            flat_dict.get("Бренд", default),
        ]