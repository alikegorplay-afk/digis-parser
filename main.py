import asyncio

import aiohttp
from loguru import logger

from service import DigisAPI

async def main():
    async with aiohttp.ClientSession() as session:
        api = DigisAPI(session, 'https://digis.ru', sleep_time=5)
        await api.start_parsing("hear.csv", True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt as e:
        logger.info("Процесс остоновлен пользователем")
    except Exception as e:
        logger.critical(f"Неизвестная ошибка: {e}")