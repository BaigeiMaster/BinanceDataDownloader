import httpx
import asyncio
from loguru import logger
from typing import Union

# ==== Customized Modules ====
from .config_loader import ConfigLoader

config = ConfigLoader.load_config()

semaphore = asyncio.Semaphore(int(config["max_semaphore"]))


class WebGet:
    @staticmethod
    async def async_fetch_with_retry(
        url, retries=20, timeout=5, backoff_factor=1
    ) -> str:
        """
        使用 httpx 进行带重试和超时的异步请求
        :param url: 请求的URL
        :param retries: 最大重试次数
        :param timeout: 超时时间（秒）
        :param backoff_factor: 每次重试增加的等待时间（秒）
        :return: 请求的响应内容
        """
        for attempt in range(retries):
            try:
                async with semaphore:
                    async with httpx.AsyncClient(timeout=timeout) as client:
                        response = await client.get(url)
                        response.raise_for_status()  # 如果响应状态码不是 2xx，抛出异常
                        logger.info(f"Successfully fetched data from {url}")
                        return response.text

            except Exception as exc:
                logger.warning(f"Attempt {attempt + 1} failed, url: {url}")
                if attempt < retries - 1:
                    sleep_time = backoff_factor * (attempt + 1)
                    logger.info(f"Retrying in {sleep_time} seconds...")
                    await asyncio.sleep(sleep_time)
                else:
                    logger.error(
                        f"All {retries} attempts failed. No more retries. url: {url}"
                    )
                    raise httpx.TimeoutException


if __name__ == "__main__":
    # print(
    #     asyncio.run(
    #         async_fetch_with_retry(
    #             "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-06.zip"
    #         )
    #     )
    # )
    # async def download_multiple_files(urls):
    #     tasks = []
    #     temp_files = []

    #     for url in urls:
    #         temp_file = tempfile.NamedTemporaryFile(delete=True)
    #         temp_files.append(temp_file.name)
    #         tasks.append(
    #             async_download_file_with_retry(
    #                 url, temp_file.name, retries=3, timeout=30, backoff_factor=1
    #             )
    #         )

    #     await asyncio.gather(*tasks)

    #     return temp_files

    # async def main():
    #     urls = [
    #         "https://data.binance.vision/data/spot/monthly/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-06.zip",
    #         "https://data.binance.vision/data/spot/monthly/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-05.zip",
    #         "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-06.zip",
    #     ]

    #     downloaded_files = await download_multiple_files(urls)
    #     for file_path in downloaded_files:
    #         print(f"Downloaded file saved to {file_path}")

    # asyncio.run(main())
    pass
