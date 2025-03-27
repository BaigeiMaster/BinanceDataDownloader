from typing import Union
import asyncio
import os
import re
from datetime import datetime, timedelta
from tqdm import tqdm

# from tqdm.asyncio import tqdm
from itertools import chain
from loguru import logger

# ==== Customized Modules ====
from utils import ConfigLoader, TimeTools
from .enums import BINANCE_DATA_URLS
from utils import PathBinance as binance_pathtool
from utils import PathLocal as local_pathtool
from .my_gospeed_api import AsyncGospeedInterface, SyncGospeedClientInterface

config = ConfigLoader.load_config()


class Downloader:

    def __init__(self) -> None:
        self.async_gs_interface = AsyncGospeedInterface()
        self.sync_gs_interface = SyncGospeedClientInterface()
        # 检查连接
        try:
            self.sync_gs_interface.get_server_info()
        except Exception as e:
            logger.error(f"连接下载器失败: {e}")
            print(f"连接下载器失败: {e}")

    @staticmethod
    def ignore_existed_file(download_paths: list[str]) -> list[str]:
        """忽略存在的文件

        Args:
            download_paths (list[str]):

        Returns:
            list[str]:
        """
        local_path: list[str] = local_pathtool.get_file_path_from_dir(
            os.path.join(config["save_downloaded_data_dir"], "data")
        )
        save_paths: list[str] = [
            os.path.join(config["save_downloaded_data_dir"], d) for d in download_paths
        ]
        result = list(set(save_paths) - set(local_path))

        return sorted(
            [
                local_pathtool.remove_subpath(p, config["save_downloaded_data_dir"])[
                    1:
                ]  # 删掉第一个/
                for p in result
            ]
        )

    async def _download_sybol_data(
        self,
        path: str,
        frequency: str,
        start_date: Union[str, None] = None,
        end_date: Union[str, None] = None,
        skip_existed: bool = True,
        skip_checksum: bool = False,
    ):
        """下载标的数据

        Args:
            path (str): 数据路径
            frequency (str): 频率 ['1m', ...]
            start_date (Union[str, None], optional)
            end_date (Union[str, None], optional))
            skip_existed (bool, optional): 跳过已有数据. Defaults to True.
            skip checksum (bool, optional): 不下载校验和 Defaults to True.
        """
        whole_data_type: str = path.split("/")[-3]
        download_paths: list = sorted(
            await binance_pathtool.async_get_path_from_website(
                # 具有"Klines"的标的有frequancy选项
                path + f"{frequency}/"
                if "Klines" in whole_data_type or "klines" in whole_data_type
                else path
            )
        )

        before_download_paths = download_paths
        download_paths: list[str] = TimeTools.time_filter(
            start_date=start_date, end_date=end_date, path_list=download_paths
        )

        logger.info(
            f"Start date:{start_date}, end_date: {end_date}, skip {len(before_download_paths) - len(download_paths)} files"
        )

        # 已存在不覆盖
        if skip_existed:
            path_after_skip: list = self.ignore_existed_file(download_paths)
            file_skiped: int = int(len(download_paths) - len(path_after_skip))
            if file_skiped != 0:
                logger.info(
                    f"Found path: {path} {file_skiped} files existed, skip them."
                )
            elif len(path_after_skip) == 0:
                logger.info(f"Path: {path} all files downloaded.")
                return
            else:
                logger.info(f"Found path: {path} no files existed, download them.")

            download_paths = path_after_skip

        # 不下载校验和
        if skip_checksum:
            download_paths = [
                dp for dp in download_paths if "CHECKSUM" not in dp.split(".")[-1]
            ]

        for dp in download_paths:
            self.async_gs_interface.tasks.append(
                {
                    "url": BINANCE_DATA_URLS.download_url.value + dp,
                    "save_dir": os.path.dirname(dp),
                }
            )

    async def create_copy(
        self,
        symbol_type: str,
        agg_period: str,
        frequency: str,
        start_date: Union[str, None] = None,
        end_date: Union[str, None] = None,
        data_type: Union[str, list, None] = None,
        trading_pair: Union[str, list, None] = None,
        key_words: Union[str, list, None] = None,
        spot_filter: bool = True,
        skip_existed: bool = True,
        skip_checksum: bool = False,
    ):
        """制作币安数据网的本地副本

        Args:
            symbol_type (str): 标的类型 [spot, option, future]
            agg_period (str): 周期 [daily, monthly]
            frequency (str): 频率 [1m, 15m, ...]
            start_date (Union[str, None], optional): 开始日期
            end_date (Union[str, None], optional): 结束日期
            data_type (Union[str, list, None], optional): 数据类型 [klines, aggTrades, trades]. Defaults to None.
            trading_pair (Union[str, list, None], optional): 交易对 eg:BTCUSDT. Defaults to None (all).
            key_words (Union[str, list, None], optional): 只下载包含关键字的数据 eg:USDT. Defaults to None.
            spot_filter (bool, optional): 现货数据过滤稳定币等. Defaults to True.
            skip_existed (bool, optional): 跳过本地已有文件. Defaults to True.
            skip_checksum (bool, optional): 不下载校验和. Defaults to True.
        """
        # delete all tasks
        await self.async_gs_interface.async_delete_all_tasks()

        whole_data_type = await binance_pathtool.async_get_data_frequency(
            symbol_type, agg_period, data_type
        )
        tasks = [
            binance_pathtool.async_get_path_from_website(d) for d in whole_data_type
        ]
        # 获取数据路径 eg.data/xxx/xxx
        paths: list[list[str]] = []
        for t in tqdm(
            asyncio.as_completed(tasks),
            total=len(tasks),
            desc="Get data paths",
        ):
            paths.append(await t)
        paths: list[str] = list(chain.from_iterable(paths))

        # 只下载指定交易对
        # data/spot/monthly/aggTrades/SHIBUAH/ 取交易对
        if trading_pair is not None:
            if isinstance(trading_pair, str):
                trading_pair = [trading_pair]
            paths = [
                p for p in paths if any(tp == p.split("/")[-2] for tp in trading_pair)
            ]

        # 关键字过滤(eg: USDT 只下载USDT交易对)
        if key_words is not None:
            if isinstance(key_words, str):
                key_words = [key_words]
            paths = [
                p
                for p in paths
                if any(p.split("/")[-2].endswith(kw) for kw in key_words)
            ]

        # 对现货进行过滤
        if symbol_type == "spot" and spot_filter:
            symbols = [p.split("/")[-2] for p in paths]
            symbols = self.spot_symbols_filter(symbols)
            paths = [p for p in paths if p.split("/")[-2] in symbols]

        tasks = [
            self._download_sybol_data(
                p,
                frequency,
                skip_existed=skip_existed,
                skip_checksum=skip_checksum,
                start_date=start_date,
                end_date=end_date,
            )
            for p in paths
        ]

        for f in tqdm(
            asyncio.as_completed(tasks),
            total=len(tasks),
            desc="Find need download files",
        ):
            await f

        if len(self.async_gs_interface.tasks) == 0:
            print("No data need to download.")
            logger.info("No data need to download.")
            return

        with tqdm(
            total=len(self.async_gs_interface.tasks), desc="Downloading", unit="task"
        ) as pbar:
            while not self.async_gs_interface.task_done:
                pos_old = self.async_gs_interface.pos
                await self.async_gs_interface.gather()
                await self.async_gs_interface.get_task_info()
                if pos_old == 0:
                    continue
                pbar.update(self.async_gs_interface.pos - pos_old)

        if len(self.async_gs_interface.failed_tasks) == 0:
            print("All tasks finished.")
            logger.info("All tasks finished.")
            return

        with tqdm(
            total=len(self.async_gs_interface.failed_tasks),
            desc="Retrying",
            unit="task",
        ) as pbar:
            while not self.async_gs_interface.failed_task_done:
                pos_old = self.async_gs_interface.retry_pos
                await self.async_gs_interface.retry_gather()
                await self.async_gs_interface.get_task_info()
                pbar.total = len(self.async_gs_interface.failed_tasks)
                if pos_old == 0:
                    continue
                pbar.update(self.async_gs_interface.retry_pos - pos_old)

    def spot_symbols_filter(self, symbols):
        others = []
        stable_symbol = [
            "BKRW",
            "USDC",
            "USDP",
            "TUSD",
            "BUSD",
            "FDUSD",
            "DAI",
            "EUR",
            "GBP",
        ]
        # stable_symbols：稳定币交易对
        stable_symbols = [s + "USDT" for s in stable_symbol]
        # special_symbols：容易误判的特殊交易对
        special_symbols = ["JUPUSDT"]
        pure_spot_symbols = []
        for symbol in symbols:
            if symbol in special_symbols:
                pure_spot_symbols.append(symbol)
                continue
            if (
                symbol.endswith("UPUSDT")
                or symbol.endswith("DOWNUSDT")
                or symbol.endswith("BULLUSDT")
                or symbol.endswith("BEARUSDT")
            ):
                others.append(symbol)
                continue
            if symbol in stable_symbols:
                others.append(symbol)
                continue
            pure_spot_symbols.append(symbol)
        logger.info("Ignore spot symbols:", others)
        print("Ignore spot symbols:", others)
        return pure_spot_symbols


if __name__ == "__main__":
    downloader = Downloader()
    # 异步下载spot/monthly/aggTrades/1m
    asyncio.run(downloader.create_copy("spot", "monthly", "1m", "aggTrades"))
