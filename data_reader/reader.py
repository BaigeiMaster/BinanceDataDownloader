from typing import Union
import os
import polars as pl
from itertools import chain
from joblib import Parallel, delayed
from tqdm import tqdm

# ==== Customized Modules ====
from utils import PathBinance, PathLocal, ConfigLoader, TimeTools
from downloader.enums import BINANCE_SPOT_HEADERS, BINANCE_SPOT_TIME_COLUMNS  # noqa

config = ConfigLoader.load_config()


class DataReader:
    @staticmethod
    def get_file_path(
        symbol_type: str,
        agg_period: str,
        data_type: str,
        data_frequency: Union[str, None] = None,
        start_date: Union[str, None] = None,
        end_date: Union[str, None] = None,
        symbols: Union[str, list, None] = None,
        need_skip_symbols: Union[str, list, None] = None,
        read_custom_file: bool = True,
    ) -> list[str]:
        """获取文件路径

        Args:
            symbol_type (str): 'spot',...
            agg_period (str): 'daily',...
            data_type (str): 'klines',...
            data_frequency (Union[str, None], optional): '1s',... Defaults to None.
            start_date (Union[str, None], optional): Defaults to None.
            end_date (Union[str, None], optional): Defaults to None.
            symbols (Union[str, list, None], optional): Defaults to None.
            need_skip_symbols (Union[str, list, None], optional): Defaults to None.
            read_custom_file (bool, optional): 读转换后的k线数据 Defaults to True.

        Raises:
            ValueError: data_frequency is required for klines

        Returns:
            list[str]:
        """
        if data_type == "klines" and data_frequency is None:
            raise ValueError("data_frequency is required for klines")
        if isinstance(symbols, str):
            symbols = [symbols]
        if isinstance(need_skip_symbols, str):
            need_skip_symbols = [need_skip_symbols]

        # 只有一个元素
        path: str = PathBinance.get_data_frequency(symbol_type, agg_period, data_type)[
            0
        ]

        dir_path: str = os.path.join(config["save_released_data_dir"], path)

        # 只取需要的标的
        if symbols:
            symbol_paths: list[str] = [
                os.path.join(dir_path, symbol) for symbol in symbols
            ]
            parquet_paths: list[list[str]] = [
                PathLocal.get_file_path_from_dir(p) for p in symbol_paths
            ]
            parquet_paths: list[str] = list(chain.from_iterable(parquet_paths))
        else:
            parquet_paths: list[str] = PathLocal.get_file_path_from_dir(dir_path)

        # 时间过滤
        if start_date is not None or end_date is not None:
            parquet_paths = TimeTools.time_filter(start_date, end_date, parquet_paths)

        # 标的过滤
        if need_skip_symbols:
            parquet_paths: list[str] = [
                p
                for p in parquet_paths
                if not any(p.split("/")[-2] == s for s in need_skip_symbols)
            ]

        # 时间频率过滤
        if data_frequency:
            kw = data_frequency
            if read_custom_file:
                kw = "customized-" + data_frequency
            parquet_paths: list[str] = [p for p in parquet_paths if kw in p]

        return parquet_paths

    @staticmethod
    def read_parquet(
        symbol_type: str,
        agg_period: str,
        data_type: str,
        data_frequency: Union[str, None] = None,
        start_date: Union[str, None] = None,
        end_date: Union[str, None] = None,
        symbols: Union[str, list, None] = None,
        need_skip_symbols: Union[str, list, None] = None,
        use_parallel: bool = True,
        read_custom_file: bool = True,
    ) -> Union[pl.DataFrame, None]:
        """_summary_

        Args:
            symbol_type (str): "spot",...
            agg_period (str): "daily",...
            data_type (str): "klines", ...
            data_frequency (str): "1m", ...
            start_date (Union[str, None], optional): Defaults to None.
            end_date (Union[str, None], optional): Defaults to None.
            symbols (Union[str, list, None], optional): Defaults to None.
            need_skip_symbols (Union[str, list, None], optional): Defaults to None.
            use_parallel (bool, optional): Defaults to True.
            read_custom_file (bool, optional): 只在data_frequency不为None时生效. Defaults to True.

        Returns:
            Union[pl.DataFrame, None]:
        """
        parquet_paths = DataReader.get_file_path(
            symbol_type=symbol_type,
            agg_period=agg_period,
            data_type=data_type,
            data_frequency=data_frequency,
            start_date=start_date,
            end_date=end_date,
            symbols=symbols,
            need_skip_symbols=need_skip_symbols,
            read_custom_file=read_custom_file,
        )
        if not parquet_paths:
            print("Data not found")
            return

        if use_parallel:
            df = pl.concat(
                Parallel(n_jobs=config["parallel_n_jobs"], prefer="threads")(
                    delayed(pl.read_parquet)(p)
                    for p in tqdm(parquet_paths, desc="Reading parquet files")
                )
            )
        else:
            df = pl.concat([pl.read_parquet(p) for p in parquet_paths])

        time_col = eval(f"BINANCE_{symbol_type}_TIME_COLUMNS.{data_frequency}")

        return df.sort(by=["symbol", "timestamp"], descending=[True, False])

    @staticmethod
    def get_file_size(
        symbol_type: str,
        agg_period: str,
        data_type: str,
        data_frequency: Union[str, None] = None,
        start_date: Union[str, None] = None,
        end_date: Union[str, None] = None,
        read_custom_file: bool = True,
    ):
        pass


if __name__ == "__main__":
    print(DataReader.read_parquet("spot", "1d"))
