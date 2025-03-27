import polars as pl
import numpy as np
from typing import Union
import os
from loguru import logger
from tqdm import tqdm

# ==== Customized Modules ====
from utils import PathBinance, PathLocal, ConfigLoader, TimeTools
from data_reader.reader import DataReader

config = ConfigLoader.load_config()


class Spot:
    @staticmethod
    def all_aggtrades_to_kline(time_period: str, agg_period: str, skip_existed=True):
        """全部aggTrades数据转换为k线数据

        Args:
            time_period (str): 转换k线的时间周期
            agg_period (str): "daily","monthly"
            skip_existed (bool, optional): 跳过已存在的文件. Defaults to True.
        """
        p: str = PathBinance.get_data_frequency(
            type_="spot", agg_period=agg_period, data_type="aggTrades"
        )[0]
        dir_path = os.path.join(config["save_downloaded_data_dir"], p)
        file_paths: list = PathLocal.get_dir_path_from_dir(dir_path)
        symbols: list = [p.split("/")[-1] for p in file_paths]
        for n, symbol in enumerate(symbols):
            print(f"Symbol: {symbol} {n+1}/{len(symbols)}")
            Spot.from_file(
                symbol=symbol,
                time_period=time_period,
                agg_period=agg_period,
                skip_existed=skip_existed,
            )

    @staticmethod
    def from_file(
        symbol: str,
        time_period: str,
        agg_period: str,
        start_date: Union[str, None] = None,
        end_date: Union[str, None] = None,
        skip_existed=True,
    ) -> pl.DataFrame:
        """读取文件转换后输出文件

        Args:
            symbol (str): 标的
            time_period (str): 转换k线的时间周期
            agg_period (str): "daily","monthly"
            start_date (Union[str, None], optional): 开始日期. Defaults to None.
            end_date (Union[str, None], optional): 结束日期. Defaults to None.
            skip_existed: (bool, optional) 跳过已存在的文件 Defaults to True.

        Returns:
        """
        logger.info(
            f"Convert aggTrades to kline. symbol: {symbol} time_period : {time_period}  agg_period: {agg_period} start_date: {start_date} end_date: {end_date} skip_existed: {skip_existed}"
        )
        # 只能拿一个symbol
        df = DataReader.read_parquet(
            symbol_type="spot",
            agg_period=agg_period,
            data_type="aggTrades",
            start_date=start_date,
            end_date=end_date,
            symbols=symbol,
        )
        df = Spot.bn_aggTrades_to_kline(df, time_period)
        path = PathBinance.get_data_frequency(
            type_="spot", agg_period=agg_period, data_type="klines"
        )[0]
        if "monthly" in path:
            # 月度数据根据月份分块
            df = df.with_columns(
                [
                    pl.datetime(
                        year=pl.col("timestamp").dt.year(),
                        month=pl.col("timestamp").dt.month(),
                        day=1,
                    ).alias("date")
                ]
            )
            for t, f in tqdm(
                df.group_by("date"),
                desc="Save kline",
                total=len(df.group_by("date").n_unique()),
            ):
                date = t[0].strftime("%Y-%m")
                save_path = os.path.join(
                    config["save_released_data_dir"],
                    path,
                    symbol,
                    f"customized-{time_period}",
                    f"{symbol}-{time_period}-{date}.parquet",
                )
                f = f.drop(["date"])
                if not os.path.exists(save_path):
                    os.makedirs(os.path.dirname(save_path), exist_ok=True)
                    f.write_parquet(save_path)
                elif not skip_existed:
                    f.write_parquet(save_path)

    @staticmethod
    def from_df(
        df: pl.DataFrame,
        time_period: str,
    ) -> pl.DataFrame:
        """读取df输出df

        Args:
            df (pl.DataFrame):
            time_period (str):

        Returns:
            pl.DataFrame:
        """
        return Spot.bn_aggTrades_to_kline(df, time_period)

    @staticmethod
    def bn_aggTrades_to_kline(at_df: pl.DataFrame, time_period: str) -> pl.DataFrame:
        """币安aggTrades数据转换为k线数据

        Args:
            at_df (pl.DataFrame):
            time_period (str):

        Returns:
            pl.DataFrame:
        """
        at_df = at_df.lazy()
        at_df = at_df.with_columns(
            [(pl.col("price") * pl.col("quantity")).alias("quote_asset_volume")]
        )

        at_df = at_df.with_columns(
            [
                pl.when(pl.col("was_the_buyer_the_maker") == True)  # noqa
                .then(pl.col("quote_asset_volume"))
                .otherwise(0)
                .alias("volume_the_maker_buy")
            ]
        )
        at_df = at_df.with_columns(
            [
                pl.when(pl.col("was_the_trade_the_best_price_match") == True)  # noqa
                .then(pl.col("quote_asset_volume"))
                .otherwise(0)
                .alias("volume_best_price_match")
            ]
        )

        at_df = at_df.sort("timestamp")
        kdf = at_df.group_by_dynamic("timestamp", every=time_period).agg(
            pl.col("symbol").last(),
            pl.col("aggregate_trade_id").first(),
            pl.col("first_trade_id").first(),
            pl.col("last_trade_id").last(),
            pl.col("quote_asset_volume").sum(),
            pl.col("quantity").sum(),
            pl.col("volume_the_maker_buy").sum(),
            pl.col("volume_best_price_match").sum(),
            pl.col("price").first().alias("open"),
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").last().alias("close"),
        )

        kdf = kdf.with_columns(
            [
                (pl.col("last_trade_id") - pl.col("first_trade_id") + 1).alias(
                    "total_trades"
                )
            ]
        )
        kdf = kdf.fill_null(0)
        kdf = kdf.with_columns([pl.col("close").fill_null(strategy="forward")])
        kdf = kdf.with_columns(
            [
                pl.when(pl.col("open") == 0)
                .then(pl.col("close"))
                .otherwise(pl.col("open"))
                .alias("open"),
                pl.when(pl.col("high") == 0)
                .then(pl.col("close"))
                .otherwise(pl.col("high"))
                .alias("high"),
                pl.when(pl.col("low") == 0)
                .then(pl.col("close"))
                .otherwise(pl.col("low"))
                .alias("low"),
            ]
        )

        kdf = kdf.with_columns(
            [
                pl.col("aggregate_trade_id").cast(pl.Int32),
                pl.col("first_trade_id").cast(pl.Int32),
                pl.col("last_trade_id").cast(pl.Int32),
                pl.col("total_trades").cast(pl.Int32),
            ]
        )
        kdf = kdf.drop(["aggregate_trade_id", "first_trade_id", "last_trade_id"])
        return kdf.collect()


# 示例代码：如何使用上面的函数
# data = {'timestamp': [...], 'ibt': [...], 'bvol': [...], 'price': [...], 'id': [...], 'fid': [...], 'lid': [...]}
# at_df = pl.DataFrame(data)
# result = bn_aggTrades_to_tbar(at_df, bar_size='5s')


if __name__ == "__main__":
    Spot.all_aggtrades_to_kline()
