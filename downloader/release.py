from loguru import logger
from joblib import Parallel, delayed
from tqdm import tqdm
import zipfile
import os
from typing import Union
import polars as pl


# ==== Customized Modules ====
from utils import ConfigLoader, TimeTools, CheckSum
from .enums import BINANCE_SPOT_HEADERS, BINANCE_SPOT_TIME_COLUMNS  # noqa
from utils import PathLocal


config = ConfigLoader().load_config()


class Release:
    @staticmethod
    def unzip(zip_file: str, skip_existed=True) -> Union[None, str]:
        """解压zip文件"""
        try:
            with zipfile.ZipFile(zip_file, "r") as zip_ref:
                output_dir = os.path.dirname(zip_file)
                output_dir = os.path.join(
                    config["save_released_data_dir"],
                    # 删掉前面的/
                    output_dir.replace(config["save_downloaded_data_dir"] + "/", ""),
                )
                if (
                    os.path.exists(
                        os.path.join(
                            output_dir,
                            os.path.basename(zip_file).replace(".zip", ".csv"),
                        )
                    )
                    and skip_existed
                ):
                    return None
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir)
                zip_ref.extractall(output_dir)
            return os.path.join(
                output_dir, os.path.basename(zip_file).replace(".zip", ".csv")
            )
        except Exception as e:
            print(zip_file)
            print(e)

    @staticmethod
    def save_parquet(csv_path: str, save_path: str):
        """csv转为parquet

        Args:
            csv_path (str):
            save_path (str):
        """
        headers: dict[str] = {}
        symbol_type: str = ""
        data_frequency: str = ""
        if "spot" in csv_path:
            symbol_type = "SPOT"
            if "aggTrades" in csv_path:
                data_frequency = "aggTrades"
            if "trades" in csv_path:
                data_frequency = "trades"
            if "klines" in csv_path:
                # data_frequency = "klines"
                return
        if not symbol_type or not data_frequency:
            raise ValueError(
                f"Unknown symbol type: {symbol_type} or data frequency: {data_frequency}"
            )

        # 根据文件类型生成文件头和时间列
        headers = eval(f"BINANCE_{symbol_type}_HEADERS.{data_frequency}.value")
        to_dt = [
            pl.from_epoch(pl.col({dt_col}), time_unit=tu)
            for dt_col, tu in eval(
                f"BINANCE_{symbol_type}_TIME_COLUMNS.{data_frequency}.value"
            ).items()
        ]

        df = pl.read_csv(csv_path, has_header=False)
        df = df.rename(headers).with_columns(to_dt)
        # 加一列symbol
        df = df.with_columns(pl.lit(csv_path.split("/")[-2]).alias("symbol"))
        # 重新排列列的顺序
        column_order = ["symbol"] + [col for col in df.columns if col != "symbol"]
        df = df.select(column_order)

        df.write_parquet(save_path)

    @staticmethod
    def zip2parquet(zip_file: str, skip_existed=True):
        csv_path: Union[str, None] = Release.unzip(zip_file, skip_existed)
        if csv_path is None:
            return
        Release.save_parquet(csv_path, save_path=csv_path.replace(".csv", ".parquet"))
        # 删除原始csv文件
        os.remove(csv_path)

    @staticmethod
    def release_binance_data(
        key_words: Union[str, list, None] = None,
        start_date: Union[None, str] = None,
        end_date: Union[None, str] = None,
        skip_existed: bool = True,
        skip_checksum: bool = False,
    ):
        """解压币安数据

        Args:
            key_words (Union[str, list, None], optional): 只有包含key_words的文件才会被解压. Defaults to None.
            start_date (Union[None, str], optional): 开始日期. Defaults to None.
            end_date (Union[None, str], optional): 结束日期. Defaults to None.
            skip_existed (bool, optional): 跳过已存在文件. Defaults to True.
            skip_checksum (bool, optional): 跳过校验和. Defaults to False.
        """
        if isinstance(key_words, str):
            key_words = [key_words]

        save_download_data_dir: str = config["save_downloaded_data_dir"]
        # 遍历文件夹 找到所有压缩包
        zip_file_paths: list = []
        for root, _, files in os.walk(save_download_data_dir):
            for file in files:
                if file.endswith(".zip"):
                    zip_file_paths.append(os.path.join(root, file))

        # 删除下载重复的文件
        # 假设不会有重复超过100次的文件
        unnecessary_file_symbol: list = [f"({n})" for n in range(1, 100)]
        need_delete_files = [
            p for p in zip_file_paths if any(x in p for x in unnecessary_file_symbol)
        ]
        if need_delete_files:
            for file in tqdm(need_delete_files, desc="Deleting duplicated files"):
                os.remove(file)
                logger.info(f"Delete duplicated file: {file}")

        # 跳过不含key_words的文件
        if key_words:
            zip_file_paths = [
                p for p in zip_file_paths if any(kw in p for kw in key_words)
            ]

        # 跳过本地文件
        if skip_existed:
            local_file_paths: list = PathLocal.get_file_path_from_dir(
                config["save_released_data_dir"]
            )
            # 只保留自动生成的文件夹
            local_file_paths = [p for p in local_file_paths if "customized" not in p]
            # 去除文件拓展名和统一路径
            local_file_paths = [
                PathLocal.remove_subpath(
                    os.path.splitext(file)[0], config["save_released_data_dir"]
                )
                for file in local_file_paths
            ]
            zip_file_paths = [
                PathLocal.remove_subpath(
                    os.path.splitext(file)[0], config["save_downloaded_data_dir"]
                )
                for file in zip_file_paths
            ]
            _ = zip_file_paths
            zip_file_paths = list(set(zip_file_paths) - set(local_file_paths))
            print(f"Skip {len(_) - len(zip_file_paths)} existed files.")
            logger.info(f"Skip {len(_) - len(zip_file_paths)} existed files.")
            zip_file_paths = [
                os.path.join(config["save_downloaded_data_dir"], p[1:]) + ".zip"
                for p in zip_file_paths
            ]

        # 日期区间过滤
        if start_date is not None or end_date is not None:
            zip_file_paths = TimeTools.time_filter(start_date, end_date, zip_file_paths)

        # 检查所有文件的校验和
        if not skip_checksum:
            bool_list: list[bool] = Parallel(
                n_jobs=config["parallel_n_jobs"], prefer="threads"
            )(
                delayed(CheckSum.verify_checksum)(p)
                for p in tqdm(zip_file_paths, desc="Checking checksum")
            )
            skip_paths: list[str] = [
                p for p, b in zip(zip_file_paths, bool_list) if not b
            ]
            if len(skip_paths) != 0:
                print(f"Skip {len(skip_paths)} invalid files. Delete them.")
                logger.info(f"Skip {len(skip_paths)} invalid files. Delete them.")
                for file in skip_paths:
                    os.remove(file)
                    if os.path.exists(file.replace(".zip", ".CHECKSUM")):
                        os.remove(file.replace(".zip", ".CHECKSUM"))
                zip_file_paths = list(set(zip_file_paths) - set(skip_paths))

        # 并行解压
        Parallel(n_jobs=config["parallel_n_jobs"], backend="threading")(
            delayed(Release.zip2parquet)(zip_file, skip_existed)
            for zip_file in tqdm(zip_file_paths, desc="Release and save parquet")
        )
        # for p in zip_file_paths:
        #     Release.zip2parquet(p, skip_existed)


if __name__ == "__main__":
    # Release.unzip(
    #     "/home/zhrdai/projects/binance_downloader/BTCUSDT/BTCUSDT-aggTrades-2017-08.zip"
    # )
    # Release.unzip(
    #     "/data/crypto_data/binance_data/data/spot/monthly/aggTrades/ATOMBTC/ATOMBTC-aggTrades-2020-09.zip"
    # )
    pass
