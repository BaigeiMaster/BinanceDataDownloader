import httpx
import asyncio
import os
import xmltodict
from typing import Union
from loguru import logger

# ==== Customized Modules ====
from downloader.enums import BINANCE_DATA_URLS, BINANCE_DATA_PATH
from .web_tools import WebGet


class Binance:
    """
    Binance Path Tool
    """

    @staticmethod
    async def async_get_path_from_website(path: str) -> list[str]:
        """从币安数据网站获取文件路径

        Args:
            path (str): 基础路径, eg: data/option/daily/BVOLIndex/BTCBVOLUSDT/

        Returns:
            list[str]: 路径列表
        """
        url = BINANCE_DATA_URLS.path_api_url.value + path
        base_url = url
        results = []
        while True:
            data = xmltodict.parse(await WebGet.async_fetch_with_retry(url=url))
            xml_data = data["ListBucketResult"]
            if "CommonPrefixes" in xml_data:
                results.extend([x["Prefix"] for x in xml_data["CommonPrefixes"]])
            elif "Contents" in xml_data:
                results.extend([x["Key"] for x in xml_data["Contents"]])
            if xml_data["IsTruncated"] == "false":
                break
            url = base_url + "&marker=" + xml_data["NextMarker"]
        return results

    @staticmethod
    async def async_get_data_frequency(
        symbol_type: str, agg_period: str, data_type: Union[str, list, None]
    ) -> list:
        path_map = {
            "spot_daily": BINANCE_DATA_PATH.spot.value + "daily/",
            "spot_monthly": BINANCE_DATA_PATH.spot.value + "monthly/",
            "futures_cm_daily": BINANCE_DATA_PATH.features_cm.value + "daily/",
            "futures_cm_monthly": BINANCE_DATA_PATH.features_cm.value + "monthly/",
            "futures_um_daily": BINANCE_DATA_PATH.features_um.value + "daily/",
            "futures_um_monthly": BINANCE_DATA_PATH.features_um.value + "monthly/",
            "option_daily": BINANCE_DATA_PATH.option.value + "daily/",
        }
        assert symbol_type in [
            "spot",
            "futures_cm",
            "futures_um",
            "option",
        ], "type_ must be one of ['spot', 'futures_cm', 'futures_um', 'option']"
        assert agg_period in [
            "daily",
            "monthly",
        ], "agg_period must be one of ['daily', 'monthly']"
        url = path_map[symbol_type + "_" + agg_period]

        if data_type:
            if isinstance(data_type, str):
                url += data_type + "/"
                url = [url]
            elif isinstance(data_type, list):
                url = [url]
                url = [u + dt + "/" for u in url for dt in data_type]
            return url

        return await Binance.async_get_path_from_website(url)

    @staticmethod
    def get_data_frequency(
        type_: str, agg_period: str, data_type: Union[str, list]
    ) -> list[str]:
        """获取数据路径

        Args:
            type_ (str): "spot","futures_cm","futures_um","option"
            agg_period (str): "daily","monthly"
            data_type (Union[str, list]): "klines","aggTrades","trades",...

        Returns:
            list:
        """
        path_map = {
            "spot_daily": BINANCE_DATA_PATH.spot.value + "daily/",
            "spot_monthly": BINANCE_DATA_PATH.spot.value + "monthly/",
            "futures_cm_daily": BINANCE_DATA_PATH.features_cm.value + "daily/",
            "futures_cm_monthly": BINANCE_DATA_PATH.features_cm.value + "monthly/",
            "futures_um_daily": BINANCE_DATA_PATH.features_um.value + "daily/",
            "futures_um_monthly": BINANCE_DATA_PATH.features_um.value + "monthly/",
            "option_daily": BINANCE_DATA_PATH.option.value + "daily/",
        }
        assert type_ in [
            "spot",
            "futures_cm",
            "futures_um",
            "option",
        ], "type_ must be one of ['spot', 'futures_cm', 'futures_um', 'option']"
        assert agg_period in [
            "daily",
            "monthly",
        ], "agg_period must be one of ['daily', 'monthly']"
        url = path_map[type_ + "_" + agg_period]

        if isinstance(data_type, str):
            url += data_type + "/"
            url = [url]
        elif isinstance(data_type, list):
            url = [url]
            url = [u + dt + "/" for u in url for dt in data_type]
        return url


class Local:
    """
    Local Path Tool
    """

    @staticmethod
    def get_file_path_from_dir(dir_path: str) -> list:
        file_paths = []  # 用于存储文件路径的列表
        for root, _, files in os.walk(dir_path):
            for file in files:
                # 构建文件的完整路径
                file_path = os.path.join(root, file)
                file_paths.append(file_path)
        return file_paths

    @staticmethod
    def get_dir_path_from_dir(dir_path: str) -> list:
        dir_paths = []  # 用于存储文件夹路径的列表
        for root, dirs, _ in os.walk(dir_path):
            for d in dirs:
                # 构建文件夹的完整路径
                dir_path = os.path.join(root, d)
                dir_paths.append(dir_path)
        return dir_paths

    @staticmethod
    def remove_subpath(full_path, subpath_to_remove):
        """
        从完整路径中删除指定子路径
        :param full_path: 完整路径
        :param subpath_to_remove: 需要删除的子路径
        :return: 删除子路径后的新路径
        """
        # 将路径标准化，以确保一致性
        full_path = os.path.normpath(full_path)
        subpath_to_remove = os.path.normpath(subpath_to_remove)

        # 查找子路径在完整路径中的位置
        index = full_path.find(subpath_to_remove)

        if index != -1:
            # 删除子路径并返回新的路径
            new_path = full_path[:index] + full_path[index + len(subpath_to_remove) :]

            # 删除多余的路径分隔符
            new_path = os.path.normpath(new_path)
            return new_path
        else:
            # 如果子路径不在完整路径中，返回原路径
            return full_path


if __name__ == "__main__":
    bn = Binance()
    # print(asyncio.run(bn.async_get_path_from_website("data/futures/um/daily/"))[:10])
    # print(asyncio.run(bn.async_get_data_frequency("futures_um", "daily")))
    lo = Local()
    # print(lo.get_file_path_from_dir("/data/crypto_data"))
    print(lo.remove_subpath("./data/aaa/data/bbb/ccc", "./data/aaa/data"))
