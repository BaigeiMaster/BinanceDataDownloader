from enum import Enum


class BINANCE_DATA_URLS(Enum):
    path_api_url: str = (
        "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix="
    )
    init_url: str = "https://data.binance.vision/?prefix="
    download_url: str = "https://data.binance.vision/"


class BINANCE_DATA_PATH(Enum):
    root_path: str = "data/"
    features_cm: str = root_path + "futures/cm/"
    features_um: str = root_path + "futures/um/"
    spot: str = root_path + "spot/"
    option: str = root_path + "option/"


class BINANCE_SPOT_TIME_COLUMNS(Enum):
    aggTrades: dict = {"timestamp": "ms"}
    trades: dict = {"timestamp": "ms"}


class BINANCE_SPOT_HEADERS(Enum):
    aggTrades: dict = {
        "column_1": "aggregate_trade_id",
        "column_2": "price",
        "column_3": "quantity",
        "column_4": "first_trade_id",
        "column_5": "last_trade_id",
        "column_6": "timestamp",
        "column_7": "was_the_buyer_the_maker",
        "column_8": "was_the_trade_the_best_price_match",
    }
    klines: dict = {
        "column_1": "Open time",
        "column_2": "Open",
        "column_3": "High",
        "column_4": "Low",
        "column_5": "Close",
        "column_6": "Volume",
        "column_7": "Close time",
        "column_8": "Quote asset volume",  # 支付的报价资产总额除以基础资产总量
        "column_9": "Number of trades",
        "column_10": "Taker buy base asset volume",  # 主动买入量，指在该时间周期内，由主动买方推动的交易量。
        "column_11": "Taker buy quote asset volume",  # 主动买入额，以报价货币计价的主动买入成交总额。
        "column_12": "Ignore",
    }
    trades: dict = {
        "column_1": "trade Id",
        "column_2": "price",
        "column_3": "qty",
        "column_4": "quoteQty",
        "column_5": "timestamp",
        "column_6": "isBuyerMaker",
        "column_7": "isBestMatch",
    }


if __name__ == "__main__":
    pass
