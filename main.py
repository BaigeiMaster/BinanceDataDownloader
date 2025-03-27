from downloader.downloader import Downloader
from downloader.release import Release
from data_transformer import aggtrades_to_kline
import asyncio

asyncio.run(
    Downloader().create_copy(
        "spot",
        "monthly",
        "1m",
        data_type="trades",
        trading_pair="BTCUSDT",
        skip_checksum=False,
        start_date="2023-01",
        end_date="2024-04",
    )
)
# Release.release_binance_data()
# aggtrades_to_kline.Spot.all_aggtrades_to_kline("1m", "monthly")
# aggtrades_to_kline.Spot.from_file("LISTAUSDT", "1m", "monthly")
