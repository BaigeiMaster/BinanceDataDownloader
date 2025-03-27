from loguru import logger
import os

# ==== Customized Modules ====
from .paths import LOGS_PATH
from .config_loader import ConfigLoader
from .path_tools import Local as PathLocal
from .path_tools import Binance as PathBinance
from .time_tools import TimeTools
from .web_tools import WebGet
from .checksum import CheckSum

# ===========日志初始化=============
# 移除所有默认的日志记录器
logger.remove()

logger.add(
    os.path.join(LOGS_PATH, "error.log"),
    format="<red>{time:YYYY-MM-DD HH:mm:ss}</red> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    rotation="10 MB",
    enqueue=True,
    backtrace=True,
    level="ERROR",
)

logger.add(
    os.path.join(LOGS_PATH, "all.log"),
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} - {message}",
    rotation="10 MB",
    enqueue=True,
    backtrace=True,
    level="DEBUG",
)
