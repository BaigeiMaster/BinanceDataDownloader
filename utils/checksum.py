import os
from loguru import logger
import hashlib


class CheckSum:
    @staticmethod
    def verify_checksum(data_path: str) -> bool:
        checksum_path = data_path + ".CHECKSUM"
        if not os.path.exists(checksum_path):
            logger.error(f"Checksum file not exists {data_path}")
            return False

        try:
            with open(checksum_path, "r") as fin:
                text = fin.read()
            checksum_standard, _ = text.strip().split()
        except Exception:
            logger.error("Error reading checksum file", checksum_path)
            return False

        with open(data_path, "rb") as file_to_check:
            data = file_to_check.read()
            checksum_value = hashlib.sha256(data).hexdigest()

        if checksum_value != checksum_standard:
            logger.error(f"Checksum error {data_path}")
            return False

        return True
