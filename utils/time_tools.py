import re
from datetime import datetime
from typing import Union

class TimeTools:
    @staticmethod
    def find_date_format(date_string: str):
        if re.findall(r"\d{4}-\d{2}-\d{2}", date_string):
            return "ymd"
        if re.findall(r"\d{4}-\d{2}", date_string):
            return "ym"
        else:
            raise ValueError("Invalid date format")

    @staticmethod
    def _date_range_filter(file_path: str, start_date: datetime, end_date: datetime):
        if TimeTools.find_date_format(file_path) == "ymd":
            target_date = datetime.strptime(
                re.findall(r"\d{4}-\d{2}-\d{2}", file_path)[0], "%Y-%m-%d"
            )
        elif TimeTools.find_date_format(file_path) == "ym":
            target_date = datetime.strptime(
                re.findall(r"\d{4}-\d{2}", file_path)[0], "%Y-%m"
            )
        else:
            raise ValueError("Invalid file date format")

        if target_date >= start_date and target_date <= end_date:
            return True
        else:
            return False

    @staticmethod
    def time_filter(
        start_date: Union[str, None], end_date: Union[str, None], path_list: list[str]
    ) -> list[str]:
        """根据日期区间过滤文件

        Args:
            start_date (Union[str, None]):
            end_date (Union[str, None]):
            path_list (list[str]):

        Raises:
            ValueError: 当日期格式错误时抛出

        Returns:
            list[str]:
        """
        if start_date is not None or end_date is not None:
            if start_date is None:
                start_date = "1990-01-01"
            if end_date is None:
                end_date = "2050-01-01"
            start_date = (
                datetime.strptime(start_date, "%Y-%m-%d")
                if TimeTools.find_date_format(start_date) == "ymd"
                else datetime.strptime(start_date, "%Y-%m")
            )
            end_date = (
                datetime.strptime(end_date, "%Y-%m-%d")
                if TimeTools.find_date_format(end_date) == "ymd"
                else datetime.strptime(end_date, "%Y-%m")
            )
            if not isinstance(start_date, datetime) or not isinstance(
                end_date, datetime
            ):
                raise ValueError("Invalid date format")

            return [
                path
                for path in path_list
                if TimeTools._date_range_filter(path, start_date, end_date)
            ]
        else:
            return path_list
