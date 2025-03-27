from gospeed_api.index import GospeedClient, AsyncGospeedClient
import os
from gospeed_api.models import TASK_STATUS
import asyncio
from loguru import logger

# ==== Customized Modules ====
from utils import ConfigLoader

config = ConfigLoader.load_config()


class SyncGospeedClientInterface:
    """Initialize object with api address."""

    client = GospeedClient("http://127.0.0.1:9999/")

    def get_server_info(self):
        """test get server info function"""
        from gospeed_api.models.get_server_info import GetServerInfo_Response

        res: GetServerInfo_Response = self.client.get_server_info()
        # If response.code property == 0, it means everything working fine.
        assert res.code == 0, "Server is not working fine."


class AsyncGospeedInterface:
    """Initialize object with api address."""

    def __init__(self) -> None:
        self.tasks: list[dict] = []
        self.pos = 0
        self.retry_pos = 0
        self.failed_tasks: list[dict] = []
        self.task_done = False
        self.failed_task_done = False
        self.ridsmap: list[dict] = []  # {rid, url, save_dir}
        self.save_dir = ""
        self.max_download_tasks = config["max_download_tasks"]

    async_client = AsyncGospeedClient("http://127.0.0.1:9999/")

    async def retry_gather(self):
        """重试调度下载任务"""
        try:
            running_tasks = await self.async_get_task_list(TASK_STATUS.RUNNING)
            if len(running_tasks) < self.max_download_tasks:
                tasks = [
                    self.async_create_a_task(task["url"], task["save_dir"])
                    for task in self.failed_tasks[
                        self.retry_pos : self.retry_pos
                        + self.max_download_tasks
                        - len(running_tasks)
                    ]
                ]
                if self.retry_pos < len(self.failed_tasks):
                    for coro in asyncio.as_completed(tasks):
                        await coro
                else:
                    self.failed_task_done = True
                self.retry_pos += self.max_download_tasks - len(running_tasks)
        except Exception as e:
            logger.error(e)

    async def gather(self):
        """调度下载任务"""
        try:
            running_tasks = await self.async_get_task_list(TASK_STATUS.RUNNING)
            if len(running_tasks) < self.max_download_tasks:
                tasks = [
                    self.async_create_a_task(task["url"], task["save_dir"])
                    for task in self.tasks[
                        self.pos : self.pos
                        + self.max_download_tasks
                        - len(running_tasks)
                    ]
                ]
                if self.pos < len(self.tasks):
                    for coro in asyncio.as_completed(tasks):
                        await coro
                else:
                    self.task_done = True
                self.pos += self.max_download_tasks - len(running_tasks)
        except Exception as e:
            logger.error(e)

    async def async_get_task_list(self, status):
        """获取任务列表

        Args:
            status (_type_): 任务状态

        Returns:
            list:
        """
        from gospeed_api.models.get_task_list import GetTaskList_Response

        data: GetTaskList_Response = await self.async_client.async_get_task_list(
            status={status}
        )
        assert data.code == 0, "Cannot get task list."
        return data.data

    async def async_delete_all_tasks(self):
        """删除所有任务"""
        # invoke delete all tasks api
        await self.async_client.async_delete_tasks(
            force=False
        )  # leave status param to None, means delete all tasks inside downloader no matter what status it is.
        await asyncio.sleep(1)

        # check if have any task exists
        res = await self.async_client.async_get_task_list()
        assert len(res.data) == 0, "There are still tasks in downloader."

    async def async_create_a_task(self, url: str, save_dir: str):
        """创建gospeed任务

        Args:
            url (str): 下载链接
            save_dir (str): 保存至(根目录为docker启动时的挂载点)
        """
        from gospeed_api.models.resolve_a_request import ResolveRequest
        from gospeed_api.models.create_a_task import (
            CreateTask_DownloadOpt,
            CreateATask_fromResolvedId,
        )
        from gospeed_api.models import TASK_STATUS

        try:
            id_resolve_response = await self.async_client.async_resolve_a_request(
                ResolveRequest(url=url)
            )
            if id_resolve_response.code != 0:
                logger.error(f"Cannot resolve resource {url}")
                self.failed_tasks.append({"url": url, "save_dir": save_dir})
                return

            # Create download task from resolved id
            rid = id_resolve_response.data.id
            filename = id_resolve_response.data.res.files[0].name
            opt = CreateTask_DownloadOpt(
                # path前加docker指定位置
                name=filename,
                path=os.path.join("/app/Downloads/", save_dir),
            )
            task = await self.async_client.async_create_a_task_from_resolved_id(
                CreateATask_fromResolvedId(rid=rid, opt=opt)
            )
            if task.code != 0:
                logger.error(f"Cannot create task {url}")
                self.failed_tasks.append({"url": url, "save_dir": save_dir})
                return

            self.ridsmap.append({"rid": task.data, "url": url, "save_dir": save_dir})

        except Exception as e:
            logger.error(f"Download {url} failed. exception: {e}")
            self.failed_tasks.append({"url": url, "save_dir": save_dir})
            return

    async def get_task_info(self):
        for data in self.ridsmap:
            rid = data["rid"]
            url = data["url"]
            save_dir = data["save_dir"]
            task_info = await self.async_client.async_get_task_info(rid=rid)
            if task_info.data.status == TASK_STATUS.DONE:
                logger.info(f"Download {url} done.")
            if task_info.data.status == TASK_STATUS.ERROR:
                logger.error(f"Download {url} failed.")
                self.failed_tasks.append({"url": url, "save_dir": save_dir})
        self.ridsmap = []
