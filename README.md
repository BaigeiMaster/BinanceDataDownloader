# binance_downloader
币安公开数据下载器，可创建 https://data.binance.vision/?prefix=data 的本地副本

## 使用方式
### 环境配置
1. 建议使用uv配置环境。linux: curl -LsSf https://astral.sh/uv/install.sh | sh windows: powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
2. 在项目目录中执行 uv sync
### 下载器配置
1. 安装gopeed下载器，建议使用docker安装。安装方式参照 https://github.com/GopeedLab/gopeed 
### 开始使用
1. uv run main.py