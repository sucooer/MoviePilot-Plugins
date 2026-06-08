import hashlib
import os
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app import schemas
from app.core.config import settings
from app.core.event import Event, eventmanager
from app.log import logger
from app.plugins import _PluginBase
from app.utils.http import RequestUtils
from app.utils.url import UrlUtils

from app.helper.storage import StorageHelper
from app.schemas import FileItem, StorageConf, TransferRenameEventData
from app.schemas.types import ChainEventType, MediaType, NotificationType
from app.core.cache import TTLCache


class OpenListMonitor(_PluginBase):
    """
    监控 OpenList 目录变化，提交新增文件给 MoviePilot 整理。
    """

    plugin_name = "OpenList 目录监控"
    plugin_desc = "监控 OpenList 目录变化，提交新增文件给 MoviePilot 做网盘内远程整理。"
    plugin_icon = "https://raw.githubusercontent.com/sucooer/MoviePilot-Plugins/main/icons/OpenList.png"
    plugin_version = "0.3.7"
    plugin_author = "sucooer"
    author_url = "https://github.com/sucooer/MoviePilot-Plugins"
    plugin_config_prefix = "openlistmonitor_"
    plugin_order = 54
    auth_level = 1

    _alist_token_cache = TTLCache(region="openlist_monitor", maxsize=32, ttl=3600)

    _enabled = False
    _onlyonce = False
    _cron = ""
    _paths = ""
    _target_storage = "alist"
    _target_path = ""
    _target_path_rules = ""
    _transfer_type = "move"
    _media_types = []
    _library_type_folder = False
    _library_category_folder = False
    _top_level_categories = "番剧"
    _background_transfer = False
    _sync_extra_files = True
    _scrape = False
    _recursive = True
    _max_depth = 5
    _delay_seconds = 1.0
    _api_interval_seconds = 0.5
    _transfer_interval_seconds = 3.0
    _max_files_per_run = 20
    _min_file_size_mb = 10
    _extensions = ""
    _refresh = False
    _clean_empty_dirs = False
    _clean_residual_files = True
    _residual_file_extensions = ""
    _residual_file_max_size_mb = 20
    _notify = True
    _scheduler: Optional[BackgroundScheduler] = None
    _event = threading.Event()
    _lock = threading.Lock()
    _running = False

    STORE_RESULT_KEY = "last_result"
    STORE_TRANSFERRED_KEY = "transferred_files"
    LEGACY_PLUGIN_ID = "AlistMonitor"

    OPENLIST_MAX_LIST_PAGE_SIZE = 500
    DIRECTORY_VISIBLE_RETRIES = 6
    DIRECTORY_VISIBLE_INTERVAL = 2

    VIDEO_EXTENSIONS = {
        ".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm", ".m4v",
        ".ts", ".m2ts", ".iso", ".bdmv", ".mpls",
        ".rmvb", ".3gp", ".vob", ".mpeg", ".mpg", ".asf", ".strm", ".tp", ".f4v",
    }
    DEFAULT_RESIDUAL_FILE_EXTENSIONS = ".jpg,.jpeg,.png,.webp,.gif,.bmp,.txt,.nfo"

    def __init__(self):
        super().__init__()
        self._last_openlist_request_at = 0.0
        self._last_transfer_submit_at = 0.0
        self._rate_limit_lock = threading.Lock()

    def init_plugin(self, config: dict = None):
        self.stop_service()

        config = self._migrate_legacy_plugin_state(config or {})
        self._enabled = bool(config.get("enabled", False))
        self._onlyonce = bool(config.get("onlyonce", False))
        self._cron = str(config.get("cron") or "").strip()
        self._paths = str(config.get("paths") or "").strip()
        self._target_storage = str(config.get("target_storage") or "alist").strip()
        self._target_path = str(config.get("target_path") or "").strip()
        self._target_path_rules = str(config.get("target_path_rules") or "").strip()
        self._transfer_type = str(config.get("transfer_type") or "move").strip()
        if self._transfer_type not in {"move", "copy"}:
            self._transfer_type = "move"
        self._media_types = self._parse_media_types(config.get("media_types"))
        self._library_type_folder = bool(config.get("library_type_folder", False))
        self._library_category_folder = bool(config.get("library_category_folder", False))
        self._top_level_categories = str(
            config.get("top_level_categories", "番剧") or ""
        ).strip()
        self._background_transfer = bool(config.get("background_transfer", False))
        self._sync_extra_files = bool(config.get("sync_extra_files", True))
        self._scrape = bool(config.get("scrape", False))
        self._recursive = bool(config.get("recursive", True))
        self._refresh = bool(config.get("refresh", False))
        self._clean_empty_dirs = bool(config.get("clean_empty_dirs", True))
        self._clean_residual_files = bool(config.get("clean_residual_files", True))
        self._residual_file_extensions = str(
            config.get("residual_file_extensions")
            or self.DEFAULT_RESIDUAL_FILE_EXTENSIONS
        ).strip()
        self._residual_file_max_size_mb = self._to_float(
            config.get("residual_file_max_size_mb"), 20, 1, 1024
        )
        self._notify = bool(config.get("notify", True))
        self._max_depth = self._to_int(config.get("max_depth"), 5, 0, 20)
        self._delay_seconds = self._to_float(config.get("delay_seconds"), 1.0, 0, 30)
        self._api_interval_seconds = self._to_float(
            config.get("api_interval_seconds"), 0.5, 0, 60
        )
        self._transfer_interval_seconds = self._to_float(
            config.get("transfer_interval_seconds"), 3.0, 0, 600
        )
        self._max_files_per_run = self._to_int(
            config.get("max_files_per_run"), 20, 1, 500
        )
        self._min_file_size_mb = self._to_int(config.get("min_file_size_mb"), 10, 1, 100000)
        self._extensions = str(config.get("extensions") or "").strip()
        self._migrate_legacy_path_config(config)

        if not self._enabled and not self._onlyonce:
            logger.info("【OpenList 目录监控】插件未启用")
            return

        if self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(
                func=self.run_check,
                trigger="date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                name="OpenList 目录监控立即执行",
            )
            self._scheduler.start()
            config["onlyonce"] = False
            self._onlyonce = False
            self.update_config(config)

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/check",
                "endpoint": self.api_check,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "立即检查目录变化",
                "description": "立即检查所有配置的 OpenList 目录是否有新文件。",
            },
            {
                "path": "/status",
                "endpoint": self.api_status,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "获取最近检查结果",
                "description": "获取最近一次目录检查的结果。",
            },
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        services = []
        if not self._enabled:
            return services
        if self._cron:
            services.append({
                "id": "OpenListMonitor",
                "name": "OpenList 目录监控定时检查",
                "trigger": CronTrigger.from_crontab(self._cron),
                "func": self.run_check,
                "kwargs": {},
            })
        return services

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        transfer_type_items = [
            {"title": "移动", "value": "move"},
            {"title": "复制", "value": "copy"},
        ]
        media_type_items = [
            {"title": MediaType.TV.value, "value": MediaType.TV.value},
            {"title": MediaType.MOVIE.value, "value": MediaType.MOVIE.value},
        ]
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "enabled", "label": "启用插件"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "onlyonce", "label": "立即运行一次"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "recursive", "label": "递归扫描"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "refresh",
                                            "label": "强制刷新缓存",
                                            "hint": "调用 OpenList 列目录时携带 refresh=true",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "clean_residual_files",
                                            "label": "清理残留文件",
                                            "hint": "目录内已无视频时，按白名单删除图片、说明等残留文件",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "residual_file_extensions",
                                            "label": "残留文件后缀",
                                            "placeholder": self.DEFAULT_RESIDUAL_FILE_EXTENSIONS,
                                            "hint": "逗号或换行分隔；默认不包含字幕，视频文件始终不会清理",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "residual_file_max_size_mb",
                                            "label": "残留文件最大MB",
                                            "type": "number",
                                            "min": 1,
                                            "hint": "超过该大小的文件不会自动删除",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "media_types",
                                            "label": "媒体类型",
                                            "items": media_type_items,
                                            "multiple": True,
                                            "chips": True,
                                            "hint": "使用 MoviePilot 识别结果过滤，默认同时处理电视剧和电影",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "library_type_folder",
                                            "label": "媒体类型目录",
                                            "hint": "使用 MoviePilot 媒体类型，在目标目录下创建 电视剧/电影",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "library_category_folder",
                                            "label": "二级分类目录",
                                            "hint": "使用 MoviePilot 分类策略，在媒体类型目录下创建日剧、动画等分类",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "top_level_categories",
                                            "label": "顶层分类",
                                            "placeholder": "番剧",
                                            "hint": "逗号或换行分隔；命中后整理到目标目录下同名顶层目录",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "max_files_per_run",
                                            "label": "单轮处理上限",
                                            "type": "number",
                                            "min": 1,
                                            "hint": "每次检查最多提交整理的文件数，剩余文件下轮继续处理",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "api_interval_seconds",
                                            "label": "API间隔秒",
                                            "type": "number",
                                            "min": 0,
                                            "step": 0.1,
                                            "hint": "OpenList 列目录、创建目录、删除目录等 API 调用间隔",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "transfer_interval_seconds",
                                            "label": "提交间隔秒",
                                            "type": "number",
                                            "min": 0,
                                            "step": 0.1,
                                            "hint": "相邻整理提交之间的间隔",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "target_path_rules",
                                            "label": "监控整理目录",
                                            "placeholder": "/网盘A/下载中 => /网盘A/媒体库\n/网盘B/下载中 => /网盘B/媒体库",
                                            "rows": 4,
                                            "hint": "一行一个源目录 => 目标媒体目录，左边扫描，右边作为整理目标",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "transfer_type",
                                            "label": "整理方式",
                                            "items": transfer_type_items,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "background_transfer",
                                            "label": "后台整理",
                                            "hint": "提交到 MoviePilot 整理队列后立即返回",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "sync_extra_files",
                                            "label": "同步字幕音轨",
                                            "hint": "远程整理主视频时，让 MoviePilot 同步同媒体附加文件",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "scrape",
                                            "label": "刮削元数据",
                                            "hint": "开启后强制整理后刮削；关闭时沿用媒体库目录设置",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "clean_empty_dirs",
                                            "label": "清理空目录",
                                            "hint": "远程移动后删除监控根目录下的空子目录",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "notify",
                                            "label": "完成通知",
                                            "hint": "本轮有整理、清理或错误时发送 MoviePilot 通知",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "cron",
                                            "label": "检查周期",
                                            "placeholder": "*/5 * * * *",
                                            "hint": "cron 表达式，建议间隔 1-10 分钟",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 2},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "max_depth",
                                            "label": "递归深度",
                                            "type": "number",
                                            "min": 0,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 2},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "delay_seconds",
                                            "label": "目录间隔秒",
                                            "type": "number",
                                            "min": 0,
                                            "step": 0.1,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 2},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "min_file_size_mb",
                                            "label": "最小文件(MB)",
                                            "type": "number",
                                            "min": 1,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 2},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "extensions",
                                            "label": "文件后缀",
                                            "placeholder": ".mp4,.mkv,.ts",
                                            "hint": "留空使用默认视频格式",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VAlert",
                        "props": {
                            "type": "info",
                            "variant": "tonal",
                            "text": "插件会读取 MoviePilot 的 OpenList 存储配置。远程整理模式会把新增主视频提交给 MoviePilot 整理链，并由 OpenList 执行网盘内移动/重命名。",
                        },
                    },
                ],
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "cron": "*/5 * * * *",
            "paths": "",
            "target_storage": "alist",
            "target_path": "",
            "target_path_rules": "",
            "transfer_type": "move",
            "media_types": [MediaType.TV.value, MediaType.MOVIE.value],
            "library_type_folder": False,
            "library_category_folder": False,
            "top_level_categories": "番剧",
            "background_transfer": False,
            "sync_extra_files": True,
            "scrape": False,
            "recursive": True,
            "max_depth": 5,
            "delay_seconds": 1.0,
            "api_interval_seconds": 0.5,
            "transfer_interval_seconds": 3.0,
            "max_files_per_run": 20,
            "min_file_size_mb": 10,
            "extensions": "",
            "refresh": False,
            "clean_empty_dirs": True,
            "clean_residual_files": True,
            "residual_file_extensions": self.DEFAULT_RESIDUAL_FILE_EXTENSIONS,
            "residual_file_max_size_mb": 20,
            "notify": True,
        }

    def get_page(self) -> List[dict]:
        last_result = self.get_data(self.STORE_RESULT_KEY) or {}
        target_rules = self._get_monitor_target_rules()
        paths = self._get_monitor_paths(target_rules)
        status_items = [
            ("状态", "扫描中" if self._running else ("已启用" if self._enabled else "未启用")),
            ("处理方式", "远程整理"),
            ("检查周期", self._cron or "-"),
            ("监控整理目录数", str(len(target_rules))),
            ("监控整理目录", "、".join(self._format_target_path_rules(target_rules)) if target_rules else "-"),
            ("目标存储", self._format_storage_name(self._target_storage)),
            ("整理方式", self._transfer_type or "-"),
            ("媒体类型", "、".join(self._media_types) if self._media_types else "-"),
            ("媒体类型目录", "是" if self._library_type_folder else "否"),
            ("二级分类目录", "是" if self._library_category_folder else "否"),
            ("顶层分类", "、".join(sorted(self._get_top_level_categories())) or "-"),
            ("刮削元数据", "强制刮削" if self._scrape else "按媒体库目录设置"),
            ("单轮处理上限", str(self._max_files_per_run)),
            ("API间隔秒", str(self._api_interval_seconds)),
            ("提交间隔秒", str(self._transfer_interval_seconds)),
            ("清理空目录", "是" if self._should_clean_empty_dirs() else "否"),
            ("清理残留文件", "是" if self._should_clean_residual_files() else "否"),
            ("残留文件后缀", ",".join(sorted(self._get_residual_file_extensions())) or "-"),
            ("残留文件最大MB", str(self._residual_file_max_size_mb)),
            ("完成通知", "是" if self._notify else "否"),
            ("最近检查", str(last_result.get("time") or "-")),
            ("新文件数", str(last_result.get("new_files", 0))),
            ("延后处理数", str(last_result.get("limited_files", 0))),
            ("跳过媒体类型", str(last_result.get("skipped_type", 0))),
            ("已整理数", str(last_result.get("transferred", 0))),
            ("已清理残留文件", str(last_result.get("cleaned_files", 0))),
            ("已清理空目录", str(last_result.get("cleaned_dirs", 0))),
            ("最近结果", str(last_result.get("message") or "-")),
        ]
        return [
            {
                "component": "VContainer",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "监控 OpenList 目录变化，远程模式会提交给 MoviePilot 整理链，由 OpenList 在网盘内移动/重命名。",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VBtn",
                                        "props": {
                                            "color": "primary",
                                            "prepend-icon": "mdi-magnify-scan",
                                        },
                                        "text": "立即检查",
                                        "events": {
                                            "click": {
                                                "api": "plugin/OpenListMonitor/check",
                                                "method": "post",
                                            }
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    self._build_status_card("运行状态", status_items),
                                ],
                            }
                        ],
                    },
                ],
            }
        ]

    def stop_service(self):
        self._event.set()
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown(wait=False)
        except Exception as e:
            logger.debug("【OpenList 目录监控】停止调度器失败: %s", e)
        self._scheduler = None
        self._event.clear()

    def api_check(self) -> schemas.Response:
        success, message, data = self.run_check()
        return schemas.Response(success=success, message=message, data=data)

    def api_status(self) -> schemas.Response:
        return schemas.Response(success=True, data=self.get_data(self.STORE_RESULT_KEY) or {})

    @eventmanager.register(ChainEventType.TransferRename)
    def on_transfer_rename(self, event: Event) -> None:
        if not self._enabled:
            return
        data = event.event_data
        if not isinstance(data, TransferRenameEventData):
            return
        source_item: Optional[FileItem] = data.source_item
        if not source_item or source_item.storage != "alist":
            return
        cleaned = self._clean_render_path(data.render_str)
        if cleaned and cleaned != data.render_str:
            data.updated = True
            data.updated_str = cleaned
            data.source = self.plugin_name
            logger.info(
                "【OpenList 目录监控】清理整理目标路径换行: %s -> %s",
                data.render_str,
                cleaned,
            )

    def run_check(self) -> Tuple[bool, str, Dict[str, Any]]:
        if not self._lock.acquire(blocking=False):
            message = "已有检查任务正在运行"
            logger.warning("【OpenList 目录监控】%s", message)
            return False, message, {}

        self._running = True
        target_rules = self._get_monitor_target_rules()
        paths = self._get_monitor_paths(target_rules)
        stats = {
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "paths": [],
            "target_path_rules": target_rules,
            "dirs": 0,
            "files": 0,
            "new_files": 0,
            "limited_files": 0,
            "skipped_type": 0,
            "transferred": 0,
            "cleaned_files": 0,
            "cleaned_dirs": 0,
            "rate_limit": {
                "media_types": self._media_types,
                "library_type_folder": self._library_type_folder,
                "library_category_folder": self._library_category_folder,
                "top_level_categories": sorted(self._get_top_level_categories()),
                "scrape": self._scrape,
                "max_files_per_run": self._max_files_per_run,
                "api_interval_seconds": self._api_interval_seconds,
                "transfer_interval_seconds": self._transfer_interval_seconds,
                "clean_residual_files": self._should_clean_residual_files(),
                "residual_file_extensions": sorted(self._get_residual_file_extensions()),
                "residual_file_max_size_mb": self._residual_file_max_size_mb,
            },
            "errors": [],
            "message": "",
        }
        try:
            if not paths:
                return self._finish("未配置监控整理目录", stats)

            conf = self._get_alist_conf()
            if not conf:
                return self._finish("未找到 OpenList 存储配置", stats)
            base_url = self._get_alist_base_url(conf)
            headers = self._get_alist_auth_header(conf)
            if not base_url or not headers:
                return self._finish("OpenList 认证失败", stats)

            stats["paths"] = paths
            new_files_found = []
            scanned_dirs = []

            for path in paths:
                if self._event.is_set():
                    break
                files = self._scan_directory(
                    base_url, headers, path, 0, stats, scanned_dirs, path
                )
                new_files_found.extend(files)

            stats["new_files"] = len(new_files_found)

            if new_files_found:
                pending_files = new_files_found[:self._max_files_per_run]
                stats["limited_files"] = max(
                    len(new_files_found) - len(pending_files), 0
                )
                if stats["limited_files"]:
                    logger.info(
                        "【OpenList 目录监控】本轮限速处理 %s/%s 个文件，剩余 %s 个下轮继续",
                        len(pending_files),
                        len(new_files_found),
                        stats["limited_files"],
                    )
                transferred = self._transfer_files(pending_files, stats)
                stats["transferred"] = transferred

            if self._should_clean_empty_dirs() and scanned_dirs:
                cleaned_dirs, cleaned_files = self._cleanup_empty_source_dirs(
                    base_url=base_url,
                    headers=headers,
                    roots=paths,
                    scanned_dirs=scanned_dirs,
                )
                stats["cleaned_dirs"] = cleaned_dirs
                stats["cleaned_files"] = cleaned_files

            if stats["errors"]:
                message = (
                    f"检查完成，发现 {stats['new_files']} 个新文件，"
                    f"已整理 {stats['transferred']} 个"
                    f"{self._format_skipped_type(stats)}"
                    f"{self._format_limited_files(stats)}"
                    f"{self._format_cleaned_files(stats)}"
                    f"{self._format_cleaned_dirs(stats)}，"
                    f"但有 {len(stats['errors'])} 个错误"
                )
            else:
                message = (
                    f"检查完成，发现 {stats['new_files']} 个新文件，"
                    f"已整理 {stats['transferred']} 个"
                    f"{self._format_skipped_type(stats)}"
                    f"{self._format_cleaned_files(stats)}"
                    f"{self._format_cleaned_dirs(stats)}"
                    f"{self._format_limited_files(stats)}"
                )

            return self._finish(message, stats)

        finally:
            self._running = False
            self._lock.release()

    def _scan_directory(
        self, base_url: str, headers: Dict[str, str], path: str,
        depth: int, stats: Dict[str, Any], scanned_dirs: Optional[List[str]] = None,
        root_path: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        new_files = []
        clean_path = self._normalize_path(path)
        monitor_root = self._normalize_path(root_path or clean_path)
        if scanned_dirs is not None:
            scanned_dirs.append(clean_path)
        listing, error = self._list_directory(base_url, headers, clean_path)
        if error:
            stats["errors"].append({"path": clean_path, "error": error})
            logger.warning("【OpenList 目录监控】扫描失败: %s - %s", clean_path, error)
            return new_files

        stats["dirs"] += 1
        items = listing.get("files", [])
        dirs = [item for item in items if item.get("is_dir")]
        file_items = [item for item in items if not item.get("is_dir")]
        stats["files"] += len(file_items)

        processed_key = self._get_processed_store_key()
        already_processed = set(self.get_data(processed_key) or [])

        for item in file_items:
            name = str(item.get("name") or "").strip()
            if not name:
                continue
            size = int(item.get("size") or 0)
            size_mb = size / (1024 * 1024)
            if size_mb < self._min_file_size_mb:
                continue

            ext = os.path.splitext(name)[1].lower()
            if not self._is_media_ext(ext):
                continue

            item_path = (
                f"{clean_path.rstrip('/')}/{name}"
                if clean_path != "/"
                else f"/{name}"
            )
            key = self._record_key({"path": item_path, "name": name})
            if key in already_processed:
                if self._transfer_type == "move":
                    logger.info(
                        "【OpenList 目录监控】源文件仍在监控目录，重新提交已记录文件: %s",
                        item_path,
                    )
                else:
                    continue

            new_files.append({
                "path": item_path,
                "name": name,
                "size": size,
                "size_mb": round(size_mb, 1),
                "ext": ext,
                "monitor_root": monitor_root,
            })

        if self._recursive and depth < self._max_depth:
            for item in dirs:
                if self._event.is_set():
                    break
                name = str(item.get("name") or "").strip()
                if not name:
                    continue
                child_path = (
                    f"{clean_path.rstrip('/')}/{name}"
                    if clean_path != "/"
                    else f"/{name}"
                )
                if self._delay_seconds > 0 and self._event.wait(self._delay_seconds):
                    break
                child_files = self._scan_directory(
                    base_url, headers, child_path, depth + 1, stats, scanned_dirs,
                    monitor_root,
                )
                new_files.extend(child_files)

        return new_files

    def _transfer_files(self, files: List[Dict[str, Any]], stats: Dict[str, Any]) -> int:
        try:
            from app.chain.transfer import TransferChain
        except Exception as e:
            message = f"加载 MoviePilot 整理链失败: {e}"
            logger.error("【OpenList 目录监控】%s", message)
            stats["errors"].append({"path": "-", "error": message})
            return 0

        transferred_key = self.STORE_TRANSFERRED_KEY
        transferred_records = set(self.get_data(transferred_key) or [])
        target_storage = self._target_storage or "alist"
        default_target_path = None
        target_path_rules = self._get_monitor_target_rules()
        transfer_type = self._transfer_type or "move"
        transfer_chain = TransferChain()
        count = 0

        for file_info in files:
            if self._event.is_set():
                break
            try:
                fileitem = self._build_alist_fileitem(file_info)
                target_path = self._resolve_target_path(
                    file_info=file_info,
                    default_target_path=default_target_path,
                    target_path_rules=target_path_rules,
                )
                prepared, prepare_error, skipped_type, transfer_options = self._prepare_remote_transfer_dirs(
                    transfer_chain=transfer_chain,
                    fileitem=fileitem,
                    target_storage=target_storage,
                    target_path=target_path,
                    transfer_type=transfer_type,
                )
                if skipped_type:
                    stats["skipped_type"] = int(stats.get("skipped_type") or 0) + 1
                    logger.info(
                        "【OpenList 目录监控】跳过未选择媒体类型: %s - %s",
                        file_info["path"],
                        prepare_error,
                    )
                    continue
                if not prepared:
                    stats["errors"].append({
                        "path": file_info["path"],
                        "error": prepare_error,
                    })
                    logger.warning(
                        "【OpenList 目录监控】远程整理准备失败 %s: %s",
                        file_info["path"], prepare_error,
                    )
                    continue

                final_target_path = transfer_options.get("target_path")
                final_library_type_folder = transfer_options.get(
                    "library_type_folder", self._library_type_folder
                )
                final_library_category_folder = transfer_options.get(
                    "library_category_folder", self._library_category_folder
                )

                logger.info(
                    "【OpenList 目录监控】提交远程整理: %s -> [%s]%s (%s)，监控根目录：%s",
                    file_info["path"],
                    self._format_storage_name(target_storage),
                    final_target_path or "按 MoviePilot 目录规则",
                    transfer_type,
                    file_info.get("monitor_root") or "-",
                )
                if not self._wait_transfer_interval():
                    break
                state, message = transfer_chain.do_transfer(
                    fileitem=fileitem,
                    target_storage=target_storage,
                    target_path=final_target_path,
                    transfer_type=transfer_type,
                    library_type_folder=final_library_type_folder,
                    library_category_folder=final_library_category_folder,
                    scrape=True if self._scrape else None,
                    min_filesize=self._min_file_size_mb,
                    force=True,
                    background=self._background_transfer,
                    manual=False,
                    sync_extra_files=self._sync_extra_files,
                )
                if state:
                    transferred_records.add(self._record_key(file_info))
                    count += 1
                    logger.info("【OpenList 目录监控】已提交整理: %s", file_info["path"])
                else:
                    error = str(message or "整理失败")
                    stats["errors"].append({"path": file_info["path"], "error": error})
                    logger.warning(
                        "【OpenList 目录监控】远程整理失败 %s: %s",
                        file_info["path"], error,
                    )
            except Exception as e:
                stats["errors"].append({"path": file_info.get("path"), "error": str(e)})
                logger.error(
                    "【OpenList 目录监控】远程整理异常 %s: %s",
                    file_info.get("path"), e,
                )

        self.save_data(transferred_key, sorted(transferred_records))
        return count

    def _prepare_remote_transfer_dirs(
        self,
        transfer_chain: Any,
        fileitem: schemas.FileItem,
        target_storage: str,
        target_path: Optional[Path],
        transfer_type: str,
    ) -> Tuple[bool, str, bool, Dict[str, Any]]:
        transfer_options = {
            "target_path": target_path,
            "library_type_folder": self._library_type_folder,
            "library_category_folder": self._library_category_folder,
        }
        if target_storage != "alist" or fileitem.storage != "alist":
            return True, "", False, transfer_options

        state, preview_data = self._preview_remote_transfer(
            transfer_chain=transfer_chain,
            fileitem=fileitem,
            target_storage=target_storage,
            transfer_path_options=transfer_options,
            transfer_type=transfer_type,
        )
        if not state:
            return False, self._format_preview_error(preview_data), False, transfer_options

        media_type_error = self._get_preview_media_type_error(preview_data)
        if media_type_error:
            return False, media_type_error, True, transfer_options

        adjusted_options = self._get_top_level_transfer_options(
            preview_data=preview_data,
            target_path=target_path,
        )
        if adjusted_options:
            transfer_options.update(adjusted_options)
            logger.info(
                "【OpenList 目录监控】顶层分类路径调整: %s -> %s",
                adjusted_options.get("top_level_category"),
                transfer_options.get("target_path"),
            )
            state, preview_data = self._preview_remote_transfer(
                transfer_chain=transfer_chain,
                fileitem=fileitem,
                target_storage=target_storage,
                transfer_path_options=transfer_options,
                transfer_type=transfer_type,
            )
            if not state:
                return False, self._format_preview_error(preview_data), False, transfer_options
            media_type_error = self._get_preview_media_type_error(preview_data)
            if media_type_error:
                return False, media_type_error, True, transfer_options

        for target_dir in self._get_preview_target_dirs(preview_data):
            state, message = self._ensure_alist_directory(target_dir)
            if not state:
                return False, message, False, transfer_options
        return True, "", False, transfer_options

    def _preview_remote_transfer(
        self,
        transfer_chain: Any,
        fileitem: schemas.FileItem,
        target_storage: str,
        transfer_path_options: Dict[str, Any],
        transfer_type: str,
    ) -> Tuple[bool, Any]:
        try:
            return transfer_chain.do_transfer(
                fileitem=fileitem,
                target_storage=target_storage,
                target_path=transfer_path_options.get("target_path"),
                transfer_type=transfer_type,
                library_type_folder=transfer_path_options.get("library_type_folder"),
                library_category_folder=transfer_path_options.get("library_category_folder"),
                scrape=True if self._scrape else None,
                min_filesize=self._min_file_size_mb,
                force=True,
                background=False,
                manual=False,
                preview=True,
                sync_extra_files=self._sync_extra_files,
            )
        finally:
            try:
                transfer_chain.jobview.remove_task(fileitem)
            except Exception as e:
                logger.debug("【OpenList 目录监控】清理预览任务失败: %s", e)

    @staticmethod
    def _format_preview_error(preview_data: Any) -> str:
        if isinstance(preview_data, dict):
            return str(preview_data.get("message") or "整理预览失败")
        return str(preview_data or "整理预览失败")

    def _get_preview_media_type_error(self, preview_data: Any) -> str:
        if not isinstance(preview_data, dict):
            return ""
        preview_media_types = {
            str(item.get("type") or "").strip()
            for item in preview_data.get("items") or []
            if str(item.get("type") or "").strip()
        }
        disallowed_media_types = preview_media_types - set(self._media_types)
        if not disallowed_media_types:
            return ""
        return (
            f"媒体类型 {', '.join(sorted(disallowed_media_types))} "
            f"不在选择范围 {', '.join(self._media_types)}"
        )

    def _get_preview_target_dirs(self, preview_data: Any) -> List[str]:
        target_dirs = []
        if not isinstance(preview_data, dict):
            return target_dirs
        for item in preview_data.get("items") or []:
            target_dir = self._clean_render_path(item.get("target_dir") or "")
            if target_dir and target_dir not in target_dirs:
                target_dirs.append(target_dir)
            target = self._clean_render_path(item.get("target") or "")
            if target:
                target_parent = Path(target).parent.as_posix()
                if target_parent and target_parent not in target_dirs:
                    target_dirs.append(target_parent)
        return target_dirs

    def _get_top_level_transfer_options(
        self,
        preview_data: Any,
        target_path: Optional[Path],
    ) -> Dict[str, Any]:
        if (
            not isinstance(preview_data, dict)
            or not target_path
            or not self._library_type_folder
            or not self._library_category_folder
        ):
            return {}

        top_level_categories = self._get_top_level_categories()
        if not top_level_categories:
            return {}

        target_root = self._normalize_path(target_path)
        media_type_dirs = {MediaType.TV.value, MediaType.MOVIE.value}
        for item in preview_data.get("items") or []:
            for key in ("target_dir", "target"):
                relative_parts = self._relative_path_parts(
                    root=target_root,
                    path=item.get(key),
                )
                if len(relative_parts) < 2:
                    continue
                media_type, category = relative_parts[0], relative_parts[1]
                if media_type not in media_type_dirs or category not in top_level_categories:
                    continue
                return {
                    "target_path": Path(target_root) / category,
                    "library_type_folder": False,
                    "library_category_folder": False,
                    "top_level_category": category,
                }
        return {}

    @staticmethod
    def _relative_path_parts(root: Any, path: Any) -> List[str]:
        clean_root = OpenListMonitor._normalize_path(root)
        clean_path = OpenListMonitor._normalize_path(
            OpenListMonitor._strip_preview_storage_prefix(
                OpenListMonitor._clean_render_path(path)
            )
        )
        if not clean_root or not clean_path or clean_path == clean_root:
            return []
        if clean_root == "/":
            return [part for part in clean_path.strip("/").split("/") if part]
        prefix = clean_root.rstrip("/") + "/"
        if not clean_path.startswith(prefix):
            return []
        return [part for part in clean_path[len(prefix):].split("/") if part]

    @staticmethod
    def _strip_preview_storage_prefix(value: Any) -> str:
        text = str(value or "").strip()
        if text.startswith("【") and "】/" in text:
            return text.split("】", 1)[1]
        if text.startswith("[") and "]/" in text:
            return text.split("]", 1)[1]
        return text

    def _ensure_alist_directory(self, path: str) -> Tuple[bool, str]:
        conf = self._get_alist_conf()
        if not conf:
            return False, "未找到 OpenList 存储配置"
        base_url = self._get_alist_base_url(conf)
        headers = self._get_alist_auth_header(conf)
        if not base_url or not headers:
            return False, "OpenList 认证失败"

        clean_path = self._normalize_path(path)
        if not clean_path or clean_path == "/":
            return True, ""

        current = ""
        for part in [p for p in clean_path.split("/") if p]:
            current = f"{current}/{part}" if current else f"/{part}"
            if self._alist_path_exists(base_url, headers, current):
                continue
            state, message = self._create_alist_directory(base_url, headers, current)
            if not state:
                return False, message

        return True, ""

    def _alist_path_exists(
        self, base_url: str, headers: Dict[str, str], path: str
    ) -> bool:
        resp = self._post_alist(
            base_url,
            "/api/fs/get",
            headers,
            json={"path": path, "password": "", "refresh": True},
        )
        if not resp or resp.status_code != 200:
            return False
        try:
            result = resp.json()
            return result.get("code") == 200
        except Exception:
            return False

    def _create_alist_directory(
        self, base_url: str, headers: Dict[str, str], path: str
    ) -> Tuple[bool, str]:
        resp = self._post_alist(
            base_url,
            "/api/fs/mkdir",
            headers,
            json={"path": path},
        )
        if not resp:
            return False, f"创建目录 {path} 失败: 无响应"
        if resp.status_code != 200:
            return False, f"创建目录 {path} 失败: HTTP {resp.status_code}"
        try:
            result = resp.json()
        except Exception as e:
            return False, f"创建目录 {path} 失败: 解析响应失败 {e}"
        if result.get("code") == 200 or self._alist_path_exists(base_url, headers, path):
            if self._wait_alist_path_exists(base_url, headers, path):
                logger.info("【OpenList 目录监控】已确认目标目录: %s", path)
                return True, ""
            return False, f"创建目录 {path} 后等待 OpenList 可见超时"
        return False, (
            f"创建目录 {path} 失败: "
            f"{result.get('message') or 'OpenList 返回错误'}"
        )

    def _wait_alist_path_exists(
        self, base_url: str, headers: Dict[str, str], path: str
    ) -> bool:
        for _ in range(self.DIRECTORY_VISIBLE_RETRIES):
            if self._alist_path_exists(base_url, headers, path):
                return True
            if self._event.wait(self.DIRECTORY_VISIBLE_INTERVAL):
                break
        return False

    def _wait_transfer_interval(self) -> bool:
        return self._wait_rate_limit(
            self._transfer_interval_seconds, "_last_transfer_submit_at"
        )

    def _wait_openlist_interval(self) -> bool:
        return self._wait_rate_limit(
            self._api_interval_seconds, "_last_openlist_request_at"
        )

    def _wait_rate_limit(self, interval: float, marker: str) -> bool:
        interval = max(float(interval or 0), 0)
        if interval <= 0:
            return not self._event.is_set()
        while not self._event.is_set():
            with self._rate_limit_lock:
                now = time.monotonic()
                last = float(getattr(self, marker, 0.0) or 0.0)
                wait_seconds = last + interval - now
                if wait_seconds <= 0:
                    setattr(self, marker, now)
                    return True
            if self._event.wait(wait_seconds):
                return False
        return False

    def _post_alist(
        self,
        base_url: str,
        endpoint: str,
        headers: Dict[str, str],
        **kwargs,
    ):
        if not self._wait_openlist_interval():
            return None
        return RequestUtils(headers=headers).post_res(
            UrlUtils.adapt_request_url(base_url, endpoint), **kwargs
        )

    def _should_clean_empty_dirs(self) -> bool:
        return self._clean_empty_dirs and self._transfer_type == "move"

    def _should_clean_residual_files(self) -> bool:
        return self._should_clean_empty_dirs() and self._clean_residual_files

    def _cleanup_empty_source_dirs(
        self,
        base_url: str,
        headers: Dict[str, str],
        roots: List[str],
        scanned_dirs: List[str],
    ) -> Tuple[int, int]:
        root_paths = {self._normalize_path(root) for root in roots if root}
        cleaned_dirs = 0
        cleaned_files = 0
        candidates = sorted(
            {self._normalize_path(path) for path in scanned_dirs if path},
            key=lambda item: item.count("/"),
            reverse=True,
        )
        for path in candidates:
            if self._event.is_set():
                break
            if path == "/" or path in root_paths:
                continue
            if not any(self._is_child_path(root, path) for root in root_paths):
                continue
            cleanable, residual_files, message = self._prepare_alist_dir_for_cleanup(
                base_url, headers, path
            )
            cleaned_files += residual_files
            if not cleanable:
                if message:
                    logger.debug("【OpenList 目录监控】跳过空目录清理 %s: %s", path, message)
                continue
            state, message = self._remove_alist_path(base_url, headers, path)
            if state:
                cleaned_dirs += 1
                logger.info("【OpenList 目录监控】已清理空目录: %s", path)
            else:
                logger.warning("【OpenList 目录监控】清理空目录失败 %s: %s", path, message)
        return cleaned_dirs, cleaned_files

    @staticmethod
    def _is_child_path(root: str, path: str) -> bool:
        if not root or not path:
            return False
        if root == "/":
            return path != "/"
        return path.startswith(root.rstrip("/") + "/")

    def _prepare_alist_dir_for_cleanup(
        self, base_url: str, headers: Dict[str, str], path: str
    ) -> Tuple[bool, int, str]:
        listing, error = self._list_directory(base_url, headers, path, refresh=True)
        if error:
            return False, 0, error

        items = listing.get("files") or []
        if not items:
            return True, 0, ""

        dirs = [item for item in items if item.get("is_dir")]
        if dirs:
            names = self._format_preview_names(dirs)
            return False, 0, f"目录仍有子目录: {names}"

        files = [item for item in items if not item.get("is_dir")]
        if not files:
            return True, 0, ""
        if not self._should_clean_residual_files():
            return False, 0, "目录不为空"

        allowed_extensions = self._get_residual_file_extensions()
        if not allowed_extensions:
            return False, 0, "未配置残留文件后缀"

        max_size_bytes = int(float(self._residual_file_max_size_mb or 0) * 1024 * 1024)
        residual_names = []
        blockers = []
        for item in files:
            name = str(item.get("name") or "").strip()
            if not name:
                blockers.append("文件名为空")
                continue
            ext = self._normalize_extension(os.path.splitext(name)[1])
            size = self._to_file_size(item.get("size"))
            if ext in self.VIDEO_EXTENSIONS:
                blockers.append(f"{name}(视频)")
                continue
            if ext not in allowed_extensions:
                blockers.append(name)
                continue
            if max_size_bytes and size > max_size_bytes:
                blockers.append(f"{name}(超过大小限制)")
                continue
            residual_names.append(name)

        if blockers:
            return False, 0, f"目录仍有非残留文件: {', '.join(blockers[:5])}"
        if not residual_names:
            return True, 0, ""

        state, message = self._remove_alist_names(
            base_url, headers, path, residual_names
        )
        if not state:
            return False, 0, f"清理残留文件失败: {message}"

        state, message = self._wait_alist_dir_items_absent(
            base_url, headers, path, residual_names
        )
        if not state:
            return False, 0, message

        listing, error = self._list_directory(base_url, headers, path, refresh=True)
        if error:
            return False, len(residual_names), error
        remaining = listing.get("files") or []
        if remaining:
            return False, len(residual_names), f"目录仍有 {len(remaining)} 个项目"

        for name in residual_names:
            logger.info(
                "【OpenList 目录监控】已清理残留文件: %s/%s",
                path.rstrip("/"),
                name,
            )
        return True, len(residual_names), ""

    @staticmethod
    def _format_preview_names(items: List[Dict[str, Any]]) -> str:
        names = [str(item.get("name") or "-") for item in items[:5]]
        return ", ".join(names) if names else "-"

    @staticmethod
    def _to_file_size(value: Any) -> int:
        try:
            return int(float(value or 0))
        except Exception:
            return 0

    def _remove_alist_names(
        self,
        base_url: str,
        headers: Dict[str, str],
        dir_path: str,
        names: List[str],
    ) -> Tuple[bool, str]:
        clean_dir = self._normalize_path(dir_path)
        clean_names = [str(name).strip() for name in names if str(name or "").strip()]
        if not clean_names:
            return True, ""
        if not clean_dir:
            return False, "目录为空"
        resp = self._post_alist(
            base_url,
            "/api/fs/remove",
            headers,
            json={"dir": clean_dir, "names": clean_names},
        )
        if not resp:
            return False, "无响应"
        if resp.status_code != 200:
            return False, f"HTTP {resp.status_code}"
        try:
            result = resp.json()
        except Exception as e:
            return False, f"解析响应失败 {e}"
        if result.get("code") == 200:
            return True, ""
        return False, str(result.get("message") or "OpenList 返回错误")

    def _wait_alist_dir_items_absent(
        self,
        base_url: str,
        headers: Dict[str, str],
        dir_path: str,
        names: List[str],
    ) -> Tuple[bool, str]:
        targets = set(names)
        message = "等待 OpenList 删除残留文件可见超时"
        for _ in range(self.DIRECTORY_VISIBLE_RETRIES):
            listing, error = self._list_directory(base_url, headers, dir_path, refresh=True)
            if error:
                message = error
            else:
                remaining = {
                    str(item.get("name") or "").strip()
                    for item in listing.get("files") or []
                    if str(item.get("name") or "").strip()
                }
                still_exists = sorted(targets & remaining)
                if not still_exists:
                    return True, ""
                message = f"残留文件仍可见: {', '.join(still_exists[:5])}"
            if self._event.wait(self.DIRECTORY_VISIBLE_INTERVAL):
                break
        return False, message

    def _remove_alist_path(
        self, base_url: str, headers: Dict[str, str], path: str
    ) -> Tuple[bool, str]:
        clean_path = self._normalize_path(path)
        if not clean_path or clean_path == "/":
            return False, "拒绝删除根目录"
        parent = Path(clean_path).parent.as_posix() or "/"
        name = Path(clean_path).name
        if not name:
            return False, "目录名为空"
        resp = self._post_alist(
            base_url,
            "/api/fs/remove",
            headers,
            json={"dir": parent, "names": [name]},
        )
        if not resp:
            return False, "无响应"
        if resp.status_code != 200:
            return False, f"HTTP {resp.status_code}"
        try:
            result = resp.json()
        except Exception as e:
            return False, f"解析响应失败 {e}"
        if result.get("code") == 200 or not self._alist_path_exists(
            base_url, headers, clean_path
        ):
            if self._wait_alist_path_not_exists(base_url, headers, clean_path):
                return True, ""
            return False, "等待 OpenList 删除可见超时"
        return False, str(result.get("message") or "OpenList 返回错误")

    def _wait_alist_path_not_exists(
        self, base_url: str, headers: Dict[str, str], path: str
    ) -> bool:
        for _ in range(self.DIRECTORY_VISIBLE_RETRIES):
            if not self._alist_path_exists(base_url, headers, path):
                return True
            if self._event.wait(self.DIRECTORY_VISIBLE_INTERVAL):
                break
        return False

    def _list_directory(
        self,
        base_url: str,
        headers: Dict[str, str],
        path: str,
        refresh: Optional[bool] = None,
    ) -> Tuple[Dict[str, Any], str]:
        files = []
        page = 1
        while True:
            resp = self._post_alist(
                base_url,
                "/api/fs/list",
                headers,
                json={
                    "path": path,
                    "password": "",
                    "page": page,
                    "per_page": self.OPENLIST_MAX_LIST_PAGE_SIZE,
                    "refresh": self._refresh if refresh is None else refresh,
                },
            )
            if not resp:
                return {}, "请求目录失败: 无响应"
            if resp.status_code != 200:
                return {}, f"HTTP {resp.status_code}"
            try:
                result = resp.json()
            except Exception as e:
                return {}, f"解析响应失败: {e}"
            if result.get("code") != 200:
                return {}, str(result.get("message") or "OpenList 返回错误")

            data = result.get("data") or {}
            content = data.get("content") or []
            for item in content:
                is_dir = (
                    bool(item.get("is_dir"))
                    or item.get("type") == "folder"
                    or item.get("type") == 1
                )
                files.append({
                    "name": item.get("name"),
                    "size": item.get("size"),
                    "is_dir": is_dir,
                    "modified": item.get("modified"),
                })

            total = data.get("filtered_total") or data.get("total") or 0
            pages_total = data.get("pages_total") or 0
            has_more = data.get("has_more")
            if not content or len(files) >= total:
                break
            if has_more is False or (pages_total and page >= pages_total):
                break
            page += 1

        return {"files": files}, ""

    def _finish(self, message: str, data: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
        success = not data.get("errors")
        data["success"] = success
        data["message"] = message
        data["finish_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.save_data(self.STORE_RESULT_KEY, data)
        if success:
            logger.info("【OpenList 目录监控】%s", message)
        else:
            logger.warning("【OpenList 目录监控】%s", message)
        self._send_finish_notification(message, data)
        return success, message, data

    def _send_finish_notification(self, message: str, data: Dict[str, Any]) -> None:
        if not self._notify:
            return
        errors = data.get("errors") or []
        has_work = any(
            int(data.get(key) or 0) > 0
            for key in (
                "new_files",
                "transferred",
                "cleaned_files",
                "cleaned_dirs",
            )
        )
        if not has_work and not errors:
            return

        title = (
            f"{self.plugin_name}完成"
            if not errors
            else f"{self.plugin_name}有错误"
        )
        target_rule_lines = self._format_target_path_rules(
            data.get("target_path_rules") or []
        )
        lines = [
            "处理方式：远程整理",
            f"监控整理目录：{('、'.join(target_rule_lines)) or '-'}",
            f"新文件：{int(data.get('new_files') or 0)}",
            f"已提交整理：{int(data.get('transferred') or 0)}",
        ]
        if int(data.get("limited_files") or 0):
            lines.append(f"延后处理：{int(data.get('limited_files') or 0)}")
        if int(data.get("skipped_type") or 0):
            lines.append(f"跳过媒体类型：{int(data.get('skipped_type') or 0)}")
        if int(data.get("cleaned_files") or 0):
            lines.append(f"清理残留文件：{int(data.get('cleaned_files') or 0)}")
        if int(data.get("cleaned_dirs") or 0):
            lines.append(f"清理空目录：{int(data.get('cleaned_dirs') or 0)}")
        if errors:
            lines.append(f"错误数：{len(errors)}")
            for error in errors[:5]:
                path = str(error.get("path") or "-")
                reason = str(error.get("error") or "未知错误")
                lines.append(f"- {path}: {reason}")
        lines.append(f"结果：{message}")

        try:
            self.post_message(
                mtype=NotificationType.Plugin,
                title=title,
                text="\n".join(lines),
            )
        except Exception as e:
            logger.warning("【OpenList 目录监控】发送完成通知失败: %s", e)

    @staticmethod
    def _format_cleaned_files(stats: Dict[str, Any]) -> str:
        cleaned_files = int(stats.get("cleaned_files") or 0)
        return f"，清理残留文件 {cleaned_files} 个" if cleaned_files else ""

    @staticmethod
    def _format_cleaned_dirs(stats: Dict[str, Any]) -> str:
        cleaned_dirs = int(stats.get("cleaned_dirs") or 0)
        return f"，清理空目录 {cleaned_dirs} 个" if cleaned_dirs else ""

    @staticmethod
    def _format_limited_files(stats: Dict[str, Any]) -> str:
        limited_files = int(stats.get("limited_files") or 0)
        return f"，延后处理 {limited_files} 个" if limited_files else ""

    @staticmethod
    def _format_skipped_type(stats: Dict[str, Any]) -> str:
        skipped_type = int(stats.get("skipped_type") or 0)
        return f"，跳过媒体类型 {skipped_type} 个" if skipped_type else ""

    @staticmethod
    def _format_storage_name(value: Any) -> str:
        storage = str(value or "").strip()
        if storage == "alist":
            return "OpenList"
        return storage or "-"

    def _is_media_ext(self, ext: str) -> bool:
        ext = self._normalize_extension(ext)
        if self._extensions:
            allowed = set(
                self._normalize_extension(e)
                for e in self._extensions.replace(",", "\n").splitlines()
                if e.strip()
            )
            return ext in allowed and ext in self.VIDEO_EXTENSIONS
        return ext in self.VIDEO_EXTENSIONS

    def _get_processed_store_key(self) -> str:
        return self.STORE_TRANSFERRED_KEY

    @staticmethod
    def _record_key(file_info: Dict[str, Any]) -> str:
        path = str(file_info.get("path") or "").strip()
        name = str(file_info.get("name") or "").strip()
        parent = path.rsplit("/", 1)[0] if "/" in path else ""
        if not parent:
            parent = "/"
        return f"{parent}|{name}"

    @staticmethod
    def _normalize_extension(value: Any) -> str:
        ext = str(value or "").strip().lower()
        if not ext:
            return ""
        return ext if ext.startswith(".") else f".{ext}"

    def _get_residual_file_extensions(self) -> set:
        raw = self._residual_file_extensions or self.DEFAULT_RESIDUAL_FILE_EXTENSIONS
        extensions = set()
        for item in raw.replace(",", "\n").splitlines():
            ext = self._normalize_extension(item)
            if ext and ext not in self.VIDEO_EXTENSIONS:
                extensions.add(ext)
        return extensions

    def _get_top_level_categories(self) -> set:
        categories = set()
        for item in str(self._top_level_categories or "").replace(",", "\n").splitlines():
            category = item.strip()
            if category:
                categories.add(category)
        return categories

    @staticmethod
    def _clean_render_path(value: Any) -> str:
        raw = str(value or "").replace("\r", "").replace("\n", "").strip()
        if not raw:
            return ""
        parts = [part.strip() for part in raw.split("/") if part.strip()]
        prefix = "/" if raw.startswith("/") else ""
        return prefix + "/".join(parts)

    @staticmethod
    def _build_alist_fileitem(file_info: Dict[str, Any]) -> schemas.FileItem:
        path = str(file_info.get("path") or "").strip()
        name = str(file_info.get("name") or Path(path).name).strip()
        ext = OpenListMonitor._normalize_extension(file_info.get("ext") or Path(name).suffix)
        return schemas.FileItem(
            storage="alist",
            type="file",
            path=path,
            name=name,
            basename=Path(name).stem,
            extension=ext.lstrip("."),
            size=int(file_info.get("size") or 0),
        )

    @classmethod
    def _get_alist_conf(cls) -> Optional[StorageConf]:
        try:
            return StorageHelper().get_storage("alist")
        except Exception as e:
            logger.debug("【OpenList 目录监控】读取 OpenList 存储配置失败: %s", e)
            return None

    @classmethod
    def _get_alist_base_url(cls, conf: StorageConf) -> str:
        if not conf or not getattr(conf, "config", None):
            return ""
        url = conf.config.get("url")
        if not url:
            return ""
        return UrlUtils.standardize_base_url(url)

    @classmethod
    def _get_alist_auth_header(cls, conf: StorageConf) -> Dict[str, str]:
        if not conf or not getattr(conf, "config", None):
            return {}
        base_url = cls._get_alist_base_url(conf)
        if not base_url:
            return {}
        cached_token = cls._alist_token_cache.get(base_url)
        if cached_token:
            return {"Authorization": str(cached_token)}
        token = str(conf.config.get("token") or "").strip()
        if token:
            cls._alist_token_cache.set(base_url, token)
            return {"Authorization": token}
        username = conf.config.get("username")
        password = conf.config.get("password")
        if not username or not password:
            return {}
        resp = RequestUtils(
            headers={"Content-Type": "application/json"}
        ).post_res(
            UrlUtils.adapt_request_url(base_url, "/api/auth/login"),
            json={"username": username, "password": password},
        )
        if not resp or resp.status_code != 200:
            logger.warning("【OpenList 目录监控】OpenList 登录失败")
            return {}
        try:
            result = resp.json()
            if result.get("code") != 200:
                logger.warning(
                    "【OpenList 目录监控】OpenList 登录失败: %s",
                    result.get("message"),
                )
                return {}
            token = str(result.get("data", {}).get("token") or "").strip()
            if token:
                cls._alist_token_cache.set(base_url, token)
                return {"Authorization": token}
        except Exception as e:
            logger.warning("【OpenList 目录监控】解析登录结果失败: %s", e)
        return {}

    def _migrate_legacy_path_config(self, config: Dict[str, Any]) -> None:
        if self._target_path_rules or not self._paths or not self._target_path:
            return
        rules = [
            {"source": path, "target": self._normalize_path(self._target_path)}
            for path in self._parse_paths(self._paths)
        ]
        text = "\n".join(self._format_target_path_rules(rules))
        if not text:
            return
        self._target_path_rules = text
        config["target_path_rules"] = text
        try:
            self.update_config(config)
            logger.info("【OpenList 目录监控】已迁移旧目录配置到监控整理目录")
        except Exception as e:
            logger.warning("【OpenList 目录监控】迁移旧目录配置失败: %s", e)

    def _migrate_legacy_plugin_state(self, config: Dict[str, Any]) -> Dict[str, Any]:
        new_config = dict(config or {})
        try:
            legacy_config = self.get_config(self.LEGACY_PLUGIN_ID)
        except Exception as e:
            legacy_config = None
            logger.debug("【OpenList 目录监控】读取旧插件配置失败: %s", e)

        if not new_config and isinstance(legacy_config, dict) and legacy_config:
            new_config = {key: value for key, value in legacy_config.items() if key}
            try:
                self.update_config(new_config)
                logger.info(
                    "【OpenList 目录监控】已迁移旧插件配置: %s -> %s",
                    self.LEGACY_PLUGIN_ID,
                    self.__class__.__name__,
                )
            except Exception as e:
                logger.warning("【OpenList 目录监控】迁移旧插件配置失败: %s", e)

        for key in (self.STORE_RESULT_KEY, self.STORE_TRANSFERRED_KEY):
            try:
                current_data = self.get_data(key)
                if current_data not in (None, "", {}, []):
                    continue
                legacy_data = self.get_data(key, plugin_id=self.LEGACY_PLUGIN_ID)
                if legacy_data in (None, ""):
                    continue
                self.save_data(key, legacy_data)
                logger.info(
                    "【OpenList 目录监控】已迁移旧插件数据: %s/%s",
                    self.LEGACY_PLUGIN_ID,
                    key,
                )
            except Exception as e:
                logger.debug("【OpenList 目录监控】迁移旧插件数据失败 %s: %s", key, e)
        return new_config

    def _get_monitor_target_rules(self) -> List[Dict[str, str]]:
        rules = self._parse_target_path_rules(self._target_path_rules)
        if rules:
            return rules
        paths = self._parse_paths(self._paths)
        if not paths:
            return []
        target = self._normalize_path(self._target_path) if self._target_path else ""
        return [{"source": path, "target": target} for path in paths]

    @staticmethod
    def _get_monitor_paths(rules: List[Dict[str, str]]) -> List[str]:
        paths = []
        seen = set()
        for rule in rules:
            source = OpenListMonitor._normalize_path(rule.get("source"))
            if source and source not in seen:
                seen.add(source)
                paths.append(source)
        return paths

    @staticmethod
    def _format_target_path_rules(rules: List[Dict[str, str]]) -> List[str]:
        lines = []
        for rule in rules:
            source = OpenListMonitor._normalize_path(rule.get("source"))
            target = OpenListMonitor._normalize_path(rule.get("target"))
            if source and target:
                lines.append(f"{source} => {target}")
        return lines

    def _resolve_target_path(
        self,
        file_info: Dict[str, Any],
        default_target_path: Optional[Path],
        target_path_rules: List[Dict[str, str]],
    ) -> Optional[Path]:
        file_path = self._normalize_path(file_info.get("path"))
        monitor_root = self._normalize_path(file_info.get("monitor_root"))
        for rule in sorted(
            target_path_rules,
            key=lambda item: item.get("source", "").count("/"),
            reverse=True,
        ):
            source = self._normalize_path(rule.get("source"))
            target = self._normalize_path(rule.get("target"))
            if not source:
                continue
            if (
                file_path == source
                or self._is_child_path(source, file_path)
                or monitor_root == source
                or self._is_child_path(source, monitor_root)
            ):
                return Path(target) if target else default_target_path
        return default_target_path

    @staticmethod
    def _parse_target_path_rules(value: Any) -> List[Dict[str, str]]:
        raw = str(value or "").replace("\r", "")
        rules = []
        seen_sources = set()
        separators = ("=>", "->", "|", "=")
        for line in raw.splitlines():
            text = line.strip()
            if not text or text.startswith("#"):
                continue
            source = ""
            target = ""
            for separator in separators:
                if separator in text:
                    source, target = text.split(separator, 1)
                    break
            if not source or not target:
                continue
            source_path = OpenListMonitor._normalize_path(source)
            target_path = OpenListMonitor._normalize_path(target)
            if not source_path or not target_path or source_path in seen_sources:
                continue
            seen_sources.add(source_path)
            rules.append({"source": source_path, "target": target_path})
        return rules

    @staticmethod
    def _parse_paths(value: Any) -> List[str]:
        raw = str(value or "").replace(",", "\n")
        paths = []
        seen = set()
        for line in raw.splitlines():
            path = OpenListMonitor._normalize_path(line)
            if path and path not in seen:
                seen.add(path)
                paths.append(path)
        return paths

    @staticmethod
    def _parse_media_types(value: Any) -> List[str]:
        valid_types = [MediaType.TV.value, MediaType.MOVIE.value]
        if isinstance(value, list):
            raw_items = value
        elif value:
            raw_items = str(value).replace(",", "\n").splitlines()
        else:
            raw_items = valid_types

        media_types = []
        for item in raw_items:
            text = str(item or "").strip()
            if text in {"tv", "TV"}:
                text = MediaType.TV.value
            elif text.lower() == "movie":
                text = MediaType.MOVIE.value
            if text in valid_types and text not in media_types:
                media_types.append(text)
        return media_types or valid_types

    @staticmethod
    def _normalize_path(value: Any) -> str:
        path = str(value or "").strip()
        if not path:
            return ""
        parts = [part for part in path.split("/") if part]
        return "/" + "/".join(parts) if parts else "/"

    @staticmethod
    def _to_int(value: Any, default: int, minimum: int, maximum: int) -> int:
        try:
            number = int(value)
        except (TypeError, ValueError):
            return default
        return min(max(number, minimum), maximum)

    @staticmethod
    def _to_float(value: Any, default: float, minimum: float, maximum: float) -> float:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return default
        return min(max(number, minimum), maximum)

    @staticmethod
    def _build_status_card(title: str, items: List[Tuple[str, str]]) -> dict:
        return {
            "component": "VCard",
            "props": {"flat": True, "border": True},
            "content": [
                {"component": "VCardTitle", "text": title},
                {
                    "component": "VCardText",
                    "content": [
                        {
                            "component": "VTable",
                            "props": {"density": "comfortable"},
                            "content": [
                                {
                                    "component": "tbody",
                                    "content": [
                                        {
                                            "component": "tr",
                                            "content": [
                                                {"component": "td", "text": key},
                                                {"component": "td", "text": value},
                                            ],
                                        }
                                        for key, value in items
                                    ],
                                }
                            ],
                        }
                    ],
                },
            ],
        }
