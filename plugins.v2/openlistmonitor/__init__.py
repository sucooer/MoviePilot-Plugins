import asyncio
import hashlib
import inspect
import os
import re
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
    plugin_version = "0.3.19"
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
    _recognition_rewrite_rules = ""
    _ai_recognition_fallback = False
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
    _skip_rename_standard_naming = False
    _scheduler: Optional[BackgroundScheduler] = None
    _event = threading.Event()
    _lock = threading.Lock()
    _running = False

    STORE_RESULT_KEY = "last_result"
    STORE_TRANSFERRED_KEY = "transferred_files"
    STORE_EXTRA_FILES_KEY = "transferred_extra_files"
    STORE_VIDEO_TARGETS_KEY = "video_target_mapping"
    LEGACY_PLUGIN_ID = "AlistMonitor"

    OPENLIST_MAX_LIST_PAGE_SIZE = 500
    DIRECTORY_VISIBLE_RETRIES = 6
    DIRECTORY_VISIBLE_INTERVAL = 2
    AI_RECOGNITION_MIN_CONFIDENCE = 0.70
    AI_RECOGNITION_MAX_CANDIDATES = 10

    VIDEO_EXTENSIONS = {
        ".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm", ".m4v",
        ".ts", ".m2ts", ".iso", ".bdmv", ".mpls",
        ".rmvb", ".3gp", ".vob", ".mpeg", ".mpg", ".asf", ".strm", ".tp", ".f4v",
    }
    DEFAULT_RESIDUAL_FILE_EXTENSIONS = ".jpg,.jpeg,.png,.webp,.gif,.bmp,.txt,.nfo"

    SUBTITLE_EXTENSIONS = {
        ".srt", ".ass", ".ssa", ".sub", ".idx", ".sup", ".pgs", ".vtt", ".smi", ".usf", ".txt",
    }

    def __init__(self):
        super().__init__()
        self._last_openlist_request_at = 0.0
        self._last_transfer_submit_at = 0.0
        self._rate_limit_lock = threading.Lock()
        self._ai_recognition_cache = {}
        self._ai_recognition_cache_lock = threading.Lock()

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
        self._recognition_rewrite_rules = str(
            config.get("recognition_rewrite_rules") or ""
        ).strip()
        self._ai_recognition_fallback = bool(
            config.get("ai_recognition_fallback", False)
        )
        with self._ai_recognition_cache_lock:
            self._ai_recognition_cache.clear()
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
        self._skip_rename_standard_naming = bool(config.get("skip_rename_standard_naming", False))
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
                                            "hint": "目录内已无主视频时，按白名单删除图片、说明和小体积视频残留",
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
                                            "hint": "逗号或换行分隔；视频后缀需显式配置，且仅清理小于主视频最小大小的文件",
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
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "ai_recognition_fallback",
                                            "label": "AI识别兜底",
                                            "hint": "原生识别失败时复用 MoviePilot LLM 配置生成候选，并通过 MoviePilot 二次校验后再整理",
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
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "recognition_rewrite_rules",
                                            "label": "识别词替换",
                                            "placeholder": "Tamon-kun Ima Docchi => Tamon-kun Ima Docchi!?\nTamon-kun Ima Docchi => Tamon's B-Side",
                                            "rows": 3,
                                            "hint": "一行一个源标题=>识别标题，仅影响 MoviePilot 识别，不修改 OpenList 原文件名",
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
                                            "model": "skip_rename_standard_naming",
                                            "label": "标准命名跳过重命名",
                                            "hint": "文件名含 SxxExx 标准剧集格式时直接移动，不重命名",
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
            "recognition_rewrite_rules": "",
            "ai_recognition_fallback": False,
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
            "skip_rename_standard_naming": False,
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
            ("识别词替换", str(len(self._parse_recognition_rewrite_rules(self._recognition_rewrite_rules)))),
            ("AI识别兜底", "是" if self._ai_recognition_fallback else "否"),
            ("刮削元数据", "强制刮削" if self._scrape else "按媒体库目录设置"),
            ("单轮处理上限", str(self._max_files_per_run)),
            ("API间隔秒", str(self._api_interval_seconds)),
            ("提交间隔秒", str(self._transfer_interval_seconds)),
            ("清理空目录", "是" if self._should_clean_empty_dirs() else "否"),
            ("清理残留文件", "是" if self._should_clean_residual_files() else "否"),
            ("残留文件后缀", ",".join(sorted(self._get_residual_file_extensions())) or "-"),
            ("残留文件最大MB", str(self._residual_file_max_size_mb)),
            ("完成通知", "是" if self._notify else "否"),
            ("标准命名跳过重命名", "是" if self._skip_rename_standard_naming else "否"),
            ("最近检查", str(last_result.get("time") or "-")),
            ("新文件数", str(last_result.get("new_files", 0))),
            ("延后处理数", str(last_result.get("limited_files", 0))),
            ("跳过媒体类型", str(last_result.get("skipped_type", 0))),
            ("已整理数", str(last_result.get("transferred", 0))),
            ("已同步附加文件", str(last_result.get("transferred_extra", 0))),
            ("AI兜底整理", str(last_result.get("ai_recognition_fallback", 0))),
            ("标准命名跳过重命名", str(last_result.get("skip_rename_standard_naming_count", 0))),
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
            "transferred_items": [],
            "ai_recognition_fallback": 0,
            "ai_recognition_fallback_items": [],
            "skip_rename_standard_naming_count": 0,
            "cleaned_files": 0,
            "cleaned_file_items": [],
            "cleaned_dirs": 0,
            "rate_limit": {
                "media_types": self._media_types,
                "library_type_folder": self._library_type_folder,
                "library_category_folder": self._library_category_folder,
                "top_level_categories": sorted(self._get_top_level_categories()),
                "recognition_rewrite_rules": self._parse_recognition_rewrite_rules(
                    self._recognition_rewrite_rules
                ),
                "ai_recognition_fallback": self._ai_recognition_fallback,
                "scrape": self._scrape,
                "skip_rename_standard_naming": self._skip_rename_standard_naming,
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

            leftover_extra = self._process_leftover_subtitles(
                base_url=base_url, headers=headers, paths=paths, stats=stats,
            )

            if self._should_clean_empty_dirs() and scanned_dirs:
                (
                    cleaned_dirs,
                    cleaned_files,
                    cleaned_file_items,
                ) = self._cleanup_empty_source_dirs(
                    base_url=base_url,
                    headers=headers,
                    roots=paths,
                    scanned_dirs=scanned_dirs,
                )
                stats["cleaned_dirs"] = cleaned_dirs
                stats["cleaned_files"] = cleaned_files
                stats["cleaned_file_items"] = cleaned_file_items

            if stats["errors"]:
                message = (
                    f"检查完成，发现 {stats['new_files']} 个新文件，"
                    f"已整理 {stats['transferred']} 个"
                    f"{self._format_extra_files(stats)}"
                    f"{self._format_skip_rename_count(stats)}"
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
                    f"{self._format_extra_files(stats)}"
                    f"{self._format_skip_rename_count(stats)}"
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
        extra_processed_key = self.STORE_EXTRA_FILES_KEY
        already_extra = set(self.get_data(extra_processed_key) or [])

        sub_names = []
        for item in file_items:
            name = str(item.get("name") or "").strip()
            if not name:
                continue
            ext = os.path.splitext(name)[1]
            if self._is_subtitle_ext(ext):
                sub_names.append(name)

        for item in file_items:
            name = str(item.get("name") or "").strip()
            if not name:
                continue
            size = int(item.get("size") or 0)
            size_mb = size / (1024 * 1024)
            if size_mb < self._min_file_size_mb:
                continue

            ext = os.path.splitext(name)[1].lower()
            if not self._is_video_ext(ext):
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

            extra_file_names = self._match_extra_files(name, sub_names)

            if self._transfer_type == "move":
                unprocessed_extra = extra_file_names
            else:
                unprocessed_extra = [
                    en for en in extra_file_names
                    if self._record_key({"path": f"{clean_path.rstrip('/')}/{en}" if clean_path != "/" else f"/{en}", "name": en}) not in already_extra
                ]

            new_files.append({
                "path": item_path,
                "name": name,
                "size": size,
                "size_mb": round(size_mb, 1),
                "ext": ext,
                "monitor_root": monitor_root,
                "extra_file_names": unprocessed_extra,
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
                recognition_meta = self._build_recognition_meta(file_info)
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
                    recognition_meta=recognition_meta,
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
                final_recognition_meta = transfer_options.get(
                    "recognition_meta", recognition_meta
                )
                final_recognition_mediainfo = transfer_options.get(
                    "recognition_mediainfo"
                )

                skip_rename = (
                    self._skip_rename_standard_naming
                    and self._is_standard_naming_format(file_info.get("name", ""))
                )
                if skip_rename:
                    logger.info(
                        "【OpenList 目录监控】标准命名格式，保留原名: %s -> [%s]%s",
                        file_info["path"],
                        self._format_storage_name(target_storage),
                        final_target_path or "按 MoviePilot 目录规则",
                    )
                    if not self._wait_transfer_interval():
                        break
                    state = self._move_file_preserve_name(
                        file_info=file_info,
                        transfer_options=transfer_options,
                        target_storage=target_storage,
                        transfer_type=transfer_type,
                    )
                    message = "" if state else "OpenList 直接移动失败"
                else:
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
                        meta=final_recognition_meta,
                        mediainfo=final_recognition_mediainfo,
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
                        sync_extra_files=False,
                    )
                if state:
                    transferred_records.add(self._record_key(file_info))
                    count += 1
                    if skip_rename:
                        transfer_options["recognition_source"] = "standard"
                        stats["skip_rename_standard_naming_count"] = int(
                            stats.get("skip_rename_standard_naming_count") or 0
                        ) + 1
                    self._record_ai_recognition_success(
                        stats=stats,
                        file_info=file_info,
                        transfer_options=transfer_options,
                    )
                    extra_count = 0
                    extra_names = file_info.get("extra_file_names") or []
                    target_dirs = transfer_options.get("preview_target_dirs") or []
                    if target_dirs:
                        source_dir = Path(str(file_info.get("path") or "")).parent.as_posix()
                        self._save_video_target_mapping(source_dir, target_dirs[0])
                    if self._sync_extra_files:
                        extra_count = self._transfer_extra_files(
                            file_info=file_info,
                            transfer_options=transfer_options,
                            stats=stats,
                            target_storage=target_storage,
                            transfer_type=transfer_type,
                        )
                    self._record_transfer_success(
                        stats=stats,
                        file_info=file_info,
                        transfer_options=transfer_options,
                        target_storage=target_storage,
                        transfer_type=transfer_type,
                        extra_count=extra_count,
                    )
                    logger.info(
                        "【OpenList 目录监控】已提交整理: %s（含 %d 个附加文件）",
                        file_info["path"], extra_count,
                    )
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
        recognition_meta: Any = None,
    ) -> Tuple[bool, str, bool, Dict[str, Any]]:
        transfer_options = {
            "target_path": target_path,
            "library_type_folder": self._library_type_folder,
            "library_category_folder": self._library_category_folder,
            "recognition_meta": recognition_meta,
            "recognition_mediainfo": None,
            "recognition_source": None,
            "ai_recognition_detail": None,
        }
        if target_storage != "alist" or fileitem.storage != "alist":
            return True, "", False, transfer_options

        state, preview_data = self._preview_remote_transfer(
            transfer_chain=transfer_chain,
            fileitem=fileitem,
            target_storage=target_storage,
            transfer_path_options=transfer_options,
            transfer_type=transfer_type,
            recognition_meta=recognition_meta,
            recognition_mediainfo=transfer_options.get("recognition_mediainfo"),
        )
        if not state:
            ai_meta, ai_mediainfo = self._build_ai_recognition_result(
                fileitem=fileitem,
                source_meta=recognition_meta,
                preview_data=preview_data,
            )
            if ai_meta and ai_mediainfo:
                transfer_options["recognition_meta"] = ai_meta
                transfer_options["recognition_mediainfo"] = ai_mediainfo
                transfer_options["recognition_source"] = "ai"
                transfer_options["ai_recognition_detail"] = self._build_ai_recognition_detail(
                    fileitem=fileitem,
                    mediainfo=ai_mediainfo,
                    reason="原生识别未命中",
                )
                state, preview_data = self._preview_remote_transfer(
                    transfer_chain=transfer_chain,
                    fileitem=fileitem,
                    target_storage=target_storage,
                    transfer_path_options=transfer_options,
                    transfer_type=transfer_type,
                    recognition_meta=ai_meta,
                    recognition_mediainfo=ai_mediainfo,
                )
                if state:
                    logger.info(
                        "【OpenList 目录监控】AI识别兜底预览成功: %s -> %s",
                        fileitem.path,
                        getattr(ai_mediainfo, "title_year", "") or getattr(ai_mediainfo, "title", ""),
                    )
                else:
                    transfer_options["recognition_meta"] = recognition_meta
                    transfer_options["recognition_mediainfo"] = None
                    transfer_options["recognition_source"] = None
                    transfer_options["ai_recognition_detail"] = None
            if not state:
                return False, self._format_preview_error(preview_data), False, transfer_options

        media_type_error = self._get_preview_media_type_error(preview_data)
        if media_type_error:
            return False, media_type_error, True, transfer_options
        episode_error = self._get_preview_episode_guard_error(preview_data)
        if episode_error:
            state, preview_data, episode_error = self._retry_preview_with_ai_recognition(
                transfer_chain=transfer_chain,
                fileitem=fileitem,
                target_storage=target_storage,
                transfer_options=transfer_options,
                transfer_type=transfer_type,
                source_meta=recognition_meta,
                reason=episode_error,
            )
            if not state:
                return False, episode_error, False, transfer_options
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
                recognition_meta=transfer_options.get("recognition_meta"),
                recognition_mediainfo=transfer_options.get("recognition_mediainfo"),
            )
            if not state:
                return False, self._format_preview_error(preview_data), False, transfer_options
            media_type_error = self._get_preview_media_type_error(preview_data)
            if media_type_error:
                return False, media_type_error, True, transfer_options
            episode_error = self._get_preview_episode_guard_error(preview_data)
            if episode_error:
                state, preview_data, episode_error = self._retry_preview_with_ai_recognition(
                    transfer_chain=transfer_chain,
                    fileitem=fileitem,
                    target_storage=target_storage,
                    transfer_options=transfer_options,
                    transfer_type=transfer_type,
                    source_meta=transfer_options.get("recognition_meta") or recognition_meta,
                    reason=episode_error,
                )
                if not state:
                    return False, episode_error, False, transfer_options
                media_type_error = self._get_preview_media_type_error(preview_data)
                if media_type_error:
                    return False, media_type_error, True, transfer_options

        preview_target_dirs = self._get_preview_target_dirs(preview_data)
        transfer_options["preview_target_dirs"] = preview_target_dirs
        transfer_options["preview_items"] = self._get_preview_items(preview_data)
        new_video_name = None
        for item in preview_data.get("items") or []:
            target = str(item.get("target") or "")
            if self._is_video_ext(Path(target).suffix):
                new_video_name = Path(target).name
                break
        transfer_options["video_new_name"] = new_video_name
        for target_dir in preview_target_dirs:
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
        recognition_meta: Any = None,
        recognition_mediainfo: Any = None,
    ) -> Tuple[bool, Any]:
        try:
            return transfer_chain.do_transfer(
                fileitem=fileitem,
                meta=recognition_meta,
                mediainfo=recognition_mediainfo,
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
                sync_extra_files=False,
            )
        finally:
            try:
                transfer_chain.jobview.remove_task(fileitem)
            except Exception as e:
                logger.debug("【OpenList 目录监控】清理预览任务失败: %s", e)

    def _retry_preview_with_ai_recognition(
        self,
        transfer_chain: Any,
        fileitem: schemas.FileItem,
        target_storage: str,
        transfer_options: Dict[str, Any],
        transfer_type: str,
        source_meta: Any,
        reason: str,
    ) -> Tuple[bool, Any, str]:
        if transfer_options.get("recognition_source") == "ai":
            return False, None, f"AI识别后仍不满足季集校验: {reason}"
        if not self._ai_recognition_fallback:
            return False, None, f"{reason}，AI识别兜底未启用"

        ai_meta, ai_mediainfo = self._build_ai_recognition_result(
            fileitem=fileitem,
            source_meta=source_meta,
            preview_data={"message": reason},
            force=True,
            require_episode_match=True,
        )
        if not ai_meta or not ai_mediainfo:
            return False, None, f"{reason}，AI识别兜底未命中可用结果"

        backup_options = {
            "recognition_meta": transfer_options.get("recognition_meta"),
            "recognition_mediainfo": transfer_options.get("recognition_mediainfo"),
            "recognition_source": transfer_options.get("recognition_source"),
            "ai_recognition_detail": transfer_options.get("ai_recognition_detail"),
        }
        transfer_options["recognition_meta"] = ai_meta
        transfer_options["recognition_mediainfo"] = ai_mediainfo
        transfer_options["recognition_source"] = "ai"
        transfer_options["ai_recognition_detail"] = self._build_ai_recognition_detail(
            fileitem=fileitem,
            mediainfo=ai_mediainfo,
            reason=f"原生识别季集校验失败：{reason}",
        )

        state, preview_data = self._preview_remote_transfer(
            transfer_chain=transfer_chain,
            fileitem=fileitem,
            target_storage=target_storage,
            transfer_path_options=transfer_options,
            transfer_type=transfer_type,
            recognition_meta=ai_meta,
            recognition_mediainfo=ai_mediainfo,
        )
        if not state:
            transfer_options.update(backup_options)
            return False, preview_data, f"AI识别兜底预览失败: {self._format_preview_error(preview_data)}"

        episode_error = self._get_preview_episode_guard_error(preview_data)
        if episode_error:
            transfer_options.update(backup_options)
            return False, preview_data, f"AI识别后仍不满足季集校验: {episode_error}"

        logger.info(
            "【OpenList 目录监控】原生识别季集校验失败，已改用AI识别: %s -> %s，原因：%s",
            getattr(fileitem, "path", "") or getattr(fileitem, "name", ""),
            getattr(ai_mediainfo, "title_year", "") or getattr(ai_mediainfo, "title", ""),
            reason,
        )
        return True, preview_data, ""

    def _transfer_extra_files(
        self,
        file_info: Dict[str, Any],
        transfer_options: Dict[str, Any],
        stats: Dict[str, Any],
        target_storage: str,
        transfer_type: str,
    ) -> int:
        extra_names = list(file_info.get("extra_file_names") or [])
        if target_storage != "alist":
            return 0

        source_path = str(file_info.get("path") or "")
        source_dir = str(Path(source_path).parent.as_posix()) if source_path else ""
        if not source_dir:
            return 0
        old_video_name = str(file_info.get("name") or Path(source_path).name)

        preview_target_dirs = transfer_options.get("preview_target_dirs") or []
        if not preview_target_dirs:
            return 0
        target_dir = preview_target_dirs[0]

        video_new_name = transfer_options.get("video_new_name")
        old_video_stem = os.path.splitext(old_video_name)[0]
        old_video_trunc = self._truncate_video_stem(old_video_stem.lower())
        new_video_stem = os.path.splitext(video_new_name)[0] if video_new_name else old_video_stem

        conf = self._get_alist_conf()
        if not conf:
            return 0
        base_url = self._get_alist_base_url(conf)
        headers = self._get_alist_auth_header(conf)
        if not base_url or not headers:
            return 0

        if not self._ensure_alist_directory(target_dir)[0]:
            logger.warning(
                "【OpenList 目录监控】字幕目标目录创建失败: %s", target_dir,
            )
            return 0

        src_dir = self._normalize_path(source_dir)
        dst_dir = self._normalize_path(target_dir)
        if src_dir == dst_dir:
            return 0

        if not extra_names:
            extra_names = self._find_matching_extra_names(
                base_url=base_url,
                headers=headers,
                source_dir=src_dir,
                video_name=old_video_name,
            )
        if not extra_names:
            logger.info(
                "【OpenList 目录监控】未匹配到同目录附加文件: %s",
                old_video_name,
            )
            return 0

        extra_key = self.STORE_EXTRA_FILES_KEY
        extra_records = set(self.get_data(extra_key) or [])
        success_count = 0

        for extra_name in extra_names:
            extra_path = f"{src_dir.rstrip('/')}/{extra_name}" if src_dir != "/" else f"/{extra_name}"
            extra_key_val = self._record_key({"path": extra_path, "name": extra_name})
            if transfer_type != "move" and extra_key_val in extra_records:
                logger.debug(
                    "【OpenList 目录监控】附加文件已记录，跳过重复复制: %s",
                    extra_path,
                )
                continue

            if not self._move_alist_extra_file(
                base_url=base_url,
                headers=headers,
                src_dir=src_dir,
                name=extra_name,
                dst_dir=dst_dir,
                old_video_stem=old_video_trunc,
                new_video_stem=new_video_stem,
                transfer_type=transfer_type,
            ):
                logger.warning(
                    "【OpenList 目录监控】附加文件移动失败: %s", extra_name,
                )
                continue

            extra_records.add(extra_key_val)
            success_count += 1

        self.save_data(extra_key, sorted(extra_records))
        if success_count:
            logger.info(
                "【OpenList 目录监控】已移动 %d 个附加文件到: %s",
                success_count, dst_dir,
            )
            stats.setdefault("transferred_extra", 0)
            stats["transferred_extra"] = int(stats["transferred_extra"]) + success_count
        return success_count

    def _move_alist_extra_file(
        self,
        base_url: str,
        headers: Dict[str, str],
        src_dir: str,
        name: str,
        dst_dir: str,
        old_video_stem: str,
        new_video_stem: str,
        transfer_type: str,
    ) -> bool:
        extra_stem, ext = os.path.splitext(name)
        if extra_stem.lower().startswith(old_video_stem.lower()):
            suffix = extra_stem[len(old_video_stem):]
            suffix = self._clean_extra_file_suffix(suffix)
        else:
            suffix = ""
        new_name = f"{new_video_stem}{suffix}{ext}"
        endpoint = "/api/fs/move" if transfer_type == "move" else "/api/fs/copy"
        resp = self._post_alist(
            base_url,
            endpoint,
            headers,
            json={
                "src_dir": src_dir,
                "names": [name],
                "dst_dir": dst_dir,
            },
        )
        if not resp:
            logger.warning("【OpenList 目录监控】移动附加文件无响应: %s", name)
            return False
        if resp.status_code != 200:
            logger.warning("【OpenList 目录监控】移动附加文件失败 %s: HTTP %s", name, resp.status_code)
            return False
        try:
            result = resp.json()
        except Exception as e:
            logger.warning("【OpenList 目录监控】移动附加文件解析响应失败 %s: %s", name, e)
            return False
        if result.get("code") != 200:
            logger.warning(
                "【OpenList 目录监控】移动附加文件失败 %s: %s",
                name, result.get("message") or "OpenList 返回错误",
            )
            return False
        if new_name != name:
            if not self._rename_alist_file(base_url, headers, dst_dir, name, new_name):
                logger.warning("【OpenList 目录监控】附加文件重命名失败: %s -> %s", name, new_name)
        logger.info(
            "【OpenList 目录监控】已移动附加文件: %s -> %s/%s",
            name, dst_dir, new_name,
        )
        return True

    def _find_matching_extra_names(
        self,
        base_url: str,
        headers: Dict[str, str],
        source_dir: str,
        video_name: str,
    ) -> List[str]:
        listing, error = self._list_directory(base_url, headers, source_dir, refresh=True)
        if error:
            logger.warning(
                "【OpenList 目录监控】重扫附加文件失败 %s: %s",
                source_dir, error,
            )
            return []
        sub_names = []
        for item in listing.get("files") or []:
            if item.get("is_dir"):
                continue
            name = str(item.get("name") or "").strip()
            if name and self._is_subtitle_ext(os.path.splitext(name)[1]):
                sub_names.append(name)
        matched = self._match_extra_files(video_name, sub_names)
        if matched:
            logger.info(
                "【OpenList 目录监控】重扫为主视频 %s 匹配到 %d 个同目录附加文件: %s",
                video_name, len(matched), ", ".join(matched[:5]),
            )
        return matched

    def _save_video_target_mapping(self, source_dir: str, target_dir: str) -> None:
        clean_source = self._normalize_path(source_dir)
        clean_target = self._normalize_path(target_dir)
        if not clean_source or not clean_target or clean_source == clean_target:
            return
        mapping_key = self.STORE_VIDEO_TARGETS_KEY
        mapping = dict(self.get_data(mapping_key) or {})
        if mapping.get(clean_source) == clean_target:
            return
        mapping[clean_source] = clean_target
        if len(mapping) > 1000:
            for key in list(mapping.keys())[:-500]:
                del mapping[key]
        self.save_data(mapping_key, mapping)

    def _lookup_video_target(self, source_dir: str) -> Optional[str]:
        clean_source = self._normalize_path(source_dir)
        if not clean_source:
            return None
        mapping = dict(self.get_data(self.STORE_VIDEO_TARGETS_KEY) or {})
        return mapping.get(clean_source)

    def _process_leftover_subtitles(
        self,
        base_url: str,
        headers: Dict[str, str],
        paths: List[str],
        stats: Dict[str, Any],
    ) -> int:
        if not self._sync_extra_files:
            return 0

        extra_key = self.STORE_EXTRA_FILES_KEY
        already_done = (
            set()
            if self._transfer_type == "move"
            else set(self.get_data(extra_key) or [])
        )
        leftovers = []
        seen_paths = set()

        for root_path in paths:
            if self._event.is_set():
                break
            clean_root = self._normalize_path(root_path)
            self._find_subtitle_files(
                base_url, headers, clean_root, 0, already_done, leftovers, seen_paths,
            )

        if not leftovers:
            return 0

        success_count = 0
        for sub_path, sub_name, source_dir in leftovers:
            if self._event.is_set():
                break
            target_dir = self._lookup_video_target(source_dir)
            if target_dir and not self._is_target_year_compatible(source_dir, target_dir):
                logger.warning(
                    "【OpenList 目录监控】残留字幕目标年份与源目录不一致，跳过缓存目标: %s -> %s",
                    source_dir, target_dir,
                )
                target_dir = None
            if not target_dir:
                target_dir = self._lookup_video_target_from_history(source_dir)
                if target_dir and not self._is_target_year_compatible(source_dir, target_dir):
                    logger.warning(
                        "【OpenList 目录监控】残留字幕目标年份与源目录不一致，跳过历史目标: %s -> %s",
                        source_dir, target_dir,
                    )
                    target_dir = None
                if target_dir:
                    self._save_video_target_mapping(source_dir, target_dir)
                    logger.info(
                        "【OpenList 目录监控】残留字幕通过转移历史匹配到目标路径: %s -> %s",
                        sub_path, target_dir,
                    )

            if not target_dir:
                logger.info(
                    "【OpenList 目录监控】残留字幕未匹配到目标路径，跳过: %s", sub_path,
                )
                continue

            old_video_stem = os.path.splitext(sub_name)[0].lower()
            trunc_stem = self._truncate_video_stem(old_video_stem)

            extra_key_val = self._record_key({"path": sub_path, "name": sub_name})
            extra_records = set(self.get_data(extra_key) or [])

            if self._move_alist_extra_file(
                base_url=base_url,
                headers=headers,
                src_dir=source_dir,
                name=sub_name,
                dst_dir=target_dir,
                old_video_stem=trunc_stem,
                new_video_stem=trunc_stem,
                transfer_type=self._transfer_type,
            ):
                extra_records.add(extra_key_val)
                self.save_data(extra_key, sorted(extra_records))
                success_count += 1
                stats.setdefault("transferred_extra", 0)
                stats["transferred_extra"] = int(stats["transferred_extra"]) + 1

        if success_count:
            logger.info(
                "【OpenList 目录监控】已整理 %d 个残留字幕文件", success_count,
            )
        return success_count

    def _collect_target_roots(self) -> List[str]:
        roots = []
        rules = self._get_monitor_target_rules()
        for rule in rules:
            target = self._normalize_path(rule.get("target") or "")
            if target and target not in roots:
                roots.append(target)
        if not roots:
            target = self._normalize_path(self._target_path) if self._target_path else ""
            if target:
                roots.append(target)
        return roots

    def _clean_extra_file_suffix(self, suffix: str) -> str:
        if not suffix:
            return suffix
        parts = suffix.split(".")
        clean_parts = [
            p for p in parts
            if p.lower() in {
                "chs", "cht", "chi", "eng", "en", "jpn", "ja",
                "kor", "ko", "spa", "es", "fre", "fra", "fr",
                "ger", "de", "ita", "it", "por", "pt", "pt-br",
                "rus", "ru", "tha", "th", "vie", "vi", "ara", "ar",
                "hin", "hi", "forced", "hi", "sdh", "cc", "default",
                "pgs", "sup",
            }
        ]
        return "." + ".".join(clean_parts) if clean_parts else suffix

    def _lookup_video_target_from_history(self, source_dir: str) -> Optional[str]:
        clean_source = self._normalize_path(source_dir)
        if not clean_source:
            return None
        try:
            import sqlite3
            conn = sqlite3.connect("/config/user.db")
            cursor = conn.cursor()
            cursor.execute(
                "SELECT src, dest FROM transferhistory ORDER BY date DESC"
            )
            for src, dest in cursor.fetchall():
                if not src or not dest:
                    continue
                src_parent = str(Path(src).parent.as_posix()) if src else ""
                if src_parent and self._normalize_path(src_parent) == clean_source:
                    dest_dir = str(Path(dest).parent.as_posix()) if dest else ""
                    if dest_dir:
                        conn.close()
                        return self._normalize_path(dest_dir)
            conn.close()
        except Exception as e:
            logger.debug(
                "【OpenList 目录监控】查询转移历史失败: %s", e,
            )
        return None

    def _find_subtitle_files(
        self,
        base_url: str,
        headers: Dict[str, str],
        path: str,
        depth: int,
        already_done: set,
        leftovers: List[Tuple[str, str, str]],
        seen_paths: set,
    ) -> None:
        clean_path = self._normalize_path(path)
        if clean_path in seen_paths:
            return
        seen_paths.add(clean_path)

        listing, error = self._list_directory(base_url, headers, clean_path)
        if error:
            return

        items = listing.get("files", [])
        dirs = []
        sub_files = []
        for item in items:
            name = str(item.get("name") or "").strip()
            if not name:
                continue
            if item.get("is_dir"):
                dirs.append(name)
            else:
                ext = os.path.splitext(name)[1]
                if self._is_subtitle_ext(ext):
                    sub_files.append(name)

        for sub_name in sub_files:
            sub_path = (
                f"{clean_path.rstrip('/')}/{sub_name}"
                if clean_path != "/"
                else f"/{sub_name}"
            )
            key = self._record_key({"path": sub_path, "name": sub_name})
            if key in already_done:
                continue
            leftovers.append((sub_path, sub_name, clean_path))

        if depth < self._max_depth:
            for dir_name in dirs:
                if self._event.is_set():
                    break
                child_path = (
                    f"{clean_path.rstrip('/')}/{dir_name}"
                    if clean_path != "/"
                    else f"/{dir_name}"
                )
                if self._delay_seconds > 0 and self._event.wait(self._delay_seconds):
                    break
                self._find_subtitle_files(
                    base_url, headers, child_path, depth + 1,
                    already_done, leftovers, seen_paths,
                )

    def _rename_alist_file(
        self, base_url: str, headers: Dict[str, str], dir_path: str, old_name: str, new_name: str,
    ) -> bool:
        clean_dir = self._normalize_path(dir_path)
        file_path = f"{clean_dir.rstrip('/')}/{old_name}" if clean_dir != "/" else f"/{old_name}"
        resp = self._post_alist(
            base_url,
            "/api/fs/rename",
            headers,
            json={"path": file_path, "name": new_name},
        )
        if not resp:
            logger.warning(
                "【OpenList 目录监控】重命名无响应: %s -> %s (path=%s)",
                old_name, new_name, file_path,
            )
            return False
        if resp.status_code != 200:
            logger.warning(
                "【OpenList 目录监控】重命名HTTP失败 %s -> %s (path=%s): HTTP %s",
                old_name, new_name, file_path, resp.status_code,
            )
            return False
        try:
            result = resp.json()
        except Exception as e:
            logger.warning(
                "【OpenList 目录监控】重命名解析失败 %s -> %s (path=%s): %s",
                old_name, new_name, file_path, e,
            )
            return False
        if result.get("code") != 200:
            logger.warning(
                "【OpenList 目录监控】重命名API失败 %s -> %s (path=%s): %s",
                old_name, new_name, file_path,
                result.get("message") or "OpenList 返回错误",
            )
            return False
        return True

    def _move_file_preserve_name(
        self,
        file_info: Dict[str, Any],
        transfer_options: Dict[str, Any],
        target_storage: str,
        transfer_type: str,
    ) -> bool:
        if target_storage != "alist":
            return False

        conf = self._get_alist_conf()
        if not conf:
            return False
        base_url = self._get_alist_base_url(conf)
        headers = self._get_alist_auth_header(conf)
        if not base_url or not headers:
            return False

        source_path = str(file_info.get("path") or "")
        if not source_path:
            return False

        source_dir = self._normalize_path(str(Path(source_path).parent.as_posix()))
        name = str(file_info.get("name") or Path(source_path).name)

        preview_target_dirs = transfer_options.get("preview_target_dirs") or []
        if not preview_target_dirs:
            return False
        target_dir = self._normalize_path(preview_target_dirs[0])

        if source_dir == target_dir:
            return True

        if not self._ensure_alist_directory(target_dir)[0]:
            return False

        endpoint = "/api/fs/move" if transfer_type == "move" else "/api/fs/copy"
        resp = self._post_alist(
            base_url,
            endpoint,
            headers,
            json={
                "src_dir": source_dir,
                "names": [name],
                "dst_dir": target_dir,
            },
        )
        if not resp:
            logger.warning(
                "【OpenList 目录监控】标准命名移动无响应: %s", source_path,
            )
            return False
        if resp.status_code != 200:
            logger.warning(
                "【OpenList 目录监控】标准命名移动HTTP失败 %s: HTTP %s",
                source_path, resp.status_code,
            )
            return False
        try:
            result = resp.json()
        except Exception as e:
            logger.warning(
                "【OpenList 目录监控】标准命名移动解析失败 %s: %s", source_path, e,
            )
            return False
        if result.get("code") != 200:
            logger.warning(
                "【OpenList 目录监控】标准命名移动失败 %s: %s",
                source_path, result.get("message") or "OpenList 返回错误",
            )
            return False

        logger.info(
            "【OpenList 目录监控】标准命名跳过重命名: %s -> %s/%s",
            source_path, target_dir, name,
        )
        return True

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

    def _get_preview_episode_count_error(self, preview_data: Any) -> str:
        return self._get_preview_episode_guard_error(preview_data)

    def _get_preview_episode_guard_error(self, preview_data: Any) -> str:
        if not isinstance(preview_data, dict):
            return ""
        for item in preview_data.get("items") or []:
            if not isinstance(item, dict):
                continue
            source_text = " ".join(
                str(value or "")
                for value in (item.get("source"), item.get("org_string"))
                if str(value or "").strip()
            )
            if not source_text:
                continue

            source_season = self._extract_source_season_number(source_text)
            expected_total = self._extract_source_total_episodes(source_text)
            source_episodes = self._extract_source_episode_numbers(source_text)
            max_source_episode = max(source_episodes) if source_episodes else 0
            media_type = str(item.get("type") or "").strip()
            if media_type and media_type != MediaType.TV.value:
                if source_season or source_episodes or expected_total:
                    return (
                        f"识别结果疑似错误：源文件包含剧集季集信息，"
                        f"但识别结果类型为 {media_type}"
                    )
                continue
            item_season = self._safe_positive_int(item.get("season"))
            item_episode = self._safe_positive_int(item.get("episode"))
            item_episode_end = self._safe_positive_int(item.get("episode_end")) or item_episode

            if source_season and item_season and source_season != item_season:
                return (
                    f"识别结果疑似错误：源文件为第 {source_season} 季，"
                    f"但识别结果为第 {item_season} 季"
                )
            if source_season and not item_season:
                return f"识别结果疑似错误：源文件为第 {source_season} 季，但识别结果缺少季号"
            if source_episodes and not item_episode:
                return (
                    f"识别结果疑似错误：源文件包含第 {', '.join(map(str, source_episodes[:5]))} 集，"
                    "但识别结果缺少集数"
                )
            if source_episodes and item_episode:
                preview_start = item_episode
                preview_end = item_episode_end or item_episode
                missing = [
                    episode for episode in source_episodes
                    if episode < preview_start or episode > preview_end
                ]
                if missing:
                    return (
                        f"识别结果疑似错误：源文件包含第 {', '.join(map(str, missing[:5]))} 集，"
                        f"但识别结果为第 {preview_start}"
                        f"{f'-{preview_end}' if preview_end != preview_start else ''} 集"
                    )

            if not expected_total and not max_source_episode:
                continue

            tmdb_id = self._extract_preview_tmdb_id(item)
            if not tmdb_id:
                continue
            season = item_season or source_season or 1
            tmdb_episode_count = self._get_tmdb_season_episode_count(tmdb_id, season)
            if not tmdb_episode_count:
                continue

            title = str(item.get("title") or f"TMDB {tmdb_id}").strip()
            if expected_total and expected_total > tmdb_episode_count:
                return (
                    f"识别结果疑似错误：源路径标记全 {expected_total} 集，"
                    f"但命中 {title} 第 {season} 季只有 {tmdb_episode_count} 集"
                )
            if max_source_episode and max_source_episode > tmdb_episode_count:
                return (
                    f"识别结果疑似错误：源文件包含第 {max_source_episode} 集，"
                    f"但命中 {title} 第 {season} 季只有 {tmdb_episode_count} 集"
                )
        return ""

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
        target_dirs.sort(key=lambda d: d.count("/"), reverse=True)
        return target_dirs

    @staticmethod
    def _get_preview_items(preview_data: Any) -> List[Dict[str, Any]]:
        if not isinstance(preview_data, dict):
            return []
        items = []
        for item in preview_data.get("items") or []:
            if isinstance(item, dict):
                items.append(dict(item))
        return items

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
    ) -> Tuple[int, int, List[str]]:
        root_paths = {self._normalize_path(root) for root in roots if root}
        cleaned_dirs = 0
        cleaned_files = 0
        cleaned_file_items = []
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
            (
                cleanable,
                residual_files,
                message,
                residual_file_items,
            ) = self._prepare_alist_dir_for_cleanup(base_url, headers, path)
            cleaned_files += residual_files
            cleaned_file_items.extend(residual_file_items)
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
        return cleaned_dirs, cleaned_files, cleaned_file_items

    @staticmethod
    def _is_child_path(root: str, path: str) -> bool:
        if not root or not path:
            return False
        if root == "/":
            return path != "/"
        return path.startswith(root.rstrip("/") + "/")

    def _prepare_alist_dir_for_cleanup(
        self, base_url: str, headers: Dict[str, str], path: str
    ) -> Tuple[bool, int, str, List[str]]:
        listing, error = self._list_directory(base_url, headers, path, refresh=True)
        if error:
            return False, 0, error, []

        items = listing.get("files") or []
        if not items:
            return True, 0, "", []

        dirs = [item for item in items if item.get("is_dir")]
        if dirs:
            names = self._format_preview_names(dirs)
            return False, 0, f"目录仍有子目录: {names}", []

        files = [item for item in items if not item.get("is_dir")]
        if not files:
            return True, 0, "", []
        if not self._should_clean_residual_files():
            return False, 0, "目录不为空", []

        allowed_extensions = self._get_residual_file_extensions()
        if not allowed_extensions:
            return False, 0, "未配置残留文件后缀", []

        max_size_bytes = int(float(self._residual_file_max_size_mb or 0) * 1024 * 1024)
        min_video_size_bytes = int(float(self._min_file_size_mb or 0) * 1024 * 1024)
        residual_names = []
        blockers = []
        for item in files:
            name = str(item.get("name") or "").strip()
            if not name:
                blockers.append("文件名为空")
                continue
            ext = self._normalize_extension(os.path.splitext(name)[1])
            size = self._to_file_size(item.get("size"))
            if ext not in allowed_extensions:
                suffix = "视频未配置为残留" if ext in self.VIDEO_EXTENSIONS else "后缀未配置"
                blockers.append(f"{name}({suffix})")
                continue
            if max_size_bytes and size > max_size_bytes:
                blockers.append(f"{name}(超过大小限制)")
                continue
            if (
                ext in self.VIDEO_EXTENSIONS
                and min_video_size_bytes
                and size >= min_video_size_bytes
            ):
                blockers.append(f"{name}(达到主视频最小大小)")
                continue
            residual_names.append(name)

        if blockers:
            return False, 0, f"目录仍有非残留文件: {', '.join(blockers[:5])}", []
        if not residual_names:
            return True, 0, "", []

        state, message = self._remove_alist_names(
            base_url, headers, path, residual_names
        )
        if not state:
            return False, 0, f"清理残留文件失败: {message}", []

        state, message = self._wait_alist_dir_items_absent(
            base_url, headers, path, residual_names
        )
        if not state:
            return False, 0, message, []

        listing, error = self._list_directory(base_url, headers, path, refresh=True)
        if error:
            return False, len(residual_names), error, []
        remaining = listing.get("files") or []
        if remaining:
            return False, len(residual_names), f"目录仍有 {len(remaining)} 个项目", []

        residual_file_items = []
        for name in residual_names:
            residual_path = f"{path.rstrip('/')}/{name}"
            residual_file_items.append(residual_path)
            logger.info(
                "【OpenList 目录监控】已清理残留文件: %s/%s",
                path.rstrip("/"),
                name,
            )
        return True, len(residual_names), "", residual_file_items

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

    def _record_transfer_success(
        self,
        stats: Dict[str, Any],
        file_info: Dict[str, Any],
        transfer_options: Dict[str, Any],
        target_storage: str,
        transfer_type: str,
        extra_count: int = 0,
    ) -> None:
        items = transfer_options.get("preview_items") or []
        source_path = str(file_info.get("path") or "")
        preview_item = self._find_preview_item(items, source_path)
        ai_detail = dict(transfer_options.get("ai_recognition_detail") or {})

        target = ""
        if preview_item:
            target = self._clean_render_path(
                preview_item.get("target") or preview_item.get("target_dir") or ""
            )
        if not target:
            preview_dirs = transfer_options.get("preview_target_dirs") or []
            target = self._clean_render_path(preview_dirs[0]) if preview_dirs else ""
        if not target:
            target_path = transfer_options.get("target_path")
            target = self._normalize_path(target_path) if target_path else ""

        title = str((preview_item or {}).get("title") or ai_detail.get("title") or "").strip()
        media_type = str((preview_item or {}).get("type") or ai_detail.get("type") or "").strip()
        season = self._safe_positive_int((preview_item or {}).get("season"))
        episode = self._safe_positive_int((preview_item or {}).get("episode"))
        episode_end = self._safe_positive_int((preview_item or {}).get("episode_end"))

        detail = {
            "source": source_path,
            "name": str(file_info.get("name") or Path(source_path).name or ""),
            "title": title,
            "type": media_type,
            "season": season or "",
            "episode": episode or "",
            "episode_end": episode_end or "",
            "target": target,
            "target_storage": self._format_storage_name(target_storage),
            "transfer_type": transfer_type,
            "recognition": (
                "AI" if transfer_options.get("recognition_source") == "ai"
                else "标准命名跳过" if transfer_options.get("recognition_source") == "standard"
                else "原生"
            ),
            "reason": str(ai_detail.get("reason") or "").strip(),
            "extra_count": int(extra_count or 0),
        }
        transferred_items = stats.setdefault("transferred_items", [])
        if isinstance(transferred_items, list):
            transferred_items.append(detail)

    @staticmethod
    def _find_preview_item(items: List[Dict[str, Any]], source_path: str) -> Dict[str, Any]:
        if not items:
            return {}
        clean_source = str(source_path or "")
        for item in items:
            if str(item.get("source") or "") == clean_source:
                return item
        source_name = Path(clean_source).name if clean_source else ""
        if source_name:
            for item in items:
                if Path(str(item.get("source") or "")).name == source_name:
                    return item
        return items[0]

    def _record_ai_recognition_success(
        self,
        stats: Dict[str, Any],
        file_info: Dict[str, Any],
        transfer_options: Dict[str, Any],
    ) -> None:
        if transfer_options.get("recognition_source") != "ai":
            return
        detail = dict(transfer_options.get("ai_recognition_detail") or {})
        if not detail:
            return
        detail["source"] = str(file_info.get("path") or detail.get("source") or "")
        detail["name"] = str(file_info.get("name") or detail.get("name") or "")
        stats["ai_recognition_fallback"] = int(
            stats.get("ai_recognition_fallback") or 0
        ) + 1
        items = stats.setdefault("ai_recognition_fallback_items", [])
        if isinstance(items, list):
            items.append(detail)

    @staticmethod
    def _build_ai_recognition_detail(
        fileitem: schemas.FileItem,
        mediainfo: Any,
        reason: str = "",
    ) -> Dict[str, Any]:
        media_type = getattr(mediainfo, "type", None)
        return {
            "source": str(getattr(fileitem, "path", "") or ""),
            "name": str(getattr(fileitem, "name", "") or ""),
            "title": str(
                getattr(mediainfo, "title_year", "")
                or getattr(mediainfo, "title", "")
                or ""
            ),
            "tmdb_id": getattr(mediainfo, "tmdb_id", None) or "",
            "type": getattr(media_type, "value", "") if media_type else "",
            "reason": str(reason or "AI识别兜底").strip(),
        }

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
                "transferred_extra",
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
        transferred_items = data.get("transferred_items") or []
        if isinstance(transferred_items, list) and transferred_items:
            lines.append("整理明细：")
            for item in transferred_items[:10]:
                lines.extend(self._format_transfer_detail_lines(item))
            if len(transferred_items) > 10:
                lines.append(f"- 其余 {len(transferred_items) - 10} 个整理条目已省略")
        ai_items = data.get("ai_recognition_fallback_items") or []
        ai_count = int(data.get("ai_recognition_fallback") or len(ai_items) or 0)
        if ai_count:
            lines.append(f"AI识别兜底：{ai_count}")
            if isinstance(ai_items, list):
                for item in ai_items[:5]:
                    source_name = str(item.get("name") or Path(str(item.get("source") or "")).name or "-")
                    media_title = str(item.get("title") or "-")
                    tmdb_id = str(item.get("tmdb_id") or "-")
                    media_type = str(item.get("type") or "-")
                    reason = str(item.get("reason") or "").strip()
                    lines.append(
                        f"- {source_name} -> {media_title}（{media_type}，TMDB {tmdb_id}）"
                    )
                    if reason:
                        lines.append(f"  原因：{reason}")
                if len(ai_items) > 5:
                    lines.append(f"- 其余 {len(ai_items) - 5} 个 AI 兜底条目已省略")
        if int(data.get("limited_files") or 0):
            lines.append(f"延后处理：{int(data.get('limited_files') or 0)}")
        if int(data.get("skipped_type") or 0):
            lines.append(f"跳过媒体类型：{int(data.get('skipped_type') or 0)}")
        if int(data.get("skip_rename_standard_naming_count") or 0):
            lines.append(f"标准命名跳过重命名：{int(data.get('skip_rename_standard_naming_count') or 0)}")
        if int(data.get("transferred_extra") or 0):
            lines.append(f"同步附加文件：{int(data.get('transferred_extra') or 0)}")
        if int(data.get("cleaned_files") or 0):
            lines.append(f"清理残留文件：{int(data.get('cleaned_files') or 0)}")
            cleaned_items = data.get("cleaned_file_items") or []
            if cleaned_items:
                lines.append("清理残留明细：")
                for item in cleaned_items[:10]:
                    lines.append(f"- {item}")
                if len(cleaned_items) > 10:
                    lines.append(f"- 其余 {len(cleaned_items) - 10} 个残留文件已省略")
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
    def _format_transfer_detail_lines(item: Dict[str, Any]) -> List[str]:
        source_name = str(item.get("name") or Path(str(item.get("source") or "")).name or "-")
        title = str(item.get("title") or "-")
        media_type = str(item.get("type") or "-")
        recognition = str(item.get("recognition") or "-")
        target = str(item.get("target") or "-")
        season = item.get("season")
        episode = item.get("episode")
        episode_end = item.get("episode_end")
        extra_count = int(item.get("extra_count") or 0)
        reason = str(item.get("reason") or "").strip()

        episode_label = ""
        if season:
            episode_label = f"S{int(season):02d}"
        if episode:
            ep_text = f"E{int(episode):02d}"
            if episode_end and int(episode_end) != int(episode):
                ep_text = f"{ep_text}-E{int(episode_end):02d}"
            episode_label = f"{episode_label}{ep_text}" if episode_label else ep_text

        summary = f"- {source_name} -> {title}"
        details = []
        if media_type and media_type != "-":
            details.append(media_type)
        if episode_label:
            details.append(episode_label)
        if recognition and recognition != "-":
            details.append(f"{recognition}识别")
        if extra_count:
            details.append(f"附加文件 {extra_count} 个")
        if details:
            summary = f"{summary}（{'，'.join(details)}）"

        lines = [summary, f"  目标：{target}"]
        if reason:
            lines.append(f"  原因：{reason}")
        return lines

    @staticmethod
    def _format_extra_files(stats: Dict[str, Any]) -> str:
        extra = int(stats.get("transferred_extra") or 0)
        return f"，附加文件 {extra} 个" if extra else ""

    @staticmethod
    def _format_skip_rename_count(stats: Dict[str, Any]) -> str:
        count = int(stats.get("skip_rename_standard_naming_count") or 0)
        return f"，跳过重命名 {count} 个" if count else ""

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

    def _get_configured_video_extensions(self) -> set:
        if not self._extensions:
            return set(self.VIDEO_EXTENSIONS)
        configured = {
            self._normalize_extension(item)
            for item in self._extensions.replace(",", "\n").splitlines()
            if str(item or "").strip()
        }
        return configured & self.VIDEO_EXTENSIONS

    def _is_video_ext(self, ext: str) -> bool:
        ext = self._normalize_extension(ext)
        return ext in self._get_configured_video_extensions()

    def _is_media_ext(self, ext: str) -> bool:
        return self._is_video_ext(ext)

    def _is_subtitle_ext(self, ext: str) -> bool:
        return self._normalize_extension(ext) in self.SUBTITLE_EXTENSIONS

    @staticmethod
    def _is_standard_naming_format(name: str) -> bool:
        return bool(re.search(r'(?:[._\- ]?S\d{2}E\d{2})', name, re.I))

    @staticmethod
    def _match_extra_files(video_name: str, all_sub_names: List[str]) -> List[str]:
        video_stem = os.path.splitext(video_name)[0].lower()
        trunc_video_stem = OpenListMonitor._truncate_video_stem(video_stem)
        matched = []
        for sub_name in all_sub_names:
            sub_stem = os.path.splitext(sub_name)[0].lower()
            if sub_stem.startswith(trunc_video_stem):
                suffix = sub_stem[len(trunc_video_stem):]
                if not suffix or suffix[0] in "._- ":
                    matched.append(sub_name)
        return matched

    @staticmethod
    def _truncate_video_stem(stem: str) -> str:
        m = re.search(r'(?:\.|(?<=_|-))s\d{1,2}e\d{1,4}', stem, re.I)
        if m:
            return stem[:m.end()]
        m = re.search(r'(?:\.|(?<=_|-))ep?\d{1,4}', stem, re.I)
        if m:
            return stem[:m.end()]
        m = re.search(r'(?:\.|(?<=_|-))\d{4}(?:\s*\.\s*\d{3,4}p)?', stem, re.I)
        if m:
            return stem[:m.start()]
        return stem

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
            if ext:
                extensions.add(ext)
        return extensions

    def _get_top_level_categories(self) -> set:
        categories = set()
        for item in str(self._top_level_categories or "").replace(",", "\n").splitlines():
            category = item.strip()
            if category:
                categories.add(category)
        return categories

    def _build_recognition_meta(self, file_info: Dict[str, Any]) -> Any:
        rules = self._parse_recognition_rewrite_rules(
            self._recognition_rewrite_rules
        )
        if not rules:
            return None

        source_path = str(file_info.get("path") or "").strip()
        if not source_path:
            return None
        source_name = str(file_info.get("name") or Path(source_path).name).strip()
        virtual_path = source_path
        virtual_name = source_name
        applied_rule = None

        for rule in rules:
            source = rule.get("source")
            target = rule.get("target")
            if not source or not target:
                continue
            if source in virtual_name:
                virtual_name = virtual_name.replace(source, target, 1)
                parent = Path(source_path).parent.as_posix()
                virtual_path = (
                    f"{parent.rstrip('/')}/{virtual_name}"
                    if parent and parent != "."
                    else virtual_name
                )
                applied_rule = rule
                break
            if source in virtual_path:
                virtual_path = virtual_path.replace(source, target, 1)
                virtual_name = Path(virtual_path).name
                applied_rule = rule
                break

        if not applied_rule or virtual_path == source_path:
            return None

        try:
            from app.core.metainfo import MetaInfoPath
        except Exception as e:
            logger.warning("【OpenList 目录监控】加载识别词替换解析器失败: %s", e)
            return None

        try:
            meta = MetaInfoPath(Path(virtual_path))
        except Exception as e:
            logger.warning(
                "【OpenList 目录监控】识别词替换解析失败 %s -> %s: %s",
                source_name,
                virtual_name,
                e,
            )
            return None
        if not meta:
            return None
        logger.info(
            "【OpenList 目录监控】应用识别词替换: %s => %s，%s -> %s",
            applied_rule.get("source"),
            applied_rule.get("target"),
            source_name,
            virtual_name,
        )
        return meta

    def _build_ai_recognition_result(
        self,
        fileitem: schemas.FileItem,
        source_meta: Any = None,
        preview_data: Any = None,
        force: bool = False,
        require_episode_match: bool = False,
    ) -> Tuple[Any, Any]:
        if not force and not self._should_ai_recognition_fallback(preview_data):
            return None, None

        source_path = str(getattr(fileitem, "path", "") or "").strip()
        source_name = str(getattr(fileitem, "name", "") or Path(source_path).name).strip()
        if not source_path and not source_name:
            return None, None

        try:
            from app.chain.media import MediaChain
            from app.core.metainfo import MetaInfo, MetaInfoPath
        except Exception as e:
            logger.warning("【OpenList 目录监控】加载AI识别依赖失败: %s", e)
            return None, None

        base_meta = source_meta
        if not base_meta:
            try:
                base_meta = MetaInfoPath(Path(source_path or source_name))
            except Exception:
                base_meta = None

        candidates = self._get_ai_recognition_candidates(
            fileitem=fileitem,
            source_meta=base_meta,
        )
        if not candidates:
            logger.info(
                "【OpenList 目录监控】AI识别未生成候选: %s",
                source_name or source_path,
            )
            return None, None

        media_chain = MediaChain()
        raw_text = source_path or source_name
        failed_candidates = []
        for candidate in candidates:
            confidence = self._to_float(candidate.get("confidence"), 0, 0, 1)
            if confidence < self.AI_RECOGNITION_MIN_CONFIDENCE:
                failed_candidates.append(
                    f"{self._format_ai_candidate(candidate)}: 置信度不足"
                )
                logger.debug(
                    "【OpenList 目录监控】AI识别候选置信度不足，跳过: %s %.2f",
                    candidate.get("name"),
                    confidence,
                )
                continue

            media_type = self._normalize_ai_media_type(
                candidate.get("media_type"),
                source_meta=base_meta,
            )
            if media_type and media_type.value not in self._media_types:
                failed_candidates.append(
                    f"{self._format_ai_candidate(candidate)}: 媒体类型未启用"
                )
                logger.debug(
                    "【OpenList 目录监控】AI识别候选媒体类型未选择，跳过: %s %s",
                    candidate.get("name"),
                    media_type.value,
                )
                continue

            try:
                meta = MetaInfo(raw_text)
            except Exception as e:
                failed_candidates.append(
                    f"{self._format_ai_candidate(candidate)}: 元数据构建失败"
                )
                logger.debug("【OpenList 目录监控】AI识别构建元数据失败: %s", e)
                continue

            meta.name = candidate.get("name")
            year = str(candidate.get("year") or "").strip()
            meta.year = year if len(year) == 4 and year.isdigit() else None

            season = self._positive_int(
                candidate.get("season"),
                getattr(base_meta, "begin_season", None),
                getattr(meta, "begin_season", None),
            )
            episode = self._positive_int(
                getattr(base_meta, "begin_episode", None),
                getattr(meta, "begin_episode", None),
                candidate.get("episode"),
            )
            meta.begin_season = season
            meta.begin_episode = episode
            if media_type:
                meta.type = media_type
            elif season or episode:
                meta.type = MediaType.TV

            if meta.type and meta.type.value not in self._media_types:
                failed_candidates.append(
                    f"{self._format_ai_candidate(candidate)}: 校验媒体类型未启用"
                )
                logger.debug(
                    "【OpenList 目录监控】AI识别候选校验类型未选择，跳过: %s %s",
                    candidate.get("name"),
                    meta.type.value,
                )
                continue

            try:
                mediainfo = media_chain.recognize_media(meta=meta, cache=False)
            except Exception as e:
                failed_candidates.append(
                    f"{self._format_ai_candidate(candidate)}: 二次校验异常"
                )
                logger.debug(
                    "【OpenList 目录监控】AI识别候选二次校验异常: %s - %s",
                    candidate.get("name"),
                    e,
                )
                continue
            if not mediainfo:
                failed_candidates.append(
                    f"{self._format_ai_candidate(candidate)}: MoviePilot未命中"
                )
                logger.debug(
                    "【OpenList 目录监控】AI识别候选二次校验未命中: %s",
                    candidate.get("name"),
                )
                continue
            if mediainfo.type and mediainfo.type.value not in self._media_types:
                failed_candidates.append(
                    f"{self._format_ai_candidate(candidate)}: 命中结果类型未启用"
                )
                logger.info(
                    "【OpenList 目录监控】AI识别命中但媒体类型未选择，跳过: %s %s",
                    getattr(mediainfo, "title_year", "") or getattr(mediainfo, "title", ""),
                    mediainfo.type.value,
                )
                continue
            if not self._is_ai_recognition_year_compatible(raw_text, candidate, mediainfo):
                failed_candidates.append(
                    f"{self._format_ai_candidate(candidate)}: 年份与源路径不一致"
                )
                logger.info(
                    "【OpenList 目录监控】AI识别命中但年份与源路径不一致，跳过: %s -> %s，源年份=%s，命中年份=%s",
                    source_name or source_path,
                    getattr(mediainfo, "title_year", "") or getattr(mediainfo, "title", ""),
                    ",".join(self._extract_plausible_years(raw_text)) or "-",
                    ",".join(self._extract_ai_result_years(candidate, mediainfo)) or "-",
                )
                continue
            if require_episode_match:
                episode_error = self._get_ai_episode_guard_error(raw_text, meta, mediainfo)
                if episode_error:
                    failed_candidates.append(
                        f"{self._format_ai_candidate(candidate)}: {episode_error}"
                    )
                    logger.info(
                        "【OpenList 目录监控】AI识别命中但季集校验未通过，跳过: %s -> %s，%s",
                        source_name or source_path,
                        getattr(mediainfo, "title_year", "") or getattr(mediainfo, "title", ""),
                        episode_error,
                    )
                    continue

            logger.info(
                "【OpenList 目录监控】AI识别兜底命中: %s -> %s，置信度 %.2f，TMDB %s",
                source_name or source_path,
                getattr(mediainfo, "title_year", "") or getattr(mediainfo, "title", ""),
                confidence,
                getattr(mediainfo, "tmdb_id", "") or "-",
            )
            return meta, mediainfo

        if failed_candidates:
            logger.info(
                "【OpenList 目录监控】AI识别候选均未通过: %s，候选=%s",
                source_name or source_path,
                "；".join(failed_candidates[:self.AI_RECOGNITION_MAX_CANDIDATES]),
            )
        return None, None

    def _should_ai_recognition_fallback(self, preview_data: Any) -> bool:
        if not self._ai_recognition_fallback:
            return False
        message = self._format_preview_error(preview_data)
        if not message:
            return False
        return "媒体信息" in message and ("未识别" in message or "无法识别" in message)

    def _get_ai_recognition_candidates(
        self,
        fileitem: schemas.FileItem,
        source_meta: Any = None,
    ) -> List[Dict[str, Any]]:
        cache_key = self._ai_recognition_cache_key(fileitem, source_meta)
        with self._ai_recognition_cache_lock:
            cached = self._ai_recognition_cache.get(cache_key)
        if cached is not None:
            return cached

        heuristic_candidates = self._build_heuristic_recognition_candidates(
            fileitem=fileitem,
            source_meta=source_meta,
        )
        try:
            candidates = self._invoke_ai_recognition_candidates(fileitem, source_meta)
        except Exception as e:
            logger.warning("【OpenList 目录监控】AI识别调用失败: %s", e)
            candidates = []

        normalized = self._normalize_ai_candidates(heuristic_candidates + candidates)
        with self._ai_recognition_cache_lock:
            self._ai_recognition_cache[cache_key] = normalized
        return normalized

    def _build_heuristic_recognition_candidates(
        self,
        fileitem: schemas.FileItem,
        source_meta: Any = None,
    ) -> List[Dict[str, Any]]:
        titles = []
        source_path = str(getattr(fileitem, "path", "") or "").strip()
        source_name = str(getattr(fileitem, "name", "") or Path(source_path).name).strip()
        path_obj = Path(source_path) if source_path else None
        source_values = [
            source_name,
            Path(source_name).stem if source_name else "",
            Path(source_path).stem if source_path else "",
            getattr(source_meta, "name", ""),
            getattr(source_meta, "title", ""),
        ]
        if path_obj:
            source_values.extend(
                part for part in reversed(path_obj.parts[:-1])
                if part and part not in {"/", ".", ".."}
            )

        for value in source_values:
            for title in self._extract_candidate_title_hints(value):
                if title and title not in titles:
                    titles.append(title)

        media_type = self._normalize_ai_media_type(None, source_meta=source_meta)
        season = getattr(source_meta, "begin_season", None) or 0
        episode = getattr(source_meta, "begin_episode", None) or 0
        candidates = []
        for title in titles:
            for variant in self._build_title_punctuation_variants(title):
                candidates.append({
                    "name": variant,
                    "year": str(getattr(source_meta, "year", "") or ""),
                    "media_type": (
                        "tv" if media_type == MediaType.TV
                        else "movie" if media_type == MediaType.MOVIE
                        else "unknown"
                    ),
                    "season": season,
                    "episode": episode,
                    "confidence": 0.58 if variant != title else 0.55,
                    "reason": "filename heuristic punctuation variant",
                })
        return candidates

    @classmethod
    def _extract_candidate_title_hints(cls, value: Any) -> List[str]:
        text = str(value or "").strip()
        if not text:
            return []
        candidates = [
            text,
            re.sub(r"[._]+", " ", text),
            re.sub(r"\s*[-_]\s*", " ", text),
        ]
        hints = []
        for item in candidates:
            title = cls._extract_candidate_title(item)
            if (
                title
                and not cls._is_noise_candidate_title(title)
                and title not in hints
            ):
                hints.append(title)
        return hints

    @staticmethod
    def _extract_candidate_title(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        text = Path(text).stem
        text = re.sub(r"^\s*(?:\[[^\]]+\]|【[^】]+】)\s*", "", text)
        while True:
            stripped = re.sub(r"\s*(?:\[[^\]]+\]|【[^】]+】)\s*$", "", text).strip()
            if stripped == text:
                break
            text = stripped
        text = re.sub(
            r"\b(?:2160p|1080p|720p|480p|web[-_. ]?dl|webrip|bluray|bdrip|"
            r"hdrip|x264|x265|h264|h265|hevc|avc|aac|flac)\b.*$",
            "",
            text,
            flags=re.I,
        ).strip()
        text = re.sub(r"\s+\.\s+", ".", text)
        text = re.sub(r"(?:\.|(?<=\s))S\d{1,2}E\d{1,4}(?!\d)\b.*$", "", text, flags=re.I).strip()
        text = re.sub(r"(?:_|-|\s)S\d{1,2}E\d{1,4}(?!\d)\b.*$", "", text, flags=re.I).strip()
        text = re.sub(r"(?:\.|(?<=\s))EP?\d{1,4}(?!\d)\b.*$", "", text, flags=re.I).strip()
        text = re.sub(r"(?:_|-|\s)EP?\d{1,4}(?!\d)\b.*$", "", text, flags=re.I).strip()
        text = re.sub(r"(?:\.|(?<=\s))\d{4}\s*\.\s*\d{3,4}p\b.*$", "", text, flags=re.I).strip()
        text = re.sub(r"\s+season\s*\d{1,2}\b.*$", "", text, flags=re.I).strip()
        text = re.sub(r"\s*\.\s*", " ", text)
        text = re.sub(r"[_]+", " ", text)
        text = re.sub(r"\s+-\s+\d{1,4}\b.*$", "", text).strip()
        text = re.sub(
            r"\s+(?:S\d{1,2}E\d{1,4}(?!\d)|EP?\d{1,4}(?!\d)|\d{1,4})\b.*$",
            "",
            text,
            flags=re.I,
        ).strip()
        text = re.sub(r"\s+", " ", text).strip(" -_.")
        return text

    @staticmethod
    def _is_noise_candidate_title(value: Any) -> bool:
        text = str(value or "").strip().lower()
        if not text:
            return True
        return bool(re.fullmatch(
            r"(?:season\s*)?\d{1,2}|s\d{1,2}|episode\s*\d{1,4}|ep?\d{1,4}|"
            r"2160p|1080p|720p|480p|web[- ]?dl|downloads?|complete|movie|tv|"
            r"anime|videos?|episodes?|sample|subtitles?|番剧|电影|剧集|season",
            text,
            flags=re.I,
        ))

    @staticmethod
    def _build_title_punctuation_variants(title: str) -> List[str]:
        clean_title = " ".join(str(title or "").split()).strip()
        if not clean_title:
            return []
        variants = [clean_title]
        ascii_mark_title = (
            clean_title.replace("！", "!")
            .replace("？", "?")
            .replace("：", ":")
        )
        if ascii_mark_title != clean_title:
            variants.append(ascii_mark_title)
        no_trailing_mark = clean_title.rstrip("!?！？")
        if no_trailing_mark and no_trailing_mark != clean_title:
            variants.append(no_trailing_mark)
        if not clean_title.endswith(("!?", "?!", "！?", "？！", "!", "?", "！", "？")):
            variants.extend([
                f"{clean_title}!?",
                f"{clean_title}?!",
                f"{clean_title}！?",
            ])
        elif clean_title.endswith(("!", "！")):
            variants.append(f"{clean_title}?")
        elif clean_title.endswith(("?", "？")):
            variants.append(f"{clean_title}!")
        deduped = []
        for item in variants:
            if item not in deduped:
                deduped.append(item)
        return deduped

    def _invoke_ai_recognition_candidates(
        self,
        fileitem: schemas.FileItem,
        source_meta: Any = None,
    ) -> List[Dict[str, Any]]:
        from langchain_core.prompts import ChatPromptTemplate
        from pydantic import BaseModel, Field

        try:
            from app.helper.llm import LLMHelper
        except ImportError:
            from app.agent.llm import LLMHelper

        class AIRecognitionCandidate(BaseModel):
            name: str = Field(default="", description="作品标题或常用别名")
            year: str = Field(default="", description="四位年份，不确定则空")
            media_type: str = Field(default="unknown", description="movie、tv 或 unknown")
            season: int = Field(default=0, description="季号，不确定则 0")
            episode: int = Field(default=0, description="集号，不确定则 0")
            confidence: float = Field(default=0.0, description="0 到 1 的置信度")
            reason: str = Field(default="", description="简短理由")

        class AIRecognitionCandidateBundle(BaseModel):
            candidates: List[AIRecognitionCandidate] = Field(
                default_factory=list,
                description="按置信度从高到低排列的候选",
            )

        llm = self._run_async_compatible(LLMHelper.get_llm(streaming=False))
        prompt = ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    """你是 MoviePilot 的影视文件名识别兜底助手。

请根据原始标题、路径、清洗提示和 MoviePilot 当前解析提示，生成最多 10 个适合交给 MoviePilot/TMDB 再次识别的候选作品名。

规则：
1. 候选 name 只保留作品名或常用正式别名，不要包含发布组、集数、分辨率、编码、字幕、网盘目录等噪音。
2. 优先输出 TMDB/MoviePilot 更容易搜到的标题：官方英文名、日文原名、中文常用名、带正确标点的罗马音、去标点罗马音。
3. 不要编造不存在的作品；不确定时返回空候选或降低 confidence。
4. 如果能从文件名判断季/集，填 season/episode；否则填 0。
5. media_type 只能是 movie、tv、unknown。
6. 罗马音标题里的 !、?、:、- 等符号可能是正式标题的一部分，需要保留一个带正确符号的候选。
7. 如果原始路径或标题包含四位年份，候选 year 必须与该年份一致；不要把年份不同的旧作品作为候选。
8. 不要只根据单个词或后缀联想作品，必须确认完整罗马音标题是该作品的正式名或常用别名。
9. 按最可能命中的顺序排列，confidence 范围为 0 到 1。""",
                ),
                (
                    "human",
                    """原始标题：
{title}

原始路径：
{path}

清洗提示：
{source_context}

MoviePilot 当前解析提示：
{meta_hint}

插件允许整理的媒体类型：
{media_types}
""",
                ),
            ]
        )
        chain = (
            prompt
            | llm.with_structured_output(AIRecognitionCandidateBundle).with_retry(
                stop_after_attempt=2
            )
        )
        result = chain.invoke(
            {
                "title": str(getattr(fileitem, "name", "") or ""),
                "path": str(getattr(fileitem, "path", "") or ""),
                "source_context": self._build_ai_source_context(fileitem, source_meta),
                "meta_hint": self._build_ai_meta_hint(source_meta),
                "media_types": "、".join(self._media_types),
            },
            config={"configurable": {"timeout": 25}},
        )
        raw_candidates = getattr(result, "candidates", []) or []
        candidates = []
        for item in raw_candidates:
            if hasattr(item, "model_dump"):
                candidates.append(item.model_dump())
            elif hasattr(item, "dict"):
                candidates.append(item.dict())
            elif isinstance(item, dict):
                candidates.append(item)
        return candidates

    def _normalize_ai_candidates(self, candidates: Any) -> List[Dict[str, Any]]:
        normalized = []
        seen_names = set()
        for item in candidates or []:
            if not isinstance(item, dict):
                continue
            name = self._clean_ai_candidate_name(item.get("name"))
            if not name or name.lower() == "unknown":
                continue
            key = name.lower()
            if key in seen_names:
                continue
            seen_names.add(key)
            normalized.append({
                "name": name,
                "year": str(item.get("year") or "").strip(),
                "media_type": str(item.get("media_type") or "unknown").strip(),
                "season": self._to_int(item.get("season"), 0, 0, 99),
                "episode": self._to_int(item.get("episode"), 0, 0, 9999),
                "confidence": self._to_float(item.get("confidence"), 0, 0, 1),
                "reason": str(item.get("reason") or "").strip(),
            })
        return sorted(
            normalized,
            key=lambda item: float(item.get("confidence") or 0),
            reverse=True,
        )[:self.AI_RECOGNITION_MAX_CANDIDATES]

    @staticmethod
    def _clean_ai_candidate_name(value: Any) -> str:
        text = " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split())
        return text.strip(" []【】")

    def _build_ai_source_context(
        self,
        fileitem: schemas.FileItem,
        source_meta: Any = None,
    ) -> Dict[str, Any]:
        source_path = str(getattr(fileitem, "path", "") or "").strip()
        source_name = str(getattr(fileitem, "name", "") or Path(source_path).name).strip()
        path_obj = Path(source_path) if source_path else None
        parent_dirs = []
        if path_obj:
            parent_dirs = [
                part for part in path_obj.parts[:-1]
                if part and part not in {"/", ".", ".."}
            ][-4:]
        title_hints = []
        for value in [source_name, Path(source_name).stem if source_name else "", *parent_dirs]:
            for title in self._extract_candidate_title_hints(value):
                if title not in title_hints:
                    title_hints.append(title)
        return {
            "filename": source_name,
            "parent_dirs": parent_dirs,
            "title_hints": title_hints[:self.AI_RECOGNITION_MAX_CANDIDATES],
            "parsed_meta": self._build_ai_meta_hint(source_meta),
        }

    @staticmethod
    def _format_ai_candidate(candidate: Dict[str, Any]) -> str:
        name = str(candidate.get("name") or "-")
        year = str(candidate.get("year") or "").strip()
        media_type = str(candidate.get("media_type") or "unknown").strip()
        confidence = float(candidate.get("confidence") or 0)
        label = name
        if year:
            label = f"{label}({year})"
        return f"{label}/{media_type}/{confidence:.2f}"

    @classmethod
    def _is_ai_recognition_year_compatible(
        cls,
        source_text: Any,
        candidate: Dict[str, Any],
        mediainfo: Any,
    ) -> bool:
        source_years = set(cls._extract_plausible_years(source_text))
        if not source_years:
            return True
        result_years = set(cls._extract_ai_result_years(candidate, mediainfo))
        if not result_years:
            return False
        return bool(source_years & result_years)

    @classmethod
    def _extract_ai_result_years(
        cls,
        candidate: Dict[str, Any],
        mediainfo: Any,
    ) -> List[str]:
        years = []
        for value in (
            getattr(mediainfo, "year", ""),
            getattr(mediainfo, "title_year", ""),
            getattr(mediainfo, "release_date", ""),
            getattr(mediainfo, "first_air_date", ""),
            getattr(mediainfo, "premiered", ""),
        ):
            years.extend(cls._extract_plausible_years(value))
        if not years:
            years.extend(cls._extract_plausible_years(candidate.get("year")))
        return cls._dedupe_values(years)

    @classmethod
    def _is_target_year_compatible(cls, source_path: Any, target_path: Any) -> bool:
        source_years = set(cls._extract_plausible_years(source_path))
        if not source_years:
            return True
        target_years = set(cls._extract_plausible_years(target_path))
        if not target_years:
            return True
        return bool(source_years & target_years)

    @classmethod
    def _extract_source_season_number(cls, value: Any) -> Optional[int]:
        text = str(value or "")
        if not text:
            return None
        patterns = (
            r"\bS(\d{1,2})(?:E\d{1,4})?\b",
            r"\bSeason[._\-\s]*(\d{1,2})\b",
            r"第\s*(\d{1,2})\s*季",
        )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.I)
            if not match:
                continue
            number = cls._safe_positive_int(match.group(1))
            if number:
                return number
        return None

    @classmethod
    def _extract_source_total_episodes(cls, value: Any) -> Optional[int]:
        text = str(value or "")
        if not text:
            return None
        patterns = (
            r"全\s*(\d{1,4})\s*[集话話]",
            r"(\d{1,4})\s*[集话話]\s*全",
            r"\bcomplete(?:d)?\s*(?:season\s*\d{1,2}\s*)?(\d{1,4})\s*(?:episodes?|eps?)\b",
            r"\b(\d{1,4})\s*(?:episodes?|eps?)\s*complete(?:d)?\b",
        )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.I)
            if not match:
                continue
            number = cls._safe_positive_int(match.group(1))
            if number:
                return number
        return None

    @classmethod
    def _extract_source_episode_numbers(cls, value: Any) -> List[int]:
        text = str(value or "")
        if not text:
            return []
        numbers = []
        patterns = (
            r"\bS\d{1,2}E(\d{1,4})(?!\d)",
            r"(?:^|[._\-\s])EP?(\d{1,4})(?!\d)",
            r"\bEpisode[._\-\s]*(\d{1,4})(?!\d)",
            r"第\s*(\d{1,4})\s*[集话話]",
        )
        for pattern in patterns:
            for match in re.finditer(pattern, text, flags=re.I):
                number = cls._safe_positive_int(match.group(1))
                if number and number not in numbers:
                    numbers.append(number)
        return numbers

    @staticmethod
    def _extract_preview_tmdb_id(item: Dict[str, Any]) -> Optional[int]:
        for key in ("target", "target_dir"):
            match = re.search(r"\{tmdb-(\d+)\}", str(item.get(key) or ""), flags=re.I)
            if not match:
                continue
            try:
                return int(match.group(1))
            except (TypeError, ValueError):
                return None
        return None

    @classmethod
    def _get_ai_episode_guard_error(cls, source_text: Any, meta: Any, mediainfo: Any) -> str:
        raw_text = str(source_text or "")
        source_season = cls._extract_source_season_number(raw_text)
        source_episodes = cls._extract_source_episode_numbers(raw_text)
        expected_total = cls._extract_source_total_episodes(raw_text)
        media_type = getattr(mediainfo, "type", None)
        media_type_value = getattr(media_type, "value", media_type)
        if (
            (source_season or source_episodes or expected_total)
            and media_type_value
            and media_type_value != MediaType.TV.value
        ):
            return f"源文件包含剧集季集信息，AI命中结果类型为 {media_type_value}"

        meta_season = cls._safe_positive_int(getattr(meta, "begin_season", None))
        meta_episode = cls._safe_positive_int(getattr(meta, "begin_episode", None))
        meta_episode_end = cls._safe_positive_int(getattr(meta, "end_episode", None)) or meta_episode

        if source_season and meta_season and source_season != meta_season:
            return f"源文件为第 {source_season} 季，AI候选为第 {meta_season} 季"
        if source_season and not meta_season:
            return f"源文件为第 {source_season} 季，AI候选缺少季号"
        if source_episodes and not meta_episode:
            return f"源文件包含第 {', '.join(map(str, source_episodes[:5]))} 集，AI候选缺少集数"
        if source_episodes and meta_episode:
            preview_start = meta_episode
            preview_end = meta_episode_end or meta_episode
            missing = [
                episode for episode in source_episodes
                if episode < preview_start or episode > preview_end
            ]
            if missing:
                return (
                    f"源文件包含第 {', '.join(map(str, missing[:5]))} 集，"
                    f"AI候选为第 {preview_start}"
                    f"{f'-{preview_end}' if preview_end != preview_start else ''} 集"
                )

        tmdb_id = cls._safe_positive_int(getattr(mediainfo, "tmdb_id", None))
        if not tmdb_id:
            return ""
        season = meta_season or source_season or cls._safe_positive_int(getattr(mediainfo, "season", None)) or 1
        episode_count = cls._get_tmdb_season_episode_count(tmdb_id, season)
        if not episode_count:
            return ""
        if expected_total and expected_total > episode_count:
            title = getattr(mediainfo, "title_year", "") or getattr(mediainfo, "title", "") or f"TMDB {tmdb_id}"
            return f"源路径标记全 {expected_total} 集，但命中 {title} 第 {season} 季只有 {episode_count} 集"
        if source_episodes and max(source_episodes) > episode_count:
            title = getattr(mediainfo, "title_year", "") or getattr(mediainfo, "title", "") or f"TMDB {tmdb_id}"
            return f"源文件包含第 {max(source_episodes)} 集，但命中 {title} 第 {season} 季只有 {episode_count} 集"
        return ""

    @staticmethod
    def _safe_positive_int(value: Any) -> Optional[int]:
        try:
            number = int(value)
        except (TypeError, ValueError):
            return None
        return number if 0 < number < 10000 else None

    @staticmethod
    def _get_tmdb_season_episode_count(tmdb_id: int, season: int) -> int:
        try:
            from app.chain.tmdb import TmdbChain
            episodes = TmdbChain().tmdb_episodes(tmdbid=tmdb_id, season=season)
            return len(episodes or [])
        except Exception as e:
            logger.debug(
                "【OpenList 目录监控】查询 TMDB 季集数失败 tmdb=%s season=%s: %s",
                tmdb_id, season, e,
            )
        return 0

    @staticmethod
    def _extract_plausible_years(value: Any) -> List[str]:
        text = str(value or "")
        if not text:
            return []
        current_year = datetime.now().year
        years = []
        for year in re.findall(r"(?<!\d)(?:19\d{2}|20\d{2})(?!\d)", text):
            number = int(year)
            if 1900 <= number <= current_year + 2 and year not in years:
                years.append(year)
        return years

    @staticmethod
    def _dedupe_values(values: List[Any]) -> List[str]:
        deduped = []
        for value in values:
            text = str(value or "").strip()
            if text and text not in deduped:
                deduped.append(text)
        return deduped

    @staticmethod
    def _build_ai_meta_hint(meta: Any) -> Dict[str, Any]:
        if not meta:
            return {}
        media_type = getattr(meta, "type", None)
        return {
            "name": getattr(meta, "name", "") or "",
            "title": getattr(meta, "title", "") or "",
            "year": getattr(meta, "year", "") or "",
            "type": getattr(media_type, "value", "") if media_type else "",
            "season": getattr(meta, "begin_season", None) or 0,
            "episode": getattr(meta, "begin_episode", None) or 0,
            "org_string": getattr(meta, "org_string", "") or "",
        }

    @staticmethod
    def _ai_recognition_cache_key(fileitem: schemas.FileItem, source_meta: Any = None) -> str:
        source_path = str(getattr(fileitem, "path", "") or getattr(fileitem, "name", "") or "")
        parent = Path(source_path).parent.as_posix() if source_path else ""
        name = str(
            getattr(source_meta, "name", "")
            or getattr(source_meta, "title", "")
            or Path(source_path).stem
        ).strip().lower()
        year = str(getattr(source_meta, "year", "") or "").strip()
        media_type = getattr(getattr(source_meta, "type", None), "value", "") or ""
        return "|".join([parent, name, year, media_type])

    @staticmethod
    def _normalize_ai_media_type(value: Any, source_meta: Any = None) -> Optional[MediaType]:
        if value == MediaType.MOVIE or value == MediaType.TV:
            return value
        text = str(value or "").strip().lower()
        if text in {"movie", "movies", "电影"}:
            return MediaType.MOVIE
        if text in {"tv", "series", "show", "电视剧", "剧集", "番剧"}:
            return MediaType.TV
        source_type = getattr(source_meta, "type", None)
        if source_type in {MediaType.MOVIE, MediaType.TV}:
            return source_type
        if getattr(source_meta, "begin_season", None) or getattr(source_meta, "begin_episode", None):
            return MediaType.TV
        return None

    @staticmethod
    def _positive_int(*values: Any) -> Optional[int]:
        for value in values:
            try:
                number = int(value)
            except (TypeError, ValueError):
                continue
            if number > 0:
                return number
        return None

    @staticmethod
    def _run_async_compatible(value: Any) -> Any:
        if not inspect.isawaitable(value):
            return value
        result: Dict[str, Any] = {}
        error: Dict[str, BaseException] = {}

        def _worker() -> None:
            try:
                result["value"] = asyncio.run(value)
            except BaseException as exc:
                error["exc"] = exc

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(value)

        thread = threading.Thread(target=_worker, daemon=True)
        thread.start()
        thread.join()
        if "exc" in error:
            raise error["exc"]
        return result.get("value")

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
    def _parse_recognition_rewrite_rules(value: Any) -> List[Dict[str, str]]:
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
            source = source.strip()
            target = target.strip()
            if not source or not target or source in seen_sources:
                continue
            seen_sources.add(source)
            rules.append({"source": source, "target": target})
        return rules

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
