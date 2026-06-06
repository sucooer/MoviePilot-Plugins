import threading
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app import schemas
from app.core.cache import TTLCache
from app.core.config import settings
from app.helper.storage import StorageHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import StorageConf
from app.utils.http import RequestUtils
from app.utils.url import UrlUtils


class OpenListStrmRefresh(_PluginBase):
    """
    定时访问 OpenList STRM 驱动目录，触发 STRM 本地文件生成。
    """

    plugin_name = "OpenList STRM 刷新"
    plugin_desc = "定时访问 OpenList STRM 驱动目录，触发 STRM 文件生成。"
    plugin_icon = "Alist_B.png"
    plugin_version = "0.1.1"
    plugin_author = "sucooer"
    author_url = "https://github.com/sucooer/MoviePilot-Plugins"
    plugin_config_prefix = "openliststrmrefresh_"
    plugin_order = 55
    auth_level = 1

    _alist_token_cache = TTLCache(region="openlist_strm_refresh_alist", maxsize=32, ttl=3600)

    _enabled = False
    _onlyonce = False
    _cron = ""
    _paths = ""
    _schedules = ""
    _recursive = True
    _max_depth = 3
    _per_page = 0
    _delay_seconds = 0.2
    _refresh = False
    _scheduler: Optional[BackgroundScheduler] = None
    _event = threading.Event()
    _lock = threading.Lock()
    _running = False

    STORE_LAST_RESULT_KEY = "last_result"

    def init_plugin(self, config: dict = None):
        self.stop_service()

        config = config or {}
        self._enabled = bool(config.get("enabled", False))
        self._onlyonce = bool(config.get("onlyonce", False))
        self._cron = str(config.get("cron") or "").strip()
        self._paths = str(config.get("paths") or "").strip()
        self._schedules = str(config.get("schedules") or "").strip()
        self._recursive = bool(config.get("recursive", True))
        self._refresh = bool(config.get("refresh", False))
        self._max_depth = self._to_int(config.get("max_depth"), 3, 0, 20)
        self._per_page = self._to_int(config.get("per_page"), 0, 0, 1000)
        self._delay_seconds = self._to_float(config.get("delay_seconds"), 0.2, 0, 30)

        if not self._enabled and not self._onlyonce:
            logger.info("【OpenList STRM 刷新】插件未启用")
            return

        if self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(
                func=self.refresh,
                trigger="date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                name="OpenList STRM 立即刷新",
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
                "path": "/refresh",
                "endpoint": self.api_refresh,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "立即访问 STRM 目录",
                "description": "立即访问配置的 OpenList STRM 驱动目录。",
            },
            {
                "path": "/status",
                "endpoint": self.api_status,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "获取最近刷新结果",
                "description": "获取最近一次访问 STRM 目录的结果。",
            },
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        services = []
        if not self._enabled:
            return services

        schedules = self._parse_schedules(self._schedules)
        for index, schedule in enumerate(schedules, start=1):
            services.append(
                {
                    "id": f"OpenListStrmRefresh{index}",
                    "name": f"OpenList STRM 刷新 {index}",
                    "trigger": CronTrigger.from_crontab(schedule["cron"]),
                    "func": self.refresh_paths,
                    "kwargs": {"paths": schedule["paths"], "schedule_name": f"计划 {index}"},
                }
            )
        if not schedules and self._cron and self._parse_paths(self._paths):
            services.append(
                {
                    "id": "OpenListStrmRefresh",
                    "name": "OpenList STRM 默认刷新",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.refresh,
                    "kwargs": {},
                }
            )
        return services

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
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
                                        "props": {"model": "recursive", "label": "递归访问子目录"},
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
                                            "label": "强制刷新",
                                            "hint": "调用 OpenList 列目录接口时携带 refresh=true",
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
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "cron",
                                            "label": "默认执行周期",
                                            "placeholder": "0 3 * * *",
                                            "hint": "仅在目录刷新计划为空时使用；5 位 cron 表达式，例如每天 03:00 执行",
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
                                            "model": "per_page",
                                            "label": "每页数量",
                                            "type": "number",
                                            "min": 0,
                                            "hint": "0 表示让 OpenList 返回全部",
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
                                            "model": "delay_seconds",
                                            "label": "目录间隔秒",
                                            "type": "number",
                                            "min": 0,
                                            "step": 0.1,
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
                                            "model": "paths",
                                            "label": "默认 STRM 驱动目录",
                                            "placeholder": "/strm\n/媒体库/STRM",
                                            "rows": 5,
                                            "hint": "仅在目录刷新计划为空时使用；一行一个 OpenList 路径",
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
                                            "model": "schedules",
                                            "label": "目录刷新计划",
                                            "placeholder": "0 */6 * * * | /strm/电影\n30 3 * * * | /strm/剧集,/strm/动漫",
                                            "rows": 6,
                                            "hint": "一行一个计划，格式为 cron | 路径；多个路径用英文逗号分隔。填写后可让不同目录使用不同频率",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VAlert",
                        "props": {
                            "type": "info",
                            "variant": "tonal",
                            "text": "插件会读取 MoviePilot 中名为 alist 的 OpenList 存储配置，无需在这里重复填写 OpenList 地址或令牌。",
                        },
                    },
                ],
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "cron": "0 3 * * *",
            "paths": "/strm",
            "schedules": "",
            "recursive": True,
            "max_depth": 3,
            "per_page": 0,
            "delay_seconds": 0.2,
            "refresh": False,
        }

    def get_page(self) -> List[dict]:
        last_result = self.get_data(self.STORE_LAST_RESULT_KEY) or {}
        paths = self._parse_paths(self._paths)
        schedules = self._parse_schedules(self._schedules)
        schedule_text = "；".join(
            f"{item['cron']} -> {', '.join(item['paths'])}" for item in schedules
        )
        status_items = [
            ("状态", "运行中" if self._running else ("已启用" if self._enabled else "未启用")),
            ("默认执行周期", self._cron or "-"),
            ("默认扫描目录", "、".join(paths) if paths else "-"),
            ("目录刷新计划", schedule_text or "-"),
            ("递归深度", str(self._max_depth) if self._recursive else "不递归"),
            ("最近运行", str(last_result.get("time") or "-")),
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
                                            "text": "OpenList STRM 生成依赖目录访问触发，本插件会定时访问配置目录。",
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
                                            "prepend-icon": "mdi-refresh",
                                        },
                                        "text": "立即刷新",
                                        "events": {
                                            "click": {
                                                "api": "plugin/OpenListStrmRefresh/refresh",
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
            logger.debug("【OpenList STRM 刷新】停止调度器失败: %s", e)
        self._scheduler = None
        self._event.clear()

    def api_refresh(self) -> schemas.Response:
        success, message, data = self.refresh()
        return schemas.Response(success=success, message=message, data=data)

    def api_status(self) -> schemas.Response:
        return schemas.Response(success=True, data=self.get_data(self.STORE_LAST_RESULT_KEY) or {})

    def refresh(self) -> Tuple[bool, str, Dict[str, Any]]:
        schedules = self._parse_schedules(self._schedules)
        if schedules:
            paths = []
            seen = set()
            for schedule in schedules:
                for path in schedule["paths"]:
                    if path not in seen:
                        seen.add(path)
                        paths.append(path)
            return self.refresh_paths(paths, "手动刷新")
        return self.refresh_paths(self._parse_paths(self._paths), "默认计划")

    def refresh_paths(self, paths: List[str], schedule_name: str = "") -> Tuple[bool, str, Dict[str, Any]]:
        if not self._lock.acquire(blocking=False):
            message = "已有刷新任务正在运行"
            logger.warning("【OpenList STRM 刷新】%s", message)
            return False, message, {}

        self._running = True
        started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        stats = {
            "time": started_at,
            "schedule": schedule_name or "手动刷新",
            "paths": self._parse_paths("\n".join(paths)),
            "dirs": 0,
            "files": 0,
            "errors": [],
        }
        try:
            if not stats["paths"]:
                return self._finish(False, "未配置 STRM 驱动目录", stats)

            conf = self._get_alist_conf()
            if not conf:
                return self._finish(False, "未找到 OpenList 存储配置", stats)
            base_url = self._get_alist_base_url(conf)
            headers = self._get_alist_auth_header(conf)
            if not base_url or not headers:
                return self._finish(False, "OpenList 认证失败", stats)

            logger.info("【OpenList STRM 刷新】开始访问目录: %s", ", ".join(stats["paths"]))
            for path in stats["paths"]:
                if self._event.is_set():
                    break
                self._visit_directory(base_url, headers, path, 0, stats)

            success = not stats["errors"]
            message = (
                f"访问完成，目录 {stats['dirs']} 个，文件 {stats['files']} 个"
                if success
                else f"访问完成但有 {len(stats['errors'])} 个错误，目录 {stats['dirs']} 个，文件 {stats['files']} 个"
            )
            return self._finish(success, message, stats)
        finally:
            self._running = False
            self._lock.release()

    def _visit_directory(self, base_url: str, headers: Dict[str, str], path: str, depth: int, stats: Dict[str, Any]):
        clean_path = self._normalize_path(path)
        listing, error = self._list_directory(base_url, headers, clean_path)
        if error:
            stats["errors"].append({"path": clean_path, "error": error})
            logger.warning("【OpenList STRM 刷新】访问目录失败: %s - %s", clean_path, error)
            return

        stats["dirs"] += 1
        files = listing.get("files", [])
        dirs = [item for item in files if item.get("is_dir")]
        stats["files"] += len(files) - len(dirs)
        logger.info(
            "【OpenList STRM 刷新】已访问目录: %s，子目录 %s 个，文件 %s 个",
            clean_path,
            len(dirs),
            len(files) - len(dirs),
        )

        if not self._recursive or depth >= self._max_depth:
            return

        for item in dirs:
            if self._event.is_set():
                return
            name = str(item.get("name") or "").strip()
            if not name:
                continue
            child_path = f"{clean_path.rstrip('/')}/{name}" if clean_path != "/" else f"/{name}"
            if self._delay_seconds > 0 and self._event.wait(self._delay_seconds):
                return
            self._visit_directory(base_url, headers, child_path, depth + 1, stats)

    def _list_directory(self, base_url: str, headers: Dict[str, str], path: str) -> Tuple[Dict[str, Any], str]:
        resp = RequestUtils(headers=headers).post_res(
            UrlUtils.adapt_request_url(base_url, "/api/fs/list"),
            json={
                "path": path,
                "password": "",
                "page": 1,
                "per_page": self._per_page,
                "refresh": self._refresh,
            },
        )
        if not resp or resp.status_code != 200:
            return {}, "请求目录失败"
        try:
            result = resp.json()
        except Exception as e:
            return {}, f"解析响应失败: {e}"
        if result.get("code") != 200:
            return {}, str(result.get("message") or "OpenList 返回错误")

        files = []
        for item in (result.get("data", {}).get("content") or []):
            is_dir = bool(item.get("is_dir")) or item.get("type") == "folder" or item.get("type") == 1
            files.append(
                {
                    "name": item.get("name"),
                    "size": item.get("size"),
                    "is_dir": is_dir,
                    "modified": item.get("modified"),
                }
            )
        return {"files": files}, ""

    def _finish(self, success: bool, message: str, data: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
        data["success"] = success
        data["message"] = message
        data["finish_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.save_data(self.STORE_LAST_RESULT_KEY, data)
        if success:
            logger.info("【OpenList STRM 刷新】%s", message)
        else:
            logger.warning("【OpenList STRM 刷新】%s", message)
        return success, message, data

    @classmethod
    def _get_alist_conf(cls) -> Optional[StorageConf]:
        try:
            return StorageHelper().get_storage("alist")
        except Exception as e:
            logger.debug("【OpenList STRM 刷新】读取 OpenList 存储配置失败: %s", e)
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

        resp = RequestUtils(headers={"Content-Type": "application/json"}).post_res(
            UrlUtils.adapt_request_url(base_url, "/api/auth/login"),
            json={"username": username, "password": password},
        )
        if not resp or resp.status_code != 200:
            logger.warning("【OpenList STRM 刷新】OpenList 登录失败，无法获取临时 token")
            return {}
        try:
            result = resp.json()
            if result.get("code") != 200:
                logger.warning("【OpenList STRM 刷新】OpenList 登录返回失败: %s", result.get("message"))
                return {}
            token = str((result.get("data") or {}).get("token") or "").strip()
            if token:
                cls._alist_token_cache.set(base_url, token)
                return {"Authorization": token}
        except Exception as e:
            logger.warning("【OpenList STRM 刷新】解析 OpenList 登录结果失败: %s", e)
        return {}

    @staticmethod
    def _parse_paths(value: Any) -> List[str]:
        raw = str(value or "").replace(",", "\n")
        paths = []
        seen = set()
        for line in raw.splitlines():
            path = OpenListStrmRefresh._normalize_path(line)
            if path and path not in seen:
                seen.add(path)
                paths.append(path)
        return paths

    @staticmethod
    def _parse_schedules(value: Any) -> List[Dict[str, Any]]:
        schedules = []
        for line in str(value or "").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "|" not in line:
                logger.warning("【OpenList STRM 刷新】忽略无效刷新计划: %s", line)
                continue
            cron, raw_paths = line.split("|", 1)
            cron = cron.strip()
            paths = OpenListStrmRefresh._parse_paths(raw_paths)
            if not cron or not paths:
                logger.warning("【OpenList STRM 刷新】忽略无效刷新计划: %s", line)
                continue
            schedules.append({"cron": cron, "paths": paths})
        return schedules

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
