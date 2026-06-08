import hashlib
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
    plugin_icon = "https://raw.githubusercontent.com/sucooer/MoviePilot-Plugins/main/icons/OpenList.png"
    plugin_version = "0.1.7"
    plugin_author = "sucooer"
    author_url = "https://github.com/sucooer/MoviePilot-Plugins"
    plugin_config_prefix = "openliststrmrefresh_"
    plugin_order = 55
    auth_level = 1

    _alist_token_cache = TTLCache(region="openlist_strm_refresh_alist", maxsize=32, ttl=3600)

    _enabled = False
    _onlyonce = False
    _cron = ""
    _default_frequency = "daily_3"
    _paths = ""
    _schedules = ""
    _change_schedules = ""
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
    STORE_SNAPSHOT_KEY = "directory_snapshots"

    def init_plugin(self, config: dict = None):
        self.stop_service()

        config = config or {}
        self._enabled = bool(config.get("enabled", False))
        self._onlyonce = bool(config.get("onlyonce", False))
        self._cron = str(config.get("cron") or "").strip()
        self._default_frequency = str(config.get("default_frequency") or "daily_3").strip()
        self._paths = str(config.get("paths") or "").strip()
        self._schedules = str(config.get("schedules") or "").strip()
        self._change_schedules = str(config.get("change_schedules") or "").strip()
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
            paths = list(schedule["paths"])
            schedule_name = f"计划 {index}"
            services.append(
                {
                    "id": f"OpenListStrmRefresh{index}",
                    "name": f"OpenList STRM 刷新 {index}",
                    "trigger": CronTrigger.from_crontab(schedule["cron"]),
                    "func": lambda paths=paths, schedule_name=schedule_name: self.refresh_paths(
                        paths, schedule_name
                    ),
                    "kwargs": {},
                }
            )
        change_schedules = self._parse_change_schedules(self._change_schedules)
        for index, schedule in enumerate(change_schedules, start=1):
            detect_schedule = dict(schedule)
            schedule_name = f"变化检测 {index}"
            services.append(
                {
                    "id": f"OpenListStrmDetect{index}",
                    "name": f"OpenList STRM 变化检测 {index}",
                    "trigger": CronTrigger.from_crontab(schedule["cron"]),
                    "func": lambda schedule=detect_schedule, schedule_name=schedule_name: self.detect_changes(
                        schedule, schedule_name
                    ),
                    "kwargs": {},
                }
            )
        default_cron = self._resolve_frequency(self._default_frequency) or self._cron
        if not schedules and not change_schedules and default_cron and self._parse_paths(self._paths):
            services.append(
                {
                    "id": "OpenListStrmRefresh",
                    "name": "OpenList STRM 默认刷新",
                    "trigger": CronTrigger.from_crontab(default_cron),
                    "func": self.refresh,
                    "kwargs": {},
                }
            )
        return services

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        frequency_items = [
            {"title": "每 15 分钟", "value": "every_15_minutes"},
            {"title": "每 30 分钟", "value": "every_30_minutes"},
            {"title": "每小时", "value": "hourly"},
            {"title": "每天 03:00", "value": "daily_3"},
            {"title": "每 6 小时", "value": "every_6_hours"},
            {"title": "每 12 小时", "value": "every_12_hours"},
            {"title": "每 2 天 03:00", "value": "every_2_days"},
            {"title": "每周一 03:00", "value": "weekly_monday_3"},
            {"title": "每月 1 日 03:00", "value": "monthly_1_3"},
            {"title": "自定义 cron", "value": "custom"},
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
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "default_frequency",
                                            "label": "默认刷新频率",
                                            "items": frequency_items,
                                            "hint": "仅在目录刷新计划为空时使用",
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
                                            "model": "cron",
                                            "label": "自定义 cron",
                                            "placeholder": "0 3 * * *",
                                            "hint": "选择自定义 cron 时使用",
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
                                            "placeholder": "每6小时 | /strm/电影\n每天03点 | /strm/剧集,/strm/动漫",
                                            "rows": 6,
                                            "hint": "一行一个计划，格式为 频率 | 路径；支持 每6小时、每12小时、每天03点、每周一03点，也可直接写 cron",
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
                                            "model": "change_schedules",
                                            "label": "变化检测计划",
                                            "placeholder": "每15分钟 | /原驱动/电影 -> /strm/电影\n每30分钟 | /原驱动/剧集 -> /strm/剧集",
                                            "rows": 6,
                                            "hint": "检测左侧源目录变化，变化后刷新右侧 STRM 目录；首次检测只建立基线",
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
            "default_frequency": "daily_3",
            "cron": "0 3 * * *",
            "paths": "/strm",
            "schedules": "",
            "change_schedules": "",
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
        change_schedules = self._parse_change_schedules(self._change_schedules)
        schedule_text = "；".join(
            f"{item['frequency']} -> {', '.join(item['paths'])}" for item in schedules
        )
        change_schedule_text = "；".join(
            f"{item['frequency']} -> {item['source']} => {', '.join(item['targets'])}"
            for item in change_schedules
        )
        status_items = [
            ("状态", "运行中" if self._running else ("已启用" if self._enabled else "未启用")),
            ("默认刷新频率", self._format_frequency(self._default_frequency, self._cron)),
            ("默认扫描目录", "、".join(paths) if paths else "-"),
            ("目录刷新计划", schedule_text or "-"),
            ("变化检测计划", change_schedule_text or "-"),
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

    def detect_changes(self, schedule: Dict[str, Any], schedule_name: str = "") -> Tuple[bool, str, Dict[str, Any]]:
        if not self._lock.acquire(blocking=False):
            message = "已有刷新任务正在运行"
            logger.warning("【OpenList STRM 刷新】%s", message)
            return False, message, {}

        self._running = True
        stats = {
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "schedule": schedule_name or "变化检测",
            "source": schedule.get("source"),
            "targets": schedule.get("targets") or [],
            "changed": False,
            "baseline": False,
            "dirs": 0,
            "files": 0,
            "errors": [],
        }
        try:
            source = self._normalize_path(schedule.get("source"))
            targets = self._parse_paths("\n".join(schedule.get("targets") or []))
            if not source or not targets:
                return self._finish(False, "变化检测计划缺少源目录或 STRM 目录", stats)

            conf = self._get_alist_conf()
            if not conf:
                return self._finish(False, "未找到 OpenList 存储配置", stats)
            base_url = self._get_alist_base_url(conf)
            headers = self._get_alist_auth_header(conf)
            if not base_url or not headers:
                return self._finish(False, "OpenList 认证失败", stats)

            snapshot, error = self._build_directory_snapshot(base_url, headers, source)
            stats["dirs"] = snapshot.get("dirs", 0)
            stats["files"] = snapshot.get("files", 0)
            if error:
                stats["errors"].append({"path": source, "error": error})
                return self._finish(False, f"检测源目录失败: {error}", stats)

            snapshots = self.get_data(self.STORE_SNAPSHOT_KEY) or {}
            snapshot_key = self._snapshot_key(source, targets)
            previous = snapshots.get(snapshot_key)
            snapshots[snapshot_key] = snapshot
            self.save_data(self.STORE_SNAPSHOT_KEY, snapshots)

            if not previous:
                stats["baseline"] = True
                return self._finish(True, f"已建立目录变化检测基线: {source}", stats)

            changed = previous.get("fingerprint") != snapshot.get("fingerprint")
            stats["changed"] = changed
            if not changed:
                return self._finish(True, f"未检测到目录变化: {source}", stats)

            logger.info("【OpenList STRM 刷新】检测到目录变化: %s，开始刷新: %s", source, ", ".join(targets))
            self._lock.release()
            self._running = False
            return self.refresh_paths(targets, schedule_name or "变化检测刷新")
        finally:
            if self._running:
                self._running = False
            try:
                self._lock.release()
            except RuntimeError:
                pass

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
            message = f"访问完成，目录 {stats['dirs']} 个，文件 {stats['files']} 个"
            if not success:
                message = (
                    f"访问完成但有 {len(stats['errors'])} 个错误，目录 {stats['dirs']} 个，"
                    f"文件 {stats['files']} 个；{self._format_errors(stats['errors'])}"
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
        if not resp:
            return {}, "请求目录失败: 无响应"
        if resp.status_code != 200:
            return {}, f"请求目录失败: HTTP {resp.status_code} {self._response_text(resp)}"
        try:
            result = resp.json()
        except Exception as e:
            return {}, f"解析响应失败: {e} {self._response_text(resp)}"
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

    def _build_directory_snapshot(self, base_url: str, headers: Dict[str, str], path: str) -> Tuple[Dict[str, Any], str]:
        items = []
        stats = {"dirs": 0, "files": 0}
        error = self._collect_directory_snapshot(base_url, headers, path, 0, items, stats)
        raw_fingerprint = "\n".join(sorted(items))
        fingerprint = hashlib.sha256(raw_fingerprint.encode("utf-8")).hexdigest()
        return {
            "path": self._normalize_path(path),
            "fingerprint": fingerprint,
            "dirs": stats["dirs"],
            "files": stats["files"],
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }, error

    def _collect_directory_snapshot(
        self,
        base_url: str,
        headers: Dict[str, str],
        path: str,
        depth: int,
        items: List[str],
        stats: Dict[str, int],
    ) -> str:
        clean_path = self._normalize_path(path)
        listing, error = self._list_directory(base_url, headers, clean_path)
        if error:
            return error

        stats["dirs"] += 1
        for item in listing.get("files", []):
            name = str(item.get("name") or "").strip()
            if not name:
                continue
            child_path = f"{clean_path.rstrip('/')}/{name}" if clean_path != "/" else f"/{name}"
            is_dir = bool(item.get("is_dir"))
            marker = "D" if is_dir else "F"
            items.append(
                f"{marker}\t{child_path}\t{item.get('size') or 0}\t{item.get('modified') or ''}"
            )
            if is_dir and self._recursive and depth < self._max_depth:
                child_error = self._collect_directory_snapshot(
                    base_url, headers, child_path, depth + 1, items, stats
                )
                if child_error:
                    return child_error
            elif not is_dir:
                stats["files"] += 1
        return ""

    def _finish(self, success: bool, message: str, data: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
        if not success and data.get("errors") and "；" not in message:
            message = f"{message}；{self._format_errors(data['errors'])}"
        data["success"] = success
        data["message"] = message
        data["finish_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.save_data(self.STORE_LAST_RESULT_KEY, data)
        if success:
            logger.info("【OpenList STRM 刷新】%s", message)
        else:
            logger.warning("【OpenList STRM 刷新】%s", message)
        return success, message, data

    @staticmethod
    def _format_errors(errors: List[Dict[str, Any]], limit: int = 3) -> str:
        if not errors:
            return ""
        parts = []
        for item in errors[:limit]:
            path = item.get("path") or "-"
            error = item.get("error") or "-"
            parts.append(f"{path}: {error}")
        if len(errors) > limit:
            parts.append(f"另有 {len(errors) - limit} 个错误")
        return "错误明细: " + "；".join(parts)

    @staticmethod
    def _response_text(resp: Any, limit: int = 200) -> str:
        try:
            text = str(getattr(resp, "text", "") or "").strip()
        except Exception:
            text = ""
        if not text:
            return ""
        return text[:limit]

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
            frequency = cron.strip()
            cron = OpenListStrmRefresh._resolve_frequency(frequency) or frequency
            paths = OpenListStrmRefresh._parse_paths(raw_paths)
            if not cron or not paths:
                logger.warning("【OpenList STRM 刷新】忽略无效刷新计划: %s", line)
                continue
            schedules.append({"cron": cron, "frequency": frequency, "paths": paths})
        return schedules

    @staticmethod
    def _parse_change_schedules(value: Any) -> List[Dict[str, Any]]:
        schedules = []
        for line in str(value or "").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "|" not in line or "->" not in line:
                logger.warning("【OpenList STRM 刷新】忽略无效变化检测计划: %s", line)
                continue
            frequency, mapping = line.split("|", 1)
            source, targets = mapping.split("->", 1)
            frequency = frequency.strip()
            cron = OpenListStrmRefresh._resolve_frequency(frequency) or frequency
            source_path = OpenListStrmRefresh._normalize_path(source)
            target_paths = OpenListStrmRefresh._parse_paths(targets)
            if not cron or not source_path or not target_paths:
                logger.warning("【OpenList STRM 刷新】忽略无效变化检测计划: %s", line)
                continue
            schedules.append(
                {
                    "cron": cron,
                    "frequency": frequency,
                    "source": source_path,
                    "targets": target_paths,
                }
            )
        return schedules

    @staticmethod
    def _snapshot_key(source: str, targets: List[str]) -> str:
        return f"{OpenListStrmRefresh._normalize_path(source)}=>{','.join(OpenListStrmRefresh._parse_paths(','.join(targets)))}"

    @staticmethod
    def _resolve_frequency(value: Any) -> str:
        text = str(value or "").strip()
        normalized = text.replace(" ", "").replace("：", ":")
        mapping = {
            "daily_3": "0 3 * * *",
            "every_15_minutes": "*/15 * * * *",
            "every_30_minutes": "*/30 * * * *",
            "hourly": "0 * * * *",
            "every_6_hours": "0 */6 * * *",
            "every_12_hours": "0 */12 * * *",
            "every_2_days": "0 3 */2 * *",
            "weekly_monday_3": "0 3 * * 1",
            "monthly_1_3": "0 3 1 * *",
            "每天03点": "0 3 * * *",
            "每天3点": "0 3 * * *",
            "每天凌晨3点": "0 3 * * *",
            "每日03点": "0 3 * * *",
            "每日3点": "0 3 * * *",
            "每15分钟": "*/15 * * * *",
            "每十五分钟": "*/15 * * * *",
            "每30分钟": "*/30 * * * *",
            "每三十分钟": "*/30 * * * *",
            "每1小时": "0 * * * *",
            "每小时": "0 * * * *",
            "每6小时": "0 */6 * * *",
            "每六小时": "0 */6 * * *",
            "每12小时": "0 */12 * * *",
            "每十二小时": "0 */12 * * *",
            "每2天": "0 3 */2 * *",
            "每两天": "0 3 */2 * *",
            "每周一03点": "0 3 * * 1",
            "每周一3点": "0 3 * * 1",
            "每月1日03点": "0 3 1 * *",
            "每月1日3点": "0 3 1 * *",
        }
        if normalized == "custom":
            return ""
        return mapping.get(normalized, "")

    @staticmethod
    def _format_frequency(value: Any, custom_cron: str = "") -> str:
        text = str(value or "").strip()
        titles = {
            "daily_3": "每天 03:00",
            "every_15_minutes": "每 15 分钟",
            "every_30_minutes": "每 30 分钟",
            "hourly": "每小时",
            "every_6_hours": "每 6 小时",
            "every_12_hours": "每 12 小时",
            "every_2_days": "每 2 天 03:00",
            "weekly_monday_3": "每周一 03:00",
            "monthly_1_3": "每月 1 日 03:00",
            "custom": f"自定义 cron: {custom_cron or '-'}",
        }
        return titles.get(text, text or "-")

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
