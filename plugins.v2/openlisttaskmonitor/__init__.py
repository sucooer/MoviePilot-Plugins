import re
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app import schemas
from app.core.config import settings
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType
from app.utils.http import RequestUtils
from app.utils.url import UrlUtils


class OpenListTaskMonitor(_PluginBase):
    plugin_name = "OpenList Task Monitor"
    plugin_desc = "监控 OpenList 复制、上传、离线下载等任务状态，完成或失败时发送通知。"
    plugin_icon = "https://raw.githubusercontent.com/sucooer/MoviePilot-Plugins/main/icons/OpenList.png"
    plugin_version = "1.0.2"
    plugin_author = "sucooer"
    author_url = "https://github.com/sucooer/MoviePilot-Plugins"
    plugin_config_prefix = "openlisttaskmonitor_"
    plugin_order = 56
    auth_level = 1

    _enabled = False
    _onlyonce = False
    _cron = ""
    _services = ""
    _monitor_types = []
    _notify_success = True
    _notify_failed = True
    _notify = True
    _scheduler: Optional[BackgroundScheduler] = None
    _event = threading.Event()
    _lock = threading.Lock()
    _running = False

    STORE_NOTIFIED_KEY = "notified_task_ids"
    STORE_RESULT_KEY = "last_result"
    STORE_RECENT_TASKS_KEY = "recent_tasks"
    MAX_NOTIFIED_IDS = 500
    MAX_RECENT_TASKS = 50

    TASK_TYPE_LABELS = {
        "copy": "复制",
        "upload": "上传",
        "offline_download": "离线下载",
        "offline_download_transfer": "离线转存",
        "decompress": "解压",
        "decompress_upload": "解压转存",
    }

    STATE_NAMES = {
        2: "succeeded",
        4: "canceled",
        5: "errored",
        7: "failed",
    }

    def __init__(self):
        super().__init__()
        self._last_request_at = 0.0
        self._rate_limit_lock = threading.Lock()
        self._service_sessions: Dict[str, dict] = {}

    def init_plugin(self, config: dict = None):
        self.stop_service()

        config = config or {}
        self._enabled = bool(config.get("enabled", False))
        self._onlyonce = bool(config.get("onlyonce", False))
        self._cron = str(config.get("cron") or "").strip()
        self._services = str(config.get("services") or "").strip()
        self._monitor_types = config.get("monitor_types") or []
        self._notify_success = bool(config.get("notify_success", True))
        self._notify_failed = bool(config.get("notify_failed", True))
        self._notify = bool(config.get("notify", True))

        if not isinstance(self._monitor_types, list):
            self._monitor_types = []

        if not self._enabled and not self._onlyonce:
            logger.info("【OpenList Task Monitor】插件未启用")
            return

        if self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(
                func=self.check,
                trigger="date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                name="OpenList Task Monitor 立即执行",
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
                "summary": "立即检查任务",
                "description": "立即检查所有 OpenList 服务的任务状态并发送通知。",
            },
            {
                "path": "/status",
                "endpoint": self.api_status,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "获取最近检查结果",
                "description": "获取最近一次任务检查的结果摘要。",
            },
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        services = []
        if not self._enabled:
            return services
        if self._cron:
            services.append({
                "id": "OpenListTaskMonitor",
                "name": "OpenList Task Monitor 定时检查",
                "trigger": CronTrigger.from_crontab(self._cron),
                "func": self.check,
                "kwargs": {},
            })
        return services

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        task_type_items = [
            {"title": label, "value": key}
            for key, label in self.TASK_TYPE_LABELS.items()
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
                                        "props": {"model": "notify_success", "label": "成功通知"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "notify_failed", "label": "失败通知"},
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
                                            "hint": "cron 表达式，建议 1-10 分钟",
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
                                        "component": "VSwitch",
                                        "props": {"model": "notify", "label": "发送通知"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "monitor_types",
                                            "label": "监控任务类型",
                                            "items": task_type_items,
                                            "multiple": True,
                                            "chips": True,
                                            "hint": "选择要监控的 OpenList 任务类型",
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
                                            "model": "services",
                                            "label": "OpenList 服务配置",
                                            "placeholder": "数据中心|https://alist1.example.com|token_abc123\n备份盘|https://alist2.example.com|admin:password123",
                                            "rows": 4,
                                            "hint": "一行一个服务：显示名称 | 地址 | 认证。认证支持 token 或 用户名:密码",
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
                            "text": "监控 OpenList 的复制、上传、离线下载等后台任务，任务完成或失败时通过 MoviePilot 通知系统发送消息。",
                        },
                    },
                ],
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "cron": "*/5 * * * *",
            "services": "",
            "monitor_types": list(self.TASK_TYPE_LABELS.keys()),
            "notify_success": True,
            "notify_failed": True,
            "notify": True,
        }

    def get_page(self) -> List[dict]:
        last_result = self.get_data(self.STORE_RESULT_KEY) or {}
        recent_tasks = self.get_data(self.STORE_RECENT_TASKS_KEY) or []
        services_list = self._parse_services()
        status_items = [
            ("状态", "检查中" if self._running else ("已启用" if self._enabled else "未启用")),
            ("检查周期", self._cron or "-"),
            ("服务数", str(len(services_list))),
            ("监控类型数", str(len(self._monitor_types))),
            ("最近检查", str(last_result.get("time") or "-")),
            ("通知成功", str(last_result.get("notify_success", 0))),
            ("通知失败", str(last_result.get("notify_failed", 0))),
            ("最近结果", str(last_result.get("message") or "-")),
        ]
        task_rows = self._build_task_rows(recent_tasks)

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
                                            "text": "监控 OpenList 后台任务状态，完成或失败时发送通知。",
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
                                            "prepend-icon": "mdi-sync",
                                        },
                                        "text": "立即检查",
                                        "events": {
                                            "click": {
                                                "api": "plugin/OpenListTaskMonitor/check",
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
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VCard",
                                        "props": {"flat": True, "border": True},
                                        "content": [
                                            {"component": "VCardTitle", "text": "最近任务"},
                                            {
                                                "component": "VCardText",
                                                "content": [
                                                    {
                                                        "component": "VTable",
                                                        "props": {"density": "compact", "hover": True},
                                                        "content": [
                                                            {
                                                                "component": "thead",
                                                                "content": [
                                                                    {
                                                                        "component": "tr",
                                                                        "content": [
                                                                            {"component": "th", "text": "服务"},
                                                                            {"component": "th", "text": "类型"},
                                                                            {"component": "th", "text": "名称"},
                                                                            {"component": "th", "text": "状态"},
                                                                            {"component": "th", "text": "大小"},
                                                                            {"component": "th", "text": "耗时"},
                                                                            {"component": "th", "text": "错误"},
                                                                        ],
                                                                    }
                                                                ],
                                                            },
                                                            {
                                                                "component": "tbody",
                                                                "content": task_rows if task_rows else [
                                                                    {
                                                                        "component": "tr",
                                                                        "content": [
                                                                            {
                                                                                "component": "td",
                                                                                "props": {"colspan": 7},
                                                                                "text": "暂无任务记录",
                                                                            }
                                                                        ],
                                                                    }
                                                                ],
                                                            },
                                                        ],
                                                    }
                                                ],
                                            },
                                        ],
                                    }
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
            logger.debug("【OpenList Task Monitor】停止调度器失败: %s", e)
        self._scheduler = None
        self._event.clear()

    def api_check(self) -> schemas.Response:
        success, message, data = self.check()
        return schemas.Response(success=success, message=message, data=data)

    def api_status(self) -> schemas.Response:
        return schemas.Response(success=True, data=self.get_data(self.STORE_RESULT_KEY) or {})

    def check(self) -> Tuple[bool, str, Dict[str, Any]]:
        if not self._lock.acquire(blocking=False):
            message = "已有检查任务正在运行"
            logger.warning("【OpenList Task Monitor】%s", message)
            return False, message, {}

        self._running = True
        stats = {
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "notify_success": 0,
            "notify_failed": 0,
            "errors": [],
            "message": "",
        }
        try:
            services = self._parse_services()
            if not services:
                return self._finish("未配置 OpenList 服务", stats)

            if not self._monitor_types:
                return self._finish("未选择监控任务类型", stats)

            notified_ids = self._load_notified_ids()
            recent_tasks = self.get_data(self.STORE_RECENT_TASKS_KEY) or []
            if not isinstance(recent_tasks, list):
                recent_tasks = []

            for svc in services:
                if self._event.is_set():
                    break
                svc_name = svc["name"]
                base_url = UrlUtils.standardize_base_url(svc["url"])
                headers = self._get_service_auth(base_url, svc["auth"])
                if not headers:
                    stats["errors"].append({"service": svc_name, "error": "认证失败"})
                    logger.warning("【OpenList Task Monitor】服务 '%s' 认证失败", svc_name)
                    continue

                for task_type in self._monitor_types:
                    if self._event.is_set():
                        break
                    if task_type not in self.TASK_TYPE_LABELS:
                        continue
                    type_label = self.TASK_TYPE_LABELS[task_type]
                    svc_notified = notified_ids.setdefault(svc_name, {})

                    tasks, error = self._get_done_tasks(base_url, headers, task_type)
                    if error:
                        stats["errors"].append({
                            "service": svc_name,
                            "type": task_type,
                            "error": error,
                        })
                        logger.warning(
                            "【OpenList Task Monitor】服务 '%s' 获取 %s 任务失败: %s",
                            svc_name, type_label, error,
                        )
                        continue

                    for task in tasks:
                        task_id = task.get("id", "")
                        if not task_id or task_id in svc_notified:
                            continue

                        task_name = task.get("name", "")
                        state = task.get("state", -1)
                        error_msg = task.get("error", "") or ""
                        creator = task.get("creator", "")
                        total_bytes = task.get("total_bytes", 0) or 0
                        start_time = task.get("start_time") or ""
                        end_time = task.get("end_time") or ""
                        progress = task.get("progress", 0) or 0

                        if state == 2 and self._notify_success and self._notify:
                            duration_str = self._calc_duration(start_time, end_time)
                            if total_bytes == 0 and duration_str in ("0秒", "-", ""):
                                logger.debug("【OpenList Task Monitor】跳过占位任务通知: %s", task_name)
                            else:
                                size_str = self._format_bytes(total_bytes)
                                self._notify_completed(
                                    svc_name, type_label, True,
                                    self._build_notify_detail(task_name, success=True,
                                                              size=size_str, duration=duration_str,
                                                              creator=creator),
                                    NotificationType.SiteMessage,
                                )
                                stats["notify_success"] += 1

                        elif state in (5, 7) and self._notify_failed and self._notify:
                            self._notify_completed(
                                svc_name, type_label, False,
                                self._build_notify_detail(task_name, success=False,
                                                          error=error_msg, start_time=start_time,
                                                          creator=creator),
                                NotificationType.SiteMessage,
                            )
                            stats["notify_failed"] += 1

                        svc_notified[task_id] = 1

                        recent_tasks.insert(0, {
                            "time": end_time or start_time or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "service": svc_name,
                            "type": type_label,
                            "name": task_name,
                            "state": state,
                            "size": total_bytes,
                            "duration": self._calc_duration(start_time, end_time),
                            "error": error_msg if state in (5, 7) else "",
                        })

            self._trim_notified_ids(notified_ids)
            self.save_data(self.STORE_NOTIFIED_KEY, notified_ids)

            recent_tasks = recent_tasks[:self.MAX_RECENT_TASKS]
            self.save_data(self.STORE_RECENT_TASKS_KEY, recent_tasks)

            parts = []
            if stats["notify_success"]:
                parts.append(f"成功通知 {stats['notify_success']} 条")
            if stats["notify_failed"]:
                parts.append(f"失败通知 {stats['notify_failed']} 条")
            if not parts:
                parts.append("无新任务")
            message = f"检查完成：{', '.join(parts)}"
            if stats["errors"]:
                message += f"，{len(stats['errors'])} 个错误"

            return self._finish(message, stats)

        finally:
            self._running = False
            self._lock.release()

    def _get_done_tasks(self, base_url: str, headers: Dict[str, str], task_type: str) -> Tuple[List[dict], str]:
        resp = self._get_alist(base_url, f"/api/task/{task_type}/done", headers)
        if not resp:
            return [], "无响应"
        if resp.status_code != 200:
            return [], f"HTTP {resp.status_code}"
        try:
            result = resp.json()
        except Exception as e:
            return [], f"解析响应失败: {e}"
        if result.get("code") != 200:
            return [], str(result.get("message") or "OpenList 返回错误")
        return result.get("data") or [], ""

    def _get_service_auth(self, base_url: str, auth_str: str) -> Optional[Dict[str, str]]:
        auth_str = auth_str.strip()
        if not auth_str:
            return {}

        if ":" in auth_str and auth_str.count(":") == 1:
            username, password = auth_str.split(":", 1)
            username = username.strip()
            password = password.strip()
            if username and password:
                resp = RequestUtils(
                    headers={"Content-Type": "application/json"}
                ).post_res(
                    UrlUtils.adapt_request_url(base_url, "/api/auth/login"),
                    json={"username": username, "password": password},
                )
                if resp and resp.status_code == 200:
                    try:
                        data = resp.json()
                        if data.get("code") == 200:
                            token = str(data.get("data", {}).get("token", ""))
                            if token:
                                return {"Authorization": token}
                    except Exception:
                        pass
                logger.warning("【OpenList Task Monitor】登录失败: %s", base_url)
                return None
        else:
            token = auth_str
            return {"Authorization": token}

        return None

    def _get_alist(self, base_url: str, endpoint: str, headers: Dict[str, str]):
        if not self._wait_rate_limit(0.5, "_last_request_at"):
            return None
        try:
            resp = RequestUtils(headers=headers).get_res(
                UrlUtils.adapt_request_url(base_url, endpoint)
            )
            return resp
        except Exception as e:
            logger.warning("【OpenList Task Monitor】请求失败: %s - %s", endpoint, e)
            return None

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

    def _parse_services(self) -> List[Dict[str, str]]:
        services = []
        for line in self._services.split("\n"):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split("|", 2)]
            if len(parts) < 2:
                continue
            name = parts[0]
            url = parts[1].rstrip("/")
            auth = parts[2] if len(parts) > 2 else ""
            if not name or not url:
                continue
            services.append({"name": name, "url": url, "auth": auth})
        return services

    def _load_notified_ids(self) -> Dict[str, Dict[str, int]]:
        data = self.get_data(self.STORE_NOTIFIED_KEY)
        if isinstance(data, dict):
            return data
        return {}

    def _trim_notified_ids(self, notified: Dict[str, Dict[str, int]]):
        for svc_name in list(notified.keys()):
            ids = notified[svc_name]
            if len(ids) > self.MAX_NOTIFIED_IDS:
                sorted_ids = sorted(ids.items(), key=lambda x: x[1])
                for task_id, _ in sorted_ids[:len(sorted_ids) - self.MAX_NOTIFIED_IDS]:
                    del ids[task_id]
            if not ids:
                del notified[svc_name]

    @staticmethod
    def _parse_task_name(task_name: str) -> Optional[Dict[str, str]]:
        m = re.match(r"^[a-z_]+\s*\[([^\]]*)\]\(([^)]*)\)(?:\s*to\s*\[([^\]]*)\]\(([^)]*)\))?$", task_name.strip())
        if not m:
            return None
        return {
            "src_label": m.group(1).strip(),
            "src_path": m.group(2).strip(),
            "dst_label": (m.group(3) or "").strip(),
            "dst_path": (m.group(4) or "").strip(),
        }

    def _build_notify_detail(self, task_name: str, success: bool, **fields) -> str:
        parsed = self._parse_task_name(task_name)
        lines = []
        if parsed:
            file_name = parsed["src_path"].rstrip("/").split("/")[-1] or "-"
            lines.append(f"文件：{file_name}")
            src = parsed["src_path"]
            if parsed["src_label"] and not src.startswith(parsed["src_label"].rstrip("/") + "/") and src != parsed["src_label"].rstrip("/"):
                src = f"{parsed['src_label'].rstrip('/')}{src}"
            lines.append(f"源：{src}")
            dst = parsed["dst_path"]
            if dst:
                if parsed["dst_label"] and not dst.startswith(parsed["dst_label"].rstrip("/") + "/") and dst != parsed["dst_label"].rstrip("/"):
                    dst = f"{parsed['dst_label'].rstrip('/')}{dst}"
                lines.append(f"目标：{dst}")
        else:
            lines.append(f"任务：{task_name}")
        if success:
            if fields.get("size"):
                lines.append(f"大小：{fields['size']}")
            if fields.get("duration"):
                lines.append(f"耗时：{fields['duration']}")
        else:
            lines.append(f"错误：{fields.get('error') or '未知错误'}")
            start_time = fields.get("start_time") or ""
            lines.append(f"开始时间：{self._format_time(start_time)}")
        lines.append(f"创建者：{fields.get('creator') or '-'}")
        return "\n".join(lines)

    @staticmethod
    def _format_time(value: str) -> str:
        if not value:
            return "-"
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            return value

    @staticmethod
    def _calc_duration(start_time: str, end_time: str) -> str:
        if not start_time or not end_time:
            return "-"
        try:
            start = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
            end = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
            delta = end - start
            total_seconds = int(delta.total_seconds())
            if total_seconds < 0:
                return "-"
            if total_seconds < 60:
                return f"{total_seconds}秒"
            if total_seconds < 3600:
                return f"{total_seconds // 60}分{total_seconds % 60}秒"
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            return f"{hours}时{minutes}分"
        except (ValueError, TypeError):
            return "-"

    @staticmethod
    def _format_bytes(size: int) -> str:
        if not size:
            return "0B"
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size < 1024:
                return f"{size:.1f}{unit}"
            size /= 1024
        return f"{size:.1f}PB"

    @staticmethod
    def _build_task_rows(tasks: List[dict]) -> List[dict]:
        rows = []
        for task in tasks[:20]:
            state = task.get("state", -1)
            if state == 2:
                state_badge = {
                    "component": "VChip",
                    "props": {"color": "success", "size": "small"},
                    "text": "成功",
                }
            elif state in (5, 7):
                state_badge = {
                    "component": "VChip",
                    "props": {"color": "error", "size": "small"},
                    "text": "失败" if state == 7 else "错误",
                }
            else:
                state_badge = {
                    "component": "VChip",
                    "props": {"color": "default", "size": "small"},
                    "text": str(state),
                }
            rows.append({
                "component": "tr",
                "content": [
                    {"component": "td", "text": task.get("service", "-")},
                    {"component": "td", "text": task.get("type", "-")},
                    {"component": "td", "props": {"class": "text-truncate", "style": "max-width:200px"}, "text": task.get("name", "-")},
                    {"component": "td", "content": [state_badge]},
                    {"component": "td", "text": OpenListTaskMonitor._format_bytes(task.get("size", 0))},
                    {"component": "td", "text": task.get("duration", "-")},
                    {"component": "td", "props": {"class": "text-caption error--text"}, "text": task.get("error", "") or ""},
                ],
            })
        return rows

    def _notify_completed(self, svc_name: str, task_type: str, success: bool, detail: str, mtype: NotificationType):
        marker = "✅" if success else "❌"
        title = f"{marker} {task_type}完成 - {svc_name}" if success else f"{marker} {task_type}失败 - {svc_name}"
        try:
            self.post_message(mtype=mtype, title=title, text=detail)
        except Exception as e:
            logger.error("【OpenList Task Monitor】发送通知失败: %s", e)

    def _finish(self, message: str, stats: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
        stats["message"] = message
        self.save_data(self.STORE_RESULT_KEY, stats)
        logger.info("【OpenList Task Monitor】%s", message)
        return True, message, stats

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
