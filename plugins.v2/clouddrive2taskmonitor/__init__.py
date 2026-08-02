import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType
from app.utils.url import UrlUtils


INSTALLED_FLAG = "_clouddrive2_client_installed"


def _install_deps():
    if os.getenv(INSTALLED_FLAG):
        return True
    try:
        subprocess.check_call(
            [
                sys.executable, "-m", "pip", "install",
                "clouddrive2-client>=0.3.0",
                "-q", "--no-cache-dir",
            ],
            timeout=120,
        )
        os.environ[INSTALLED_FLAG] = "1"
        return True
    except Exception as e:
        logger.error("【CloudDrive2 Task Monitor】安装依赖失败: %s", e)
        return False


class CloudDrive2TaskMonitor(_PluginBase):
    plugin_name = "CloudDrive2 Task Monitor"
    plugin_desc = "监控 CloudDrive2 复制/移动任务状态，完成或失败时发送通知。"
    plugin_icon = "https://raw.githubusercontent.com/sucooer/MoviePilot-Plugins/main/icons/Cloudrive_A.png"
    plugin_version = "1.0.4"
    plugin_author = "sucooer"
    author_url = "https://github.com/sucooer/MoviePilot-Plugins"
    plugin_config_prefix = "clouddrive2taskmonitor_"
    plugin_order = 57
    auth_level = 1

    _enabled = False
    _onlyonce = False
    _cron = ""
    _hosts = ""
    _username = ""
    _password = ""
    _notify = True
    _scheduler: Optional[BackgroundScheduler] = None
    _event = threading.Event()
    _lock = threading.Lock()
    _running = False

    STORE_NOTIFIED_KEY = "notified_task_keys"
    STORE_RESULT_KEY = "last_result"
    MAX_NOTIFIED_KEYS = 500

    TASK_MODE_LABELS = {0: "复制", 1: "移动"}
    TASK_STATUS_LABELS = {0: "待处理", 1: "扫描中", 2: "已扫描", 3: "已完成", 4: "已失败"}

    def __init__(self):
        super().__init__()

    def init_plugin(self, config: dict = None):
        self.stop_service()
        config = config or {}
        self._enabled = bool(config.get("enabled", False))
        self._onlyonce = bool(config.get("onlyonce", False))
        self._cron = str(config.get("cron") or "").strip()
        self._hosts = str(config.get("hosts") or "").strip()
        self._username = str(config.get("username") or "").strip()
        self._password = str(config.get("password") or "").strip()
        self._notify = bool(config.get("notify", True))

        if not self._enabled and not self._onlyonce:
            logger.info("【CloudDrive2 Task Monitor】插件未启用")
            return

        if self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(
                func=self.check,
                trigger="date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                name="CloudDrive2 Task Monitor 立即执行",
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
                "description": "立即检查所有 CloudDrive2 服务的任务状态并发送通知。",
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
                "id": "CloudDrive2TaskMonitor",
                "name": "CloudDrive2 Task Monitor 定时检查",
                "trigger": CronTrigger.from_crontab(self._cron),
                "func": self.check,
                "kwargs": {},
            })
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
                                        "props": {"model": "notify", "label": "发送通知"},
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
                                        "component": "VTextField",
                                        "props": {
                                            "model": "username",
                                            "label": "用户名",
                                            "placeholder": "admin",
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
                                            "model": "password",
                                            "label": "密码",
                                            "type": "password",
                                            "placeholder": "password",
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
                                            "model": "hosts",
                                            "label": "CloudDrive2 服务配置",
                                            "placeholder": "主盘|192.168.1.10:19798\n备份盘|192.168.1.11:19798",
                                            "rows": 4,
                                            "hint": "一行一个服务：显示名称 | 主机:端口",
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
                            "text": "监控 CloudDrive2 的复制/移动任务，任务完成或失败时通过 MoviePilot 通知系统发送消息。首次使用会自动安装依赖 clouddrive2-client，耗时约 30 秒。",
                        },
                    },
                ],
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "cron": "*/5 * * * *",
            "hosts": "",
            "username": "",
            "password": "",
            "notify": True,
        }

    def get_page(self) -> List[dict]:
        last_result = self.get_data(self.STORE_RESULT_KEY) or {}
        services_list = self._parse_hosts()
        status_items = [
            ("状态", "检查中" if self._running else ("已启用" if self._enabled else "未启用")),
            ("检查周期", self._cron or "-"),
            ("服务数", str(len(services_list))),
            ("最近检查", str(last_result.get("time") or "-")),
            ("通知成功", str(last_result.get("notify_success", 0))),
            ("通知失败", str(last_result.get("notify_failed", 0))),
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
                                            "text": "监控 CloudDrive2 的复制/移动任务，任务完成或失败时发送通知。",
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
                                                "api": "plugin/CloudDrive2TaskMonitor/check",
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
            logger.debug("【CloudDrive2 Task Monitor】停止调度器失败: %s", e)
        self._scheduler = None
        self._event.clear()

    def api_check(self):
        from app import schemas
        success, message, data = self.check()
        return schemas.Response(success=success, message=message, data=data)

    def api_status(self):
        from app import schemas
        return schemas.Response(success=True, data=self.get_data(self.STORE_RESULT_KEY) or {})

    def _ensure_deps(self) -> bool:
        try:
            from clouddrive2_client import CloudDriveClient
            return True
        except ImportError:
            if not _install_deps():
                return False
            import importlib
            importlib.invalidate_caches()
            try:
                from clouddrive2_client import CloudDriveClient
                return True
            except ImportError:
                return False

    def _parse_hosts(self) -> List[Dict[str, str]]:
        services = []
        for line in self._hosts.split("\n"):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split("|", 1)]
            if len(parts) < 2:
                continue
            name = parts[0]
            host = parts[1].rstrip("/")
            if not name or not host:
                continue
            services.append({"name": name, "host": host})
        return services

    def check(self) -> Tuple[bool, str, Dict[str, Any]]:
        if not self._lock.acquire(blocking=False):
            message = "已有检查任务正在运行"
            logger.warning("【CloudDrive2 Task Monitor】%s", message)
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
            services = self._parse_hosts()
            if not services:
                return self._finish("未配置 CloudDrive2 服务", stats)

            if not self._username or not self._password:
                return self._finish("未配置用户名或密码", stats)

            if not self._ensure_deps():
                return self._finish("依赖安装失败，请检查日志", stats)

            from clouddrive2_client import CloudDriveClient

            notified_keys = self._load_notified_keys()

            for svc in services:
                if self._event.is_set():
                    break
                svc_name = svc["name"]
                svc_host = svc["host"]

                logger.info("【CloudDrive2 Task Monitor】检查服务 '%s' (%s)", svc_name, svc_host)

                try:
                    client = CloudDriveClient(svc_host)
                    if not client.authenticate(self._username, self._password):
                        stats["errors"].append({"service": svc_name, "error": "认证失败"})
                        logger.warning("【CloudDrive2 Task Monitor】服务 '%s' 认证失败", svc_name)
                        continue

                    result = client.get_copy_tasks()

                    for task in result.copyTasks:
                        task_key = f"{svc_name}|{task.sourcePath}|{task.destPath}|{task.taskMode}"
                        if task_key in notified_keys:
                            continue

                        mode_label = self.TASK_MODE_LABELS.get(task.taskMode, str(task.taskMode))
                        state = task.status

                        if state == 3:
                            if self._notify:
                                total_bytes = task.totalBytes or 0
                                total_files = task.totalFiles or 0
                                total_folders = task.totalFolders or 0
                                duration_str = self._calc_duration(task.startTime, task.endTime)
                                size_str = self._format_bytes(total_bytes)
                                self._notify_completed(
                                    svc_name, mode_label, True,
                                    self._build_notify_detail(
                                        source=task.sourcePath, dest=task.destPath,
                                        size=size_str, duration=duration_str,
                                        total_files=total_files, total_folders=total_folders,
                                    ),
                                    NotificationType.SiteMessage,
                                )
                                stats["notify_success"] += 1
                            notified_keys[task_key] = 1

                        elif state == 4:
                            if self._notify:
                                error_msgs = [e.message for e in task.errors] if task.errors else ["未知错误"]
                                self._notify_completed(
                                    svc_name, mode_label, False,
                                    self._build_notify_detail(
                                        source=task.sourcePath, dest=task.destPath,
                                        error="；".join(error_msgs),
                                    ),
                                    NotificationType.SiteMessage,
                                )
                                stats["notify_failed"] += 1
                            notified_keys[task_key] = 1

                except Exception as e:
                    stats["errors"].append({"service": svc_name, "error": str(e)})
                    logger.error("【CloudDrive2 Task Monitor】服务 '%s' 检查异常: %s", svc_name, e)

            self._trim_notified_keys(notified_keys)
            self.save_data(self.STORE_NOTIFIED_KEY, notified_keys)

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

        except Exception as e:
            logger.error("【CloudDrive2 Task Monitor】检查异常: %s", e)
            stats["errors"].append(str(e))
            return self._finish(f"检查异常：{e}", stats)

        finally:
            self._running = False
            self._lock.release()

    def _load_notified_keys(self) -> Dict[str, int]:
        data = self.get_data(self.STORE_NOTIFIED_KEY)
        if isinstance(data, dict):
            return data
        return {}

    def _trim_notified_keys(self, notified: Dict[str, int]):
        if len(notified) > self.MAX_NOTIFIED_KEYS:
            sorted_keys = sorted(notified.keys())
            for k in sorted_keys[:len(sorted_keys) - self.MAX_NOTIFIED_KEYS]:
                del notified[k]

    def _build_notify_detail(self, source: str, dest: str, **fields) -> str:
        lines = []
        file_name = source.rstrip("/").split("/")[-1] if source else "-"
        lines.append(f"文件：{file_name}")
        lines.append(f"源：{source}")
        lines.append(f"目标：{dest}")
        if fields.get("size"):
            lines.append(f"大小：{fields['size']}")
        if fields.get("duration"):
            lines.append(f"耗时：{fields['duration']}")
        if fields.get("total_files") or fields.get("total_folders"):
            lines.append(f"文件数：{fields['total_files']} 文件夹数：{fields['total_folders']}")
        if fields.get("error"):
            lines.append(f"错误：{fields['error']}")
        return "\n\n".join(lines)

    def _notify_completed(self, svc_name: str, task_type: str, success: bool, detail: str, mtype: NotificationType):
        marker = "✅" if success else "❌"
        title = f"{marker} {task_type}完成 - {svc_name}" if success else f"{marker} {task_type}失败 - {svc_name}"
        try:
            self.post_message(mtype=mtype, title=title, text=detail)
        except Exception as e:
            logger.error("【CloudDrive2 Task Monitor】发送通知失败: %s", e)

    def _finish(self, message: str, stats: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
        stats["message"] = message
        self.save_data(self.STORE_RESULT_KEY, stats)
        logger.info("【CloudDrive2 Task Monitor】%s", message)
        return True, message, stats

    @staticmethod
    def _calc_duration(start_time, end_time) -> str:
        if not start_time or not end_time:
            return "-"
        try:
            if hasattr(start_time, "seconds"):
                start = datetime.fromtimestamp(start_time.seconds + start_time.nanos / 1e9)
                end = datetime.fromtimestamp(end_time.seconds + end_time.nanos / 1e9)
            else:
                start = datetime.fromisoformat(str(start_time).replace("Z", "+00:00"))
                end = datetime.fromisoformat(str(end_time).replace("Z", "+00:00"))
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
        except (ValueError, TypeError, AttributeError):
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
