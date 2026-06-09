import os
import re
import shutil
import tempfile
import threading
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import boto3
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import Body

from app import schemas
from app.core.config import settings
from app.helper.system import SystemHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType

lock = threading.Lock()


class S3Backup(_PluginBase):
    plugin_name = "S3备份"
    plugin_desc = "定时通过 S3 备份数据库和配置文件，并支持从 S3 恢复。"
    plugin_icon = "https://raw.githubusercontent.com/sucooer/MoviePilot-Plugins/main/icons/S3.png"
    plugin_version = "0.2.5"
    plugin_author = "sucooer"
    author_url = "https://github.com/sucooer"
    plugin_config_prefix = "s3backup_"
    plugin_order = 60
    auth_level = 1

    _enabled: bool = False
    _notify: bool = False
    _onlyonce: bool = False
    _cron: str = ""
    _max_count: int = 10
    _endpoint_url: str = ""
    _region_name: str = ""
    _bucket_name: str = ""
    _access_key_id: str = ""
    _secret_access_key: str = ""
    _session_token: str = ""
    _prefix: str = ""
    _storage_class: str = ""
    _use_path_style: bool = False
    _skip_ssl_verify: bool = False
    _restart_after_restore: bool = True
    _scheduler: Optional[BackgroundScheduler] = None
    _event = threading.Event()
    _s3_client = None

    BACKUP_NAME_PATTERN = re.compile(r"^MoviePilot-S3-Backup-\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}\.zip$")
    ROLLBACK_NAME_PATTERN = re.compile(r"^MoviePilot-PreRestore-\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}\.zip$")
    STORE_LAST_BACKUP_KEY = "last_backup_result"
    STORE_LAST_RESTORE_KEY = "last_restore_result"
    STORE_PENDING_RESTORE_KEY = "pending_restore_request"
    BACKUP_FILE_PATTERNS = ["user.db*"]
    BACKUP_DIRECT_FILES = ["app.env", "category.yaml"]
    BACKUP_DIRECT_DIRS = ["cookies"]

    def init_plugin(self, config: dict = None):
        self.stop_service()

        config = config or {}
        self._enabled = bool(config.get("enabled", False))
        self._notify = bool(config.get("notify", False))
        self._onlyonce = bool(config.get("onlyonce", False))
        self._cron = str(config.get("cron") or "").strip()
        self._endpoint_url = self._normalize_endpoint_url(config.get("endpoint_url"))
        self._region_name = str(config.get("region_name") or "").strip()
        self._bucket_name = str(config.get("bucket_name") or "").strip()
        self._access_key_id = str(config.get("access_key_id") or "").strip()
        self._secret_access_key = str(config.get("secret_access_key") or "").strip()
        self._session_token = str(config.get("session_token") or "").strip()
        self._prefix = self._normalize_prefix(config.get("prefix"))
        self._storage_class = str(config.get("storage_class") or "").strip()
        self._use_path_style = bool(config.get("use_path_style", False))
        self._skip_ssl_verify = bool(config.get("skip_ssl_verify", False))
        self._restart_after_restore = bool(config.get("restart_after_restore", True))

        try:
            self._max_count = max(0, int(config.get("max_count", 10)))
        except (TypeError, ValueError):
            logger.error("配置错误: max_count 必须为整数，已回退到 10")
            self._max_count = 10

        if not self._enabled and not self._onlyonce:
            logger.info("S3备份未启用")
            return

        try:
            self._s3_client = self._create_s3_client()
        except Exception as err:
            message = f"S3 客户端初始化失败: {err}"
            logger.error(message)
            self._notify_failed(message)
            return

        self._scheduler = BackgroundScheduler(timezone=settings.TZ)

        if self._onlyonce:
            logger.info("S3备份服务立即运行一次")
            self._scheduler.add_job(
                func=self.backup,
                trigger="date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                name="S3备份立即执行"
            )
            config["onlyonce"] = False
            self._onlyonce = False
            self.update_config(config)

        if self._scheduler.get_jobs():
            self._scheduler.print_jobs()
            self._scheduler.start()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/backup",
                "endpoint": self.api_backup,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "执行一次 S3 备份",
                "description": "手动触发一次数据库和配置文件的 S3 备份。"
            },
            {
                "path": "/backups",
                "endpoint": self.api_list_backups,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "列出远端备份",
                "description": "列出当前 S3 前缀下可恢复的备份文件。"
            },
            {
                "path": "/restore",
                "endpoint": self.api_restore,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "从 S3 恢复备份",
                "description": "按对象 key 恢复备份，恢复前会自动创建本地回滚快照。"
            },
            {
                "path": "/cancel_restore",
                "endpoint": self.api_cancel_restore,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "取消待确认恢复",
                "description": "取消当前待确认的恢复请求。"
            }
        ]

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
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件"
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "notify",
                                            "label": "发送通知"
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "onlyonce",
                                            "label": "立即运行一次"
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "restart_after_restore",
                                            "label": "恢复后自动重启"
                                        }
                                    }
                                ]
                            }
                        ]
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
                                            "model": "use_path_style",
                                            "label": "Path Style",
                                            "hint": "部分 S3 兼容服务必须开启。若使用 MinIO、私有 S3 或访问报签名错误，可尝试开启",
                                            "persistent-hint": True
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "skip_ssl_verify",
                                            "label": "跳过 SSL 校验",
                                            "hint": "仅在自签名证书或测试环境下使用，公网正式环境不建议开启",
                                            "persistent-hint": True
                                        }
                                    }
                                ]
                            }
                        ]
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
                                            "label": "执行周期",
                                            "placeholder": "0 3 * * *",
                                            "hint": "5 位 cron 表达式。示例：每天凌晨 3 点执行填 0 3 * * *",
                                            "persistent-hint": True
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "max_count",
                                            "label": "最大保留备份数",
                                            "type": "number",
                                            "min": "0",
                                            "hint": "远端最多保留多少个备份文件。填 0 表示不自动清理",
                                            "persistent-hint": True
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 8},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "endpoint_url",
                                            "label": "S3 Endpoint",
                                            "placeholder": "https://s3.amazonaws.com",
                                            "hint": "S3 兼容接口地址。建议填写完整 URL，例如 https://s3.bitiful.net",
                                            "persistent-hint": True
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "region_name",
                                            "label": "Region",
                                            "placeholder": "ap-southeast-1",
                                            "hint": "对象存储区域名。按服务商控制台填写；如果文档要求可留空，也可以先留空测试",
                                            "persistent-hint": True
                                        }
                                    }
                                ]
                            }
                        ]
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
                                            "model": "bucket_name",
                                            "label": "Bucket 名称",
                                            "hint": "真实桶名，必须与对象存储控制台中的 Bucket 名称完全一致",
                                            "persistent-hint": True
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "prefix",
                                            "label": "对象前缀",
                                            "placeholder": "moviepilot/backups",
                                            "hint": "桶内的目录前缀，可留空。示例：moviepilot/backups",
                                            "persistent-hint": True
                                        }
                                    }
                                ]
                            }
                        ]
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
                                            "model": "access_key_id",
                                            "label": "Access Key ID",
                                            "hint": "对象存储访问密钥 ID",
                                            "persistent-hint": True
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "secret_access_key",
                                            "label": "Secret Access Key",
                                            "type": "password",
                                            "hint": "对象存储访问密钥 Secret",
                                            "persistent-hint": True
                                        }
                                    }
                                ]
                            }
                        ]
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
                                            "model": "session_token",
                                            "label": "Session Token",
                                            "type": "password",
                                            "hint": "仅在使用临时凭证时填写，长期 AK/SK 可留空",
                                            "persistent-hint": True
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "storage_class",
                                            "label": "Storage Class",
                                            "placeholder": "STANDARD_IA",
                                            "hint": "可选，对象存储类型。如 STANDARD、STANDARD_IA；不确定可留空",
                                            "persistent-hint": True
                                        }
                                    }
                                ]
                            }
                        ]
                    },
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
                                            "text": "备份内容为 user.db*、app.env、category.yaml 和 cookies。恢复前会自动创建一次本地回滚压缩包。"
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "notify": True,
            "onlyonce": False,
            "restart_after_restore": True,
            "cron": "0 3 * * *",
            "max_count": 10,
            "endpoint_url": "",
            "region_name": "",
            "bucket_name": "",
            "access_key_id": "",
            "secret_access_key": "",
            "session_token": "",
            "prefix": "moviepilot/backups",
            "storage_class": "",
            "use_path_style": False,
            "skip_ssl_verify": False
        }

    def get_page(self) -> List[dict]:
        last_backup = self.get_data(self.STORE_LAST_BACKUP_KEY) or {}
        last_restore = self.get_data(self.STORE_LAST_RESTORE_KEY) or {}
        pending_restore = self.get_data(self.STORE_PENDING_RESTORE_KEY) or {}
        backups, load_error = self._get_page_backups(limit=10)

        action_bar = [
            {
                "component": "VBtn",
                "props": {
                    "color": "primary",
                    "variant": "flat",
                    "prepend-icon": "mdi-cloud-upload"
                },
                "text": "立即备份",
                "events": {
                    "click": {
                        "api": "plugin/S3Backup/backup",
                        "method": "post",
                        "params": {}
                    }
                }
            },
            {
                "component": "VBtn",
                "props": {
                    "color": "default",
                    "variant": "tonal",
                    "prepend-icon": "mdi-refresh"
                },
                "text": "刷新列表",
                "events": {
                    "click": {
                        "api": "plugin/S3Backup/backups",
                        "method": "get",
                        "params": {}
                    }
                }
            }
        ]

        backup_rows = self._build_backup_rows(backups, pending_restore)

        return [
            {
                "component": "VContainer",
                "content": [
                    {
                        "component": "div",
                        "props": {"class": "d-flex align-center flex-wrap ga-3 mb-4"},
                        "content": [
                            {
                                "component": "h2",
                                "props": {"class": "text-h5 m-0"},
                                "text": "S3 备份"
                            },
                            {"component": "VSpacer"},
                            *action_bar
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            self._build_key_value_card(
                                "当前配置",
                                [
                                    ("状态", "已启用" if self._enabled else "未启用"),
                                    ("Bucket", self._bucket_name or "未配置"),
                                    ("Prefix", self._prefix or "/"),
                                    ("Cron", self._cron or "未配置"),
                                    ("自动重启", "是" if self._restart_after_restore else "否"),
                                ]
                            ),
                            self._build_key_value_card(
                                "最近操作",
                                [
                                    ("最后备份", self._format_status(last_backup)),
                                    ("最后恢复", self._format_status(last_restore)),
                                ]
                            )
                        ]
                    },
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
                                            "type": load_error and "warning" or "info",
                                            "variant": "tonal",
                                            "text": load_error or "点击恢复会先进入确认状态。确认恢复后会先创建本地回滚快照，再覆盖当前数据库、配置文件和当前密码配置；页面仅展示最近 10 条远端备份。"
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    *self._build_pending_restore_alert(pending_restore),
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
                                            {
                                                "component": "VCardTitle",
                                                "text": "远端备份列表"
                                            },
                                            {
                                                "component": "VCardText",
                                                "content": [
                                                    {
                                                        "component": "VTable",
                                                        "props": {"density": "comfortable", "hover": True},
                                                        "content": [
                                                            {
                                                                "component": "thead",
                                                                "content": [
                                                                    {
                                                                        "component": "tr",
                                                                        "content": [
                                                                            {"component": "th", "text": "时间"},
                                                                            {"component": "th", "text": "文件名"},
                                                                            {"component": "th", "text": "大小"},
                                                                            {"component": "th", "text": "对象 Key"},
                                                                            {"component": "th", "text": "操作"}
                                                                        ]
                                                                    }
                                                                ]
                                                            },
                                                            {
                                                                "component": "tbody",
                                                                "content": backup_rows or [
                                                                    {
                                                                        "component": "tr",
                                                                        "content": [
                                                                            {
                                                                                "component": "td",
                                                                                "props": {"colspan": 5},
                                                                                "text": "暂无可恢复的远端备份"
                                                                            }
                                                                        ]
                                                                    }
                                                                ]
                                                            }
                                                        ]
                                                    }
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        services = []
        if self._enabled and self._cron:
            services.append(
                {
                    "id": "S3Backup",
                    "name": "S3备份",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.backup,
                    "kwargs": {}
                }
            )
        return services

    def stop_service(self):
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._event.set()
                    self._scheduler.shutdown()
                    self._event.clear()
                self._scheduler = None
        except Exception as err:
            logger.error(f"停止 S3 备份服务失败: {err}")

    def api_backup(self) -> schemas.Response:
        success, message = self.backup()
        return schemas.Response(success=success, message=message)

    def api_list_backups(self) -> schemas.Response:
        try:
            backups = self._list_backup_objects(include_rollbacks=False)
            return schemas.Response(success=True, data=backups)
        except Exception as err:
            return schemas.Response(success=False, message=f"获取备份列表失败: {err}", data=[])

    def api_cancel_restore(self) -> schemas.Response:
        self.del_data(self.STORE_PENDING_RESTORE_KEY)
        logger.info("已取消待确认的恢复请求")
        return schemas.Response(success=True, message="已取消待确认的恢复请求")

    def api_restore(
        self,
        payload: Optional[dict] = Body(default=None),
        key: Optional[str] = None,
        restart: Optional[bool] = None,
        confirm: Optional[bool] = None
    ) -> schemas.Response:
        payload = payload or {}
        restore_key = key or payload.get("key")
        payload_restart = payload.get("restart")
        payload_confirm = payload.get("confirm")
        if restart is None and payload_restart is not None:
            restart = bool(payload_restart)
        if confirm is None and payload_confirm is not None:
            confirm = bool(payload_confirm)
        auto_restart = self._restart_after_restore if restart is None else bool(restart)
        if not confirm:
            pending = {
                "key": restore_key,
                "restart": auto_restart,
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            self.save_data(self.STORE_PENDING_RESTORE_KEY, pending)
            message = "已进入恢复确认状态，请再次确认。恢复会覆盖当前数据库、配置文件和当前密码配置。"
            logger.warning(message)
            return schemas.Response(success=True, message=message, data=pending)
        self.del_data(self.STORE_PENDING_RESTORE_KEY)
        success, message, data = self.restore(key=restore_key, restart=auto_restart)
        return schemas.Response(success=success, message=message, data=data)

    def backup(self) -> Tuple[bool, str]:
        if not self._prepare_runtime():
            message = "S3 客户端未初始化，请检查插件配置"
            logger.error(message)
            self._notify_failed(message)
            return False, message

        if not lock.acquire(blocking=False):
            message = "已有备份或恢复任务正在执行，跳过本次运行"
            logger.warning(message)
            return False, message

        try:
            logger.info("开始执行 S3 备份")
            self._check_bucket_access()

            local_file = self._backup_and_zip_file(prefix_name="MoviePilot-S3-Backup")
            if not local_file:
                message = "创建本地备份压缩包失败"
                logger.error(message)
                self._record_status(self.STORE_LAST_BACKUP_KEY, False, message)
                self._notify_completed(message)
                return False, message

            object_key = self._build_object_key(local_file)
            try:
                self._upload_to_s3(local_file, object_key)
            finally:
                if os.path.exists(local_file):
                    os.remove(local_file)
                    logger.info(f"已清理本地临时文件: {local_file}")

            if self._max_count:
                self._clean_old_backups(self._max_count)

            message = f"备份成功，已上传到 s3://{self._bucket_name}/{object_key}"
            self._record_status(self.STORE_LAST_BACKUP_KEY, True, message, {"key": object_key})
            logger.info(message)
            self._notify_completed(message)
            return True, message
        except Exception as err:
            message = f"S3 备份失败: {err}"
            self._record_status(self.STORE_LAST_BACKUP_KEY, False, message)
            logger.error(message)
            self._notify_completed(message)
            return False, message
        finally:
            lock.release()

    def restore(self, key: str, restart: bool = True) -> Tuple[bool, str, Dict[str, Any]]:
        key = str(key or "").strip()
        if not key:
            return False, "缺少要恢复的备份对象 key", {}

        if not self._prepare_runtime():
            return False, "S3 客户端未初始化，请检查插件配置", {}

        if not lock.acquire(blocking=False):
            message = "已有备份或恢复任务正在执行，跳过本次运行"
            logger.warning(message)
            return False, message, {}

        temp_dir = None
        local_zip_path = None
        extracted_dir = None
        rollback_zip_path = None
        try:
            logger.info(f"开始执行 S3 恢复，目标对象: {key}，恢复后重启: {'是' if restart else '否'}")
            self._check_bucket_access()
            backup_objects = {item["key"]: item for item in self._list_backup_objects(include_rollbacks=False)}
            if key not in backup_objects:
                return False, f"未找到远端备份对象: {key}", {}

            rollback_zip_path = self._backup_and_zip_file(prefix_name="MoviePilot-PreRestore")
            if not rollback_zip_path:
                return False, "创建恢复前本地回滚快照失败，已中止恢复", {}
            logger.info(f"恢复前本地回滚快照已创建: {rollback_zip_path}")

            temp_dir = Path(tempfile.mkdtemp(prefix="moviepilot-s3-restore-"))
            local_zip_path = temp_dir / Path(key).name
            extracted_dir = temp_dir / "extracted"
            extracted_dir.mkdir(parents=True, exist_ok=True)

            logger.info(f"开始从 S3 下载备份文件: s3://{self._bucket_name}/{key}")
            self._download_from_s3(key, str(local_zip_path))
            logger.info(f"S3 备份下载完成: {local_zip_path}")
            self._extract_archive(str(local_zip_path), extracted_dir)
            logger.info(f"备份压缩包解压完成: {extracted_dir}")
            restored_items = self._restore_from_directory(extracted_dir)
            logger.info(f"恢复完成，已覆盖项目: {', '.join(restored_items)}")

            rollback_path = str(Path(rollback_zip_path))
            message = "恢复成功"
            data = {
                "restored_key": key,
                "rollback_snapshot": rollback_path,
                "restored_items": restored_items,
                "restart_triggered": False
            }

            if restart:
                logger.info("恢复成功，准备触发 MoviePilot 自动重启")
                ret, msg = SystemHelper.restart()
                data["restart_triggered"] = bool(ret)
                if ret:
                    message = "恢复成功，已触发系统重启"
                else:
                    message = f"恢复成功，但自动重启失败: {msg}"

            self._record_status(self.STORE_LAST_RESTORE_KEY, True, message, data)
            self._notify_completed(message)
            return True, message, data
        except Exception as err:
            message = f"S3 恢复失败: {err}"
            self._record_status(
                self.STORE_LAST_RESTORE_KEY,
                False,
                message,
                {"rollback_snapshot": str(rollback_zip_path) if rollback_zip_path else ""}
            )
            logger.error(message)
            self._notify_completed(message)
            return False, message, {}
        finally:
            try:
                if local_zip_path and Path(local_zip_path).exists():
                    Path(local_zip_path).unlink()
                if temp_dir and Path(temp_dir).exists():
                    shutil.rmtree(temp_dir, ignore_errors=True)
            finally:
                lock.release()

    def _prepare_runtime(self) -> bool:
        if self._s3_client:
            return True
        if not self._bucket_name:
            return False
        try:
            self._s3_client = self._create_s3_client()
            return True
        except Exception as err:
            logger.error(f"S3 客户端初始化失败: {err}")
            return False

    def _create_s3_client(self):
        client_kwargs = {
            "service_name": "s3",
            "aws_access_key_id": self._access_key_id or None,
            "aws_secret_access_key": self._secret_access_key or None,
            "aws_session_token": self._session_token or None,
            "region_name": self._region_name or None,
            "endpoint_url": self._endpoint_url or None,
            "verify": not self._skip_ssl_verify,
            "config": Config(
                s3={"addressing_style": "path" if self._use_path_style else "auto"},
                retries={"max_attempts": 3, "mode": "standard"}
            )
        }
        return boto3.client(**client_kwargs)

    def _check_bucket_access(self):
        try:
            self._s3_client.head_bucket(Bucket=self._bucket_name)
        except ClientError as err:
            raise RuntimeError(f"无法访问 Bucket {self._bucket_name}: {err}") from err

    def _upload_to_s3(self, local_file: str, object_key: str):
        extra_args = {}
        if self._storage_class:
            extra_args["StorageClass"] = self._storage_class

        try:
            upload_kwargs = {
                "Filename": local_file,
                "Bucket": self._bucket_name,
                "Key": object_key
            }
            if extra_args:
                upload_kwargs["ExtraArgs"] = extra_args
            self._s3_client.upload_file(**upload_kwargs)
        except (ClientError, BotoCoreError) as err:
            raise RuntimeError(f"上传到 S3 失败: {err}") from err

    def _download_from_s3(self, key: str, local_path: str):
        try:
            self._s3_client.download_file(self._bucket_name, key, local_path)
        except (ClientError, BotoCoreError) as err:
            raise RuntimeError(f"从 S3 下载备份失败: {err}") from err

    def _list_backup_objects(self, include_rollbacks: bool = False) -> List[dict]:
        prefix = self._prefix
        continuation_token = None
        matched = []

        while True:
            params = {"Bucket": self._bucket_name, "Prefix": prefix} if prefix else {"Bucket": self._bucket_name}
            if continuation_token:
                params["ContinuationToken"] = continuation_token
            response = self._s3_client.list_objects_v2(**params)
            for obj in response.get("Contents", []):
                key = obj.get("Key", "")
                file_name = key.rsplit("/", 1)[-1]
                is_backup = bool(self.BACKUP_NAME_PATTERN.match(file_name))
                is_rollback = bool(self.ROLLBACK_NAME_PATTERN.match(file_name))
                if is_backup or (include_rollbacks and is_rollback):
                    matched.append(
                        {
                            "key": key,
                            "name": file_name,
                            "size": obj.get("Size", 0),
                            "last_modified": obj.get("LastModified").isoformat() if obj.get("LastModified") else ""
                        }
                    )
            if not response.get("IsTruncated"):
                break
            continuation_token = response.get("NextContinuationToken")

        matched.sort(key=lambda item: item.get("name", ""), reverse=True)
        return matched

    @classmethod
    def _backup_and_zip_file(cls, prefix_name: str) -> str:
        try:
            config_path = Path(settings.CONFIG_PATH)
            current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            backup_name = f"{prefix_name}-{current_time}"
            backup_path = config_path / backup_name
            zip_file_path = str(backup_path) + ".zip"

            backup_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"本地临时备份目录: {backup_path}")

            for pattern in cls.BACKUP_FILE_PATTERNS:
                for item_path in config_path.glob(pattern):
                    if item_path.is_file():
                        shutil.copy2(item_path, backup_path / item_path.name)
                        logger.info(f"正在备份文件: {item_path}")

            for file_name in cls.BACKUP_DIRECT_FILES:
                item_path = config_path / file_name
                if item_path.exists() and item_path.is_file():
                    shutil.copy2(item_path, backup_path / item_path.name)
                    logger.info(f"正在备份文件: {item_path}")

            for dir_name in cls.BACKUP_DIRECT_DIRS:
                item_path = config_path / dir_name
                if item_path.exists() and item_path.is_dir():
                    shutil.copytree(item_path, backup_path / item_path.name)
                    logger.info(f"正在备份目录: {item_path}")

            shutil.make_archive(base_name=str(backup_path), format="zip", root_dir=str(backup_path))
            shutil.rmtree(backup_path)
            logger.info(f"成功创建 ZIP 备份文件: {zip_file_path}")
            return zip_file_path
        except Exception as err:
            logger.error(f"创建备份 ZIP 文件失败: {err}")
            return ""

    def _clean_old_backups(self, max_count: int):
        matched_items = sorted(
            self._list_backup_objects(include_rollbacks=False),
            key=lambda item: item.get("name", "")
        )
        excess_count = len(matched_items) - max_count
        if excess_count <= 0:
            logger.info(f"S3 备份数量 {len(matched_items)}，未超过保留上限 {max_count}")
            return

        logger.info(f"S3 备份数量 {len(matched_items)}，将删除 {excess_count} 个旧备份")
        for item in matched_items[:-max_count]:
            key = item["key"]
            try:
                self._s3_client.delete_object(Bucket=self._bucket_name, Key=key)
                logger.info(f"已删除旧备份: s3://{self._bucket_name}/{key}")
            except (ClientError, BotoCoreError) as err:
                logger.error(f"删除旧备份失败 {key}: {err}")

    def _build_object_key(self, local_file: str) -> str:
        file_name = os.path.basename(local_file)
        if self._prefix:
            return f"{self._prefix}/{file_name}"
        return file_name

    @staticmethod
    def _normalize_prefix(value: Any) -> str:
        return str(value or "").strip().strip("/")

    @staticmethod
    def _normalize_endpoint_url(value: Any) -> str:
        endpoint = str(value or "").strip()
        if not endpoint:
            return ""
        if "://" not in endpoint:
            endpoint = f"https://{endpoint}"
        return endpoint.rstrip("/")

    @staticmethod
    def _extract_archive(zip_path: str, target_dir: Path):
        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(target_dir)
        except Exception as err:
            raise RuntimeError(f"解压备份文件失败: {err}") from err

    @classmethod
    def _restore_from_directory(cls, extracted_dir: Path) -> List[str]:
        config_path = Path(settings.CONFIG_PATH)
        restored = []

        for pattern in cls.BACKUP_FILE_PATTERNS:
            regex = re.compile("^" + pattern.replace(".", r"\.").replace("*", ".*") + "$")
            for item_path in extracted_dir.iterdir():
                if item_path.is_file() and regex.match(item_path.name):
                    shutil.copy2(item_path, config_path / item_path.name)
                    restored.append(item_path.name)

        for file_name in cls.BACKUP_DIRECT_FILES:
            source_path = extracted_dir / file_name
            if source_path.exists() and source_path.is_file():
                shutil.copy2(source_path, config_path / file_name)
                restored.append(file_name)

        for dir_name in cls.BACKUP_DIRECT_DIRS:
            source_path = extracted_dir / dir_name
            target_path = config_path / dir_name
            if source_path.exists() and source_path.is_dir():
                if target_path.exists():
                    shutil.rmtree(target_path)
                shutil.copytree(source_path, target_path)
                restored.append(f"{dir_name}/")

        if not restored:
            raise RuntimeError("备份包中未找到可恢复的文件")
        return restored

    @staticmethod
    def _format_status(status: dict) -> str:
        if not status:
            return "暂无记录"
        time_text = S3Backup._format_display_time(status.get("time")) or "未知时间"
        message = status.get("message") or "无消息"
        return f"{time_text} {message}"

    def _get_page_backups(self, limit: int = 10) -> Tuple[List[dict], str]:
        try:
            if not self._prepare_runtime():
                return [], "S3 客户端未初始化，无法读取远端备份列表"
            backups = self._list_backup_objects(include_rollbacks=False)
            return backups[:limit], ""
        except Exception as err:
            logger.error(f"读取页面备份列表失败: {err}")
            return [], f"读取远端备份列表失败: {err}"

    def _build_backup_rows(self, backups: List[dict], pending_restore: Optional[dict] = None) -> List[dict]:
        rows = []
        pending_key = (pending_restore or {}).get("key")
        pending_restart = bool((pending_restore or {}).get("restart"))
        for item in backups:
            key = item.get("key", "")
            is_pending = pending_key == key
            rows.append(
                {
                    "component": "tr",
                    "content": [
                        {"component": "td", "text": self._format_backup_time(item)},
                        {"component": "td", "text": item.get("name", "-")},
                        {"component": "td", "text": self._format_size(item.get("size", 0))},
                        {
                            "component": "td",
                            "props": {"class": "text-caption"},
                            "text": key
                        },
                        {
                            "component": "td",
                            "content": [
                                {
                                    "component": "div",
                                    "props": {"class": "d-flex flex-wrap ga-2"},
                                    "content": [
                                        {
                                            "component": "VBtn",
                                            "props": {
                                                "size": "small",
                                                "color": "warning",
                                                "variant": "tonal"
                                            },
                                            "text": "选择恢复",
                                            "events": {
                                                "click": {
                                                    "api": "plugin/S3Backup/restore",
                                                    "method": "post",
                                                    "params": {
                                                        "key": key,
                                                        "restart": False,
                                                        "confirm": False
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "component": "VBtn",
                                            "props": {
                                                "size": "small",
                                                "color": "error",
                                                "variant": "flat"
                                            },
                                            "text": "选择恢复并重启",
                                            "events": {
                                                "click": {
                                                    "api": "plugin/S3Backup/restore",
                                                    "method": "post",
                                                    "params": {
                                                        "key": key,
                                                        "restart": True,
                                                        "confirm": False
                                                    }
                                                }
                                            }
                                        },
                                        *(
                                            [
                                                {
                                                    "component": "VChip",
                                                    "props": {
                                                        "size": "small",
                                                        "color": pending_restart and "error" or "warning",
                                                        "variant": "tonal"
                                                    },
                                                    "text": pending_restart and "待确认: 恢复并重启" or "待确认: 恢复"
                                                }
                                            ] if is_pending else []
                                        )
                                    ]
                                }
                            ]
                        }
                    ]
                }
            )
        return rows

    @staticmethod
    def _build_key_value_card(title: str, items: List[Tuple[str, str]]) -> dict:
        rows = []
        for label, value in items:
            rows.append(
                {
                    "component": "div",
                    "props": {"class": "d-flex justify-space-between py-1 ga-4"},
                    "content": [
                        {
                            "component": "span",
                            "props": {"class": "text-medium-emphasis"},
                            "text": label
                        },
                        {
                            "component": "span",
                            "props": {"class": "text-right"},
                            "text": str(value)
                        }
                    ]
                }
            )

        return {
            "component": "VCol",
            "props": {"cols": 12, "md": 6},
            "content": [
                {
                    "component": "VCard",
                    "props": {"flat": True, "border": True},
                    "content": [
                        {"component": "VCardTitle", "text": title},
                        {
                            "component": "VCardText",
                            "content": rows
                        }
                    ]
                }
            ]
        }

    @staticmethod
    def _format_size(size: Any) -> str:
        try:
            value = float(size or 0)
        except (TypeError, ValueError):
            return "-"
        units = ["B", "KB", "MB", "GB", "TB"]
        index = 0
        while value >= 1024 and index < len(units) - 1:
            value /= 1024
            index += 1
        return f"{value:.2f} {units[index]}"

    @staticmethod
    def _format_backup_time(item: dict) -> str:
        if item.get("last_modified"):
            raw = str(item.get("last_modified"))
            try:
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                local_dt = dt.astimezone(ZoneInfo(settings.TZ))
                return local_dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                return raw
        name = str(item.get("name") or "")
        match = re.search(r"(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})", name)
        if match:
            return match.group(1).replace("_", " ")
        return "-"

    def _build_pending_restore_alert(self, pending_restore: dict) -> List[dict]:
        if not pending_restore or not pending_restore.get("key"):
            return []

        key = pending_restore.get("key")
        restart = bool(pending_restore.get("restart"))
        selected_at = self._format_display_time(pending_restore.get("time")) or "-"

        return [
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
                                    "type": "error",
                                    "variant": "tonal",
                                    "title": "恢复二次确认",
                                    "text": (
                                        f"你已选择恢复备份：{key}。这会覆盖当前数据库、配置文件和当前密码配置。"
                                        f" 选择时间：{selected_at}"
                                    )
                                },
                                "content": [
                                    {
                                        "component": "div",
                                        "props": {"class": "d-flex flex-wrap ga-2 mt-3"},
                                        "content": [
                                            {
                                                "component": "VBtn",
                                                "props": {
                                                    "color": "error",
                                                    "variant": "flat",
                                                    "prepend-icon": "mdi-alert"
                                                },
                                                "text": restart and "确认恢复并重启" or "确认恢复",
                                                "events": {
                                                    "click": {
                                                        "api": "plugin/S3Backup/restore",
                                                        "method": "post",
                                                        "params": {
                                                            "key": key,
                                                            "restart": restart,
                                                            "confirm": True
                                                        }
                                                    }
                                                }
                                            },
                                            {
                                                "component": "VBtn",
                                                "props": {
                                                    "color": "default",
                                                    "variant": "tonal",
                                                    "prepend-icon": "mdi-close"
                                                },
                                                "text": "取消恢复",
                                                "events": {
                                                    "click": {
                                                        "api": "plugin/S3Backup/cancel_restore",
                                                        "method": "post",
                                                        "params": {}
                                                    }
                                                }
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]

    def _record_status(self, key: str, success: bool, message: str, data: Optional[dict] = None):
        payload = {
            "success": success,
            "message": message,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data": data or {}
        }
        self.save_data(key, payload)

    @staticmethod
    def _format_display_time(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if dt.tzinfo:
                dt = dt.astimezone(ZoneInfo(settings.TZ))
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
        match = re.match(r"^(\d{4}-\d{2}-\d{2})[T_ ](\d{2}[-:]\d{2}[-:]\d{2})", text)
        if match:
            return f"{match.group(1)} {match.group(2).replace('-', ':')}"
        return text

    def _notify_completed(self, message: str):
        if not self._notify:
            return
        self.post_message(
            mtype=NotificationType.SiteMessage,
            title="【S3备份任务】",
            text=f"{message}\n时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )

    def _notify_failed(self, message: str):
        if not self._notify:
            return
        self.post_message(
            mtype=NotificationType.SiteMessage,
            title="【S3备份失败】",
            text=f"{message}\n时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
