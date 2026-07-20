import hashlib
import os
import re
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
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

from app.helper.storage import StorageHelper
from app.schemas import StorageConf


class OpenListMetadataSync(_PluginBase):
    plugin_name = "OpenList Metadata Sync"
    plugin_desc = "通过 OpenList 同步本地元数据文件，支持双向同步、修改时间对比、自定义同步规则。"
    plugin_icon = "https://raw.githubusercontent.com/sucooer/MoviePilot-Plugins/main/icons/OpenList.png"
    plugin_version = "1.0.0"
    plugin_author = "sucooer"
    author_url = "https://github.com/sucooer/MoviePilot-Plugins"
    plugin_config_prefix = "openlistmetadatasync_"
    plugin_order = 55
    auth_level = 1

    _enabled = False
    _onlyonce = False
    _cron = ""
    _path_rules = ""
    _extensions = ".nfo,.jpg,.jpeg,.png,.webp,.gif,.bmp"
    _download_metadata = True
    _cloud_missing_action = "upload"
    _special_dir_names = (
        "extrafanart, exfanarts, extrafanarts, extras, specials, shorts, scenes, "
        "featurettes, behind the scenes, trailers, interviews"
    )
    _check_mtime = False
    _notify = True
    _scheduler: Optional[BackgroundScheduler] = None
    _event = threading.Event()
    _lock = threading.Lock()
    _running = False

    STORE_RESULT_KEY = "last_result"
    OPENLIST_MAX_LIST_PAGE_SIZE = 500

    DEFAULT_SPECIAL_DIR_NAMES = {
        "extrafanart", "exfanarts", "extrafanarts", "extras", "specials",
        "shorts", "scenes", "featurettes", "behind the scenes", "trailers",
        "interviews",
    }

    def __init__(self):
        super().__init__()
        self._last_openlist_request_at = 0.0
        self._rate_limit_lock = threading.Lock()

    def init_plugin(self, config: dict = None):
        self.stop_service()

        config = config or {}
        self._enabled = bool(config.get("enabled", False))
        self._onlyonce = bool(config.get("onlyonce", False))
        self._cron = str(config.get("cron") or "").strip()
        self._path_rules = str(config.get("path_rules") or "").strip()
        self._extensions = str(config.get("extensions") or ".nfo,.jpg,.jpeg,.png,.webp,.gif,.bmp").strip()
        self._download_metadata = bool(config.get("download_metadata", True))
        self._cloud_missing_action = str(config.get("cloud_missing_action") or "upload").strip()
        self._special_dir_names = str(config.get("special_dir_names") or self._special_dir_names).strip()
        self._check_mtime = bool(config.get("check_mtime", False))
        self._notify = bool(config.get("notify", True))

        if self._cloud_missing_action not in {"delete", "upload", "keep"}:
            self._cloud_missing_action = "upload"

        if not self._enabled and not self._onlyonce:
            logger.info("【OpenList Metadata Sync】插件未启用")
            return

        if self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(
                func=self.sync,
                trigger="date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                name="OpenList Metadata Sync 立即执行",
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
                "path": "/sync",
                "endpoint": self.api_sync,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "立即执行同步",
                "description": "立即执行一次元数据双向同步。",
            },
            {
                "path": "/status",
                "endpoint": self.api_status,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "获取最近同步结果",
                "description": "获取最近一次同步的结果摘要。",
            },
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        services = []
        if not self._enabled:
            return services
        if self._cron:
            services.append({
                "id": "OpenListMetadataSync",
                "name": "OpenList Metadata Sync 定时同步",
                "trigger": CronTrigger.from_crontab(self._cron),
                "func": self.sync,
                "kwargs": {},
            })
        return services

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        cloud_missing_items = [
            {"title": "删除", "value": "delete"},
            {"title": "上传", "value": "upload"},
            {"title": "保留", "value": "keep"},
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
                                        "props": {
                                            "model": "download_metadata",
                                            "label": "下载元数据",
                                            "hint": "关闭时仅报告差异，不做任何写入操作",
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
                                            "model": "check_mtime",
                                            "label": "检查修改时间",
                                            "hint": "开启后按修改时间双向同步两端共有的文件",
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
                                            "label": "同步周期",
                                            "placeholder": "0 4 * * *",
                                            "hint": "5 位 cron 表达式。留空仅手动触发",
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
                                        "component": "VSelect",
                                        "props": {
                                            "model": "cloud_missing_action",
                                            "label": "网盘不存在的元数据",
                                            "items": cloud_missing_items,
                                            "hint": "上传：父目录名在特殊目录名中时可自动创建远端目录",
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
                                            "model": "extensions",
                                            "label": "同步文件后缀",
                                            "placeholder": ".nfo,.jpg,.jpeg,.png,.webp,.gif,.bmp",
                                            "hint": "逗号或换行分隔，仅匹配这些后缀的文件",
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
                                            "model": "path_rules",
                                            "label": "同步目录映射",
                                            "placeholder": "/本地/元数据路径1 => /远端/路径1\n/本地/元数据路径2 => /远端/路径2",
                                            "rows": 4,
                                            "hint": "一行一个本地路径 => 远端路径，左侧扫描本地，右侧对应 OpenList 目录",
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
                                            "model": "special_dir_names",
                                            "label": "特殊目录名",
                                            "placeholder": self._special_dir_names,
                                            "rows": 3,
                                            "hint": "逗号或换行分隔。云端缺失且父目录名为这些值时，自动创建远端目录并上传",
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
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "notify",
                                            "label": "完成通知",
                                            "hint": "同步完成后发送 MoviePilot 通知",
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
                            "text": "通过 OpenList(AList) 同步本地元数据文件到网盘或从网盘下载到本地。"
                        },
                    },
                ],
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "cron": "0 4 * * *",
            "path_rules": "",
            "extensions": ".nfo,.jpg,.jpeg,.png,.webp,.gif,.bmp",
            "download_metadata": True,
            "cloud_missing_action": "upload",
            "special_dir_names": self._special_dir_names,
            "check_mtime": False,
            "notify": True,
        }

    def get_page(self) -> List[dict]:
        last_result = self.get_data(self.STORE_RESULT_KEY) or {}
        status_items = [
            ("状态", "同步中" if self._running else ("已启用" if self._enabled else "未启用")),
            ("同步周期", self._cron or "-"),
            ("下载元数据", "是" if self._download_metadata else "否（只读报告）"),
            ("网盘缺失处理", self._cloud_missing_action or "-"),
            ("检查修改时间", "是" if self._check_mtime else "否"),
            ("同步映射数", str(len(self._parse_path_rules()))),
            ("最近同步", str(last_result.get("time") or "-")),
            ("检查本地数", str(last_result.get("local_files", 0))),
            ("检查远端数", str(last_result.get("remote_files", 0))),
            ("已下载", str(last_result.get("downloaded", 0))),
            ("已上传", str(last_result.get("uploaded", 0))),
            ("已删除", str(last_result.get("deleted", 0))),
            ("已覆盖（远端→本地）", str(last_result.get("downloaded_newer", 0))),
            ("已覆盖（本地→远端）", str(last_result.get("uploaded_newer", 0))),
            ("跳过（两端存在）", str(last_result.get("skipped_common", 0))),
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
                                            "text": "通过 OpenList(AList) 同步本地元数据到网盘或从网盘下载到本地。"
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
                                        "text": "立即同步",
                                        "events": {
                                            "click": {
                                                "api": "plugin/OpenListMetadataSync/sync",
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
            logger.debug("【OpenList Metadata Sync】停止调度器失败: %s", e)
        self._scheduler = None
        self._event.clear()

    def api_sync(self) -> schemas.Response:
        success, message, data = self.sync()
        return schemas.Response(success=success, message=message, data=data)

    def api_status(self) -> schemas.Response:
        return schemas.Response(success=True, data=self.get_data(self.STORE_RESULT_KEY) or {})

    def sync(self) -> Tuple[bool, str, Dict[str, Any]]:
        if not self._lock.acquire(blocking=False):
            message = "已有同步任务正在运行"
            logger.warning("【OpenList Metadata Sync】%s", message)
            return False, message, {}

        self._running = True
        stats = {
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "path_rules": [],
            "local_files": 0,
            "remote_files": 0,
            "downloaded": 0,
            "uploaded": 0,
            "deleted": 0,
            "downloaded_newer": 0,
            "uploaded_newer": 0,
            "skipped_common": 0,
            "errors": [],
            "message": "",
        }
        try:
            conf = self._get_alist_conf()
            if not conf:
                return self._finish("未找到 OpenList 存储配置", stats)

            base_url = self._get_alist_base_url(conf)
            headers = self._get_alist_auth_header(conf)
            if not base_url or not headers:
                return self._finish("OpenList 认证失败", stats)

            rules = self._parse_path_rules()
            if not rules:
                return self._finish("未配置同步目录映射", stats)

            stats["path_rules"] = rules
            ext_set = self._parse_extensions()

            for rule in rules:
                if self._event.is_set():
                    break
                self._sync_rule(base_url, headers, rule, ext_set, stats)

            parts = []
            if stats["downloaded"]:
                parts.append(f"下载 {stats['downloaded']} 个")
            if stats["uploaded"]:
                parts.append(f"上传 {stats['uploaded']} 个")
            if stats["deleted"]:
                parts.append(f"删除 {stats['deleted']} 个")
            if stats["downloaded_newer"]:
                parts.append(f"远端更新 {stats['downloaded_newer']} 个")
            if stats["uploaded_newer"]:
                parts.append(f"本地更新 {stats['uploaded_newer']} 个")
            if stats["skipped_common"]:
                parts.append(f"跳过 {stats['skipped_common']} 个")

            if not self._download_metadata:
                message = "只读报告模式，未执行任何写入操作"
            elif parts:
                message = f"同步完成：{', '.join(parts)}"
            else:
                message = "同步完成，两端一致，无需操作"

            if stats["errors"]:
                message += f"，{len(stats['errors'])} 个错误"

            return self._finish(message, stats)

        finally:
            self._running = False
            self._lock.release()

    def _sync_rule(
        self,
        base_url: str,
        headers: Dict[str, str],
        rule: Dict[str, str],
        ext_set: set,
        stats: Dict[str, Any],
    ):
        local_root = rule["source"]
        remote_root = rule["target"]

        local_root_path = Path(local_root)
        if not local_root_path.exists():
            stats["errors"].append({"path": local_root, "error": "本地路径不存在"})
            logger.warning("【OpenList Metadata Sync】本地路径不存在: %s", local_root)
            return

        logger.info("【OpenList Metadata Sync】扫描本地: %s", local_root)
        local_files = self._scan_local(local_root_path, ext_set)
        stats["local_files"] += len(local_files)
        logger.info("【OpenList Metadata Sync】本地发现 %s 个文件", len(local_files))

        logger.info("【OpenList Metadata Sync】扫描远端: %s", remote_root)
        remote_files_dict, error = self._scan_remote_to_dict(base_url, headers, remote_root, ext_set)
        if error:
            stats["errors"].append({"path": remote_root, "error": error})
            logger.warning("【OpenList Metadata Sync】扫描远端失败: %s - %s", remote_root, error)
            return
        stats["remote_files"] += len(remote_files_dict)
        logger.info("【OpenList Metadata Sync】远端发现 %s 个文件", len(remote_files_dict))

        if not self._download_metadata:
            logger.info("【OpenList Metadata Sync】只读报告模式，跳过写入操作")
            return

        self._process_cloud_missing(
            base_url, headers, local_root, remote_root,
            local_files, remote_files_dict, stats,
        )

        if self._download_metadata:
            self._process_download_metadata(
                base_url, headers, local_root, remote_root,
                local_files, remote_files_dict, stats,
            )

        if self._check_mtime:
            self._process_mtime_sync(
                base_url, headers, local_root, remote_root,
                local_files, remote_files_dict, stats,
            )
        else:
            stats["skipped_common"] += self._count_common_files(local_files, remote_files_dict)

    def _scan_local(self, root: Path, ext_set: set) -> Dict[str, Path]:
        result = {}
        for file_path in root.rglob("*"):
            if self._event.is_set():
                break
            if not file_path.is_file():
                continue
            if ext_set and file_path.suffix.lower() not in ext_set:
                continue
            rel_path = file_path.relative_to(root).as_posix()
            result[rel_path] = file_path
        return result

    def _scan_remote_to_dict(
        self,
        base_url: str,
        headers: Dict[str, str],
        remote_root: str,
        ext_set: set,
    ) -> Tuple[Dict[str, dict], str]:
        all_files = {}
        listing, error = self._list_directory_recursive(base_url, headers, remote_root, ext_set)
        if error:
            return {}, error
        for item in listing:
            rel_path = self._rel_path(item["path"], remote_root)
            if rel_path:
                all_files[rel_path] = item
        return all_files, ""

    def _list_directory_recursive(
        self,
        base_url: str,
        headers: Dict[str, str],
        path: str,
        ext_set: set,
        max_pages: int = 100,
    ) -> Tuple[List[Dict[str, Any]], str]:
        results = []
        stack = [path]
        visited = set()

        while stack and not self._event.is_set():
            current = stack.pop(0)
            if current in visited:
                continue
            visited.add(current)
            files, error = self._list_directory(base_url, headers, current)
            if error:
                return [], f"列目录失败 {current}: {error}"
            for item in files.get("files", []):
                full_path = self._normalize_path(f"{current}/{item['name']}")
                if item.get("is_dir"):
                    stack.append(full_path)
                else:
                    ext = Path(item["name"]).suffix.lower()
                    if ext_set and ext not in ext_set:
                        continue
                    results.append({
                        "name": item["name"],
                        "path": full_path,
                        "size": item.get("size", 0),
                        "modified": item.get("modified", ""),
                    })

        return results, ""

    def _list_directory(
        self,
        base_url: str,
        headers: Dict[str, str],
        path: str,
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
                    "refresh": False,
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

    def _process_cloud_missing(
        self,
        base_url: str,
        headers: Dict[str, str],
        local_root: Path,
        remote_root: str,
        local_files: Dict[str, Path],
        remote_files: Dict[str, dict],
        stats: Dict[str, Any],
    ):
        if self._cloud_missing_action == "keep":
            logger.info("【OpenList Metadata Sync】策略为保留，跳过网盘缺失处理")
            return

        for rel_path, local_path in local_files.items():
            if self._event.is_set():
                break
            if rel_path in remote_files:
                continue

            if self._cloud_missing_action == "delete":
                try:
                    local_path.unlink()
                    stats["deleted"] += 1
                    logger.info("【OpenList Metadata Sync】已删除本地文件（网盘不存在）: %s", rel_path)
                except Exception as e:
                    stats["errors"].append({"path": rel_path, "error": f"删除失败: {e}"})
                    logger.warning("【OpenList Metadata Sync】删除本地文件失败: %s - %s", rel_path, e)

            elif self._cloud_missing_action == "upload":
                parent_rel = str(Path(rel_path).parent)
                if parent_rel == ".":
                    parent_rel = ""
                remote_parent = self._normalize_path(f"{remote_root}/{parent_rel}") if parent_rel else remote_root
                parent_dir_name = Path(rel_path).parent.name if parent_rel else ""

                if self._alist_path_exists(base_url, headers, remote_parent):
                    self._upload_file(base_url, headers, local_path, remote_parent, rel_path, stats)
                elif parent_dir_name in self._get_special_dir_names():
                    logger.info("【OpenList Metadata Sync】特殊目录 '%s' 不存在，创建并上传: %s", parent_dir_name, rel_path)
                    if self._ensure_alist_directory(base_url, headers, remote_parent):
                        self._upload_file(base_url, headers, local_path, remote_parent, rel_path, stats)
                    else:
                        stats["errors"].append({"path": rel_path, "error": "创建远端目录失败"})
                else:
                    try:
                        local_path.unlink()
                        stats["deleted"] += 1
                        logger.info("【OpenList Metadata Sync】父目录不存在，已删除本地文件: %s", rel_path)
                    except Exception as e:
                        stats["errors"].append({"path": rel_path, "error": f"删除失败: {e}"})

    def _process_download_metadata(
        self,
        base_url: str,
        headers: Dict[str, str],
        local_root: Path,
        remote_root: str,
        local_files: Dict[str, Path],
        remote_files: Dict[str, dict],
        stats: Dict[str, Any],
    ):
        for rel_path, remote_item in remote_files.items():
            if self._event.is_set():
                break
            if rel_path in local_files:
                continue

            remote_full_path = remote_item["path"]
            local_target = local_root / rel_path

            local_target.parent.mkdir(parents=True, exist_ok=True)
            if self._download_file(base_url, headers, remote_full_path, local_target):
                stats["downloaded"] += 1
                logger.info("【OpenList Metadata Sync】已下载: %s", rel_path)
            else:
                stats["errors"].append({"path": rel_path, "error": "下载失败"})

    def _process_mtime_sync(
        self,
        base_url: str,
        headers: Dict[str, str],
        local_root: Path,
        remote_root: str,
        local_files: Dict[str, Path],
        remote_files: Dict[str, dict],
        stats: Dict[str, Any],
    ):
        for rel_path, local_path in local_files.items():
            if self._event.is_set():
                break
            remote_item = remote_files.get(rel_path)
            if not remote_item:
                continue

            local_mtime = local_path.stat().st_mtime
            remote_modified = remote_item.get("modified", "")
            remote_mtime = self._parse_alist_mtime(remote_modified)

            if remote_mtime is None:
                stats["skipped_common"] += 1
                continue

            if remote_mtime > local_mtime:
                if self._download_file(base_url, headers, remote_item["path"], local_path):
                    stats["downloaded_newer"] += 1
                    logger.info("【OpenList Metadata Sync】远端更新，已下载覆盖: %s", rel_path)
                else:
                    stats["errors"].append({"path": rel_path, "error": "下载覆盖失败"})
            elif remote_mtime < local_mtime:
                remote_parent = str(Path(remote_item["path"]).parent)
                if self._upload_file(base_url, headers, local_path, remote_parent, rel_path, stats):
                    stats["uploaded_newer"] += 1
                    logger.info("【OpenList Metadata Sync】本地更新，已上传覆盖: %s", rel_path)
            else:
                stats["skipped_common"] += 1

    def _upload_file(
        self,
        base_url: str,
        headers: Dict[str, str],
        local_path: Path,
        remote_parent: str,
        rel_path: str,
        stats: Dict[str, Any],
    ) -> bool:
        try:
            file_data = local_path.read_bytes()
        except Exception as e:
            stats["errors"].append({"path": rel_path, "error": f"读取本地文件失败: {e}"})
            return False

        remote_full_path = self._normalize_path(f"{remote_parent}/{Path(rel_path).name}")
        put_url = UrlUtils.adapt_request_url(base_url, "/api/fs/put")
        upload_headers = dict(headers)
        upload_headers["Content-Type"] = "application/octet-stream"

        if not self._wait_openlist_interval():
            return False

        try:
            resp = requests.put(
                put_url,
                params={"path": remote_full_path},
                headers=upload_headers,
                data=file_data,
                timeout=300,
            )
            if resp.status_code != 200:
                stats["errors"].append({"path": rel_path, "error": f"上传失败: HTTP {resp.status_code}"})
                return False
            result = resp.json()
            if result.get("code") == 200:
                stats["uploaded"] += 1
                return True
            else:
                stats["errors"].append({"path": rel_path, "error": f"上传失败: {result.get('message')}"})
                return False
        except Exception as e:
            stats["errors"].append({"path": rel_path, "error": f"上传异常: {e}"})
            return False

    def _download_file(
        self,
        base_url: str,
        headers: Dict[str, str],
        remote_path: str,
        local_path: Path,
    ) -> bool:
        resp = self._post_alist(
            base_url,
            "/api/fs/get",
            headers,
            json={"path": remote_path},
        )
        if not resp or resp.status_code != 200:
            return False
        try:
            result = resp.json()
        except Exception:
            return False
        if result.get("code") != 200:
            return False

        raw_url = result.get("data", {}).get("raw_url") or result.get("data", {}).get("url") or ""
        if not raw_url:
            return False

        if not self._wait_openlist_interval():
            return False

        try:
            dl_resp = requests.get(raw_url, timeout=300, stream=True)
            if dl_resp.status_code != 200:
                return False
            local_path.parent.mkdir(parents=True, exist_ok=True)
            with open(local_path, "wb") as f:
                for chunk in dl_resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            return True
        except Exception as e:
            logger.warning("【OpenList Metadata Sync】下载文件失败: %s - %s", remote_path, e)
            return False

    def _alist_path_exists(
        self,
        base_url: str,
        headers: Dict[str, str],
        path: str,
    ) -> bool:
        clean_path = self._normalize_path(path)
        if not clean_path or clean_path == "/":
            return True
        resp = self._post_alist(
            base_url,
            "/api/fs/get",
            headers,
            json={"path": clean_path, "password": "", "refresh": True},
        )
        if not resp or resp.status_code != 200:
            return False
        try:
            result = resp.json()
            return result.get("code") == 200
        except Exception:
            return False

    def _ensure_alist_directory(
        self,
        base_url: str,
        headers: Dict[str, str],
        path: str,
    ) -> bool:
        clean_path = self._normalize_path(path)
        if not clean_path or clean_path == "/":
            return True

        current = ""
        for part in [p for p in clean_path.split("/") if p]:
            current = f"{current}/{part}" if current else f"/{part}"
            if self._alist_path_exists(base_url, headers, current):
                continue
            if not self._create_alist_directory(base_url, headers, current):
                return False
        return True

    def _create_alist_directory(
        self,
        base_url: str,
        headers: Dict[str, str],
        path: str,
    ) -> bool:
        resp = self._post_alist(
            base_url,
            "/api/fs/mkdir",
            headers,
            json={"path": path},
        )
        if not resp:
            return False
        if resp.status_code != 200:
            return False
        try:
            result = resp.json()
            return result.get("code") == 200 or self._alist_path_exists(base_url, headers, path)
        except Exception:
            return False

    def _get_alist_conf(self) -> Optional[StorageConf]:
        try:
            return StorageHelper().get_storage("alist")
        except Exception as e:
            logger.debug("【OpenList Metadata Sync】读取 OpenList 存储配置失败: %s", e)
            return None

    def _get_alist_base_url(self, conf: StorageConf) -> str:
        if not conf or not getattr(conf, "config", None):
            return ""
        url = conf.config.get("url")
        if not url:
            return ""
        return UrlUtils.standardize_base_url(url)

    def _get_alist_auth_header(self, conf: StorageConf) -> Dict[str, str]:
        if not conf or not getattr(conf, "config", None):
            return {}
        base_url = self._get_alist_base_url(conf)
        if not base_url:
            return {}
        token = str(conf.config.get("token") or "").strip()
        if token:
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
            logger.warning("【OpenList Metadata Sync】OpenList 登录失败")
            return {}
        try:
            result = resp.json()
            if result.get("code") != 200:
                logger.warning("【OpenList Metadata Sync】OpenList 登录失败: %s", result.get("message"))
                return {}
            token = str(result.get("data", {}).get("token") or "").strip()
            if token:
                return {"Authorization": token}
        except Exception as e:
            logger.warning("【OpenList Metadata Sync】解析登录结果失败: %s", e)
        return {}

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

    def _wait_openlist_interval(self) -> bool:
        return self._wait_rate_limit(
            0.5, "_last_openlist_request_at"
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

    def _parse_path_rules(self) -> List[Dict[str, str]]:
        rules = []
        seen_sources = set()
        for line in self._path_rules.split("\n"):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=>" not in line:
                continue
            parts = line.split("=>", 1)
            source = parts[0].strip().rstrip("/")
            target = self._normalize_path(parts[1].strip())
            if not source or not target or source in seen_sources:
                continue
            seen_sources.add(source)
            rules.append({"source": source, "target": target})
        return rules

    def _parse_extensions(self) -> set:
        exts = set()
        for part in re.split(r"[,;\n\s]+", self._extensions):
            part = part.strip().lower()
            if not part:
                continue
            if not part.startswith("."):
                part = f".{part}"
            exts.add(part)
        return exts

    def _get_special_dir_names(self) -> set:
        names = set()
        for part in re.split(r"[,;\n]+", self._special_dir_names):
            part = part.strip().lower()
            if part:
                names.add(part)
        if not names:
            names = self.DEFAULT_SPECIAL_DIR_NAMES
        return names

    @staticmethod
    def _normalize_path(value: Any) -> str:
        path = str(value or "").strip()
        if not path:
            return ""
        parts = [part for part in path.split("/") if part]
        return "/" + "/".join(parts) if parts else "/"

    @staticmethod
    def _rel_path(full_path: str, root: str) -> str:
        full = OpenListMetadataSync._normalize_path(full_path)
        root = OpenListMetadataSync._normalize_path(root)
        if full == root:
            return ""
        if root == "/":
            return full.lstrip("/")
        if full.startswith(root + "/"):
            return full[len(root) + 1:]
        return full.lstrip("/")

    @staticmethod
    def _parse_alist_mtime(modified_str: str) -> Optional[float]:
        if not modified_str:
            return None
        try:
            dt = datetime.fromisoformat(modified_str.replace("Z", "+00:00"))
            return dt.timestamp()
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _count_common_files(local_files: Dict[str, Any], remote_files: Dict[str, Any]) -> int:
        return len(set(local_files.keys()) & set(remote_files.keys()))

    def _finish(self, message: str, stats: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
        stats["message"] = message
        self.save_data(self.STORE_RESULT_KEY, stats)
        logger.info("【OpenList Metadata Sync】%s", message)
        if self._notify:
            self.post_message(
                mtype=NotificationType.SiteMessage,
                title="【OpenList Metadata Sync】同步完成",
                text=message,
            )
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
