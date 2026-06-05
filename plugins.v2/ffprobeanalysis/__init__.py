from json import JSONDecodeError, loads
from pathlib import Path
from re import IGNORECASE, search as re_search
from secrets import token_urlsafe
from subprocess import TimeoutExpired, run
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, unquote

from fastapi import Body

from app import schemas
from app.core.cache import TTLCache
from app.core.config import settings
from app.core.event import Event, eventmanager
from app.helper.storage import StorageHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import StorageConf
from app.schemas import FileItem, TransferRenameBuildEventData
from app.schemas.types import ChainEventType
from app.utils.http import RequestUtils
from app.utils.url import UrlUtils


class FFprobeAnalysis(_PluginBase):
    """
    ffprobe 命名补充
    """

    plugin_name = "FFprobe分析"
    plugin_desc = "整理重命名时调用 ffprobe，补全命名模板中的 videoFormat、videoCodec、videoBit、audioCodec、fps、effect，支持 STRM 与 OpenList"
    plugin_icon = "ffmpeg.png"
    plugin_version = "0.2.2"
    plugin_author = "sucooer"
    author_url = "https://github.com/sucooer/MoviePilot-Plugins"
    plugin_config_prefix = "ffprobeanalysis_"
    plugin_order = 50
    auth_level = 1

    FFPROBE_TIMEOUT_SEC = 120

    _OVERWRITE_FILL_MISSING = "fill_missing"
    _OVERWRITE_ALWAYS = "always"

    _VIDEO_CODEC_MAP = {
        "h264": "H264",
        "avc": "H264",
        "hevc": "H265",
        "h265": "H265",
        "av1": "AV1",
        "vp9": "VP9",
        "vp8": "VP8",
        "mpeg2video": "MPEG2",
        "vc1": "VC1",
        "mpeg4": "MPEG4",
    }

    _AUDIO_CODEC_MAP = {
        "aac": "AAC",
        "eac3": "EAC3",
        "ac3": "AC3",
        "dts": "DTS",
        "truehd": "Dolby TrueHD",
        "flac": "FLAC",
        "opus": "OPUS",
        "mp3": "MP3",
        "vorbis": "Vorbis",
    }

    _HEIGHT_SNAP_TIERS: Tuple[Tuple[int, int], ...] = (
        (4320, 48),
        (2880, 48),
        (2160, 48),
        (1920, 40),
        (1800, 40),
        (1600, 40),
        (1536, 40),
        (1440, 40),
        (1366, 32),
        (1280, 32),
        (1200, 32),
        (1152, 32),
        (1080, 40),
        (1050, 32),
        (1024, 32),
        (960, 32),
        (900, 28),
        (864, 28),
        (854, 24),
        (800, 24),
        (768, 24),
        (720, 32),
        (704, 24),
        (640, 24),
        (600, 20),
        (576, 20),
        (540, 20),
        (528, 20),
        (512, 20),
        (506, 20),
        (480, 24),
        (468, 20),
        (456, 20),
        (432, 20),
        (408, 16),
        (400, 16),
        (360, 16),
        (320, 16),
        (288, 16),
        (272, 16),
        (240, 16),
        (228, 12),
        (180, 12),
        (168, 12),
        (144, 12),
        (120, 12),
    )

    _HEIGHT_FORMAT_BUCKETS: Tuple[Tuple[int, str], ...] = tuple(
        (height, f"{height}p") for height, _ in _HEIGHT_SNAP_TIERS
    )

    _WIDTH_FORMAT_BUCKETS: Tuple[Tuple[int, str], ...] = (
        (7680, "4320p"),
        (3840, "2160p"),
        (2560, "1440p"),
        (1920, "1080p"),
        (1280, "720p"),
        (854, "480p"),
        (640, "360p"),
    )

    _DV_CODEC_TAGS = frozenset({"dvh1", "dvhe", "dva1", "dvav"})

    _probe_cache = TTLCache(region="ffprobe_naming", maxsize=2048, ttl=3600)
    _alist_token_cache = TTLCache(region="ffprobe_naming_alist", maxsize=32, ttl=3600)

    def __init__(self) -> None:
        """
        初始化
        """
        super().__init__()
        self._enabled = False
        self._overwrite_mode = type(self)._OVERWRITE_FILL_MISSING

    def init_plugin(self, config: dict = None) -> None:
        """
        初始化插件

        :param config (dict): 插件配置字典
        """
        if not config:
            return
        cls = type(self)
        prev_enabled = self._enabled
        self._enabled = bool(config.get("enabled"))
        if prev_enabled and not self._enabled:
            cls._clear_probe_cache()
        mode = config.get("overwrite_mode") or cls._OVERWRITE_FILL_MISSING
        self._overwrite_mode = (
            mode
            if mode in (cls._OVERWRITE_FILL_MISSING, cls._OVERWRITE_ALWAYS)
            else cls._OVERWRITE_FILL_MISSING
        )

    def get_state(self) -> bool:
        """
        获取插件状态

        :return bool: 插件是否启用
        """
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """
        获取插件命令

        :return List: 插件命令列表
        """
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        """
        获取插件API

        :return List: 插件 API 列表
        """
        return [
            {
                "path": "/ffprobe_naming",
                "endpoint": self.api_ffprobe_naming,
                "auth": "bear",
                "methods": ["POST"],
                "summary": "ffprobe命名分析",
                "description": "分析媒体文件并返回命名模板字段，支持 local 和 alist 存储",
            },
            {
                "path": "/alist_list",
                "endpoint": self.api_alist_list,
                "auth": "bear",
                "methods": ["GET"],
                "summary": "AList 目录列表",
                "description": "列出 AList 指定目录下的文件和文件夹",
            },
            {
                "path": "/alist_nav",
                "endpoint": self.api_alist_nav,
                "auth": "bear",
                "methods": ["GET"],
                "summary": "导航到 AList 目录",
                "description": "保存导航状态并返回目录列表",
            },
            {
                "path": "/alist_rename",
                "endpoint": self.api_alist_rename,
                "auth": "bear",
                "methods": ["POST"],
                "summary": "重命名 AList 文件",
                "description": "重命名 AList 文件并刷新列表",
            },
            {
                "path": "/alist_rename_once",
                "endpoint": self.api_alist_rename_once,
                "allow_anonymous": True,
                "methods": ["GET", "POST"],
                "summary": "重命名 AList 文件",
                "description": "使用最近一次分析结果的一次性令牌重命名 AList 文件",
            },
            {
                "path": "/clear_analysis",
                "endpoint": self.api_clear_analysis,
                "auth": "bear",
                "methods": ["GET"],
                "summary": "清除分析结果",
                "description": "清除最近一次分析结果和重命名表单",
            },
            {
                "path": "/clear_analysis_once",
                "endpoint": self.api_clear_analysis_once,
                "allow_anonymous": True,
                "methods": ["GET", "POST"],
                "summary": "清除分析结果",
                "description": "使用最近一次分析结果的一次性令牌清除重命名表单",
            },
        ]

    def api_ffprobe_naming(self, path: str = "", storage: str = "local") -> schemas.Response:
        """
        API：分析文件并返回命名建议
        """
        if not path:
            return schemas.Response(success=False, message="请提供要分析的文件路径")
        path = str(path).strip()
        storage = str(storage).strip().lower()
        parts = [p for p in path.split("/") if p]
        path = "/" + "/".join(parts) if parts else "/"
        logger.info("【FFprobe分析】API 分析文件: %s (storage=%s)", path, storage)
        if storage == "alist":
            probe_target = self._resolve_alist_probe_target_by_path(path)
        else:
            probe_target = self._resolve_probe_target(path)
        if not probe_target:
            return schemas.Response(success=False, message=f"无法解析文件目标: {path}")
        probe_json = self._run_ffprobe(probe_target)
        if not probe_json:
            return schemas.Response(success=False, message=f"ffprobe 分析失败: {path}")
        fields = self._probe_to_rename_fields(probe_json)
        if not fields:
            return schemas.Response(success=False, message=f"未解析到可用媒体信息: {path}")

        # 生成建议文件名: {stem}.{videoFormat}.{videoCodec}.{videoBit}.{effect}.{ext}
        ext = Path(path).suffix
        stem = Path(path).stem
        tags = []
        for key in ["videoFormat", "videoCodec", "videoBit", "effect"]:
            if fields.get(key):
                tags.append(fields[key])
        suffix = ".".join(tags)
        suggested = f"{stem}.{suffix}{ext}" if suffix else Path(path).name

        fields_text = " | ".join(f"{k}={v}" for k, v in fields.items())
        self.save_data("last_analysis", {
            "path": path,
            "time": __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "fields": fields,
            "suggested": suggested,
            "nonce": token_urlsafe(16),
        })
        return schemas.Response(success=True, message=f"建议命名: {suggested}\n{fields_text}", data={
            "file_path": path, "suggested": suggested, "fields": fields,
        })

    def api_alist_list(self, path: str = "/") -> schemas.Response:
        """
        API：列出 AList 目录内容
        """
        path = str(path).strip() or "/"
        listing = self._fetch_alist_listing(path)
        if listing.get("error"):
            return schemas.Response(success=False, message=listing["error"])
        return schemas.Response(success=True, data={"path": path, "files": listing["files"]})

    def api_alist_nav(self, path: str = "/") -> schemas.Response:
        """
        API：导航到 AList 目录并保存状态
        """
        path = str(path).strip() or "/"
        parts = [p for p in path.split("/") if p]
        path = "/" + "/".join(parts)
        self.save_data("alist_current_path", path)
        return self.api_alist_list(path)

    def api_alist_rename(
        self,
        payload: Optional[dict] = Body(default=None),
        path: Optional[str] = "",
        new_name: Optional[str] = "",
    ) -> schemas.Response:
        """
        API：重命名 AList 文件
        """
        payload = payload if isinstance(payload, dict) else {}
        last_result = self.get_data("last_analysis") or {}
        path = str(path or payload.get("path") or last_result.get("path") or "").strip()
        new_name = str(new_name or payload.get("new_name") or payload.get("name") or "").strip()
        if new_name == "{{new_name}}":
            new_name = ""
        if not new_name and path == str(last_result.get("path") or "").strip():
            new_name = str(last_result.get("suggested") or "").strip()
        if not path or not new_name:
            logger.warning("【FFprobe分析】AList 重命名缺少参数: path=%s, new_name=%s", path, new_name)
            return schemas.Response(success=False, message="缺少路径或新文件名")
        parts = [p for p in path.split("/") if p]
        clean_path = "/" + "/".join(parts) if parts else "/"

        conf = self._get_alist_conf()
        if not conf:
            return schemas.Response(success=False, message="未找到 OpenList 存储配置")
        base_url = self._get_alist_base_url(conf)
        headers = self._get_alist_auth_header(conf)
        if not base_url or not headers:
            return schemas.Response(success=False, message="OpenList 认证失败")

        logger.info("【FFprobe分析】AList 重命名: %s -> %s", clean_path, new_name)
        resp = RequestUtils(headers=headers).post_res(
            UrlUtils.adapt_request_url(base_url, "/api/fs/rename"),
            json={"path": clean_path, "name": new_name},
        )
        if not resp:
            return schemas.Response(success=False, message="请求重命名失败")
        try:
            result = resp.json()
        except Exception as e:
            return schemas.Response(success=False, message=f"解析响应失败: {e}")
        if result.get("code") != 200:
            logger.warning("【FFprobe分析】AList 重命名失败: %s", result.get("message"))
            return schemas.Response(success=False, message=f"重命名失败: {result.get('message')}")

        dir_path = clean_path.rsplit("/", 1)[0] or "/"
        self.save_data("alist_current_path", dir_path) if self.get_data("alist_current_path") == clean_path else None
        self.save_data("last_analysis", None)
        return schemas.Response(success=True, message=f"已重命名为: {new_name}")

    def api_clear_analysis(self) -> schemas.Response:
        """
        API：清除分析结果
        """
        self.save_data("last_analysis", None)
        return schemas.Response(success=True, message="已取消重命名", data={})

    def api_alist_rename_once(
        self,
        payload: Optional[dict] = Body(default=None),
        path: Optional[str] = "",
        new_name: Optional[str] = "",
        nonce: Optional[str] = "",
    ) -> schemas.Response:
        """
        API：使用最近一次分析结果的一次性令牌重命名 AList 文件
        """
        payload = payload if isinstance(payload, dict) else {}
        path = str(path or payload.get("path") or "").strip()
        new_name = str(new_name or payload.get("new_name") or payload.get("name") or "").strip()
        nonce = str(nonce or payload.get("nonce") or "").strip()
        valid, message, clean_path = self._validate_last_analysis_nonce(path, nonce)
        if not valid:
            return schemas.Response(success=False, message=message)
        return self.api_alist_rename(path=clean_path, new_name=new_name)

    def api_clear_analysis_once(
        self,
        payload: Optional[dict] = Body(default=None),
        path: Optional[str] = "",
        nonce: Optional[str] = "",
    ) -> schemas.Response:
        """
        API：使用最近一次分析结果的一次性令牌清除分析结果
        """
        payload = payload if isinstance(payload, dict) else {}
        path = str(path or payload.get("path") or "").strip()
        nonce = str(nonce or payload.get("nonce") or "").strip()
        valid, message, _ = self._validate_last_analysis_nonce(path, nonce)
        if not valid:
            return schemas.Response(success=False, message=message)
        return self.api_clear_analysis()

    def _validate_last_analysis_nonce(self, path: str, nonce: str) -> Tuple[bool, str, str]:
        """
        校验最近一次分析结果的一次性页面令牌
        """
        last_result = self.get_data("last_analysis") or {}
        expected_path = str(last_result.get("path") or "").strip()
        expected_nonce = str(last_result.get("nonce") or "").strip()
        path = str(path or "").strip()
        nonce = str(nonce or "").strip()
        if not expected_path or not expected_nonce:
            return False, "没有可用的重命名任务", ""
        if not path or not nonce:
            return False, "缺少重命名令牌", ""
        parts = [p for p in path.split("/") if p]
        clean_path = "/" + "/".join(parts) if parts else "/"
        if clean_path != expected_path or nonce != expected_nonce:
            return False, "重命名令牌无效或已过期", ""
        return True, "", clean_path

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面

        :return Tuple: (页面配置列表, 表单默认值字典)
        """
        cls = type(self)
        overwrite_items = [
            {"title": "仅补全缺失或空值", "value": cls._OVERWRITE_FILL_MISSING},
            {"title": "始终用 ffprobe 覆盖上述键", "value": cls._OVERWRITE_ALWAYS},
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
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 8},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "overwrite_mode",
                                            "label": "写入策略",
                                            "items": overwrite_items,
                                            "hint": (
                                                "针对 videoFormat、videoCodec、videoBit、audioCodec、fps、effect："
                                                "仅补全＝缺或空才写入；始终覆盖＝以 ffprobe 为准覆盖"
                                            ),
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
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "density": "compact",
                                        },
                                        "content": [
                                            {
                                                "component": "div",
                                                "props": {
                                                    "class": "text-body-2",
                                                },
                                                "text": (
                                                    "说明：支持本地文件、STRM，以及 OpenList(AList) 文件管理中的媒体文件；"
                                                    "OpenList 会自动尝试获取 raw_url/下载地址后再调用 ffprobe"
                                                ),
                                            },
                                        ],
                                    },
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "density": "compact",
                                            "class": "mt-2",
                                        },
                                        "content": [
                                            {
                                                "component": "div",
                                                "props": {
                                                    "class": "text-subtitle-2 mb-2",
                                                },
                                                "text": (
                                                    "可写入重命名模板的占位符"
                                                    "（需在系统「重命名格式」中自行加入对应变量）"
                                                ),
                                            },
                                            {
                                                "component": "div",
                                                "props": {
                                                    "class": "text-body-2",
                                                },
                                                "text": (
                                                    "{{videoFormat}} — 分辨率档（如 2160p、1080p，由视频高度推断）"
                                                ),
                                            },
                                            {
                                                "component": "div",
                                                "props": {
                                                    "class": "text-body-2 mt-1",
                                                },
                                                "text": "{{videoCodec}} — 视频编码（如 H264、H265）",
                                            },
                                            {
                                                "component": "div",
                                                "props": {
                                                    "class": "text-body-2 mt-1",
                                                },
                                                "text": "{{videoBit}} — 视频位深（如 8bit、10bit）",
                                            },
                                            {
                                                "component": "div",
                                                "props": {
                                                    "class": "text-body-2 mt-1",
                                                },
                                                "text": "{{audioCodec}} — 音频编码与声道（如 EAC3 5.1、Dolby TrueHD Dolby Atmos 7.1、AAC 2.0）",
                                            },
                                            {
                                                "component": "div",
                                                "props": {
                                                    "class": "text-body-2 mt-1",
                                                },
                                                "text": "{{fps}} — 帧率（无小数，四舍五入整数，如 24、30）",
                                            },
                                            {
                                                "component": "div",
                                                "props": {
                                                    "class": "text-body-2 mt-1",
                                                },
                                                "text": (
                                                    "{{effect}} — 动态范围/特效标签（如 DoVi、HDR10、HDR10+、HLG、SDR，"
                                                    "与系统模板变量同名）"
                                                ),
                                            },
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "overwrite_mode": cls._OVERWRITE_FILL_MISSING,
        }

    def get_page(self) -> Optional[List[dict]]:
        """
        获取插件页面：AList 文件浏览器
        """
        current_path = str(self.get_data("alist_current_path") or "/")
        conf = self._get_alist_conf()

        if not conf:
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
                                                "type": "warning",
                                                "variant": "tonal",
                                            },
                                            "text": "未检测到 OpenList(AList) 存储配置，请先在系统设置中添加 AList 存储。"
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]

        listing = self._fetch_alist_listing(current_path)
        files = listing.get("files", [])
        load_error = listing.get("error")
        last_result = self.get_data("last_analysis") or {}
        clear_api = (
            f"plugin/FFprobeAnalysis/clear_analysis_once?"
            f"path={quote(str(last_result.get('path') or ''))}"
            f"&nonce={quote(str(last_result.get('nonce') or ''))}"
        )
        rename_api = (
            f"plugin/FFprobeAnalysis/alist_rename_once?"
            f"path={quote(str(last_result.get('path') or ''))}"
            f"&nonce={quote(str(last_result.get('nonce') or ''))}"
        )

        dirs = [f for f in files if f.get("is_dir")]
        file_items = [f for f in files if not f.get("is_dir")]

        display_path = current_path if current_path != "/" else "根目录"
        parent_path = current_path.rstrip("/").rsplit("/", 1)[0] or "/" if current_path != "/" else "/"
        if parent_path == "":
            parent_path = "/"

        rows = []

        # 向上按钮
        if current_path != "/":
            up_api = f"plugin/FFprobeAnalysis/alist_nav?path={quote(parent_path)}"
            rows.append({
                "component": "tr",
                "content": [
                    {
                        "component": "td",
                        "props": {"colspan": 4},
                        "content": [
                            {
                                "component": "VBtn",
                                "props": {
                                    "size": "small",
                                    "variant": "text",
                                    "color": "primary",
                                    "prepend-icon": "mdi-arrow-up-bold",
                                },
                                "text": "..",
                                "events": {
                                    "click": {"api": up_api, "method": "get"}
                                }
                            }
                        ]
                    }
                ]
            })

        # 目录行
        for d in dirs:
            name = d.get("name", "")
            dir_path = f"{current_path.rstrip('/')}/{name}"
            nav_api = f"plugin/FFprobeAnalysis/alist_nav?path={quote(dir_path)}"
            rows.append({
                "component": "tr",
                "content": [
                    {
                        "component": "td",
                        "content": [
                            {
                                "component": "VBtn",
                                "props": {
                                    "size": "small",
                                    "variant": "text",
                                    "color": "primary",
                                    "prepend-icon": "mdi-folder",
                                },
                                "text": name,
                                "events": {
                                    "click": {"api": nav_api, "method": "get"}
                                }
                            }
                        ]
                    },
                    {"component": "td", "text": "-"},
                    {"component": "td", "text": "目录"},
                    {"component": "td", "text": ""},
                ]
            })

        # 文件行
        for f in file_items:
            name = f.get("name", "")
            size = self._format_size(f.get("size", 0))
            file_path = f"{current_path.rstrip('/')}/{name}"
            analyze_api = f"plugin/FFprobeAnalysis/ffprobe_naming?path={quote(file_path)}&storage=alist"
            rows.append({
                "component": "tr",
                "content": [
                    {"component": "td", "text": name},
                    {"component": "td", "text": size},
                    {"component": "td", "text": "文件"},
                    {
                        "component": "td",
                        "content": [
                            {
                                "component": "div",
                                "props": {"class": "d-flex ga-2"},
                                "content": [
                                    {
                                        "component": "VBtn",
                                        "props": {
                                            "size": "small",
                                            "color": "primary",
                                            "variant": "tonal",
                                            "prepend-icon": "mdi-rename",
                                        },
                                        "text": "重命名",
                                        "events": {
                                            "click": {"api": analyze_api, "method": "post"}
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                ]
            })

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
                                            "type": load_error and "warning" or "info",
                                            "variant": "tonal",
                                        },
                                        "text": load_error or f"当前目录: {display_path}，共 {len(dirs)} 个目录，{len(file_items)} 个文件"
                                    }
                                ]
                            }
                        ]
                    },
                    # 重命名表单
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VCard",
                                        "props": {"flat": True, "border": True, "color": "primary", "variant": "tonal"},
                                        "content": [
                                            {
                                                "component": "VCardTitle",
                                                "text": f"重命名: {last_result['path'].rsplit('/', 1)[-1]}"
                                            },
                                            {
                                                "component": "VCardText",
                                                "content": [
                                                    {
                                                        "component": "div",
                                                        "content": [
                                                            {"component": "div", "props": {"class": "text-caption text-medium-emphasis mb-1"}, "text": "原文件名"},
                                                            {"component": "div", "props": {"class": "text-body-2"}, "text": last_result["path"].rsplit("/", 1)[-1]},
                                                        ]
                                                    },
                                                    {
                                                        "component": "div",
                                                        "props": {"class": "mt-3"},
                                                        "content": [
                                                            {"component": "div", "props": {"class": "text-caption text-medium-emphasis mb-1"}, "text": "建议文件名"},
                                                            {
                                                                "component": "div",
                                                                "props": {
                                                                    "class": "text-body-2 pa-3",
                                                                    "style": "background: rgba(145,85,253,.12); border-radius: 8px; border: 1px solid rgba(145,85,253,.35); color: rgb(145,85,253);",
                                                                },
                                                                "text": last_result.get("suggested", ""),
                                                            },
                                                        ]
                                                    },
                                                    {
                                                        "component": "div",
                                                        "props": {"class": "d-flex ga-2 mt-3"},
                                                        "content": [
                                                            {
                                                                "component": "VBtn",
                                                                "props": {
                                                                    "color": "primary",
                                                                    "prepend-icon": "mdi-check",
                                                                },
                                                                "text": "确认重命名",
                                                                "events": {
                                                                    "click": {
                                                                        "api": rename_api,
                                                                        "method": "post",
                                                                    }
                                                                }
                                                            },
                                                            {
                                                                "component": "VBtn",
                                                                "props": {
                                                                    "color": "grey",
                                                                    "variant": "text",
                                                                    "prepend-icon": "mdi-close",
                                                                },
                                                                "text": "取消",
                                                                "events": {
                                                                    "click": {
                                                                        "api": clear_api,
                                                                        "method": "get"
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
                    } if last_result.get("path") else None,
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
                                                "text": "文件列表"
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
                                                                            {"component": "th", "text": "名称"},
                                                                            {"component": "th", "text": "大小"},
                                                                            {"component": "th", "text": "类型"},
                                                                            {"component": "th", "text": "操作"},
                                                                        ]
                                                                    }
                                                                ]
                                                            },
                                                            {
                                                                "component": "tbody",
                                                                "content": rows or [
                                                                    {
                                                                        "component": "tr",
                                                                        "content": [
                                                                            {
                                                                                "component": "td",
                                                                                "props": {"colspan": 4},
                                                                                "text": "目录为空"
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

    @classmethod
    def _format_size(cls, size_bytes: Any) -> str:
        try:
            size = int(size_bytes)
        except (TypeError, ValueError):
            return "-"
        if size <= 0:
            return "-"
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size < 1024:
                return f"{size:.1f} {unit}" if unit != "B" else f"{size} B"
            size /= 1024
        return f"{size:.1f} PB"

    def _fetch_alist_listing(self, path: str) -> dict:
        conf = self._get_alist_conf()
        if not conf:
            return {"files": [], "error": "未找到 OpenList 存储配置"}
        base_url = self._get_alist_base_url(conf)
        headers = self._get_alist_auth_header(conf)
        if not base_url or not headers:
            return {"files": [], "error": "OpenList 认证失败"}
        parts = [p for p in path.split("/") if p]
        clean_path = "/" + "/".join(parts) if parts else "/"
        resp = RequestUtils(headers=headers).post_res(
            UrlUtils.adapt_request_url(base_url, "/api/fs/list"),
            json={"path": clean_path, "password": "", "page": 1, "per_page": 0, "refresh": False},
        )
        if not resp or resp.status_code != 200:
            return {"files": [], "error": f"请求目录失败: {path}"}
        try:
            result = resp.json()
        except Exception as e:
            return {"files": [], "error": f"解析响应失败: {e}"}
        if result.get("code") != 200:
            return {"files": [], "error": f"AList 返回错误: {result.get('message')}"}
        files = []
        for item in (result.get("data", {}).get("content") or []):
            is_dir = item.get("is_dir", False) or item.get("type") == "folder" or item.get("type") == 1
            files.append({
                "name": item.get("name"),
                "size": item.get("size"),
                "is_dir": is_dir,
                "modified": item.get("modified"),
            })
        return {"files": files, "error": ""}

    def stop_service(self) -> None:
        """
        停止插件服务
        """
        type(self)._clear_probe_cache()

    @classmethod
    def _clear_probe_cache(cls) -> None:
        """
        清空 ffprobe 结果缓存（类级共享，停止或关闭插件时释放）
        """
        try:
            cls._probe_cache.clear()
        except Exception as e:
            logger.debug("【FFprobe分析】清理探测缓存失败 %s", e)

    @classmethod
    def _parse_frame_rate(cls, rate: Optional[str]) -> Optional[str]:
        """
        将 ffprobe 的帧率字符串转为无小数点的整型展示字符串（四舍五入）

        :param rate: 如 24000/1001 或 30
        :return: 如 24（由 23.976… 舍入）或 30
        """
        if not rate or rate in ("0/0", "N/A"):
            return None
        if "/" in rate:
            parts = rate.split("/", 1)
            try:
                num = int(parts[0].strip())
                den = int(parts[1].strip())
            except (ValueError, IndexError):
                return None
            if den == 0:
                return None
            value = num / den
        else:
            try:
                value = float(rate)
            except ValueError:
                return None
        if value <= 0:
            return None
        return str(int(round(value)))

    @classmethod
    def _snap_height_to_standard(cls, height: int) -> int:
        """
        将因 mod16 裁剪、轻微缩放导致的高度吸附到常见标准值

        :param height: ffprobe 报告的帧高度
        :return: 吸附后的高度（未命中任一容差则原样返回）
        """
        for target, tolerance in cls._HEIGHT_SNAP_TIERS:
            if abs(height - target) <= tolerance:
                return target
        return height

    @classmethod
    def _height_to_video_format(
        cls, height: Optional[int], width: Optional[int] = None
    ) -> Optional[str]:
        """
        根据视频宽/高生成分辨率标签：

        先按 width 优先匹配标准档（兼容 21:9、2.39:1 等信箱比宽屏片源），
        否则回退到按 height 吸附后降序分档，未命中则用「{height}p」
        """
        w_int: Optional[int] = None
        if width is not None:
            try:
                w_int = int(width)
            except (TypeError, ValueError):
                w_int = None
            if w_int is not None and w_int <= 0:
                w_int = None
        if w_int is not None:
            for min_w, label in cls._WIDTH_FORMAT_BUCKETS:
                if w_int >= min_w:
                    return label
        if height is None:
            return None
        try:
            h = int(height)
        except (TypeError, ValueError):
            return None
        if h <= 0:
            return None
        h = cls._snap_height_to_standard(h)
        for min_h, label in cls._HEIGHT_FORMAT_BUCKETS:
            if h >= min_h:
                return label
        return f"{h}p"

    @classmethod
    def _map_video_codec(cls, codec_name: Optional[str]) -> Optional[str]:
        if not codec_name:
            return None
        key = codec_name.lower().strip()
        return cls._VIDEO_CODEC_MAP.get(key, codec_name.upper())

    @classmethod
    def _map_audio_codec(cls, codec_name: Optional[str]) -> Optional[str]:
        if not codec_name:
            return None
        key = codec_name.lower().strip()
        return cls._AUDIO_CODEC_MAP.get(key, codec_name.upper())

    @classmethod
    def _normalize_audio_channel_tag(
        cls,
        channel_layout: Optional[str],
        channels: Optional[Any],
    ) -> Optional[str]:
        """
        从 ffprobe 的声道布局或声道数生成短标签（如 7.1、5.1、2.0）

        与编码名以空格连接，如 Dolby TrueHD 7.1、EAC3 5.1（避免 EAC3 与 5.1 连成 EAC35.1）
        """
        layout_raw = (channel_layout or "").strip()
        if layout_raw:
            layout = layout_raw.split("(", 1)[0].strip()
            low = layout.lower()
            aliases = {
                "mono": "1.0",
                "stereo": "2.0",
                "quad": "4.0",
            }
            if low in aliases:
                return aliases[low]
            cleaned = layout.replace(" ", "")
            if cleaned and all(c.isdigit() or c == "." for c in cleaned):
                return cleaned
        try:
            n = int(channels) if channels is not None else 0
        except (TypeError, ValueError):
            n = 0
        if n <= 0:
            return None
        count_map = {
            1: "1.0",
            2: "2.0",
            3: "2.1",
            4: "4.0",
            5: "5.0",
            6: "5.1",
            7: "6.1",
            8: "7.1",
            10: "7.1.2",
            12: "7.1.4",
        }
        return count_map.get(n)

    @classmethod
    def _audio_stream_has_dolby_atmos_ffprobe(cls, audio_s: Dict[str, Any]) -> bool:
        """
        根据 ffprobe 音频流的 profile、codec_tag_string、tags 等判断是否含 Dolby Atmos

        Dolby TrueHD / EAC3 等与 Atmos 为不同概念；仅在元数据标明 Atmos 时为 True

        除英文「Atmos」外，识别常见中文轨标题「杜比全景声」等（无 profile 的旧版 ffprobe）
        """
        parts: List[str] = []
        prof = audio_s.get("profile")
        if prof:
            parts.append(str(prof))
        cln = audio_s.get("codec_long_name")
        if cln:
            parts.append(str(cln))
        cts = audio_s.get("codec_tag_string")
        if cts:
            parts.append(str(cts))
        tags = audio_s.get("tags")
        if isinstance(tags, dict):
            for v in tags.values():
                if v:
                    parts.append(str(v))
        joined = " ".join(parts)
        if "atmos" in joined.lower():
            return True
        return "全景声" in joined

    @classmethod
    def _format_audio_codec_label(
        cls,
        codec_name: Optional[str],
        channel_layout: Optional[str],
        channels: Optional[Any],
        *,
        dolby_atmos: bool = False,
    ) -> Optional[str]:
        """
        编码名、可选 Dolby Atmos、声道标签（均空格分隔），用于 rename_dict audioCodec

        含 Atmos 时顺序为「基带编码 Dolby Atmos 声道」，如 Dolby TrueHD Dolby Atmos 7.1
        """
        ac = cls._map_audio_codec(codec_name)
        if not ac:
            return None
        tag = cls._normalize_audio_channel_tag(channel_layout, channels)
        parts: List[str] = [ac]
        if dolby_atmos:
            parts.append("Dolby Atmos")
        if tag:
            parts.append(tag)
        return " ".join(parts)

    @classmethod
    def _video_stream_hdr_flags(cls, video_s: Dict[str, Any]) -> Tuple[bool, bool]:
        """
        从视频流 side_data 与 codec_tag 判断是否含 Dolby Vision / HDR10+ 元数据

        :param video_s: ffprobe 单路视频流 dict
        :return: (has_dovi, has_hdr10plus)
        """
        has_dovi = False
        has_hdr10plus = False
        tag = (video_s.get("codec_tag_string") or "").strip().lower()
        if tag in cls._DV_CODEC_TAGS:
            has_dovi = True
        side_list = video_s.get("side_data_list")
        if not isinstance(side_list, list):
            return has_dovi, has_hdr10plus
        for item in side_list:
            if not isinstance(item, dict):
                continue
            sdt = (item.get("side_data_type") or "").lower()
            if "dovi" in sdt or "dolby vision" in sdt:
                has_dovi = True
            if "smpte2094-40" in sdt or "2094-40" in sdt:
                has_hdr10plus = True
            if "hdr10+" in sdt and "dynamic" in sdt:
                has_hdr10plus = True
        return has_dovi, has_hdr10plus

    @classmethod
    def _infer_effect_from_video_stream(cls, video_s: Dict[str, Any]) -> Optional[str]:
        """
        根据 ffprobe 色彩与 side_data 推断与 MoviePilot 模板变量 effect 对应的标签

        输出与常见资源命名接近的短标签，多个以空格连接（如 DoVi、HDR10+）

        :param video_s: ffprobe 单路视频流 dict
        :return: 供 rename_dict["effect"] 使用的字符串，无法判断则 None
        """
        has_dovi, has_hdr10plus = cls._video_stream_hdr_flags(video_s)
        ct = (video_s.get("color_transfer") or "").lower().strip()
        cp = (video_s.get("color_primaries") or "").lower().strip()
        cs = (video_s.get("color_space") or "").lower().strip()

        tokens: List[str] = []
        if has_dovi:
            tokens.append("DoVi")
        if has_hdr10plus:
            tokens.append("HDR10+")
        if not has_dovi and not has_hdr10plus:
            if ct == "smpte2084":
                tokens.append("HDR10")
            elif "arib-std-b67" in ct:
                tokens.append("HLG")

        if not tokens:
            primaries_ok = not cp or cp == "bt709"
            transfer_sdr = ct == "bt709" or (not ct and cs == "bt709")
            if transfer_sdr and primaries_ok:
                tokens.append("SDR")

        if not tokens:
            return None
        return " ".join(tokens)

    @classmethod
    def _pick_video_audio_streams(
        cls, streams: List[Dict[str, Any]]
    ) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """
        选取首路视频；音轨优先 disposition.default，与播放器默认轨一致，避免误用非主音轨
        """
        video_s: Optional[Dict[str, Any]] = None
        audio_s: Optional[Dict[str, Any]] = None
        audios: List[Dict[str, Any]] = []
        for s in streams:
            if not isinstance(s, dict):
                continue
            ct = s.get("codec_type")
            if ct == "video" and video_s is None:
                video_s = s
            elif ct == "audio":
                audios.append(s)
        if audios:
            for s in audios:
                disp = s.get("disposition")
                if isinstance(disp, dict) and disp.get("default") == 1:
                    audio_s = s
                    break
            if audio_s is None:
                audio_s = audios[0]
        return video_s, audio_s

    @classmethod
    def _extract_video_bit_depth(cls, video_s: Dict[str, Any]) -> Optional[str]:
        """
        从 ffprobe 视频流提取位深，如 8bit、10bit

        :param video_s: ffprobe 视频流字典
        :return: 位深字符串或 None
        """
        bps = str(video_s.get("bits_per_raw_sample") or "").strip()
        if bps and bps.isdigit():
            return f"{bps}bit"
        pix = str(video_s.get("pix_fmt") or "").strip().lower()
        if pix:
            m = re_search(r"(\d+)(le|be)", pix)
            if m:
                n = int(m.group(1))
                if n > 0:
                    return f"{n}bit"
        prof = str(video_s.get("profile") or "").strip()
        if prof:
            m = re_search(r"(?:main|high)\s*(\d+)", prof, IGNORECASE)
            if m:
                return f"{m.group(1)}bit"
        if pix:
            return "8bit"
        return None

    @classmethod
    def _probe_to_rename_fields(cls, probe_json: Dict[str, Any]) -> Dict[str, str]:
        """
        从 ffprobe JSON 提取写入 rename_dict 的命名模板字段
        """
        out: Dict[str, str] = {}
        streams = probe_json.get("streams")
        if not isinstance(streams, list):
            return out
        video_s, audio_s = cls._pick_video_audio_streams(streams)
        if video_s:
            height = video_s.get("height")
            try:
                h_int = int(height) if height is not None else None
            except (TypeError, ValueError):
                h_int = None
            width = video_s.get("width")
            try:
                w_int = int(width) if width is not None else None
            except (TypeError, ValueError):
                w_int = None
            vf = cls._height_to_video_format(h_int, w_int)
            if vf:
                out["videoFormat"] = vf
            vc = cls._map_video_codec(video_s.get("codec_name"))
            if vc:
                out["videoCodec"] = vc
            vb = cls._extract_video_bit_depth(video_s)
            if vb:
                out["videoBit"] = vb
            fps = cls._parse_frame_rate(
                video_s.get("avg_frame_rate")
            ) or cls._parse_frame_rate(video_s.get("r_frame_rate"))
            if fps:
                out["fps"] = fps
            eff = cls._infer_effect_from_video_stream(video_s)
            if eff:
                out["effect"] = eff
        if audio_s:
            atmos = cls._audio_stream_has_dolby_atmos_ffprobe(audio_s)
            ac = cls._format_audio_codec_label(
                audio_s.get("codec_name"),
                audio_s.get("channel_layout"),
                audio_s.get("channels"),
                dolby_atmos=atmos,
            )
            if ac:
                out["audioCodec"] = ac
        return out

    @classmethod
    def _normalize_strm_target(cls, raw: str) -> str:
        """
        规范化 STRM 首行内容，便于 ffprobe 作为 -i 参数使用

        :param raw: 行内原始文本（已去掉首尾空白）
        :return: 规范化后的地址或路径，无效则空字符串
        """
        line = raw.strip()
        if not line:
            return ""
        if len(line) >= 2 and line[0] == line[-1] and line[0] in "\"'":
            line = line[1:-1].strip()
        if "%" in line:
            try:
                line = unquote(line)
            except Exception:
                pass
        return line.strip()

    @classmethod
    def _resolve_probe_target(cls, source_path: str) -> Optional[str]:
        """
        普通文件直接返回路径；STRM 读取首条有效行并规范化后作为真实地址
        """
        p = Path(source_path)
        if p.suffix.lower() != ".strm":
            return source_path.strip()
        try:
            text = p.read_text(encoding="utf-8-sig", errors="replace")
        except OSError as e:
            logger.warning("【FFprobe分析】读取 STRM 失败 %s: %s", source_path, e)
            return None
        for line in text.splitlines():
            line = line.strip()
            if not line or line.lstrip().startswith("#"):
                continue
            normalized = cls._normalize_strm_target(line)
            if normalized:
                return normalized
        logger.warning("【FFprobe分析】STRM 内容为空 %s", source_path)
        return None

    @classmethod
    def _get_alist_conf(cls) -> Optional[StorageConf]:
        """
        获取 OpenList(AList) 存储配置
        """
        try:
            return StorageHelper().get_storage("alist")
        except Exception as e:
            logger.debug("【FFprobe分析】读取 OpenList 存储配置失败: %s", e)
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
            logger.warning("【FFprobe分析】OpenList 登录失败，无法获取临时 token")
            return {}
        try:
            result = resp.json()
            if result.get("code") != 200:
                logger.warning("【FFprobe分析】OpenList 登录返回失败：%s", result.get("message"))
                return {}
            token = str((result.get("data") or {}).get("token") or "").strip()
            if token:
                cls._alist_token_cache.set(base_url, token)
                return {"Authorization": token}
        except Exception as e:
            logger.warning("【FFprobe分析】解析 OpenList 登录结果失败: %s", e)
        return {}

    @classmethod
    def _resolve_alist_probe_target(cls, source_item: FileItem) -> Optional[str]:
        """
        通过 OpenList(AList) API 获取文件直链或下载地址，用于 ffprobe 探测
        """
        return cls._resolve_alist_probe_target_by_path(
            str(getattr(source_item, "path", ""))
        )

    @classmethod
    def _resolve_alist_probe_target_by_path(cls, alist_path: str) -> Optional[str]:
        """
        根据 AList 路径直接获取直链/下载地址
        """
        conf = cls._get_alist_conf()
        if not conf or not getattr(conf, "config", None):
            logger.debug("【FFprobe分析】未找到 OpenList 存储配置")
            return None

        base_url = cls._get_alist_base_url(conf)
        headers = cls._get_alist_auth_header(conf)
        if not base_url or not headers:
            logger.debug("【FFprobe分析】OpenList 基础配置不完整，无法获取直链")
            return None

        resp = RequestUtils(headers=headers).post_res(
            UrlUtils.adapt_request_url(base_url, "/api/fs/get"),
            json={
                "path": alist_path,
                "password": "",
                "page": 1,
                "per_page": 0,
                "refresh": False,
            },
        )
        if not resp or resp.status_code != 200:
            logger.warning("【FFprobe分析】请求 OpenList 文件信息失败：%s", alist_path)
            return None

        try:
            result = resp.json()
        except Exception as e:
            logger.warning("【FFprobe分析】解析 OpenList 文件信息失败: %s", e)
            return None

        if result.get("code") != 200:
            logger.warning("【FFprobe分析】OpenList 文件信息返回失败：%s", result.get("message"))
            return None

        data = result.get("data") or {}
        raw_url = str(data.get("raw_url") or "").strip()
        if raw_url:
            return raw_url

        download_url = UrlUtils.adapt_request_url(base_url, f"/d{alist_path}")
        sign = str(data.get("sign") or "").strip()
        if sign:
            download_url = f"{download_url}?sign={sign}"
        return download_url

    @classmethod
    def _should_apply_key(
        cls, overwrite_mode: str, key: str, rename_dict: Dict[str, Any], new_val: str
    ) -> bool:
        if not new_val:
            return False
        if overwrite_mode == cls._OVERWRITE_ALWAYS:
            return True
        cur = rename_dict.get(key)
        if cur is None:
            return True
        if isinstance(cur, str):
            cur_stripped = cur.strip()
            if not cur_stripped:
                return True
            if key == "audioCodec" and not re_search(
                r"(?:^|\s)\d+\.\d+$", cur_stripped
            ):
                return True
        return False

    @classmethod
    def _run_ffprobe(cls, probe_target: str) -> Optional[Dict[str, Any]]:
        cmd = [
            "ffprobe",
            "-v",
            "error",
            "-print_format",
            "json",
            "-show_streams",
            "-show_format",
            "-analyzeduration",
            str(20 * 1024 * 1024),
            "-probesize",
            str(20 * 1024 * 1024),
            "-i",
            probe_target,
        ]
        try:
            proc = run(
                cmd,
                capture_output=True,
                text=True,
                timeout=cls.FFPROBE_TIMEOUT_SEC,
            )
        except TimeoutExpired:
            logger.warning(
                "【FFprobe分析】ffprobe 超时(%ss) target=%s",
                cls.FFPROBE_TIMEOUT_SEC,
                probe_target,
            )
            return None
        except OSError as e:
            logger.warning("【FFprobe分析】无法执行 ffprobe: %s", e)
            return None
        if proc.returncode != 0:
            err = (proc.stderr or "").strip() or proc.stdout
            logger.debug(
                "【FFprobe分析】ffprobe 失败 rc=%s target=%s err=%s",
                proc.returncode,
                probe_target,
                err[:500] if err else "",
            )
            return None
        try:
            return loads(proc.stdout)
        except JSONDecodeError as e:
            logger.warning("【FFprobe分析】ffprobe JSON 解析失败: %s", e)
            return None

    @eventmanager.register(ChainEventType.TransferRenameBuild)
    def on_transfer_rename_build(self, event: Event) -> None:
        """
        处理 TransferRenameBuild 链式事件，在主程序首次渲染前把 ffprobe
        解析到的字段写入 rename_dict

        与渲染后的 TransferRename 字符串改写类插件天然分层、互不冲突

        :param event (Event): 链式事件对象，包含 rename_dict 与文件路径信息
        """
        if not self._enabled:
            return
        data = event.event_data
        if not isinstance(data, TransferRenameBuildEventData):
            return
        source_path: Optional[str] = data.source_path
        source_item: Optional[FileItem] = data.source_item
        if not source_path or not str(source_path).strip():
            logger.debug("【FFprobe分析】source_path 为空，跳过本次重命名补全")
            return
        if not source_item:
            logger.debug("【FFprobe分析】source_item 为空，跳过本次重命名补全")
            return
        source_path = str(source_path).strip()
        rename_dict = data.rename_dict
        if not isinstance(rename_dict, dict):
            return

        if source_item.type != "file":
            logger.debug("【FFprobe分析】圆盘整理跳过本次重命名补全")
            return

        if Path(source_path).suffix.lower() not in settings.RMT_MEDIAEXT:
            logger.debug("【FFprobe分析】文件后缀不是媒体文件，跳过本次重命名补全")
            return

        cls = type(self)
        if source_item.storage == "local":
            probe_target = cls._resolve_probe_target(source_path)
        elif source_item.storage == "alist":
            probe_target = cls._resolve_alist_probe_target(source_item)
        else:
            logger.debug(
                "【FFprobe分析】暂不支持的存储类型 %s，跳过本次重命名补全",
                source_item.storage,
            )
            return
        if not probe_target:
            return

        probe_json = self._probe_cache.get(probe_target)
        if probe_json is None:
            probe_json = cls._run_ffprobe(probe_target)
            if probe_json is None:
                return
            self._probe_cache.set(probe_target, probe_json)

        fields = cls._probe_to_rename_fields(probe_json)
        if not fields:
            logger.debug(
                "【FFprobe分析】未解析到可用媒体信息 target=%s", probe_target
            )
            return

        for key, val in fields.items():
            if cls._should_apply_key(self._overwrite_mode, key, rename_dict, val):
                rename_dict[key] = val
