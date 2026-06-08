# MoviePilot-Plugins

`MoviePilot-Plugins` 是 MoviePilot 的插件源码与插件索引仓库。

当前仓库包含以下 V2 插件：

- `S3Backup`
  路径：[plugins.v2/s3backup](/root/MoviePilot-Plugins/plugins.v2/s3backup/__init__.py)
  功能：定时通过 S3 备份数据库和配置文件，并支持从 S3 恢复
- `MediaCoverGen`
  路径：[plugins.v2/mediacovergen](/root/MoviePilot-Plugins/plugins.v2/mediacovergen/__init__.py)
  功能：生成 Emby/Jellyfin 媒体库动态或静态封面
- `FFprobeAnalysis`
  路径：[plugins.v2/ffprobeanalysis](/root/MoviePilot-Plugins/plugins.v2/ffprobeanalysis/__init__.py)
  功能：通过 ffprobe 分析媒体信息，补全重命名字段，并支持 AList 文件重命名
- `OpenListStrmRefresh`
  路径：[plugins.v2/openliststrmrefresh](/root/MoviePilot-Plugins/plugins.v2/openliststrmrefresh/__init__.py)
  功能：定时访问 OpenList STRM 驱动目录，并支持检测源目录变化后刷新对应 STRM 目录
- `AlistMonitor`
  路径：[plugins.v2/alistmonitor](/root/MoviePilot-Plugins/plugins.v2/alistmonitor/__init__.py)
  功能：监控 OpenList 目录变化，提交新增文件给 MoviePilot 做网盘内远程整理

当前仓库结构：

```text
MoviePilot-Plugins/
├── plugins.v2/
│   ├── ffprobeanalysis/
│   ├── alistmonitor/
│   ├── mediacovergen/
│   ├── openliststrmrefresh/
│   └── s3backup/
├── icons/
├── fonts/
├── images/
├── docs/
└── package.v2.json
```

插件索引：

- [package.v2.json](/root/MoviePilot-Plugins/package.v2.json)

开发与发布时请优先遵循：

- [V2_Plugin_Development.md](/root/MoviePilot-Plugins/docs/V2_Plugin_Development.md)
- [Repository_Guide.md](/root/MoviePilot-Plugins/docs/Repository_Guide.md)
