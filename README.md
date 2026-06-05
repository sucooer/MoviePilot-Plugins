# MoviePilot-Plugins

`MoviePilot-Plugins` 是 MoviePilot 的插件源码与插件索引仓库。

当前仓库已包含一个 V2 插件：

- `S3Backup`
  路径：[plugins.v2/s3backup](/root/MoviePilot-Plugins/plugins.v2/s3backup/__init__.py)
  功能：定时通过 S3 备份数据库和配置文件，并支持从 S3 恢复

当前仓库结构：

```text
MoviePilot-Plugins/
├── plugins.v2/
│   └── s3backup/
├── icons/
│   └── S3.png
├── docs/
└── package.v2.json
```

插件索引：

- [package.v2.json](/root/MoviePilot-Plugins/package.v2.json)

图标资源：

- [S3.png](/root/MoviePilot-Plugins/icons/S3.png)

开发与发布时请优先遵循：

- [V2_Plugin_Development.md](/root/MoviePilot-Plugins/docs/V2_Plugin_Development.md)
- [Repository_Guide.md](/root/MoviePilot-Plugins/docs/Repository_Guide.md)
