# S3Backup

MoviePilot V2 插件：定时将数据库和配置文件打包后上传到 S3 或兼容 S3 的对象存储，并支持从 S3 恢复。

当前版本支持：

- 定时执行备份
- 立即运行一次
- 手动调用插件 API 触发备份
- 列出远端备份
- 按对象 key 恢复备份
- 恢复前自动创建本地回滚快照
- 恢复后可选自动重启 MoviePilot
- 按最大保留数量清理旧备份
- AWS S3 / MinIO / Cloudflare R2 / 阿里云 OSS S3 兼容接口

默认备份内容：

- `user.db*`
- `app.env`
- `category.yaml`
- `cookies/`
