# Changelog

## 0.1.0 — Rust rewrite

### 重写

- 全部 Python 代码用 Rust 重写：
  - `tg_searcher/indexer.py` → `crates/core/src/index.rs` + `crates/core/src/image.rs`
  - `tg_searcher/backend_bot.py` → `crates/telegram/src/backend.rs`
  - `tg_searcher/frontend_bot.py` → `crates/app/src/frontend.rs` + `crates/telegram/src/frontend.rs`
  - `tg_searcher/session.py` → `crates/telegram/src/session.rs`
  - `tg_searcher/main.py` → `crates/cli/src/main.rs`
- Whoosh + jieba → tantivy + `jieba-rs` 自定义 tokenizer。
- Telethon → grammers（后端 MTProto userbot）+ teloxide（前端 Bot API）。
- Redis / FakeRedis → `redis` 异步连接 + 内存 TTL 缓存自动回退。

### 搜索

- 保留中文分词、`AND`/`OR`/`NOT`、括号、引号短语、`*`/`?` 通配符。
- 新增独立的模糊搜索回退与可配置阈值：
  - YAML：`search.fuzzy.threshold`
  - 环境变量：`TG_SEARCH_FUZZY_THRESHOLD`
  - 单次查询：`/s --threshold=0.35 ...`、`/s --exact ...`
- 通过字符 n-gram 辅助字段 + `FuzzyTermQuery` 召回候选，再用与索引实现解耦的
  相似度函数（token 覆盖率 / bigram Dice）过滤。
- 分页、会话过滤、纯文本/仅文件筛选、HTML 高亮、按时间倒序。

### 新增：OpenCV 图片搜索

- `opencv` crate（ORB + BFMatcher + pHash + 缩略图）。
- `/img2text`：以图搜文。
- `/img2img`：以图搜图，并回发相似图片缩略图。
- 支持发送图片时在 caption 写命令，或回复图片后使用命令；可结合“已选择会话”
  限定范围。
- 图片描述子持久化到 `images/<backend>/index.bin`，缩略图存 `thumbs/`。

### 工程化

- 四 crate workspace：`core` / `app` / `telegram` / `cli`。
- `SearchBackend` 与 `Messenger` trait 解耦，提供 `FakeBackend`/`FakeMessenger`。
- 90 个测试：核心单元、回归、系统（端到端假 Telegram）、CLI。
- 配置支持 `${ENV_VAR}` 展开（注释中的占位符不会被误展开）；token 不进入
  源码/样例；Debug 输出隐藏 token。
- Dockerfile / docker-compose 样例、跨平台构建文档（Linux/macOS/Windows）。
- 打包脚本：
  - `scripts/package-windows.ps1`：便携目录/zip，随附 OpenCV 与 MSVC 运行时；
  - `scripts/package-linux.sh`：tar.gz，`ldd` 收集非 glibc 依赖 + 启动脚本；
  - `scripts/package-docker.sh`：构建并导出 Docker 镜像 tar.gz。
- Windows 便携打包脚本：随附 `opencv_world4100.dll` 与 MSVC 运行时，
  修复“找不到 opencv_world4100.dll”的启动错误。
- 一键启动脚本 `start-bot.cmd` / `start-bot.ps1`：自动查找可执行文件、
  加载 `.env`、首次运行生成配置、启动前校验，支持 `-Check`/`-BotSmoke`/
  `-Clear`/`-Debug`/`-Restart`，并随 Windows 便携包分发。
- 实测通过真实 bot token 的 `--bot-smoke`（只验证 Bot API，不连接 userbot）。
