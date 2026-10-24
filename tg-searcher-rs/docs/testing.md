# 测试

## 运行

```bash
cargo test --workspace
```

所有测试都离线运行，不需要 Telegram 凭证或 Redis。图像测试需要 OpenCV 运行时
DLL/共享库在 `PATH`/`LD_LIBRARY_PATH` 中。

## 分层

### 1. 单元测试（core / app）

- `core::config`：YAML 解析、`${ENV}` 展开、缺失变量、重复 ID、代理 URL、
  阈值/图像参数范围；
- `core::text`：HTML 转义、CJK 安全的 `brief_content`、会话 ID、命令拆分；
- `core::query`：隐式 AND、`NOT/AND/OR` 优先级、括号、短语、通配符、错误；
- `core::fuzzy`：中文/英文相似度、无关文本低分、阈值是否生效、文件名参与；
- `core::index`：增删改、布尔/短语/通配符、过滤、排序分页、模糊回退、
  `--exact`、随机、chat id、清空；
- `core::tokenizer`：jieba tokenizer 与 bigram；
- `core::image`：ORB/pHash 提取、缩放稳定性、同图匹配、异图拒绝、缩略图、
  持久化重开、以图搜文的 caption；
- `app::commands`：命令别名、参数解析、`@bot` 后缀；
- `app::render`：HTML 转义、按钮、截断闭合标签、图片结果；
- `app::cache`：TTL、集合、Redis 地址解析。

### 2. 回归测试（app::tests/system.rs）

对照原 Python 行为，端到端验证：

- CJK 搜索与 `【筛选】` 按钮；
- 翻页回调、`file_only` 筛选；
- 模糊阈值默认/`--threshold`/`--exact` 三种行为；
- `/chats` 按钮选择 + 回复选择消息限定会话搜索；
- `/download_chat` → 搜索新索引、`/monitor_chat`、`/stat`；
- 非管理员 `/clear` 被拒绝；`/clear all` 二次确认并清空；
- `/img2text`、`/img2img`（发送图片/回复图片）、无图片时的用法提示；
- 普通文本搜索、未知命令、帮助、`/find_chat_id`、`/refresh_chat_names`；
- 使用统计、private mode、HTML 注入转义。

这些测试使用 `app/src/testing.rs` 的假 Messenger 与真实 tantivy/OpenCV 索引，
在临时目录中运行。

### 3. CLI 测试（crates/cli/tests/cli.rs）

- `--check` 成功与摘要输出；
- 缺失环境变量时明确失败；
- `${VAR}` 展开与 `TG_SEARCH_FUZZY_THRESHOLD` 覆盖；
- 未知参数被 clap 拒绝。

### 4. 实机冒烟（可选）

使用真实 bot token 验证 Bot API 连通性，不连接 userbot：

```bash
export TG_BOT_TOKEN='<token>'
# 临时生成一个使用该 token 的 searcher.yaml（不要提交）
./target/release/tg-searcher -f /tmp/smoke.yaml --bot-smoke
```

测试后删除临时配置与 shell 历史中的 token；仓库中的样例只保留
`${TG_BOT_TOKEN}` 占位符。

本仓库的验收记录：

- `cargo test --workspace`：90 passed / 0 failed；
- `cargo clippy --workspace --all-targets`：本项目代码 0 warning；
- `cargo build --release`：通过；
- `--bot-smoke`：通过真实 Bot API（经本地代理）验证 token；
- `searcher.sample.yaml --check`：通过；
- Windows 便携包：在仅保留 `C:\Windows\System32` 的 `PATH` 下解压运行
  `tg-searcher.exe --version` 与 `--check` 均通过（随附 OpenCV/VC 运行时 DLL）；
- 一键启动脚本：`start-bot.cmd -Version/-Check`、首次运行生成
  `searcher.yaml`/`.env` 均实测通过；
- Linux/Docker 打包脚本：通过 `shellcheck` 0.11，LF 换行；Linux/Docker 运行时
  打包需在对应平台执行（本机没有 Linux shell 与 Docker daemon）。
