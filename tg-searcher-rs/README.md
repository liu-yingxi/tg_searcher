# TG Searcher (Rust)

Telegram 消息搜索机器人，使用 Rust 重写了原 Python 项目
[SharzyL/tg_searcher](https://github.com/SharzyL/tg_searcher)，并新增了基于
OpenCV 的**以图搜文**与**以图搜图**功能。

- **后端**：`grammers`（纯 Rust MTProto）userbot，负责登录账号、下载历史消息、
  实时索引新增/编辑/删除消息。
- **前端**：`teloxide`（Telegram Bot API）机器人，负责命令交互、内联按钮、翻页、
  筛选和图片搜索。
- **文本索引**：`tantivy` + `jieba-rs`，支持中文分词、布尔语法、短语、通配符，
  并提供**可配置阈值的模糊搜索回退**。
- **图像索引**：`opencv` crate（ORB 特征 + pHash + 缩略图），通过
  `/img2text` 与 `/img2img` 两个独立命令使用。

## 功能

Telegram 对中文等 CJK 语言的分词支持较弱，直接搜索经常搜不到内容。本项目会为
会话建立本地索引，并由 bot 提供搜索。默认先进行精确/布尔搜索；如果结果不足，
再自动进行模糊匹配。阈值是一个独立配置项，不需要修改搜索实现：

```yaml
search:
  fuzzy:
    threshold: 0.5   # 0.0 几乎全部匹配，1.0 仅完全匹配
```

也可以用环境变量 `TG_SEARCH_FUZZY_THRESHOLD` 或单次查询参数覆盖：

```text
/s --threshold=0.35 机器学习
/s --exact 机器学习
```

### 普通用户命令

| 命令 | 说明 |
| --- | --- |
| `/s`, `/search`, `/ss` | 搜索消息；也可以直接发送普通文本 |
| `/chats [关键词]` | 列出/筛选已索引会话，点击按钮选择 |
| `/random` | 随机返回一条已索引消息 |
| `/img2text [--chat ID]` | 回复一张图片，搜索相关文字消息 |
| `/img2img [--chat ID]` | 回复一张图片，搜索相似图片 |
| `/help` | 帮助 |

搜索语法支持 `"精确短语"`、`AND`、`OR`、`NOT`、括号、`*`/`?` 通配符。
选择会话后，回复 bot 的“☑️ 已选择”消息再发送搜索词，即可只搜索该会话。

### 管理员命令

| 命令 | 说明 |
| --- | --- |
| `/download_chat [--min ID] [--max ID] [对话...]` | 下载并索引历史消息 |
| `/monitor_chat 对话...` | 加入实时监控 |
| `/clear [对话...|all]` | 清除索引；`all` 需要 60 秒内二次确认 |
| `/stat` | 查看索引/监控/图片索引状态 |
| `/find_chat_id 关键词` | 按名称查找会话 ID |
| `/refresh_chat_names` | 刷新会话名称缓存 |
| `/usage` | 使用统计（需要 Redis 或内存缓存） |

### 图片搜索

图片消息在索引阶段会被下载并计算：

- ORB 局部特征（旋转/缩放鲁棒）；
- 64 位感知哈希 pHash（近似重复检测、候选预筛）；
- 320px 缩略图（`/img2img` 直接回发）。

搜索时先用 pHash 缩小候选，再用 BFMatcher（Hamming + Lowe ratio test）计算
匹配分数。接受条件与相似度阈值由 `image.min_good_matches` /
`image.min_similarity` 控制。

使用方式（任选其一）：

1. 发送图片，并在图片说明（caption）中写 `/img2text` 或 `/img2img`；
2. 回复一张图片，然后发送 `/img2text` 或 `/img2img`；
3. 先选择会话，再回复该会话的选择消息发送图片命令，可限定会话范围。

## 快速开始

### 1. 准备

- Rust stable（1.88+）；
- OpenCV 4.x 开发包；
- Clang / libclang（`opencv` crate 生成绑定所需）；
- Redis（可选，不启用时自动使用内存缓存）；
- Telegram `api_id` / `api_hash`、userbot 手机号、bot token、管理员用户 ID。

各平台的安装方法见 [docs/build.md](docs/build.md)。

### 2. 编译

```bash
cargo build --release
```

### 3. 配置

```bash
cp searcher.sample.yaml searcher.yaml
cp .env.example .env
# 编辑 .env 与 searcher.yaml
set -a; source .env; set +a          # Linux/macOS
# PowerShell: Get-Content .env | ForEach-Object { ... }，或手动设置环境变量
```

`searcher.yaml` 中的 `${TG_BOT_TOKEN}`、`${TG_API_ID}` 等会从环境变量展开；
变量缺失会直接报错，不会以空密钥启动。**不要把真实 token 写进仓库。**

### 4. 运行

```bash
./target/release/tg-searcher -f searcher.yaml
```

首次运行会提示输入 Telegram 登录验证码（如有两步验证还会提示密码）。登录成功后
后端会开始监听，前端会向管理员发送启动状态消息。

### Windows 便携包

Windows 上 OpenCV 官方发行包只提供动态库，直接复制 `tg-searcher.exe` 会提示
`找不到 opencv_world4100.dll`。使用打包脚本生成零依赖目录/压缩包：

```powershell
.\scripts\package-windows.ps1 -OpenCvBin "C:\opencv\build\x64\vc16\bin"
# 产物：dist\windows\（含 exe、opencv_world4100.dll、MSVC 运行时）
#       dist\tg-searcher-windows-x64.zip
```

目标机器不需要安装 OpenCV 或 VC++ 运行时。若 PowerShell 禁止运行脚本，可改用
同目录的 cmd 包装器（仅本次调用绕过策略）：

```powershell
.\scripts\package-windows.cmd -OpenCvBin "C:\opencv\build\x64\vc16\bin"
```

或手动执行：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\package-windows.ps1 `
  -OpenCvBin "C:\opencv\build\x64\vc16\bin"
```

#### 一键启动

仓库根目录与便携包内都有：

- `start-bot.cmd`：双击即可，自动绕过 PowerShell 执行策略；
- `start-bot.ps1`：完整启动逻辑。

首次运行会自动从样例创建 `searcher.yaml` 和 `.env` 并暂停，等你填写
API 凭证、bot token 和管理员 ID；再次运行即可。

```powershell
.\start-bot.cmd                 # 校验配置并启动
.\start-bot.cmd -Check          # 只校验配置
.\start-bot.cmd -BotSmoke       # 只验证 bot token
.\start-bot.cmd -Version        # 显示版本
.\start-bot.cmd -Clear          # 清空索引后启动
.\start-bot.cmd -Debug          # debug 日志
.\start-bot.cmd -NoCheck        # 跳过启动前校验
.\start-bot.cmd -Init           # 只生成缺少的配置/.env
```

`.env` 中已存在的同名环境变量优先于 `.env`；脚本不会打印 token。

Linux / Docker 脚本首次使用前请赋予执行权限：

```bash
chmod +x scripts/package-linux.sh scripts/package-docker.sh
```

常用参数：

```bash
tg-searcher -f searcher.yaml --check       # 仅校验配置
tg-searcher -f searcher.yaml --bot-smoke   # 仅验证 bot token（不连接 userbot）
tg-searcher -f searcher.yaml -c            # 清空索引后启动
tg-searcher -f searcher.yaml --debug
```

## 部署

- Docker / docker-compose：见 [Dockerfile](Dockerfile) 与
  [docker-compose.sample.yaml](docker-compose.sample.yaml)。
- 手动部署、各平台依赖、代理说明：见 [docs/build.md](docs/build.md)。
- 完整配置项：见 [docs/configuration.md](docs/configuration.md)。

### 打包脚本

| 平台 | 命令 | 产物 |
| --- | --- | --- |
| Windows | `.\scripts\package-windows.cmd -OpenCvBin ...` | `dist\tg-searcher-windows-x64.zip`（含 OpenCV/VC 运行时） |
| Linux | `./scripts/package-linux.sh` | `dist/linux-<arch>/tg-searcher-linux-<arch>.tar.gz`（含非 glibc 依赖与启动脚本） |
| Docker | `./scripts/package-docker.sh` | `dist/tg-searcher-docker-image.tar.gz` |

Linux 便携包默认用 `ldd` 收集 OpenCV、libstdc++、图像编解码库等非 glibc
依赖，并生成 `tg-searcher.sh`（设置 `LD_LIBRARY_PATH`）。目标机需为不低于
构建机的 glibc 发行版；若不想打包依赖，可用 `--no-bundle-libs`，目标机自行
安装 OpenCV。Docker 包可在其他机器 `docker load -i ...` 后运行。

> 首次登录必须在交互式终端进行。Docker 下可先执行
> `docker compose run --rm tg-searcher` 完成登录，再以守护进程方式启动。

## 测试

```bash
cargo test --workspace
```

测试分为：

- **单元测试**：配置解析、查询语法、中文分词、模糊相似度与阈值、tantivy 索引、
  ORB/pHash 图像匹配、渲染与按钮；
- **回归测试**：对照原 Python 行为（命令别名、分页/筛选、选择会话、`/clear all`
  二次确认、HTML 转义、帮助文本等）；
- **系统测试**：使用假 Telegram 传输 + 真实 tantivy/OpenCV 索引，端到端跑完
  搜索、翻页、下载、监控、清除、图片搜索等流程。

详见 [docs/testing.md](docs/testing.md)。

## 安全

- 配置支持 `${ENV_VAR}` 展开，仓库中的样例只包含占位符；
- `BotFrontendConfig` 的 `Debug` 实现会隐藏 token；
- `.gitignore` / `.dockerignore` 排除了 `.env`、session 文件、运行时数据；
- 管理命令仅管理员可用，`/clear all` 需要二次确认。

## 目录结构

```text
crates/core      配置、文本索引、模糊匹配、OpenCV 图像索引
crates/app       与 Telegram 无关的前端逻辑、命令、渲染、缓存、测试替身
crates/telegram  grammers userbot 后端 + teloxide bot 前端
crates/cli       tg-searcher 可执行程序
docs/            构建、配置、架构、测试文档
```

## 许可证

MIT。原项目版权归 SharzyL 及贡献者所有；本重写项目同样以 MIT 发布，详见
[LICENSE](LICENSE)。
