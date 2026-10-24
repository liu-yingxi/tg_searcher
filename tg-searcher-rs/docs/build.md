# 构建与部署

本项目目标平台为 Linux、macOS 和 Windows，代码中没有平台特定逻辑。唯一的平台
差异来自 OpenCV 原生库与 `opencv` crate 的绑定生成工具链。

## 依赖总览

| 依赖 | 用途 | 是否必需 |
| --- | --- | --- |
| Rust 1.88+ | 编译 | 是 |
| OpenCV 4.x（core/imgproc/imgcodecs/features2d/flann） | 图像搜索 | 是 |
| Clang / libclang | `opencv` crate 生成 Rust 绑定 | 是（编译期） |
| Redis 6+ | 翻页/选择会话上下文、使用统计 | 否（自动退回内存缓存） |
| CMake / pkg-config | Linux/macOS 上发现 OpenCV | 通常需要 |

`opencv` crate 使用 `clang-runtime` 特性；如果构建时找不到 libclang，请设置
`LIBCLANG_PATH`。部分平台还需要 `CLANG_PATH` 指向 `clang` 可执行文件。

## Linux（Debian/Ubuntu）

```bash
sudo apt-get update
sudo apt-get install -y build-essential pkg-config clang libclang-dev libopencv-dev redis-server
cargo build --release
```

如果 OpenCV 安装在非标准路径：

```bash
export PKG_CONFIG_PATH=/opt/opencv/lib/pkgconfig:$PKG_CONFIG_PATH
# 或显式指定
export OPENCV_LINK_PATHS=/opt/opencv/lib
export OPENCV_LINK_LIBS=opencv_world4100   # 或 opencv_core,opencv_imgproc,...
export OPENCV_INCLUDE_PATHS=/opt/opencv/include
```

### Linux 便携包

```bash
chmod +x scripts/package-linux.sh
./scripts/package-linux.sh
# 产物：dist/linux-x86_64/tg-searcher-linux-x86_64.tar.gz
```

脚本行为：

1. `cargo build --release --locked`；
2. 用 `ldd` 收集非 glibc 依赖（OpenCV、libstdc++、图像编解码库等）到 `lib/`；
3. 生成 `tg-searcher.sh`，运行时将 `lib/` 加入 `LD_LIBRARY_PATH`；
4. 若安装了 `patchelf`，同时写入 `$ORIGIN/lib` 的 rpath；
5. 打包并生成 `.sha256`。

目标机要求：与构建机相同架构、glibc 版本不低于构建机。若目标机已安装
OpenCV，可用 `--no-bundle-libs` 生成仅含二进制/配置的包。

```bash
./scripts/package-linux.sh --no-bundle-libs
./scripts/package-linux.sh --skip-build --output /tmp/out
```

> `ldd`/`tar`/`sha256sum` 需在构建机可用。

## macOS

```bash
brew install opencv llvm pkg-config redis
export PKG_CONFIG_PATH="$(brew --prefix opencv)/lib/pkgconfig:$PKG_CONFIG_PATH"
export LIBCLANG_PATH="$(brew --prefix llvm)/lib"
export CLANG_PATH="$(brew --prefix llvm)/bin/clang"
cargo build --release
```

Apple Silicon 上 Homebrew 前缀通常为 `/opt/homebrew`。macOS 没有提供附加打包
脚本；`target/release/tg-searcher` 配合 `brew install opencv` 即可运行。

## Windows（MSVC）

1. 安装 Visual Studio 2022（Desktop development with C++，即 MSVC 工具链）。
2. 安装 LLVM（建议 19 或更新）；把 `clang.exe` 与 `libclang.dll` 所在目录加入
   `PATH`，并设置 `LIBCLANG_PATH`、`CLANG_PATH`。
3. 安装 OpenCV 4.x Windows 包，例如解压到 `C:\opencv`，得到：
   - `C:\opencv\build\include`
   - `C:\opencv\build\x64\vc16\lib`（含 `opencv_world4100.lib`）
   - `C:\opencv\build\x64\vc16\bin`（运行时 DLL）
4. 设置构建环境（PowerShell）：

```powershell
$env:OPENCV_INCLUDE_PATHS = "C:\opencv\build\include"
$env:OPENCV_LINK_PATHS    = "C:\opencv\build\x64\vc16\lib"
$env:OPENCV_LINK_LIBS     = "opencv_world4100"
$env:OPENCV_MSVC_CRT      = "dynamic"
$env:LIBCLANG_PATH        = "C:\Program Files\LLVM\bin"
$env:CLANG_PATH           = "C:\Program Files\LLVM\bin\clang.exe"
$env:Path = "C:\opencv\build\x64\vc16\bin;C:\Program Files\LLVM\bin;$env:Path"
cargo build --release
```

运行前确保 `opencv_world4100.dll` 在 `PATH` 中，或把它复制到可执行文件旁。

### Windows 便携包（推荐）

```powershell
.\scripts\package-windows.ps1 -OpenCvBin "C:\opencv\build\x64\vc16\bin"
```

脚本会：

1. `cargo build --release --locked`；
2. 复制 `tg-searcher.exe`、`opencv_world*.dll`；
3. 复制 `msvcp140*.dll`、`vcruntime140*.dll`、`concrt140.dll`；
4. 生成 `dist\windows\` 与 `dist\tg-searcher-windows-x64.zip`。

目标机器无需安装 OpenCV 或 VC++ 运行时。首次登录仍是交互式的，建议在终端中
运行而不是双击。

如果 PowerShell 报“因为在此系统上禁止运行脚本”，推荐使用一次性包装器：

```powershell
.\scripts\package-windows.cmd -OpenCvBin "C:\opencv\build\x64\vc16\bin"
```

它只对本次 `powershell.exe` 调用使用 `-ExecutionPolicy Bypass`，不会修改系统策略；
也可以手动执行：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\package-windows.ps1 `
  -OpenCvBin "C:\opencv\build\x64\vc16\bin"
```

或在当前会话临时放行（关闭窗口即失效）：

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force
.\scripts\package-windows.ps1 -OpenCvBin "C:\opencv\build\x64\vc16\bin"
```

`-OpenCvBin` 必须指向包含 `opencv_world*.dll` 的目录；如果已设置
`OPENCV_LINK_PATHS`，脚本会自动推导，可省略该参数。

### 一键启动（Windows）

打包目录和仓库根目录都带有：

- `start-bot.cmd`：双击运行，内部用本次调用的 `-ExecutionPolicy Bypass`；
- `start-bot.ps1`：查找 exe、加载 `.env`、校验配置并启动。

```powershell
.\start-bot.cmd                 # 校验后启动
.\start-bot.cmd -Check          # 只校验配置
.\start-bot.cmd -BotSmoke       # 只验证 bot token
.\start-bot.cmd -Version        # 显示版本
.\start-bot.cmd -Clear          # 清空索引后启动
.\start-bot.cmd -Debug          # debug 日志
.\start-bot.cmd -Restart        # 异常退出后自动重启
.\start-bot.cmd -NoCheck        # 跳过启动前校验
.\start-bot.cmd -Init           # 只生成缺少的 searcher.yaml/.env
```

首次运行若缺少配置，会从 `searcher.sample.yaml` / `.env.example` 创建
`searcher.yaml` / `.env` 并暂停，填写后再次运行即可。`.env` 中已有的同名
环境变量优先；脚本只输出变量数量，不打印 token。

> **关于静态链接**：OpenCV 官方 Windows 发行包不包含静态库（`opencv_world4100.lib`
> 只是导入库），因此默认构建无法完全静态链接 OpenCV。要做到单文件，需要从
> OpenCV 源码以 `BUILD_SHARED_LIBS=OFF` 重新编译静态库，并在链接时手动提供
> 全部第三方依赖；即使如此，仍需 MSVC 运行时（或改用 `/MT`，但与 Rust 默认
> `/MD` 冲突）。本项目采用随包分发 DLL 的便携方案，运行效果等价且构建可重复。
> 如确需静态 OpenCV，可在 `docs/build.md` 的基础上自行构建并调整
> `OPENCV_LINK_LIBS`。


> 如果使用 vcpkg：`vcpkg install opencv4[core,imgproc,imgcodecs,features2d]`，
> 设置 `VCPKG_ROOT` 后 `opencv` crate 会自动发现。
>
> 如果构建脚本报 `D8050` / 调试记录过长，可保留仓库中 `[profile.dev]
> debug = 0` 的设置（本项目已默认关闭 debuginfo 以避开该 MSVC 限制）。

### 关于代理

- 后端 userbot（grammers）只支持 **SOCKS5** 代理：
  `proxy: socks5://user:pass@host:1080`；
- 前端 bot（teloxide / Bot API）支持 HTTP(S) 与 SOCKS5；
- 配置中的 HTTP 代理会用于前端，后端会记录警告并忽略。

## Docker

```bash
docker build -t tg-searcher .
cp searcher.sample.yaml config/searcher.yaml
cp .env.example .env    # 填写密钥
docker compose -f docker-compose.sample.yaml run --rm tg-searcher   # 首次登录
docker compose -f docker-compose.sample.yaml up -d
```

镜像内使用 Debian 的 OpenCV/Clang 包，因此同一 Dockerfile 适用于 Linux
（x86_64/arm64，取决于基础镜像与 OpenCV 包）。

### Docker 镜像归档

```bash
chmod +x scripts/package-docker.sh
./scripts/package-docker.sh
# 默认构建 tg-searcher:latest，校验 --version，然后导出：
#   dist/tg-searcher-docker-image.tar.gz (+ sha256)
```

自定义：

```bash
./scripts/package-docker.sh --image tg-searcher:1.0 --output dist/tg-searcher-1.0.tar.gz
PLATFORM=linux/arm64 ./scripts/package-docker.sh   # 需要 buildx
./scripts/package-docker.sh --no-verify            # 跳过镜像内 --version
```

在其他机器导入并运行：

```bash
docker load -i tg-searcher-docker-image.tar.gz
docker run --rm -it \
  -v "$PWD/config:/app/config" \
  -v "$PWD/data:/app/data" \
  --env-file .env \
  tg-searcher:latest -f /app/config/searcher.yaml
```

> 首次 userbot 登录需要 `-it`；登录后可用 `docker compose up -d` 常驻。
> 构建机需安装 Docker；`PLATFORM=linux/arm64` 需要 `docker buildx`。

## 数据目录

```
<runtime_dir>/<common.name>/
├── session/          # grammers SQLite session（含登录凭证，务必保护）
├── index/<backend>/  # tantivy 文本索引
├── images/<backend>/ # 图像描述子 index.bin + thumbs/
└── tmp/              # 图片下载临时文件
```

迁移时整体复制即可；`--clear` 会删除文本与图片索引（session 保留）。

## 首次登录

`UserSession::connect` 会在未授权时从 stdin 读取验证码和两步验证密码。因此：

- 本地运行：直接在终端运行；
- Docker：先 `docker compose run --rm -it tg-searcher` 完成登录，再 Ctrl-P/Q
  或退出后用 `up -d`；
- systemd：可先手动运行一次二进制完成登录。

登录成功后 session 会持久化，后续启动无需验证码。
