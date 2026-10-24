# 架构

## 分层

```text
                Telegram Bot API                     Telegram MTProto
                       │                                   │
             crates/telegram/frontend.rs          crates/telegram/backend.rs
             (teloxide: polling, callbacks,       (grammers: login, history,
              photo download/upload)               updates, media download)
                       │                                   │
                       └──────────────┬────────────────────┘
                                      │
                            crates/app (transport-neutral)
                  commands / render / cache / Frontend handlers
                                      │
                            crates/core
             config ─ text index (tantivy) ─ fuzzy ─ image index (OpenCV)
```

`app` 通过两个 trait 与 Telegram 解耦：

- `SearchBackend`（`app/src/backend.rs`）：搜索、随机消息、状态、会话解析/名称、
  历史下载、监控、清除、图片搜索；
- `Messenger`（`app/src/messenger.rs`）：发文本/编辑/回调查询/发图片。

因此 `app` 可以在没有 Telegram 的情况下，用 `app/src/testing.rs` 中的
`FakeBackend` + `FakeMessenger` + 真实 `TextIndex`/`ImageStore` 做完整系统测试。

## 与原 Python 模块的对应

| Python | Rust |
| --- | --- |
| `tg_searcher/main.py` | `crates/cli/src/main.rs` |
| `common.py` | `crates/core/src/config.rs`、`text.rs` |
| `session.py` | `crates/telegram/src/session.rs` |
| `indexer.py` | `crates/core/src/index.rs`（文本）、`image.rs`（图像） |
| `backend_bot.py` | `crates/telegram/src/backend.rs` |
| `frontend_bot.py` | `crates/app/src/frontend.rs` + `crates/telegram/src/frontend.rs` |
| Whoosh + jieba | tantivy + jieba-rs（自定义 tokenizer） |
| Whoosh 模糊/通配符 | 自定义查询解析器 + ngram/FuzzyTerm + 可配置相似度回退 |
| Telethon | grammers（后端）/ teloxide（前端） |
| Redis / FakeRedis | `app/src/cache.rs` |

## 文本索引细节

Schema（`crates/core/src/index.rs`）：

| 字段 | 类型 | 用途 |
| --- | --- | --- |
| `url` | STRING | 唯一键，按 URL 更新/删除 |
| `chat_id` | i64 fast | 会话过滤 |
| `content` | TEXT(jieba, positions) | 正文，短语查询/高亮 |
| `filename` | TEXT(jieba) | 文件名 |
| `content_grams` / `filename_grams` | TEXT(ngram) | 模糊回退候选 |
| `post_time` | i64 fast | 按时间倒序排序 |
| `sender` | stored | 展示 |
| `has_file` | u64 fast | 纯文本/仅文件筛选 |

搜索流程：

1. 解析查询为 AST（词、短语、通配符、AND/OR/NOT）；
2. AST → tantivy 查询（多字段 term/phrase/regex）；
3. 叠加会话与文件类型过滤；
4. 有结果则按 `post_time desc` 分页，高亮用 `SnippetGenerator`；
5. 结果不足时用 ngram + FuzzyTerm 取候选，按独立相似度函数过滤阈值，
   排序后分页，使用手动高亮。

## 图像索引细节

每个 backend 一个 `ImageStore`：

- `index.bin`：bincode 序列化的 `Vec<ImageRecord>`；
- `thumbs/*.jpg`：缩略图；
- `ImageRecord`：URL、会话、时间、正文、文件名、pHash、ORB 描述子、关键点数。

匹配：

1. 查询图 ORB + pHash；
2. 若图片数量超过 `phash_candidate_limit`，按 pHash 汉明距离取候选；
3. BFMatcher(Hamming) kNN + Lowe ratio test；
4. `similarity = 0.85 * good / min(kp) + 0.15 * (1 - phash_distance/64)`；
5. 同时满足 `min_good_matches` 与 `min_similarity` 才返回。

`/img2text` 返回消息正文/文件名与链接；`/img2img` 额外回发匹配图片的缩略图。

## 并发与持久化

- 文本/图像索引各自 `Mutex`，异步接口通过 `spawn_blocking` 执行搜索，避免阻塞
  Tokio 运行时；
- 每个 Telegram session 只有一个 grammers update stream，按 session 分发给共享
  该 session 的全部 backend；
- 每个 storefront 独立运行 teloxide 长轮询；
- 监控列表在启动时从文本索引的 `chat_id` 词条重建（与原项目一致）；
- 图片索引持久化到 `images/<backend>/index.bin`。
