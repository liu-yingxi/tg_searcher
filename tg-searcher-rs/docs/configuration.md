# 配置指南

配置文件为 YAML，默认 `./searcher.yaml`，可用 `-f` 指定。支持 `${ENV_VAR}`
环境变量展开；变量不存在时会启动失败。

## 最小配置

```yaml
common:
  name: my_searcher
  runtime_dir: ./data
  api_id: 123456
  api_hash: your_api_hash

sessions:
  - name: alice
    phone: '+15551234567'

backends:
  - id: pub_index
    use_session: alice

frontends:
  - type: bot
    id: public
    use_backend: pub_index
    config:
      admin_id: 123456789
      bot_token: ${TG_BOT_TOKEN}
```

`${TG_BOT_TOKEN}` 会被环境变量替换；示例中不包含真实 token。

## common

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `name` | string | 实例名，运行时数据位于 `<runtime_dir>/<name>/` |
| `runtime_dir` | path | 索引、session、图片、临时文件根目录 |
| `api_id` / `api_hash` | int / string | [my.telegram.org](https://my.telegram.org) 申请 |
| `proxy` | string? | `socks5://host:port` 或 `http://host:port` |

## search.fuzzy

模糊搜索只在精确搜索返回结果少于 `fallback_below_results` 时触发
（`mode: fallback`，默认）。阈值 `threshold` 是独立参数，可通过配置、环境变量
`TG_SEARCH_FUZZY_THRESHOLD` 或 `/s --threshold=0.x` 覆盖。

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `enabled` | true | 关闭后只做精确搜索 |
| `mode` | fallback | `fallback` / `always` / `off` |
| `threshold` | 0.5 | 相似度阈值，范围 0..1；越低召回越高 |
| `min_query_chars` | 2 | 短于此长度的查询不触发模糊 |
| `max_edit_distance` | 2 | FuzzyTermQuery 编辑距离（0..3） |
| `ngram_size` | 2 | 模糊辅助字段的字符 n-gram 大小 |
| `candidate_limit` | 500 | 模糊候选上限 |
| `fallback_below_results` | 1 | 精确结果少于该值时回退 |

相似度计算（`crates/core/src/fuzzy.rs`）取以下两者的最大值：

1. jieba 分词后的加权 token 覆盖率（子串或归一化 Levenshtein）；
2. 查询与最佳句段的字符 bigram Dice 系数。

## image

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `enabled` | true | 是否索引/搜索图片 |
| `max_features` | 500 | 每图 ORB 特征数 |
| `match_ratio` | 0.75 | Lowe ratio test 阈值 |
| `min_good_matches` | 8 | 最少有效匹配数 |
| `min_similarity` | 0.12 | ORB 内点比例阈值（0..1） |
| `thumbnail_max_side` | 320 | 缩略图最大边 |
| `jpeg_quality` | 80 | 缩略图质量 |
| `max_download_bytes` | 20 MiB | 超过则不下载图片 |
| `index_history` | true | 下载历史时是否索引图片 |
| `max_images_per_chat` | 0 | 每会话图片上限，0 为无限 |
| `phash_candidate_limit` | 512 | 送入 ORB 匹配的 pHash 候选数 |
| `result_limit` | 10 | 图片搜索结果上限 |
| `send_photo_limit` | 3 | `/img2img` 最多回发几张图 |

## sessions

```yaml
sessions:
  - name: alice
    phone: '+15551234567'
```

一个 session 可被多个 backend 复用；更新会分发给所有 backend。

## backends

```yaml
backends:
  - id: pub_index
    use_session: alice
    config:
      monitor_all: false
      exclude_chats:
        - 342843148
        - "@some_channel"
```

| 字段 | 说明 |
| --- | --- |
| `id` | 全局唯一 |
| `use_session` | 引用 sessions[].name |
| `monitor_all` | 监控所有会话（排除列表除外） |
| `exclude_chats` | 数字 ID 或用户名，永不索引 |

## frontends

```yaml
frontends:
  - type: bot
    id: public
    use_backend: pub_index
    config:
      admin_id: 123456789
      bot_token: ${TG_BOT_TOKEN}
      page_len: 10
      redis: 127.0.0.1:6379
      no_redis: false
      private_mode: false
      private_whitelist: [123456789]
```

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `type` | bot | 目前只支持 `bot` |
| `admin_id` | 必填 | 管理员用户 ID 或用户名 |
| `bot_token` | 必填 | BotFather token；建议 `${TG_BOT_TOKEN}` |
| `page_len` | 10 | 每页结果数 |
| `redis` | - | `host:port`，空则内存缓存 |
| `no_redis` | false | 强制内存缓存 |
| `private_mode` | false | 仅白名单可用 |
| `private_whitelist` | [] | 允许使用的用户 ID；管理员自动包含 |

## 校验

```bash
tg-searcher -f searcher.yaml --check
```

会检查：必填字段、ID 唯一性、session/backend 引用、token 非空、页码、
模糊阈值范围、图像参数范围、代理 URL 等。
