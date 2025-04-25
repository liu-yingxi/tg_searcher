# -*- coding: utf-8 -*-
import html
import re # 用于剥离 HTML
from time import time
from typing import Optional, List, Tuple, Set, Union, Any
from datetime import datetime
from traceback import format_exc
from argparse import ArgumentParser, ArgumentError
import shlex
import asyncio

import redis
import whoosh.index # 用于捕获 LockError
from telethon import TelegramClient, events, Button
from telethon.tl.types import BotCommand, BotCommandScopePeer, BotCommandScopeDefault, MessageEntityMentionName
from telethon.tl.custom import Message as TgMessage
from telethon.tl.functions.bots import SetBotCommandsRequest
import telethon.errors.rpcerrorlist as rpcerrorlist
from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError, ResponseError as RedisResponseError

# 项目内导入 (带 Fallback)
try:
    from .common import CommonBotConfig, get_logger, get_share_id, remove_first_word, brief_content
    from .backend_bot import BackendBot, EntityNotFoundError
    from .indexer import SearchResult, IndexMsg
except ImportError:
    # 如果作为独立脚本运行或导入失败，提供基本的 fallback 定义
    print("Warning: Assuming relative imports fail, define fallbacks if needed.")
    class CommonBotConfig: pass
    def get_logger(name): import logging; return logging.getLogger(name)
    def get_share_id(x): return int(x) if isinstance(x, (int, str)) and str(x).lstrip('-').isdigit() else 0 # 更安全的 fallback
    def remove_first_word(s): return ' '.join(s.split()[1:]) if len(s.split()) > 1 else ''
    def brief_content(s, l=70): s=str(s); return (s[:l] + '...') if len(s) > l else s # 更新默认长度并确保是字符串
    class BackendBot: pass
    class EntityNotFoundError(Exception):
        def __init__(self, entity='Unknown'): self.entity = entity; super().__init__(f"Entity not found: {entity}")
    class SearchResult:
        def __init__(self, hits=None, is_last_page=True, total_results=0): self.hits=hits or []; self.is_last_page=is_last_page; self.total_results=total_results
    class IndexMsg: pass


logger = get_logger('frontend_bot')


class BotFrontendConfig:
    """存储 Frontend Bot 配置的类"""
    @staticmethod
    def _parse_redis_cfg(redis_cfg: str) -> Tuple[str, int]:
        """解析 Redis 'host:port' 配置字符串"""
        if not isinstance(redis_cfg, str) or not redis_cfg:
            raise ValueError("Invalid Redis config string")
        colon_idx = redis_cfg.find(':')
        # 如果没有冒号，则假定只有主机名，使用默认端口 6379
        if colon_idx < 0:
            return redis_cfg, 6379
        try:
            host = redis_cfg[:colon_idx] if colon_idx > 0 else 'localhost' # 允许 ':port' 表示 localhost
            port = int(redis_cfg[colon_idx + 1:])
            if port <= 0 or port > 65535: raise ValueError("Port out of range")
            return host, port
        except (ValueError, TypeError) as e:
            # 捕获端口转换错误或范围错误
            raise ValueError(f"Invalid Redis host:port format in '{redis_cfg}': {e}")

    def __init__(self, **kw: Any):
        """从关键字参数初始化配置"""
        try:
            # 必需的配置项
            self.bot_token: str = kw['bot_token']
            self.admin: Union[int, str] = kw['admin_id'] # 管理员 ID 或用户名
        except KeyError as e:
            # 如果缺少必需项，抛出 ValueError
            raise ValueError(f"Missing required config key: {e}")

        # 可选配置项及其默认值
        self.page_len: int = kw.get('page_len', 10) # 搜索结果每页长度
        # 确保 page_len 是正数
        if not isinstance(self.page_len, int) or self.page_len <= 0:
            logger.warning(f"Invalid page_len '{self.page_len}', using default 10.")
            self.page_len = 10

        self.no_redis: bool = kw.get('no_redis', False) # 是否禁用 Redis
        self.redis_host: Optional[Tuple[str, int]] = None # Redis 连接信息 (host, port)

        # 如果未禁用 Redis，则解析 Redis 配置
        if not self.no_redis:
             try:
                  # 获取 Redis 配置字符串，默认为 'localhost:6379'
                  redis_cfg = kw.get('redis', 'localhost:6379')
                  if redis_cfg:
                      # 解析 'host:port' 字符串
                      self.redis_host = self._parse_redis_cfg(redis_cfg)
                  else:
                      # 如果配置为空，则禁用 Redis
                      logger.warning("Redis config string is empty. Disabling Redis.")
                      self.no_redis = True
             except ValueError as e:
                  # 解析出错，记录错误并禁用 Redis
                  logger.error(f"Error parsing redis config '{kw.get('redis')}': {e}. Disabling Redis.")
                  self.no_redis = True
             except KeyError:
                  # 如果配置中根本没有 'redis' 键，也禁用 Redis
                  logger.info("Redis config key 'redis' not found. Disabling Redis.")
                  self.no_redis = True

        # 私密模式相关配置
        self.private_mode: bool = kw.get('private_mode', False) # 是否启用私密模式
        self.private_whitelist: Set[int] = set() # 私密模式白名单 (用户 ID)
        raw_whitelist = kw.get('private_whitelist', []) # 从配置获取原始白名单列表

        # 解析白名单列表，确保成员是整数 ID
        if isinstance(raw_whitelist, list):
             for item in raw_whitelist:
                 try:
                     self.private_whitelist.add(int(item))
                 except (ValueError, TypeError):
                     # 忽略无法转换为整数的项
                     logger.warning(f"Could not parse private_whitelist item '{item}' as int.")
        elif raw_whitelist:
            # 如果格式不是列表，记录警告
            logger.warning("private_whitelist format incorrect (expected list of integers), ignoring.")


class FakeRedis:
    """
    一个简单的内存字典，模拟部分 Redis 功能 (get, set(ex), delete, ping, sadd, scard, expire)。
    用于在无 Redis 环境下运行，数据在重启后会丢失。
    """
    def __init__(self):
        self._data = {} # 存储格式: { key: (value, expiry_timestamp_or_None) }
        self._logger = get_logger('FakeRedis')
        self._logger.warning("Using FakeRedis: Data is volatile and will be lost on restart.")

    def get(self, key):
        """模拟 Redis GET 命令，检查过期时间"""
        v = self._data.get(key)
        if v:
            value, expiry = v
            # 如果没有过期时间，或者过期时间在未来，则返回值
            if expiry is None or expiry > time():
                return value
            # 如果已过期，则删除键并返回 None
            elif expiry <= time():
                del self._data[key]
        return None # 键不存在或已过期

    def set(self, key, val, ex=None):
        """模拟 Redis SET 命令，支持 EX (过期时间，秒)"""
        expiry = time() + ex if ex is not None and isinstance(ex, (int, float)) and ex > 0 else None
        # Redis 存储的是字节串，这里简单转为字符串模拟
        self._data[key] = (str(val), expiry)

    def delete(self, *keys):
        """模拟 Redis DEL 命令"""
        count = 0
        for k in keys:
            if k in self._data:
                del self._data[k]
                count += 1
        return count # 返回删除的键数量

    def ping(self):
        """模拟 Redis PING 命令"""
        return True # FakeRedis 总是 "在线"

    def sadd(self, key, *values):
        """模拟 Redis SADD 命令"""
        # 尝试获取现有的集合和过期时间
        v = self._data.get(key)
        current_set = set()
        expiry = None
        added_count = 0
        # 如果键存在且值是集合且未过期
        if v and isinstance(v[0], set) and (v[1] is None or v[1] > time()):
            current_set, expiry = v
        elif v and (not isinstance(v[0], set) or (v[1] is not None and v[1] <= time())):
             # 如果键存在但不是集合或已过期，则视为新集合
             if key in self._data: del self._data[key] # 清理旧数据
             expiry = None # 重置过期时间

        # 将要添加的值转换为字符串集合
        values_to_add = {str(v) for v in values}
        # 遍历要添加的值
        for val in values_to_add:
            if val not in current_set:
                current_set.add(val)
                added_count += 1

        # 更新存储
        self._data[key] = (current_set, expiry)
        return added_count # 返回新添加元素的数量

    def scard(self, key):
        """模拟 Redis SCARD 命令"""
        v = self._data.get(key)
        # 检查键存在、值是集合、且未过期
        if v and isinstance(v[0], set) and (v[1] is None or v[1] > time()):
            return len(v[0]) # 返回集合大小
        elif v and v[1] is not None and v[1] <= time():
             if key in self._data: del self._data[key] # 清理过期数据
        return 0 # 键不存在、不是集合或已过期

    def expire(self, key, seconds):
        """模拟 Redis EXPIRE 命令"""
        if key in self._data:
            value, _ = self._data[key]
            # 检查是否已过期
            if self.get(key) is not None: # Use get to implicitly check expiry
                expiry = time() + seconds if isinstance(seconds, (int, float)) and seconds > 0 else None
                if expiry: # Only set if new expiry is valid
                    self._data[key] = (value, expiry)
                    return 1 # 设置成功
                else: # If seconds <= 0, treat as delete
                    del self._data[key]
                    return 1 # "Expire" (delete) successful
        return 0 # 键不存在或已过期

    # Fake pipeline for basic usage tracking compatibility
    def pipeline(self):
        """返回自身以模拟 pipeline，调用将立即执行"""
        return self
    def execute(self):
        """模拟 pipeline execute，无操作"""
        pass


class BotFrontend:
    """处理用户交互、命令解析、结果展示的前端 Bot 类"""
    # 帮助文本 - 用户
    HELP_TEXT_USER = """
**可用命令:**
/s `关键词` - 搜索消息 (或 `/search`, `/ss`；直接发送非命令文本也可)。
/chats `[关键词]` - 列出/筛选已索引的对话，并提供选择按钮。
/random - 从已索引的消息中随机返回一条。
/help - 显示此帮助信息。

**使用 /chats 选择对话后:**
- 回复带有 "☑️ 已选择" 的消息 + 搜索词，可仅搜索该对话。
- 再次使用 /chats 或 /s 可取消选择。
"""
    # 帮助文本 - 管理员
    HELP_TEXT_ADMIN = """
**通用命令:**
/s `关键词` - 搜索消息 (或 `/search`, `/ss`；直接发送非命令文本也可)。
/chats `[关键词]` - 列出/筛选已索引的对话，并提供选择按钮。
/random - 从已索引的消息中随机返回一条。
/help - 显示此帮助信息。

**管理员命令:**
/download_chat `[选项] [对话...]` - 下载并索引指定对话的历史消息。
    选项: `--min ID` (起始消息ID), `--max ID` (结束消息ID, 0为无限制)
    对话: 对话的用户名、链接或 ID。可指定多个。
/monitor_chat `对话...` - 将指定对话加入实时监控列表 (新消息会自动索引)。
/clear `[对话...|all]` - 清除索引数据。
    `对话...`: 清除指定对话的索引。
    `all`: 清除所有对话的索引。
/stat - 查看后端索引状态和监控列表。
/find_chat_id `关键词` - 根据名称或用户名查找对话的 ID。
/refresh_chat_names - 强制刷新后端存储的对话名称缓存。
/usage - 查看机器人使用统计 (需要 Redis)。

**使用 /chats 选择对话后:**
- 回复带有 "☑️ 已选择" 的消息 + 搜索词，可仅搜索该对话。
- 回复带有 "☑️ 已选择" 的消息 + 管理命令 (如 /download_chat, /monitor_chat, /clear)，可对该对话执行操作 (如果命令本身支持)。
"""
    # 渲染搜索结果时，单条消息内容的最大显示字符数
    MAX_TEXT_DISPLAY_LENGTH = 250 # 增加默认长度以显示更多上下文
    # 高亮 HTML 片段的安全长度限制 (防止 Whoosh 生成极端过长的片段)
    MAX_HIGHLIGHT_HTML_LENGTH = 500 # 增加一点容错空间

    def __init__(self, common_cfg: CommonBotConfig, cfg: BotFrontendConfig, frontend_id: str, backend: BackendBot):
        """初始化 Frontend Bot"""
        self.backend = backend # 后端 Bot 实例
        self.id = frontend_id # 前端实例 ID
        self._common_cfg = common_cfg # 通用配置
        # 初始化 Telethon 客户端
        self.bot = TelegramClient(str(common_cfg.session_dir / f'frontend_{self.id}.session'),
                                  api_id=common_cfg.api_id, api_hash=common_cfg.api_hash, proxy=common_cfg.proxy)
        self._cfg = cfg # 前端特定配置

        # 初始化 Redis 连接或 FakeRedis
        if cfg.no_redis or cfg.redis_host is None:
            self._redis = FakeRedis() # 使用内存模拟
        else:
            try:
                # 尝试连接真实 Redis
                self._redis = Redis(host=cfg.redis_host[0], port=cfg.redis_host[1], decode_responses=True)
                self._redis.ping() # 测试连接
                logger.info(f"Successfully connected to Redis at {cfg.redis_host[0]}:{cfg.redis_host[1]}")
            except RedisConnectionError as e:
                logger.critical(f'Redis connection failed {cfg.redis_host}: {e}. Falling back to FakeRedis.')
                self._redis = FakeRedis(); self._cfg.no_redis = True # 连接失败，降级到 FakeRedis
            except RedisResponseError as e:
                # 处理可能的 Redis 配置错误 (例如密码错误或 MISCONF)
                logger.critical(f'Redis configuration error (e.g., auth, MISCONF?) {cfg.redis_host}: {e}. Falling back to FakeRedis.')
                self._redis = FakeRedis(); self._cfg.no_redis = True
            except Exception as e:
                # 处理其他未知 Redis 初始化错误
                logger.critical(f'Redis init unexpected error {cfg.redis_host}: {e}. Falling back to FakeRedis.')
                self._redis = FakeRedis(); self._cfg.no_redis = True

        self._logger = logger # 使用模块级 logger
        self._admin_id: Optional[int] = None # 解析后的管理员 share_id
        self.username: Optional[str] = None # Bot 自己的用户名
        self.my_id: Optional[int] = None # Bot 自己的用户 ID

        # Redis Keys 定义 (用于统计)
        self._TOTAL_USERS_KEY = f'{self.id}:total_users' # 存储所有互动过的用户 ID (Set)
        self._ACTIVE_USERS_KEY = f'{self.id}:active_users_15m' # 存储 15 分钟内活跃的用户 ID (Set with TTL)
        self._ACTIVE_USER_TTL = 900 # 活跃用户记录的过期时间 (15分钟)

        # --- 命令参数解析器定义 ---
        # /download_chat 命令的解析器
        self.download_arg_parser = ArgumentParser(prog="/download_chat", description="下载对话历史记录并索引", add_help=False, exit_on_error=False)
        self.download_arg_parser.add_argument('--min', type=int, default=0, help="起始消息 ID (不包含此 ID，从之后的消息开始)")
        self.download_arg_parser.add_argument('--max', type=int, default=0, help="结束消息 ID (不包含此 ID，0 表示无上限)")
        self.download_arg_parser.add_argument('chats', type=str, nargs='*', help="一个或多个对话的 ID、用户名或链接")

        # /monitor_chat 和 /clear 命令共用的解析器
        self.chat_ids_parser = ArgumentParser(prog="/monitor_chat | /clear", description="监控对话或清除索引", add_help=False, exit_on_error=False)
        self.chat_ids_parser.add_argument('chats', type=str, nargs='*', help="一个或多个对话的 ID、用户名或链接。对于 /clear，也可以是 'all'")


    async def start(self):
        """启动 Frontend Bot"""
        logger.info(f'Attempting to start frontend bot {self.id}...')
        # 1. 解析管理员 ID
        try:
            if not self._cfg.admin:
                # 如果配置中没有 admin_id，记录严重错误
                logger.critical("Admin ID ('admin_id') is not configured in the frontend config.")
                raise ValueError("Admin ID not configured.")
            # 使用 backend 的方法将配置的管理员标识符（可能是用户名或ID）解析为 share_id
            self._admin_id = await self.backend.str_to_chat_id(str(self._cfg.admin))
            logger.info(f"Admin identifier '{self._cfg.admin}' resolved to share_id: {self._admin_id}")
            # 如果启用了私密模式，自动将管理员加入白名单
            if self._cfg.private_mode and self._admin_id:
                self._cfg.private_whitelist.add(self._admin_id)
                logger.info(f"Admin {self._admin_id} automatically added to private whitelist.")
        except EntityNotFoundError:
            # 如果找不到管理员实体
            logger.critical(f"Admin entity '{self._cfg.admin}' not found by the backend session.")
            self._admin_id = None # 标记管理员 ID 无效
        except (ValueError, TypeError) as e:
            # 处理无效的管理员配置格式
            logger.critical(f"Invalid admin config '{self._cfg.admin}': {e}")
            self._admin_id = None
        except Exception as e:
            # 处理解析管理员 ID 过程中的其他未知错误
            logger.critical(f"Unexpected error resolving admin '{self._cfg.admin}': {e}", exc_info=True)
            self._admin_id = None

        # 如果管理员 ID 未能成功解析，记录警告
        if not self._admin_id:
            logger.error("Could not resolve a valid admin ID. Proceeding without admin-specific functionalities.")

        # 2. 再次检查 Redis 连接 (如果未使用 FakeRedis)
        if not isinstance(self._redis, FakeRedis):
             try:
                 self._redis.ping() # 尝试 PING Redis 服务器
                 logger.info(f"Redis connection confirmed at {self._cfg.redis_host}")
             except (RedisConnectionError, RedisResponseError) as e:
                 # 如果启动过程中 Redis 连接失败，记录严重错误并降级到 FakeRedis
                 logger.critical(f'Redis connection check failed during start: {e}. Falling back to FakeRedis.')
                 self._redis = FakeRedis()
                 self._cfg.no_redis = True # 标记 Redis 已禁用

        # 3. 启动 Telethon 客户端
        try:
            logger.info(f"Logging in with bot token...")
            # 使用配置的 bot_token 启动客户端
            await self.bot.start(bot_token=self._cfg.bot_token)
            # 获取机器人自身的信息
            me = await self.bot.get_me()
            if me:
                self.username, self.my_id = me.username, me.id
                logger.info(f'Bot login successful: @{self.username} (ID: {self.my_id})')
                # 将机器人自身的 ID 加入后端的排除列表，防止索引自身消息
                if self.my_id:
                    try:
                        bot_share_id = get_share_id(self.my_id)
                        self.backend.excluded_chats.add(bot_share_id)
                        logger.info(f"Bot's own ID {self.my_id} (share_id {bot_share_id}) excluded from backend indexing.")
                    except Exception as e:
                        logger.error(f"Failed to get share_id for bot's own ID {self.my_id}: {e}")
            else:
                # 如果获取自身信息失败，记录严重错误
                logger.critical("Failed to get bot's own information after login.")
                # 可能需要考虑是否要停止启动

            # 4. 注册 Bot 命令
            await self._register_commands()
            logger.info('Bot commands registered with Telegram.')

            # 5. 注册消息和回调处理钩子
            self._register_hooks()
            logger.info('Event handlers registered.')

            # 6. 发送启动成功消息给管理员 (如果管理员 ID 有效)
            if self._admin_id:
                 try:
                     # 获取当前的索引状态信息 (限制长度以防过长)
                     status_msg = await self.backend.get_index_status(length_limit = 4000 - 100) # 预留空间
                     # 向管理员发送启动成功和状态信息
                     await self.bot.send_message(
                         self._admin_id,
                         f'✅ Bot frontend 启动成功 ({self.id})\n\n{status_msg}',
                         parse_mode='html',
                         link_preview=False # 禁用链接预览
                     )
                 except Exception as e:
                     # 如果获取状态或发送消息失败，记录错误并发送简化的通知
                     logger.error(f"Failed to get/send initial status to admin {self._admin_id}: {e}", exc_info=True)
                     try:
                         await self.bot.send_message(self._admin_id, f'⚠️ Bot frontend ({self.id}) 启动，但获取初始状态失败: {type(e).__name__}')
                     except Exception as final_e:
                         logger.error(f"Failed even to send the simplified startup notification to admin: {final_e}")

            logger.info(f"Frontend bot {self.id} started successfully and is now running.")
        except Exception as e:
            # 捕获启动过程中的任何其他严重错误
            logger.critical(f"Frontend bot {self.id} failed to start: {e}", exc_info=True)
            # 可能需要在这里引发异常或退出程序，取决于部署方式
            # raise e

    def _track_user_activity(self, user_id: Optional[int]):
        """记录用户活动到 Redis (如果可用)，用于 /usage 统计"""
        # 如果 Redis 被禁用，或者 user_id 无效，或者 user_id 是机器人自身或管理员，则不记录
        if self._cfg.no_redis or not user_id or user_id == self.my_id or user_id == self._admin_id:
            return
        try:
            user_id_str = str(user_id) # Redis set 成员通常是字符串
            # 使用 Redis pipeline 批量执行命令以提高效率
            # (FakeRedis 的 pipeline 是空操作，直接执行)
            pipe = self._redis.pipeline()
            # 将用户 ID 添加到总用户集合 (永久)
            pipe.sadd(self._TOTAL_USERS_KEY, user_id_str)
            # 将用户 ID 添加到活跃用户集合
            pipe.sadd(self._ACTIVE_USERS_KEY, user_id_str)
            # 为活跃用户集合设置/刷新过期时间
            pipe.expire(self._ACTIVE_USERS_KEY, self._ACTIVE_USER_TTL)
            # 执行 pipeline 中的所有命令
            pipe.execute()
        except RedisResponseError as e:
            # 特别处理 Redis MISCONF 错误，这通常表示 Redis 配置问题 (如 RDB 保存失败)
            # 发生此错误时，降级到 FakeRedis 以免阻塞机器人功能
            if "MISCONF" in str(e) and not isinstance(self._redis, FakeRedis):
                 logger.error(f"Redis MISCONF error during usage tracking. Disabling Redis for this frontend instance. Error: {e}")
                 self._redis = FakeRedis() # 切换到 FakeRedis
                 self._cfg.no_redis = True # 标记为禁用
            else:
                 # 记录其他 Redis 响应错误
                 logger.warning(f"Redis ResponseError during usage tracking for user {user_id}: {e}")
        except RedisConnectionError as e:
            # 记录 Redis 连接错误
            logger.warning(f"Redis ConnectionError during usage tracking for user {user_id}: {e}")
            # 考虑是否在此处也降级到 FakeRedis
        except Exception as e:
            # 记录其他未知错误
            logger.warning(f"Unexpected error during usage tracking for user {user_id}: {e}", exc_info=True)

    async def _callback_handler(self, event: events.CallbackQuery.Event):
        """处理按钮回调查询 (CallbackQuery)"""
        try:
            # 记录回调的基本信息
            self._logger.info(f'Callback received: User={event.sender_id}, Chat={event.chat_id}, MsgID={event.message_id}, Data={event.data!r}')
            # 记录用户活动
            self._track_user_activity(event.sender_id)

            # 检查回调数据是否有效
            if not event.data:
                await event.answer("无效的回调操作。", alert=True)
                return
            try:
                # 回调数据通常是字节串，需要解码
                query_data = event.data.decode('utf-8')
            except Exception:
                await event.answer("无效的回调数据格式。", alert=True)
                return
            query_data = query_data.strip()
            if not query_data:
                await event.answer("空的回调操作。", alert=True)
                return

            # 解析回调数据：通常格式为 "action=value"
            parts = query_data.split('=', 1)
            if len(parts) != 2:
                await event.answer("回调操作格式错误。", alert=True)
                return
            action, value = parts[0], parts[1]

            # 定义 Redis 键的前缀和当前消息的标识符
            redis_prefix = f'{self.id}:' # 区分不同前端实例的缓存
            bot_chat_id, result_msg_id = event.chat_id, event.message_id

            # 定义用于存储搜索上下文的 Redis 键
            # 这些键包含了触发搜索的消息 ID，用于关联上下文
            query_key = f'{redis_prefix}query_text:{bot_chat_id}:{result_msg_id}'  # 存储原始搜索查询文本
            chats_key = f'{redis_prefix}query_chats:{bot_chat_id}:{result_msg_id}' # 存储搜索限定的对话 ID 列表 (逗号分隔)
            filter_key = f'{redis_prefix}query_filter:{bot_chat_id}:{result_msg_id}'# 存储当前的文件过滤器 ('all', 'text_only', 'file_only')
            page_key = f'{redis_prefix}query_page:{bot_chat_id}:{result_msg_id}'  # 存储当前显示的页码

            # --- Action 1: 处理翻页 ('search_page') 或 筛选 ('search_filter') ---
            if action == 'search_page' or action == 'search_filter':
                 # 从 Redis 获取与此消息关联的搜索上下文
                 current_filter = "all"; current_chats_str = None; current_query = None; current_page = 1
                 # 只有在 Redis 可用时才尝试获取
                 if not self._cfg.no_redis:
                     try:
                         # 使用 pipeline 一次性获取所有相关的键值
                         pipe = self._redis.pipeline()
                         pipe.get(filter_key)
                         pipe.get(chats_key)
                         pipe.get(query_key)
                         pipe.get(page_key)
                         results = pipe.execute() # 返回结果列表，顺序与 get 调用一致
                         # 解析获取到的值
                         current_filter = results[0] or "all" # 过滤器，默认为 'all'
                         current_chats_str = results[1]      # 对话 ID 字符串，可能为 None
                         current_query = results[2]          # 原始查询文本，可能为 None
                         current_page = int(results[3] or 1) # 当前页码，默认为 1
                     except (RedisResponseError, RedisConnectionError) as e:
                         # 处理 Redis 获取错误
                         self._logger.error(f"Redis error getting search context in callback ({bot_chat_id}:{result_msg_id}): {e}")
                         await event.answer("缓存服务暂时遇到问题，无法处理翻页/筛选。", alert=True)
                         return
                     except ValueError:
                          # 处理页码转换错误
                          self._logger.error(f"Invalid page number in Redis cache for {bot_chat_id}:{result_msg_id}")
                          await event.answer("缓存的页码无效。", alert=True)
                          return
                     except Exception as e:
                         # 处理其他未知错误
                         self._logger.error(f"Unexpected error getting context from Redis: {e}", exc_info=True)
                         await event.answer("获取搜索上下文时出错。", alert=True)
                         return

                 # 检查是否成功获取到原始查询文本，如果没有，则认为上下文已过期
                 if current_query is None:
                     try:
                         # 尝试编辑原消息提示用户过期
                         await event.edit("这次搜索的信息已过期，请重新发起搜索。", buttons=None) # 清除旧按钮
                     except Exception as edit_e:
                         self._logger.warning(f"Failed to edit message to show expired context: {edit_e}")
                     # 清理可能残留的 Redis 键
                     if not self._cfg.no_redis:
                         try: self._redis.delete(query_key, chats_key, filter_key, page_key)
                         except Exception as del_e: self._logger.error(f"Error deleting expired Redis keys: {del_e}")
                     await event.answer("搜索已过期。", alert=True)
                     return

                 # 根据回调的 action 和 value 确定新的页码和过滤器
                 new_page, new_filter = current_page, current_filter
                 if action == 'search_page':
                      # 如果是翻页操作
                      try:
                          new_page = int(value) # 获取目标页码
                          if new_page <= 0: raise ValueError("Page number must be positive")
                      except (ValueError, TypeError):
                          await event.answer("无效的页码。", alert=True)
                          return
                 else: # action == 'search_filter'
                      # 如果是筛选操作
                      # 验证新的过滤器值是否有效
                      temp_filter = value if value in ["all", "text_only", "file_only"] else "all"
                      # 如果过滤器发生了变化
                      if temp_filter != current_filter:
                           new_filter = temp_filter # 更新过滤器
                           new_page = 1 # 过滤器改变时，重置到第一页
                      # 如果过滤器未变，则页码和过滤器都保持不变 (相当于点击了当前选中的过滤器)

                 # 更新 Redis 中的上下文信息 (如果 Redis 可用且页码或过滤器有变化)
                 context_changed = (new_page != current_page or new_filter != current_filter)
                 if not self._cfg.no_redis and context_changed:
                     try:
                         pipe = self._redis.pipeline()
                         # 更新变化的键 (页码和过滤器) 并设置过期时间
                         pipe.set(page_key, new_page, ex=3600) # 1 小时过期
                         pipe.set(filter_key, new_filter, ex=3600)
                         # 刷新未变化的键 (查询文本和对话列表) 的过期时间
                         if current_query is not None: pipe.expire(query_key, 3600)
                         if current_chats_str is not None: pipe.expire(chats_key, 3600)
                         pipe.execute()
                     except (RedisResponseError, RedisConnectionError) as e:
                         # 记录更新 Redis 时的错误，但不中断操作
                         self._logger.error(f"Redis error updating search context in callback: {e}")
                         # 可以考虑通知用户，但可能影响体验

                 # 准备执行搜索
                 # 将存储的对话 ID 字符串转换回列表
                 chats = [int(cid) for cid in current_chats_str.split(',')] if current_chats_str else None
                 self._logger.info(f'Callback executing search: Query="{brief_content(current_query, 50)}", Chats={chats}, Filter={new_filter}, Page={new_page}')

                 # 调用后端执行搜索
                 start_time = time()
                 try:
                     result = self.backend.search(current_query, chats, self._cfg.page_len, new_page, file_filter=new_filter)
                 except Exception as e:
                     self._logger.error(f"Backend search failed during callback processing: {e}", exc_info=True)
                     await event.answer("后端搜索时发生错误。", alert=True)
                     return
                 search_time = time() - start_time

                 # 渲染新的搜索结果文本和按钮
                 response_text = await self._render_response_text(result, search_time)
                 new_buttons = self._render_respond_buttons(result, new_page, current_filter=new_filter)

                 # 尝试编辑原始消息以显示新结果
                 try:
                     await event.edit(response_text, parse_mode='html', buttons=new_buttons, link_preview=False)
                     await event.answer() # 向 Telegram 确认回调已处理
                 except rpcerrorlist.MessageNotModifiedError:
                     # 如果消息内容和按钮没有变化，也需要 answer
                     await event.answer("内容未改变。")
                 except rpcerrorlist.MessageIdInvalidError:
                     # 如果原始消息已被删除或无法访问
                     await event.answer("无法更新结果，原消息可能已被删除。", alert=True)
                 except rpcerrorlist.MessageTooLongError:
                      # 如果渲染后的消息仍然太长
                      self._logger.error(f"MessageTooLongError during callback edit (query: {brief_content(current_query)}). Truncated length: {len(response_text)}")
                      await event.answer("生成的搜索结果过长，无法显示。", alert=True)
                 except Exception as e:
                     # 处理编辑消息时的其他错误
                     self._logger.error(f"Failed to edit message during callback: {e}", exc_info=True)
                     await event.answer("更新搜索结果时出错。", alert=True)

            # --- Action 2: 处理选择聊天 ('select_chat') ---
            elif action == 'select_chat':
                 try:
                      # 获取选择的 chat_id (share_id)
                      chat_id = int(value)
                      try:
                          # 尝试获取对话名称
                          chat_name = await self.backend.translate_chat_id(chat_id)
                      except EntityNotFoundError:
                          # 如果找不到，使用占位符
                          chat_name = f"未知对话 ({chat_id})"
                      except Exception as e:
                           self._logger.error(f"Error translating chat_id {chat_id} in select_chat: {e}")
                           chat_name = f"对话 {chat_id} (获取名称出错)"

                      # 准备提示用户已选择对话的文本 (使用 Markdown)
                      reply_prompt = f'☑️ 已选择: **{html.escape(chat_name)}** (`{chat_id}`)\n\n请回复此消息以在此对话中搜索或执行管理操作。'
                      # 编辑原消息，显示提示，并移除按钮
                      await event.edit(reply_prompt, parse_mode='markdown', buttons=None)

                      # 将选择的 chat_id 存储到 Redis (如果可用)，以便后续回复可以识别上下文
                      if not self._cfg.no_redis:
                          try:
                              # 键名包含消息 ID，表示此选择与这条 "已选择" 消息相关联
                              select_key = f'{redis_prefix}select_chat:{bot_chat_id}:{result_msg_id}'
                              # 存储 chat_id，设置过期时间 (例如 1 小时)
                              self._redis.set(select_key, chat_id, ex=3600)
                              self._logger.info(f"Chat {chat_id} selected by user {event.sender_id} via message {result_msg_id}, context stored in Redis key {select_key}")
                          except (RedisResponseError, RedisConnectionError) as e:
                              # 记录 Redis 错误，并提示用户可能存在的问题
                              self._logger.error(f"Redis error setting selected chat context: {e}")
                              await event.answer("对话已选择，但缓存服务暂时遇到问题，后续操作可能受影响。", alert=True)
                          except Exception as e:
                              self._logger.error(f"Unexpected error setting selected chat context to Redis: {e}")
                              await event.answer("对话已选择，但保存上下文时出错。", alert=True)

                      # 向 Telegram 确认回调处理完成
                      await event.answer(f"已选择对话: {chat_name}")
                 except ValueError:
                     await event.answer("无效的对话 ID。", alert=True)
                 except Exception as e:
                     self._logger.error(f"Error processing select_chat callback: {e}", exc_info=True)
                     await event.answer("选择对话时发生内部错误。", alert=True)

            # --- Action 3: 处理占位按钮 ('noop') ---
            elif action == 'noop':
                # 对于仅用于显示的按钮 (如页码指示器)，只需 answer 即可
                await event.answer()

            # --- 处理未知的 action ---
            else:
                await event.answer(f"未知的操作类型: {action}", alert=True)

        # --- 通用错误处理 (捕获 Redis 错误和其他顶层异常) ---
        except (RedisResponseError, RedisConnectionError) as e:
            # 处理回调处理过程中发生的 Redis 错误
            self._logger.error(f"Redis error during callback processing: {e}")
            # 再次检查 MISCONF 错误以降级
            if "MISCONF" in str(e) and not self._cfg.no_redis and not isinstance(self._redis, FakeRedis):
                self._logger.error("Falling back to FakeRedis due to MISCONF error during callback processing.")
                self._redis = FakeRedis()
                self._cfg.no_redis = True
            try:
                # 尝试通知用户缓存问题
                await event.answer("缓存服务暂时遇到问题，请稍后再试或联系管理员。", alert=True)
            except Exception:
                pass # 如果连 answer 都失败，则忽略
        except Exception as e:
             # 捕获所有其他未预料的异常
             self._logger.error(f"Exception in callback handler: {e}", exc_info=True)
             try:
                 # 尝试向用户发送通用错误提示
                 await event.answer("处理您的请求时发生内部错误。", alert=True)
             except Exception as final_e:
                 # 如果连发送错误提示都失败，记录下来
                 self._logger.error(f"Failed to answer callback even after encountering an error: {final_e}")

    # --- 省略其他方法 (如 _normal_msg_handler, _admin_msg_handler 等) ---
    # --- 它们不需要修改 ---

    async def _render_response_text(self, result: SearchResult, used_time: float) -> str:
        """
        将搜索结果渲染为发送给用户的 HTML 文本。
        - 优化标题格式
        - 优化链接显示
        - 优先保留高亮（带长度检查）
        - 限制纯文本长度
        - 移除空消息占位符
        - 处理整体消息长度限制
        """
        # 如果结果无效或没有命中，返回提示信息
        if not isinstance(result, SearchResult) or not result.hits:
             return "没有找到相关的消息。"

        # 使用列表存储消息片段，最后 join
        sb = [f'找到 {result.total_results} 条结果，耗时 {used_time:.3f} 秒:\n\n']
        # 遍历当前页的命中结果
        for i, hit in enumerate(result.hits, 1): # 页内序号从 1 开始
            try:
                msg = hit.msg # 获取关联的 IndexMsg 对象
                # 健全性检查
                if not isinstance(msg, IndexMsg):
                     sb.append(f"<b>{i}.</b> 错误: 无效的消息数据结构。\n\n")
                     continue

                # 1. 获取并格式化对话标题
                try:
                    title = await self.backend.translate_chat_id(msg.chat_id)
                except EntityNotFoundError:
                    title = f"未知对话 ({msg.chat_id})"
                except Exception as te:
                    self._logger.warning(f"Error translating chat_id {msg.chat_id} for rendering: {te}")
                    title = f"对话 {msg.chat_id} (获取名称出错)"

                # 2. 构建消息头 (序号, 粗体标题, 代码块时间)
                hdr_parts = [f"<b>{i}. {html.escape(title)}</b>"] # 序号和转义后的标题
                if isinstance(msg.post_time, datetime):
                    hdr_parts.append(f'<code>[{msg.post_time.strftime("%y-%m-%d %H:%M")}]</code>') # 格式化时间
                else:
                    hdr_parts.append('<code>[无效时间]</code>') # 时间无效时的占位符
                sb.append(' '.join(hdr_parts) + '\n') # 添加消息头和换行

                # 3. 添加链接和消息内容
                link_added = False # 标记是否已添加主要链接 (文件或跳转)

                # 3.1 文件链接优先
                if msg.filename and msg.url:
                    # 如果有文件名和 URL，创建文件下载链接
                    sb.append(f'<a href="{html.escape(msg.url)}">📎 {html.escape(msg.filename)}</a>\n')
                    link_added = True
                elif msg.filename:
                    # 只有文件名，不可点击，直接显示
                     sb.append(f"📎 {html.escape(msg.filename)}\n")

                # 3.2 处理消息文本 (优先高亮，其次截断)
                display_text = ""
                # 检查 Whoosh 是否生成了高亮片段
                if hit.highlighted:
                    # 检查高亮 HTML 是否过长 (防止异常情况)
                    if len(hit.highlighted) < self.MAX_HIGHLIGHT_HTML_LENGTH:
                        # 长度在接受范围内，直接使用 Whoosh 生成的带 <b> 标签的 HTML
                        display_text = hit.highlighted
                        # self._logger.debug(f"Using highlighted content for {msg.url}: {display_text[:100]}...") # Debugging
                    else:
                        # 如果高亮 HTML 过长，则剥离 HTML 标签后截断，避免破坏消息格式
                        plain_highlighted = self._strip_html(hit.highlighted)
                        display_text = html.escape(brief_content(plain_highlighted, self.MAX_TEXT_DISPLAY_LENGTH))
                        self._logger.warning(f"Highlight HTML for {msg.url} was too long ({len(hit.highlighted)} chars). Using stripped and truncated plain text instead.")
                elif msg.content:
                    # 如果没有高亮片段，但有原始内容，则截断并转义原始内容
                    display_text = html.escape(brief_content(msg.content, self.MAX_TEXT_DISPLAY_LENGTH))
                # 如果既无高亮也无原始内容 (或原始内容为空)，display_text 保持为空字符串 ""

                # 3.3 添加 "跳转到消息" 链接 (如果前面未添加文件链接且有 URL)
                if not link_added and msg.url:
                    sb.append(f'<a href="{html.escape(msg.url)}">跳转到消息</a>\n')
                    link_added = True # 标记已添加链接

                # 3.4 添加处理后的文本内容 (如果非空)
                if display_text:
                    sb.append(f"{display_text}\n")
                # 如果 display_text 为空 (例如，无文本内容的消息或仅含文件的消息且未高亮文件名)，则不添加额外的空行

                # 4. 在每个结果后添加一个空行作为分隔
                sb.append("\n")

            except Exception as e:
                 # 捕获渲染单条结果时的错误
                 sb.append(f"<b>{i}.</b> 渲染此条结果时出错: {type(e).__name__}\n\n")
                 # 尝试安全地获取消息 URL 用于日志记录
                 msg_url = getattr(getattr(hit, 'msg', None), 'url', 'N/A')
                 self._logger.error(f"Error rendering search hit (msg URL: {msg_url}): {e}", exc_info=True)

        # 5. 处理 Telegram 消息长度限制 (4096 字符)
        final_text = ''.join(sb)
        max_len = 4096 # Telegram HTML 消息最大长度
        if len(final_text) > max_len:
             # 定义截断提示
             cutoff_msg = "\n\n...(结果过多，仅显示部分)"
             # 计算截断点，并尝试在最后一个完整结果后截断
             cutoff_point = max_len - len(cutoff_msg) - 10 # 留出余量
             # 从截断点向前查找最后一个双换行符 (通常是一个结果的结束)
             last_nl = final_text.rfind('\n\n', 0, cutoff_point)
             # 如果找到了双换行符，在其后截断，否则在计算出的截断点截断
             final_text = final_text[:last_nl if last_nl != -1 else cutoff_point] + cutoff_msg
             self._logger.warning(f"Search result text was truncated to {len(final_text)} characters.")

        return final_text

    def _strip_html(self, text: str) -> str:
        """简单的 HTML 标签剥离器，用于从高亮文本中获取纯文本"""
        # 使用正则表达式替换所有 <...> 标签为空字符串
        return re.sub('<[^>]*>', '', text) if text else ''

    # --- 省略 _render_respond_buttons 和其他未修改的方法 ---
    # --- 它们保持不变 ---

    def _render_respond_buttons(self, result: SearchResult, cur_page_num: int, current_filter: str = "all") -> Optional[List[List[Button]]]:
        """生成包含中文筛选和翻页按钮的列表 (中文)"""
        if not isinstance(result, SearchResult) or result.total_results == 0: # 如果没有结果，不显示按钮
            return None

        buttons = [] # 存储按钮行

        # --- 第一行：筛选按钮 (中文) ---
        filter_buttons = []
        filters = {"all": "全部", "text_only": "纯文本", "file_only": "仅文件"}
        for f_key, f_text in filters.items():
            # 如果是当前选中的过滤器，在文字两边加上【】
            button_text = f"【{f_text}】" if current_filter == f_key else f_text
            # 回调数据包含 action 和 value
            filter_buttons.append(Button.inline(button_text, f'search_filter={f_key}'))
        buttons.append(filter_buttons) # 添加筛选按钮行

        # --- 第二行：翻页按钮 (中文) ---
        try:
            # 计算总页数
            page_len = max(1, self._cfg.page_len) # 防止 page_len 为 0 或负数
            total_pages = (result.total_results + page_len - 1) // page_len
        except Exception as e:
            # 处理计算页数时可能发生的错误 (虽然不太可能)
            self._logger.error(f"Error calculating total pages: {e}")
            total_pages = 1 # 默认为 1 页

        # 只有当总页数大于 1 时才显示翻页按钮
        if total_pages > 1:
            page_buttons = []
            # 如果当前不是第一页，添加 "上一页" 按钮
            if cur_page_num > 1:
                page_buttons.append(Button.inline('⬅️ 上一页', f'search_page={cur_page_num - 1}'))

            # 添加页码指示器按钮 (例如 "2/10")，使用 noop action 表示不可点击
            page_buttons.append(Button.inline(f'{cur_page_num}/{total_pages}', 'noop'))

            # 如果当前不是最后一页，添加 "下一页" 按钮
            # result.is_last_page 是 Whoosh search_page 返回的标志
            # 同时检查 cur_page_num < total_pages 作为双重保险
            if not result.is_last_page and cur_page_num < total_pages:
                page_buttons.append(Button.inline('下一页 ➡️', f'search_page={cur_page_num + 1}'))

            # 如果生成了任何翻页按钮，则添加到按钮列表中
            if page_buttons:
                buttons.append(page_buttons)

        # 返回按钮列表 (如果为空则返回 None)
        return buttons if buttons else None
