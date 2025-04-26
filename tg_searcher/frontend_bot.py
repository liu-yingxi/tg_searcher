# -*- coding: utf-8 -*-
import html
import re # 用于剥离 HTML
from time import time
from typing import Optional, List, Tuple, Set, Union, Any, Dict # 添加 Dict
from datetime import datetime
from traceback import format_exc
from argparse import ArgumentParser, ArgumentError
import shlex
import asyncio

import redis
import whoosh.index # 用于捕获 LockError
from telethon import TelegramClient, events, Button
from telethon.tl.types import BotCommand, BotCommandScopePeer, BotCommandScopeDefault, MessageEntityMentionName, InputPeerUser, InputPeerChat, InputPeerChannel
from telethon.tl.custom import Message as TgMessage
from telethon.tl.functions.bots import SetBotCommandsRequest
import telethon.errors.rpcerrorlist as rpcerrorlist
from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError, ResponseError as RedisResponseError

# 项目内导入 (带 Fallback) - 使用包含文件索引的版本
try:
    from .common import CommonBotConfig, get_logger, get_share_id, remove_first_word, brief_content
    from .backend_bot import BackendBot, EntityNotFoundError
    from .indexer import SearchResult, IndexMsg, SearchHit # 确保 IndexMsg 和 SearchHit 被导入
except ImportError:
    # Fallback 定义保持不变，但注意 Indexer 相关类需要匹配包含文件字段的版本
    print("Warning: Assuming relative imports fail, define fallbacks if needed.")
    class CommonBotConfig: pass
    def get_logger(name): import logging; return logging.getLogger(name)
    def get_share_id(x): return int(x) if isinstance(x, (int, str)) and str(x).lstrip('-').isdigit() else 0
    def remove_first_word(s): return ' '.join(s.split()[1:]) if len(s.split()) > 1 else ''
    def brief_content(s, l=70): s=str(s); return (s[:l] + '...') if len(s) > l else s
    class BackendBot: pass
    class EntityNotFoundError(Exception):
        def __init__(self, entity='Unknown'): self.entity = entity; super().__init__(f"Entity not found: {entity}")
    class SearchResult:
        def __init__(self, hits=None, is_last_page=True, total_results=0, current_page=1):
            self.hits=hits or [];
            self.is_last_page=is_last_page;
            self.total_results=total_results
            self.current_page = current_page
    class IndexMsg: # Fallback 需要包含 filename 和 has_file
        def __init__(self, content='', url='', chat_id=0, post_time=None, sender='', filename=None):
            self.content = content
            self.url = url
            self.chat_id = chat_id
            self.post_time = post_time or datetime.now()
            self.sender = sender
            self.filename = filename
            self.has_file = 1 if filename else 0 # 增加 has_file
    class SearchHit:
        def __init__(self, msg: IndexMsg, highlighted: str):
            self.msg = msg
            self.highlighted = highlighted


logger = get_logger('frontend_bot')


class BotFrontendConfig:
    """存储 Frontend Bot 配置的类"""
    @staticmethod
    def _parse_redis_cfg(redis_cfg: str) -> Tuple[str, int]:
        """解析 Redis 'host:port' 配置字符串"""
        if not isinstance(redis_cfg, str) or not redis_cfg:
            raise ValueError("Invalid Redis config string")
        colon_idx = redis_cfg.find(':')
        if colon_idx < 0:
            return redis_cfg, 6379
        try:
            host = redis_cfg[:colon_idx] if colon_idx > 0 else 'localhost'
            port = int(redis_cfg[colon_idx + 1:])
            if port <= 0 or port > 65535: raise ValueError("Port out of range")
            return host, port
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid Redis host:port format in '{redis_cfg}': {e}")

    def __init__(self, **kw: Any):
        """从关键字参数初始化配置"""
        try:
            self.bot_token: str = kw['bot_token']
            self.admin: Union[int, str] = kw['admin_id']
        except KeyError as e:
            raise ValueError(f"Missing required config key: {e}")

        self.page_len: int = kw.get('page_len', 10)
        if not isinstance(self.page_len, int) or self.page_len <= 0:
            logger.warning(f"Invalid page_len '{self.page_len}', using default 10.")
            self.page_len = 10

        self.no_redis: bool = kw.get('no_redis', False)
        self.redis_host: Optional[Tuple[str, int]] = None

        if not self.no_redis:
             try:
                  redis_cfg = kw.get('redis', 'localhost:6379')
                  if redis_cfg:
                      self.redis_host = self._parse_redis_cfg(redis_cfg)
                  else:
                      logger.warning("Redis config string is empty. Disabling Redis.")
                      self.no_redis = True
             except ValueError as e:
                  logger.error(f"Error parsing redis config '{kw.get('redis')}': {e}. Disabling Redis.")
                  self.no_redis = True
             except KeyError:
                  logger.info("Redis config key 'redis' not found. Disabling Redis.")
                  self.no_redis = True

        self.private_mode: bool = kw.get('private_mode', False)
        self.private_whitelist: Set[int] = set()
        raw_whitelist = kw.get('private_whitelist', [])

        if isinstance(raw_whitelist, list):
             for item in raw_whitelist:
                 try:
                     self.private_whitelist.add(int(item))
                 except (ValueError, TypeError):
                     logger.warning(f"Could not parse private_whitelist item '{item}' as int.")
        elif raw_whitelist:
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
        v = self._data.get(key)
        if v:
            value, expiry = v
            if expiry is None or expiry > time():
                return value
            elif expiry <= time():
                if key in self._data: del self._data[key]
        return None

    def set(self, key, val, ex=None):
        expiry = time() + ex if ex is not None and isinstance(ex, (int, float)) and ex > 0 else None
        self._data[key] = (str(val), expiry)

    def delete(self, *keys):
        count = 0
        for k in keys:
            if k in self._data:
                del self._data[k]
                count += 1
        return count

    def ping(self):
        return True

    def sadd(self, key, *values):
        v = self._data.get(key)
        current_set = set()
        expiry = None
        added_count = 0
        if v and isinstance(v[0], set) and (v[1] is None or v[1] > time()):
            current_set, expiry = v
        elif v and (not isinstance(v[0], set) or (v[1] is not None and v[1] <= time())):
             if key in self._data: del self._data[key]
             expiry = None

        values_to_add = {str(v) for v in values}
        for val in values_to_add:
            if val not in current_set:
                current_set.add(val)
                added_count += 1

        self._data[key] = (current_set, expiry)
        return added_count

    def scard(self, key):
        v = self._data.get(key)
        if v and isinstance(v[0], set) and (v[1] is None or v[1] > time()):
            return len(v[0])
        elif v and v[1] is not None and v[1] <= time():
             if key in self._data: del self._data[key]
        return 0

    def expire(self, key, seconds):
        if key in self._data:
            value, current_expiry = self._data[key]
            if current_expiry is None or current_expiry > time():
                if isinstance(seconds, (int, float)) and seconds > 0:
                    new_expiry = time() + seconds
                    self._data[key] = (value, new_expiry)
                    return 1
                else:
                    del self._data[key]
                    return 1
            else:
                del self._data[key]
        return 0

    def pipeline(self):
        return self
    def execute(self):
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
    MAX_TEXT_DISPLAY_LENGTH = 120
    MAX_HIGHLIGHT_HTML_LENGTH = 300
    MAX_FILENAME_DISPLAY_LENGTH = 60

    def __init__(self, common_cfg: CommonBotConfig, cfg: BotFrontendConfig, frontend_id: str, backend: BackendBot):
        self.backend = backend
        self.id = frontend_id
        self._common_cfg = common_cfg
        self.bot = TelegramClient(str(common_cfg.session_dir / f'frontend_{self.id}.session'),
                                  api_id=common_cfg.api_id, api_hash=common_cfg.api_hash, proxy=common_cfg.proxy)
        self._cfg = cfg

        if cfg.no_redis or cfg.redis_host is None:
            self._redis = FakeRedis()
        else:
            try:
                self._redis = Redis(host=cfg.redis_host[0], port=cfg.redis_host[1], decode_responses=True)
                self._redis.ping()
                logger.info(f"Successfully connected to Redis at {cfg.redis_host[0]}:{cfg.redis_host[1]}")
            except RedisConnectionError as e:
                logger.critical(f'Redis connection failed {cfg.redis_host}: {e}. Falling back to FakeRedis.')
                self._redis = FakeRedis(); self._cfg.no_redis = True
            except RedisResponseError as e:
                logger.critical(f'Redis configuration error (e.g., auth, MISCONF?) {cfg.redis_host}: {e}. Falling back to FakeRedis.')
                self._redis = FakeRedis(); self._cfg.no_redis = True
            except Exception as e:
                logger.critical(f'Redis init unexpected error {cfg.redis_host}: {e}. Falling back to FakeRedis.')
                self._redis = FakeRedis(); self._cfg.no_redis = True

        self._logger = logger
        self._admin_id: Optional[int] = None
        self.username: Optional[str] = None
        self.my_id: Optional[int] = None
        
        # 使用固定的、所有实例共享的键名
        self._TOTAL_USERS_KEY = 'tgsearcher_shared:total_users'
        self._ACTIVE_USERS_KEY = 'tgsearcher_shared:active_users_15m'
        self._ACTIVE_USER_TTL = 900

        self.download_arg_parser = ArgumentParser(prog="/download_chat", description="下载对话历史记录并索引", add_help=False, exit_on_error=False)
        self.download_arg_parser.add_argument('--min', type=int, default=0, help="起始消息 ID (不包含此 ID，从之后的消息开始)")
        self.download_arg_parser.add_argument('--max', type=int, default=0, help="结束消息 ID (不包含此 ID，0 表示无上限)")
        self.download_arg_parser.add_argument('chats', type=str, nargs='*', help="一个或多个对话的 ID、用户名或链接")

        self.chat_ids_parser = ArgumentParser(prog="/monitor_chat | /clear", description="监控对话或清除索引", add_help=False, exit_on_error=False)
        self.chat_ids_parser.add_argument('chats', type=str, nargs='*', help="一个或多个对话的 ID、用户名或链接。对于 /clear，也可以是 'all'")


    async def start(self):
        logger.info(f'Attempting to start frontend bot {self.id}...')
        try:
            if not self._cfg.admin:
                logger.critical("Admin ID ('admin_id') is not configured in the frontend config.")
                raise ValueError("Admin ID not configured.")
            self._admin_id = await self.backend.str_to_chat_id(str(self._cfg.admin))
            logger.info(f"Admin identifier '{self._cfg.admin}' resolved to share_id: {self._admin_id}")
            if self._cfg.private_mode and self._admin_id:
                self._cfg.private_whitelist.add(self._admin_id)
                logger.info(f"Admin {self._admin_id} automatically added to private whitelist.")
        except EntityNotFoundError:
            logger.critical(f"Admin entity '{self._cfg.admin}' not found by the backend session.")
            self._admin_id = None
        except (ValueError, TypeError) as e:
            logger.critical(f"Invalid admin config '{self._cfg.admin}': {e}")
            self._admin_id = None
        except Exception as e:
            logger.critical(f"Unexpected error resolving admin '{self._cfg.admin}': {e}", exc_info=True)
            self._admin_id = None

        if not self._admin_id:
            logger.error("Could not resolve a valid admin ID. Proceeding without admin-specific functionalities.")

        if not isinstance(self._redis, FakeRedis):
             try:
                 self._redis.ping()
                 logger.info(f"Redis connection confirmed at {self._cfg.redis_host}")
             except (RedisConnectionError, RedisResponseError) as e:
                 logger.critical(f'Redis connection check failed during start: {e}. Falling back to FakeRedis.')
                 self._redis = FakeRedis()
                 self._cfg.no_redis = True

        try:
            logger.info(f"Logging in with bot token...")
            await self.bot.start(bot_token=self._cfg.bot_token)
            me = await self.bot.get_me()
            if me:
                self.username, self.my_id = me.username, me.id
                logger.info(f'Bot login successful: @{self.username} (ID: {self.my_id})')
                if self.my_id:
                    try:
                        bot_share_id = get_share_id(self.my_id)
                        self.backend.excluded_chats.add(bot_share_id)
                        logger.info(f"Bot's own ID {self.my_id} (share_id {bot_share_id}) excluded from backend indexing.")
                    except Exception as e:
                        logger.error(f"Failed to get share_id for bot's own ID {self.my_id}: {e}")
            else:
                logger.critical("Failed to get bot's own information after login.")

            await self._register_commands()
            self._register_hooks()
            logger.info('Event handlers registered.')

            if self._admin_id:
                 try:
                     status_msg = await self.backend.get_index_status(length_limit = 4000 - 100)
                     await self.bot.send_message(
                         self._admin_id,
                         f'✅ Bot frontend 启动成功 ({self.id})\n\n{status_msg}',
                         parse_mode='html',
                         link_preview=False
                     )
                 except Exception as e:
                     logger.error(f"Failed to get/send initial status to admin {self._admin_id}: {e}", exc_info=True)
                     try:
                         await self.bot.send_message(self._admin_id, f'⚠️ Bot frontend ({self.id}) 启动，但获取初始状态失败: {type(e).__name__}')
                     except Exception as final_e:
                         logger.error(f"Failed even to send the simplified startup notification to admin: {final_e}")

            logger.info(f"Frontend bot {self.id} started successfully and is now running.")
        except Exception as e:
            logger.critical(f"Frontend bot {self.id} failed to start: {e}", exc_info=True)
            raise e

    def _track_user_activity(self, user_id: Optional[int]):
        if self._cfg.no_redis or not user_id or user_id == self.my_id or user_id == self._admin_id:
            return
        try:
            user_id_str = str(user_id)
            pipe = self._redis.pipeline()
            pipe.sadd(self._TOTAL_USERS_KEY, user_id_str)
            pipe.sadd(self._ACTIVE_USERS_KEY, user_id_str)
            pipe.expire(self._ACTIVE_USERS_KEY, self._ACTIVE_USER_TTL)
            pipe.execute()
        except RedisResponseError as e:
            if "MISCONF" in str(e) and not isinstance(self._redis, FakeRedis):
                 logger.error(f"Redis MISCONF error during usage tracking. Disabling Redis for this frontend instance. Error: {e}")
                 self._redis = FakeRedis()
                 self._cfg.no_redis = True
            else:
                 logger.warning(f"Redis ResponseError during usage tracking for user {user_id}: {e}")
        except RedisConnectionError as e:
            logger.warning(f"Redis ConnectionError during usage tracking for user {user_id}: {e}")
        except Exception as e:
            logger.warning(f"Unexpected error during usage tracking for user {user_id}: {e}", exc_info=True)


    async def _callback_handler(self, event: events.CallbackQuery.Event):
        try:
            self._logger.info(f'Callback received: User={event.sender_id}, Chat={event.chat_id}, MsgID={event.message_id}, Data={event.data!r}')
            self._track_user_activity(event.sender_id)

            if not event.data:
                await event.answer("无效的回调操作。", alert=True)
                return
            try:
                query_data = event.data.decode('utf-8')
            except Exception:
                await event.answer("无效的回调数据格式。", alert=True)
                return
            query_data = query_data.strip()
            if not query_data:
                await event.answer("空的回调操作。", alert=True)
                return

            parts = query_data.split('=', 1)
            if len(parts) != 2:
                await event.answer("回调操作格式错误。", alert=True)
                return
            action, value = parts[0], parts[1]

            redis_prefix = f'{self.id}:'
            bot_chat_id, result_msg_id = event.chat_id, event.message_id

            query_key = f'{redis_prefix}query_text:{bot_chat_id}:{result_msg_id}'
            chats_key = f'{redis_prefix}query_chats:{bot_chat_id}:{result_msg_id}'
            filter_key = f'{redis_prefix}query_filter:{bot_chat_id}:{result_msg_id}'
            page_key = f'{redis_prefix}query_page:{bot_chat_id}:{result_msg_id}'

            if action == 'search_page' or action == 'search_filter':
                 current_filter = "all"; current_chats_str = None; current_query = None; current_page = 1
                 if not self._cfg.no_redis:
                     try:
                         pipe = self._redis.pipeline()
                         pipe.get(filter_key)
                         pipe.get(chats_key)
                         pipe.get(query_key)
                         pipe.get(page_key)
                         results = pipe.execute()
                         redis_filter, redis_chats_str, redis_query, redis_page = results

                         if redis_filter is not None: current_filter = redis_filter
                         if redis_chats_str is not None: current_chats_str = redis_chats_str
                         if redis_query is not None: current_query = redis_query
                         if redis_page is not None: current_page = int(redis_page)

                     except (RedisResponseError, RedisConnectionError) as e:
                         self._logger.error(f"Redis error getting search context in callback ({bot_chat_id}:{result_msg_id}): {e}")
                         await event.answer("缓存服务暂时遇到问题，无法处理翻页/筛选。", alert=True)
                         return
                     except ValueError:
                          self._logger.error(f"Invalid page number in Redis cache for {bot_chat_id}:{result_msg_id}")
                          current_page = 1
                          if not self._cfg.no_redis:
                              try: self._redis.delete(page_key)
                              except Exception: pass
                     except Exception as e:
                         self._logger.error(f"Unexpected error getting context from Redis: {e}", exc_info=True)
                         await event.answer("获取搜索上下文时出错。", alert=True)
                         return

                 if current_query is None:
                     try:
                         await event.edit("这次搜索的信息已过期，请重新发起搜索。", buttons=None)
                     except Exception as edit_e:
                         self._logger.warning(f"Failed to edit message to show expired context: {edit_e}")
                     if not self._cfg.no_redis:
                         try: self._redis.delete(query_key, chats_key, filter_key, page_key)
                         except Exception as del_e: self._logger.error(f"Error deleting expired Redis keys: {del_e}")
                     await event.answer("搜索已过期。", alert=True)
                     return

                 new_page, new_filter = current_page, current_filter
                 is_filter_action = (action == 'search_filter')

                 if action == 'search_page':
                      try:
                          new_page = int(value)
                          if new_page <= 0: raise ValueError("Page number must be positive")
                      except (ValueError, TypeError):
                          await event.answer("无效的页码。", alert=True)
                          return
                 else: # action == 'search_filter'
                      temp_filter = value if value in ["all", "text_only", "file_only"] else "all"
                      if temp_filter != current_filter:
                           new_filter = temp_filter
                           new_page = 1

                 context_changed = (new_page != current_page or new_filter != current_filter)
                 if not self._cfg.no_redis and context_changed:
                     try:
                         pipe = self._redis.pipeline()
                         pipe.set(page_key, new_page, ex=3600)
                         pipe.set(filter_key, new_filter, ex=3600)
                         if current_query is not None: pipe.expire(query_key, 3600)
                         if current_chats_str is not None: pipe.expire(chats_key, 3600)
                         pipe.execute()
                     except (RedisResponseError, RedisConnectionError) as e:
                         self._logger.error(f"Redis error updating search context in callback: {e}")

                 chats = [int(cid) for cid in current_chats_str.split(',')] if current_chats_str else None
                 self._logger.info(f'Callback executing search: Query="{brief_content(current_query, 50)}", Chats={chats}, Filter={new_filter}, Page={new_page}')

                 start_time = time()
                 response_text = ""
                 new_buttons = None
                 result = None
                 try:
                     if not current_query or current_query.isspace():
                         response_text = "关联的搜索关键词无效，请重新搜索。"
                         new_buttons = None
                     else:
                         result = self.backend.search(current_query, chats, self._cfg.page_len, new_page, file_filter=new_filter)
                         search_time = time() - start_time

                         if result.total_results == 0 and is_filter_action:
                             filter_map = {"text_only": "纯文本", "file_only": "仅文件"}
                             filter_name = filter_map.get(new_filter, new_filter)
                             response_text = (
                                 f"在 **{filter_name}** 筛选条件下，未找到与 "
                                 f"“<code>{html.escape(brief_content(current_query, 50))}</code>” 相关的消息。"
                             )
                             new_buttons = self._render_respond_buttons(result, new_page, current_filter=new_filter)
                         else:
                             response_text = await self._render_response_text(result, search_time)
                             new_buttons = self._render_respond_buttons(result, new_page, current_filter=new_filter)

                 except Exception as e:
                     self._logger.error(f"Backend search failed during callback processing: {e}", exc_info=True)
                     await event.answer("后端搜索时发生错误。", alert=True)
                     return

                 try:
                     if not response_text:
                         response_text = "处理时出现未知错误。"
                         self._logger.error("Response text became empty unexpectedly during callback handling.")

                     await event.edit(response_text, parse_mode='html', buttons=new_buttons, link_preview=False)
                     await event.answer()
                 except rpcerrorlist.MessageNotModifiedError:
                     await event.answer("内容未改变。")
                 except rpcerrorlist.MessageIdInvalidError:
                     await event.answer("无法更新结果，原消息可能已被删除。", alert=True)
                 except rpcerrorlist.MessageTooLongError:
                      self._logger.error(f"MessageTooLongError during callback edit (query: {brief_content(current_query)}). Truncated length: {len(response_text)}")
                      await event.answer("生成的搜索结果过长，无法显示。", alert=True)
                 except Exception as e:
                     self._logger.error(f"Failed to edit message during callback: {e}", exc_info=True)
                     await event.answer("更新搜索结果时出错。", alert=True)

            elif action == 'select_chat':
                 try:
                      chat_id = int(value)
                      try:
                          chat_name = await self.backend.translate_chat_id(chat_id)
                      except EntityNotFoundError:
                          chat_name = f"未知对话 ({chat_id})"
                      except Exception as e:
                           self._logger.error(f"Error translating chat_id {chat_id} in select_chat: {e}")
                           chat_name = f"对话 {chat_id} (获取名称出错)"

                      reply_prompt = f'☑️ 已选择: **{html.escape(chat_name)}** (`{chat_id}`)\n\n请回复此消息以在此对话中搜索或执行管理操作。'
                      await event.edit(reply_prompt, parse_mode='markdown', buttons=None, link_preview=False)

                      if not self._cfg.no_redis:
                          try:
                              select_key = f'{redis_prefix}select_chat:{bot_chat_id}:{result_msg_id}'
                              self._redis.set(select_key, chat_id, ex=3600)
                              self._logger.info(f"Chat {chat_id} selected by user {event.sender_id} via message {result_msg_id}, context stored in Redis key {select_key}")
                          except (RedisResponseError, RedisConnectionError) as e:
                              self._logger.error(f"Redis error setting selected chat context: {e}")
                              await event.answer("对话已选择，但缓存服务暂时遇到问题，后续操作可能受影响。", alert=True)
                          except Exception as e:
                              self._logger.error(f"Unexpected error setting selected chat context to Redis: {e}")
                              await event.answer("对话已选择，但保存上下文时出错。", alert=True)
                      else:
                           await event.answer(f"已选择对话: {chat_name} (无缓存)")

                 except ValueError:
                     await event.answer("无效的对话 ID。", alert=True)
                 except Exception as e:
                     self._logger.error(f"Error processing select_chat callback: {e}", exc_info=True)
                     await event.answer("选择对话时发生内部错误。", alert=True)

            elif action == 'noop':
                await event.answer()

            else:
                await event.answer(f"未知的操作类型: {action}", alert=True)

        except (RedisResponseError, RedisConnectionError) as e:
            self._logger.error(f"Redis error during callback processing: {e}")
            if "MISCONF" in str(e) and not self._cfg.no_redis and not isinstance(self._redis, FakeRedis):
                self._logger.error("Falling back to FakeRedis due to MISCONF error during callback processing.")
                self._redis = FakeRedis()
                self._cfg.no_redis = True
            try:
                await event.answer("缓存服务暂时遇到问题，请稍后再试或联系管理员。", alert=True)
            except Exception:
                pass
        except Exception as e:
             self._logger.error(f"Exception in callback handler: {e}", exc_info=True)
             try:
                 await event.answer("处理您的请求时发生内部错误。", alert=True)
             except Exception as final_e:
                 self._logger.error(f"Failed to answer callback even after encountering an error: {final_e}")



    async def _render_response_text(self, result: SearchResult, used_time: float) -> str:
        """将搜索结果渲染为发送给用户的 HTML 文本"""
        if not isinstance(result, SearchResult) or not result.hits:
             if isinstance(result, SearchResult) and result.total_results > 0:
                 return f"没有找到相关的消息 (页码无效？总共 {result.total_results} 条)。"
             return "没有找到相关的消息。"

        current_page = result.current_page
        total_pages = (result.total_results + self._cfg.page_len - 1) // self._cfg.page_len if self._cfg.page_len > 0 else 1
        sb = [f'共搜索到 {result.total_results} 个结果 (第 {current_page}/{total_pages} 页)，耗时 {used_time:.3f} 秒:\n\n']

        start_index = (current_page - 1) * self._cfg.page_len + 1
        for i, hit in enumerate(result.hits, start=start_index):
            try:
                msg = hit.msg
                if not isinstance(msg, IndexMsg):
                     sb.append(f"<b>{i}.</b> 错误: 无效的消息数据结构。\n\n")
                     continue
                if not msg.url:
                     sb.append(f"<b>{i}.</b> 错误: 消息缺少 URL。\n\n")
                     continue

                try:
                    title = await self.backend.translate_chat_id(msg.chat_id)
                except EntityNotFoundError:
                    title = f"未知对话 ({msg.chat_id})"
                except Exception as te:
                    self._logger.warning(f"Error translating chat_id {msg.chat_id} for rendering: {te}")
                    title = f"对话 {msg.chat_id} (获取名称出错)"

                hdr_parts = [f"<b>{i}. {html.escape(title)}</b>"]
                if isinstance(msg.post_time, datetime):
                    hdr_parts.append(f'<code>[{msg.post_time.strftime("%y-%m-%d %H:%M")}]</code>')
                else:
                    hdr_parts.append('<code>[无效时间]</code>')
                sb.append(' '.join(hdr_parts) + '\n')

                display_content = ""
                additional_content = ""
                link_text_type = "none"
                escaped_url = html.escape(msg.url)

                if msg.filename:
                    display_content = f"📎 {html.escape(brief_content(msg.filename, self.MAX_FILENAME_DISPLAY_LENGTH))}"
                    link_text_type = "filename"
                    if msg.content:
                        additional_content = html.escape(brief_content(msg.content, self.MAX_TEXT_DISPLAY_LENGTH))
                elif hit.highlighted:
                    if len(hit.highlighted) < self.MAX_HIGHLIGHT_HTML_LENGTH:
                        display_content = hit.highlighted
                        link_text_type = "highlight"
                    else:
                        plain_highlighted = self._strip_html(hit.highlighted)
                        display_content = html.escape(brief_content(plain_highlighted, self.MAX_TEXT_DISPLAY_LENGTH))
                        link_text_type = "content"
                        self._logger.debug(f"Highlight HTML for {msg.url} too long ({len(hit.highlighted)} chars > {self.MAX_HIGHLIGHT_HTML_LENGTH}). Using stripped/truncated plain text.")
                elif msg.content:
                    display_content = html.escape(brief_content(msg.content, self.MAX_TEXT_DISPLAY_LENGTH))
                    link_text_type = "content"
                else:
                     display_content = "[查看消息]"
                     link_text_type = "default"
                     self._logger.debug(f"Message {msg.url} has no filename or content, using default link text.")

                if display_content:
                    sb.append(f'<a href="{escaped_url}">{display_content}</a>\n')
                    if link_text_type == "filename" and additional_content:
                        sb.append(f"{additional_content}\n")
                else:
                    sb.append(f'<a href="{escaped_url}">[无法显示内容]</a>\n')
                    self._logger.warning(f"Failed to generate display_content for msg {msg.url}, even with fallback.")

                sb.append("\n")

            except Exception as e:
                 sb.append(f"<b>{i}.</b> 渲染此条结果时出错: {type(e).__name__}\n\n")
                 msg_url = getattr(getattr(hit, 'msg', None), 'url', 'N/A')
                 self._logger.error(f"Error rendering search hit (msg URL: {msg_url}): {e}", exc_info=True)

        final_text = ''.join(sb)
        max_len = 4096
        if len(final_text) > max_len:
             cutoff_msg = "\n\n...(结果过多，仅显示部分)"
             cutoff_point = max_len - len(cutoff_msg) - 20
             last_nl = final_text.rfind('\n\n', 0, cutoff_point)
             final_text = final_text[:last_nl if last_nl != -1 else cutoff_point] + cutoff_msg
             self._logger.warning(f"Search result text was truncated to {len(final_text)} characters.")

        return final_text.strip()

    def _strip_html(self, text: str) -> str:
        return re.sub('<[^>]*>', '', text) if text else ''

    def _render_respond_buttons(self, result: SearchResult, cur_page_num: int, current_filter: str = "all") -> Optional[List[List[Button]]]:
        if not isinstance(result, SearchResult):
            return None

        buttons = []
        filter_buttons = []
        filters = {"all": "全部", "text_only": "纯文本", "file_only": "仅文件"}
        for f_key, f_text in filters.items():
            button_text = f"【{f_text}】" if current_filter == f_key else f_text
            filter_buttons.append(Button.inline(button_text, f'search_filter={f_key}'))
        buttons.append(filter_buttons)

        if result.total_results > 0: # 只有在有结果时才计算和显示翻页按钮
            try:
                page_len = max(1, self._cfg.page_len)
                total_pages = (result.total_results + page_len - 1) // page_len
            except Exception as e:
                self._logger.error(f"Error calculating total pages: {e}")
                total_pages = 1

            if total_pages > 1:
                page_buttons = []
                if cur_page_num > 1:
                    page_buttons.append(Button.inline('⬅️ 上一页', f'search_page={cur_page_num - 1}'))
                page_buttons.append(Button.inline(f'{cur_page_num}/{total_pages}', 'noop'))
                if not result.is_last_page and cur_page_num < total_pages:
                    page_buttons.append(Button.inline('下一页 ➡️', f'search_page={cur_page_num + 1}'))
                if page_buttons:
                    buttons.append(page_buttons)

        return buttons if buttons else None

    async def _register_commands(self):
        user_commands = [
            BotCommand('s', '搜索消息 (支持关键词)'),
            BotCommand('search', '搜索消息 (同 /s)'),
            BotCommand('ss', '搜索消息 (同 /s)'),
            BotCommand('chats', '列出/筛选已索引对话 (支持关键词)'),
            BotCommand('random', '随机返回一条消息'),
            BotCommand('help', '显示帮助信息'),
        ]
        admin_commands = user_commands + [
            BotCommand('download_chat', '[选项] [对话...] - 下载并索引历史记录'),
            BotCommand('monitor_chat', '对话... - 添加对话到实时监控'),
            BotCommand('clear', '[对话...|all] - 清除索引数据'),
            BotCommand('stat', '查看后端索引状态'),
            BotCommand('find_chat_id', '关键词 - 查找对话 ID'),
            BotCommand('refresh_chat_names', '强制刷新对话名称缓存'),
            BotCommand('usage', '查看机器人使用统计'),
        ]

        try:
            await self.bot(SetBotCommandsRequest(
                scope=BotCommandScopeDefault(),
                lang_code='',
                commands=user_commands
            ))
            if self._admin_id:
                try:
                    admin_peer = await self.bot.get_input_entity(self._admin_id)
                    if not isinstance(admin_peer, (InputPeerUser, InputPeerChat, InputPeerChannel)):
                         logger.error(f"Resolved admin peer for {self._admin_id} is not a valid User/Chat/Channel type: {type(admin_peer)}")
                    else:
                        await self.bot(SetBotCommandsRequest(
                            scope=BotCommandScopePeer(peer=admin_peer),
                            lang_code='',
                            commands=admin_commands
                        ))
                        logger.info(f"Admin commands set successfully for admin {self._admin_id}.")
                except ValueError as e:
                    logger.error(f"Failed to get input entity for admin_id {self._admin_id} when setting commands: {e}")
                except Exception as e:
                    logger.error(f"An unexpected error occurred while setting admin commands for admin_id {self._admin_id}: {e}", exc_info=True)
            else:
                 logger.warning("Admin ID not valid, skipping setting admin-specific commands.")

            logger.info("Bot commands registration process completed.")
        except Exception as e:
            logger.error(f"Failed to set bot commands (possibly default commands): {e}", exc_info=True)

    def _register_hooks(self):
        self.bot.add_event_handler(self._callback_handler, events.CallbackQuery())
        self.bot.add_event_handler(self._message_dispatcher, events.NewMessage())
        logger.info("Message and callback handlers registered.")

    async def _message_dispatcher(self, event: events.NewMessage.Event):
        user_id = event.sender_id
        chat_id = event.chat_id
        message = event.message
        message_text = message.text if message else ""

        self._logger.info(f"Received message: User={user_id}, Chat={chat_id}, Text='{brief_content(message_text, 100)}', IsReply={event.is_reply}")
        self._track_user_activity(user_id)

        if self._cfg.private_mode:
            if user_id not in self._cfg.private_whitelist and user_id != self._admin_id:
                self._logger.warning(f"Ignoring message from user {user_id} due to private mode and not in whitelist.")
                return

        is_admin = (self._admin_id is not None and user_id == self._admin_id)

        is_command = message_text and message_text.startswith('/')
        command_handled = False
        if is_command:
             parts = message_text.split(maxsplit=1)
             command = parts[0].lower().lstrip('/')
             if self.username and command.endswith(f'@{self.username.lower()}'):
                 command = command[:-len(f'@{self.username.lower()}')]
             args_str = parts[1] if len(parts) > 1 else ""

             handler = None
             if command in ['s', 'search', 'ss']: handler = self._handle_search_cmd
             elif command == 'chats': handler = self._handle_chats_cmd
             elif command == 'random': handler = self._handle_random_cmd
             elif command == 'help': handler = self._handle_help_cmd
             elif is_admin:
                 if command == 'download_chat': handler = self._handle_download_cmd
                 elif command == 'monitor_chat': handler = self._handle_monitor_cmd
                 elif command == 'clear': handler = self._handle_clear_cmd
                 elif command == 'stat': handler = self._handle_stat_cmd
                 elif command == 'find_chat_id': handler = self._handle_find_chat_id_cmd
                 elif command == 'refresh_chat_names': handler = self._handle_refresh_names_cmd # <--- 确认分发
                 elif command == 'usage': handler = self._handle_usage_cmd

             if handler:
                 # **添加调试日志**
                 self._logger.debug(f"Dispatching command '{command}' to handler {handler.__name__}")
                 try:
                     await handler(event, args_str)
                     command_handled = True
                 except ArgumentError as e:
                      await event.reply(f"❌ 命令参数错误: {e}\n\n请使用 `/help` 查看用法。")
                      command_handled = True
                 except EntityNotFoundError as e:
                     await event.reply(f"❌ 操作失败: {e}")
                     command_handled = True
                 except whoosh.index.LockError:
                      logger.error("Index lock detected during command handling.")
                      await event.reply("⚠️ 索引当前正在被其他操作锁定，请稍后再试。")
                      command_handled = True
                 except Exception as e:
                     logger.error(f"Error handling command '{command}': {e}\n{format_exc()}")
                     await event.reply(f"🆘 处理命令时发生内部错误: {type(e).__name__}")
                     command_handled = True
             elif command:
                 logger.debug(f"Unknown command received: /{command}")
                 command_handled = True

        if not command_handled and message_text:
             mentioned = False
             if message and message.mentioned and message.entities:
                 for entity in message.entities:
                     if isinstance(entity, MessageEntityMentionName) and entity.user_id == self.my_id:
                         mentioned = True; break
             if event.is_private or mentioned:
                  query_text = message_text.strip()
                  if mentioned and self.username and query_text.lower().startswith(f'@{self.username.lower()}'):
                      query_text = remove_first_word(query_text).strip()
                  if query_text:
                      self._logger.info(f"Handling non-command text as search query: '{brief_content(query_text)}'")
                      try:
                          await self._handle_search_cmd(event, query_text)
                      except Exception as e:
                          logger.error(f"Error handling non-command search: {e}\n{format_exc()}")
                          await event.reply(f"🆘 执行搜索时发生内部错误: {type(e).__name__}")
                  else:
                      self._logger.debug("Ignoring message containing only mention or whitespace.")


    async def _handle_help_cmd(self, event: events.NewMessage.Event, args_str: str):
        is_admin = (self._admin_id is not None and event.sender_id == self._admin_id)
        help_text = self.HELP_TEXT_ADMIN if is_admin else self.HELP_TEXT_USER
        await event.reply(help_text, parse_mode='markdown', link_preview=False)

    async def _handle_search_cmd(self, event: events.NewMessage.Event, query_text: str):
        query_text = query_text.strip()
        if not query_text:
             await event.reply("请输入要搜索的关键词。")
             return

        self._logger.info(f"Executing search for query: '{brief_content(query_text)}'")

        target_chats: Optional[List[int]] = None
        selected_chat_id: Optional[int] = None
        self._logger.debug(f"Checking reply status for search: event.is_reply = {event.is_reply}")
        if event.is_reply:
             replied_msg = await event.get_reply_message()
             self._logger.debug(f"Attempting to get replied message: replied_msg found = {bool(replied_msg)}")
             if replied_msg and replied_msg.sender_id == self.my_id and replied_msg.text and '☑️ 已选择:' in replied_msg.text:
                 redis_read_success = False
                 if not self._cfg.no_redis:
                     try:
                         redis_prefix = f'{self.id}:'
                         select_key = f'{redis_prefix}select_chat:{event.chat_id}:{replied_msg.id}'
                         self._logger.debug(f"Attempting to read selected chat_id from Redis key: {select_key}")
                         cached_id = self._redis.get(select_key)
                         self._logger.debug(f"Value read from Redis: {cached_id!r}")
                         if cached_id:
                              selected_chat_id = int(cached_id)
                              redis_read_success = True
                     except (ValueError, TypeError, RedisConnectionError, RedisResponseError) as e:
                         self._logger.warning(f"Failed to get selected chat_id from Redis key {select_key}: {e}")

                 if selected_chat_id is None:
                     self._logger.debug(f"Redis unavailable or key not found, attempting to parse chat_id from replied text.")
                     try:
                         # 使用修正后的正则表达式
                         match = re.search(r'\(`(-?\d+)`\)', replied_msg.text)
                         if match:
                             selected_chat_id = int(match.group(1))
                             self._logger.debug(f"Parsed chat_id from text: {selected_chat_id}")
                         else:
                              self._logger.warning(f"Could not find chat_id pattern in replied text: {replied_msg.text}")
                     except (ValueError, TypeError):
                          self._logger.warning(f"Failed to parse chat_id from replied text: {replied_msg.text}")

                 if selected_chat_id:
                     target_chats = [selected_chat_id]
                     self._logger.info(f"Search restricted to selected chat {selected_chat_id} based on reply.")
                 else:
                      self._logger.warning(f"Detected reply to 'selected chat' message (Redis read success: {redis_read_success}), but failed to extract chat_id for search filtering.")

        start_time = time()
        try:
            result = self.backend.search(query_text, target_chats, self._cfg.page_len, 1, file_filter="all")
            search_time = time() - start_time
        except Exception as e:
            self._logger.error(f"Backend search call failed: {e}", exc_info=True)
            await event.reply(f"🆘 后端搜索时发生错误: {type(e).__name__}")
            return

        response_text = await self._render_response_text(result, search_time)
        buttons = self._render_respond_buttons(result, 1, current_filter="all")

        try:
            sent_msg = await event.reply(response_text, parse_mode='html', buttons=buttons, link_preview=False)

            if not self._cfg.no_redis and result.total_results > 0 and sent_msg:
                try:
                    redis_prefix = f'{self.id}:'
                    bot_chat_id, result_msg_id = sent_msg.chat_id, sent_msg.id
                    query_key = f'{redis_prefix}query_text:{bot_chat_id}:{result_msg_id}'
                    chats_key = f'{redis_prefix}query_chats:{bot_chat_id}:{result_msg_id}'
                    filter_key = f'{redis_prefix}query_filter:{bot_chat_id}:{result_msg_id}'
                    page_key = f'{redis_prefix}query_page:{bot_chat_id}:{result_msg_id}'

                    pipe = self._redis.pipeline()
                    pipe.set(query_key, query_text, ex=3600)
                    if target_chats:
                        pipe.set(chats_key, ','.join(map(str, target_chats)), ex=3600)
                    else:
                         pipe.delete(chats_key)
                    pipe.set(filter_key, "all", ex=3600)
                    pipe.set(page_key, 1, ex=3600)
                    pipe.execute()
                    self._logger.debug(f"Search context saved to Redis for msg {result_msg_id}. Query: '{brief_content(query_text)}', Chats: {target_chats}")
                except (RedisConnectionError, RedisResponseError) as e:
                    self._logger.error(f"Redis error saving search context: {e}")
                except Exception as e:
                    self._logger.error(f"Unexpected error saving search context to Redis: {e}", exc_info=True)

        except rpcerrorlist.MessageTooLongError:
            self._logger.error(f"MessageTooLongError sending initial search result (query: {brief_content(query_text)}).")
            await event.reply("❌ 搜索结果过长，无法显示。请尝试更精确的关键词。")
        except Exception as e:
            self._logger.error(f"Error sending search result: {e}", exc_info=True)
            await event.reply(f"🆘 发送搜索结果时发生错误: {type(e).__name__}")

    async def _handle_chats_cmd(self, event: events.NewMessage.Event, args_str: str):
        filter_query = args_str.strip()
        try:
            monitored_ids = self.backend.monitored_chats - self.backend.excluded_chats
            if not monitored_ids:
                 await event.reply("目前没有正在监控或已索引的对话。")
                 return

            tasks = [asyncio.create_task(self.backend.translate_chat_id(chat_id), name=f"translate-{chat_id}") for chat_id in monitored_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            valid_chats = {}
            fetch_errors = 0
            for chat_id, res in zip(monitored_ids, results):
                 if isinstance(res, Exception):
                     fetch_errors += 1
                     self._logger.warning(f"Error fetching name for chat {chat_id} in /chats: {res}")
                     valid_chats[chat_id] = f"对话 {chat_id} (获取名称出错)"
                 elif isinstance(res, str):
                     valid_chats[chat_id] = res
                 else:
                      fetch_errors += 1
                      valid_chats[chat_id] = f"对话 {chat_id} (未知类型: {type(res)})"

            if fetch_errors > 0:
                self._logger.warning(f"Encountered {fetch_errors} errors fetching chat names for /chats list.")

            filtered_chats = {}
            if filter_query:
                 filter_lower = filter_query.lower()
                 for chat_id, name in valid_chats.items():
                     if filter_lower in name.lower() or filter_query in str(chat_id):
                         filtered_chats[chat_id] = name
                 if not filtered_chats:
                      await event.reply(f"找不到名称或 ID 中包含“{html.escape(filter_query)}”的已索引对话。")
                      return
            else:
                 filtered_chats = valid_chats

            buttons = []
            sorted_chats = sorted(filtered_chats.items(), key=lambda item: item[1])
            max_buttons_per_row = 2
            max_total_buttons = 90
            current_row = []
            button_count = 0

            for chat_id, name in sorted_chats:
                 if button_count >= max_total_buttons:
                     self._logger.warning(f"/chats exceeded max button limit ({max_total_buttons}). Truncating list.")
                     break
                 button_text = brief_content(name, 30)
                 current_row.append(Button.inline(button_text, f'select_chat={chat_id}'))
                 button_count += 1
                 if len(current_row) == max_buttons_per_row:
                     buttons.append(current_row)
                     current_row = []

            if current_row: buttons.append(current_row)

            if not buttons:
                 if fetch_errors > 0 and not valid_chats:
                      await event.reply("获取对话列表时出错，请稍后再试。")
                 else:
                      await event.reply("找不到匹配的对话。" if filter_query else "目前没有已索引的对话。")
                 return

            message_text = f"找到 {len(filtered_chats)} 个匹配的已索引对话"
            if filter_query: message_text += f" (筛选条件: “{html.escape(filter_query)}”)"
            message_text += ":\n请点击下方按钮选择一个对话以进行后续操作。"
            if button_count >= max_total_buttons:
                message_text += "\n\n(列表过长，仅显示部分对话)"

            await event.reply(message_text, buttons=buttons)

        except Exception as e:
            self._logger.error(f"Error in /chats handler: {e}", exc_info=True)
            await event.reply("🆘 处理 /chats 命令时发生内部错误。")

    async def _handle_random_cmd(self, event: events.NewMessage.Event, args_str: str):
        try:
            random_msg = self.backend.rand_msg()
            if not random_msg or not isinstance(random_msg, IndexMsg):
                 await event.reply("无法获取随机消息。")
                 return

            fake_hit = SearchHit(random_msg, highlighted="")
            fake_result = SearchResult([fake_hit], is_last_page=True, total_results=1, current_page=1)
            response_text = await self._render_response_text(fake_result, 0.0)
            await event.reply(response_text, parse_mode='html', link_preview=False)

        except IndexError:
             await event.reply("索引库中没有任何消息可供随机选择。")
        except Exception as e:
             self._logger.error(f"Error handling /random: {e}", exc_info=True)
             await event.reply("🆘 获取随机消息时发生错误。")

    async def _handle_download_cmd(self, event: events.NewMessage.Event, args_str: str):
        if not (self._admin_id is not None and event.sender_id == self._admin_id): return

        try:
            args = self.download_arg_parser.parse_args(shlex.split(args_str))
        except ArgumentError as e:
            await event.reply(f"❌ 参数错误: {e}\n\n用法: `/download_chat [--min ID] [--max ID] [对话ID/用户名/链接...]`")
            return

        target_chats_input = args.chats
        min_id, max_id = args.min, args.max
        target_chat_identifiers: List[Union[int, str]] = list(target_chats_input)

        selected_chat_id: Optional[int] = None
        if not target_chat_identifiers and event.is_reply:
            replied_msg = await event.get_reply_message()
            if replied_msg and replied_msg.sender_id == self.my_id and replied_msg.text and '☑️ 已选择:' in replied_msg.text:
                 if not self._cfg.no_redis:
                     try:
                         redis_prefix = f'{self.id}:'
                         select_key = f'{redis_prefix}select_chat:{event.chat_id}:{replied_msg.id}'
                         cached_id = self._redis.get(select_key)
                         if cached_id: selected_chat_id = int(cached_id)
                     except Exception as e: self._logger.warning(f"Redis error getting chat_id for download: {e}")
                 if selected_chat_id is None:
                     try:
                         match = re.search(r'\(`(-?\d+)`\)', replied_msg.text)
                         if match: selected_chat_id = int(match.group(1))
                     except Exception: pass
                 if selected_chat_id:
                     target_chat_identifiers = [selected_chat_id]
                     self._logger.info(f"Download target set to {selected_chat_id} based on reply.")

        if not target_chat_identifiers:
            await event.reply("请指定至少一个对话的 ID、用户名、链接，或回复一个已选择的对话消息。")
            return

        if min_id < 0 or max_id < 0:
            await event.reply("❌ 消息 ID (min/max) 不能为负数。")
            return
        if max_id != 0 and max_id <= min_id:
            await event.reply("❌ 最大消息 ID (`--max`) 必须大于最小消息 ID (`--min`)。")
            return

        status_msg = await event.reply(f"⏳ 正在准备下载 {len(target_chat_identifiers)} 个对话...")
        success_count = 0
        fail_count = 0
        results_log = []
        last_update_time = 0

        async def progress_callback(chat_identifier: str, current_msg_id: int, dl_count: int):
            nonlocal last_update_time
            now = time()
            if (now - last_update_time > 5) or (dl_count > 0 and dl_count % 1000 == 0) :
                try:
                    await status_msg.edit(f"⏳ 正在下载 {chat_identifier}: 已处理约 {dl_count} 条消息 (当前 ID: {current_msg_id})...")
                    last_update_time = now
                except rpcerrorlist.MessageNotModifiedError: pass
                except rpcerrorlist.MessageIdInvalidError: pass
                except rpcerrorlist.FloodWaitError as flood_e:
                     logger.warning(f"Flood wait ({flood_e.seconds}s) while updating download progress for {chat_identifier}. Skipping update.")
                     await asyncio.sleep(flood_e.seconds + 1)
                     last_update_time = time()
                except Exception as e: logger.warning(f"Error updating download progress: {e}")

        tasks = []
        for chat_input in target_chat_identifiers:
             tasks.append(self._process_single_download(chat_input, min_id, max_id, progress_callback))

        download_results = await asyncio.gather(*tasks)

        for success, message in download_results:
            if success: success_count += 1
            else: fail_count += 1
            results_log.append(message)

        final_report = f"下载任务完成 ({success_count} 成功, {fail_count} 失败):\n\n" + "\n".join(results_log)
        max_report_len = 4000
        if len(final_report) > max_report_len:
             final_report = final_report[:max_report_len - 50] + "\n\n...(报告过长，已截断)"
        try:
            await status_msg.edit(final_report)
        except Exception as e:
            logger.error(f"Failed to edit final download status message: {e}")
            await event.reply(final_report)


    async def _process_single_download(self, chat_input: Union[int, str], min_id: int, max_id: int, progress_callback: callable) -> Tuple[bool, str]:
        chat_identifier = str(chat_input)
        share_id = -1
        try:
            # Resolve to share_id first for logging and identification
            share_id = await self.backend.str_to_chat_id(chat_input)
            chat_identifier = f"对话 {share_id}"
            try:
                chat_name = await self.backend.translate_chat_id(share_id)
            except Exception:
                chat_name = "(未知名称)"
            chat_identifier = f'"{html.escape(chat_name)}" ({share_id})' # Use html.escape for name

            start_dl_time = time()
            local_callback = lambda cur_id, count: progress_callback(chat_identifier, cur_id, count)
            # Pass the original chat_input to backend's download_history
            await self.backend.download_history(chat_input, min_id, max_id, call_back=local_callback)
            dl_time = time() - start_dl_time

            return True, f"✅ 成功下载并索引 {chat_identifier} (耗时 {dl_time:.2f} 秒)"

        except EntityNotFoundError as e:
            # Use the original input in the error message if share_id resolution failed early
            id_repr = share_id if share_id != -1 else html.escape(str(chat_input))
            return False, f"❌ 找不到对话 {id_repr}: {e}"
        except ValueError as e:
            id_repr = share_id if share_id != -1 else html.escape(str(chat_input))
            return False, f"❌ 无法下载 {id_repr}: {e}"
        except whoosh.index.LockError:
            id_repr = share_id if share_id != -1 else html.escape(str(chat_input))
            logger.error(f"Index locked during download history for {id_repr}")
            return False, f"❌ 索引被锁定，无法写入 {id_repr} 的数据。"
        except Exception as e:
            id_repr = share_id if share_id != -1 else html.escape(str(chat_input))
            logger.error(f"Error downloading history for {id_repr}: {e}", exc_info=True)
            return False, f"❌ 下载 {id_repr} 时发生未知错误: {type(e).__name__}"


    async def _handle_clear_cmd(self, event: events.NewMessage.Event, args_str: str):
        if not (self._admin_id is not None and event.sender_id == self._admin_id): return

        try:
            args = self.chat_ids_parser.parse_args(shlex.split(args_str))
        except ArgumentError as e:
            await event.reply(f"❌ 参数错误: {e}\n\n用法: `/clear [对话ID/用户名/链接... | all]`")
            return

        target_chats_input = args.chats
        clear_all = 'all' in [c.lower() for c in target_chats_input]
        target_chat_identifiers: List[Union[int, str]] = list(target_chats_input) if not clear_all else []

        selected_chat_id: Optional[int] = None
        if not target_chat_identifiers and not clear_all and event.is_reply:
            replied_msg = await event.get_reply_message()
            if replied_msg and replied_msg.sender_id == self.my_id and replied_msg.text and '☑️ 已选择:' in replied_msg.text:
                 if not self._cfg.no_redis:
                     try:
                         redis_prefix = f'{self.id}:'
                         select_key = f'{redis_prefix}select_chat:{event.chat_id}:{replied_msg.id}'
                         cached_id = self._redis.get(select_key)
                         if cached_id: selected_chat_id = int(cached_id)
                     except Exception as e: self._logger.warning(f"Redis error getting chat_id for clear: {e}")
                 if selected_chat_id is None:
                     try:
                         match = re.search(r'\(`(-?\d+)`\)', replied_msg.text)
                         if match: selected_chat_id = int(match.group(1))
                     except Exception: pass
                 if selected_chat_id:
                     target_chat_identifiers = [selected_chat_id]
                     self._logger.info(f"Clear target set to {selected_chat_id} based on reply.")

        if clear_all:
             confirm_key = f"{self.id}:confirm_clear_all:{event.chat_id}:{event.sender_id}"
             is_pending = False
             if not self._cfg.no_redis:
                 try:
                     if self._redis.get(confirm_key) == "pending":
                         is_pending = True
                         self._redis.delete(confirm_key)
                 except Exception as e:
                     logger.error(f"Redis error checking clear all confirmation: {e}")

             if is_pending:
                 status_msg = await event.reply("⏳ 确认收到，正在清除所有索引...")
                 try:
                     self.backend.clear(chat_ids=None)
                     await status_msg.edit("✅ 已清除所有索引数据。")
                 except whoosh.index.LockError:
                     logger.error("Index locked during clear all confirmation.")
                     await status_msg.edit("❌ 索引被锁定，无法清除全部数据。")
                 except Exception as e:
                     logger.error(f"Error clearing all index data after confirmation: {e}", exc_info=True)
                     await status_msg.edit(f"🆘 清除所有索引时发生错误: {type(e).__name__}")
             else:
                 try:
                     if not self._cfg.no_redis:
                         self._redis.set(confirm_key, "pending", ex=60)
                         await event.reply("⚠️ **警告!** 您确定要清除 **所有** 对话的索引数据吗？此操作不可恢复。\n\n**请在 60 秒内再次发送 `/clear all` 进行确认。**")
                     else:
                          await event.reply("⚠️ **警告!** 您确定要清除 **所有** 对话的索引数据吗？此操作不可恢复。\n\n**由于 Redis 未启用，无法进行二次确认。如果您确定，请再次发送 `/clear all --force` (此功能暂未实现，请先启用 Redis 或手动删除索引)。**")
                 except Exception as e:
                     logger.error(f"Error setting clear all confirmation: {e}")
                     await event.reply("设置确认状态时出错，请重试。")
                 return

        elif target_chat_identifiers:
             status_msg = await event.reply(f"⏳ 正在准备清除 {len(target_chat_identifiers)} 个对话的索引...")
             share_ids_to_clear = []
             results_log = []
             processed_inputs = set()

             for chat_input in target_chat_identifiers:
                 input_key = str(chat_input)
                 if input_key in processed_inputs: continue
                 processed_inputs.add(input_key)
                 try:
                     share_id = await self.backend.str_to_chat_id(chat_input)
                     share_ids_to_clear.append(share_id)
                     try: name = await self.backend.translate_chat_id(share_id)
                     except Exception: name = "(未知名称)"
                     results_log.append(f"准备清除: \"{html.escape(name)}\" ({share_id})")
                 except EntityNotFoundError:
                     results_log.append(f"❌ 找不到对话: {html.escape(str(chat_input))}")
                 except Exception as e:
                     results_log.append(f"❌ 解析对话时出错 {html.escape(str(chat_input))}: {type(e).__name__}")

             if not share_ids_to_clear:
                 await status_msg.edit("没有找到有效的对话进行清除。\n\n" + "\n".join(results_log))
                 return

             prep_report = "⏳ 准备清除以下对话的索引:\n\n" + "\n".join(results_log)
             max_prep_len = 3000
             if len(prep_report) > max_prep_len:
                  prep_report = prep_report[:max_prep_len] + "\n...(列表过长，已截断)"
             await status_msg.edit(prep_report, parse_mode='html') # Use HTML for name escaping
             await asyncio.sleep(1)

             try:
                 self.backend.clear(chat_ids=share_ids_to_clear)
                 await status_msg.edit(f"✅ 已清除指定的 {len(share_ids_to_clear)} 个对话的索引数据。")
             except whoosh.index.LockError:
                  logger.error("Index locked during specific chat clear.")
                  await status_msg.edit("❌ 索引被锁定，无法清除指定对话的数据。")
             except Exception as e:
                  logger.error(f"Error clearing specific chats: {e}", exc_info=True)
                  await status_msg.edit(f"🆘 清除指定对话时发生错误: {type(e).__name__}")

        else:
             await event.reply("请指定要清除的对话 ID/用户名/链接，或回复一个已选择的对话消息，或使用 `all` 清除全部。")


    async def _handle_stat_cmd(self, event: events.NewMessage.Event, args_str: str):
        if not (self._admin_id is not None and event.sender_id == self._admin_id): return
        status_msg = None
        try:
            status_msg = await event.reply("⏳ 正在获取后端状态...")
            status_text = await self.backend.get_index_status()
            await status_msg.edit(status_text, parse_mode='html', link_preview=False)
        except Exception as e:
            logger.error(f"Error getting/sending backend status: {e}", exc_info=True)
            err_reply = f"🆘 获取后端状态时出错: {type(e).__name__}"
            try:
                if status_msg: await status_msg.edit(err_reply)
                else: await event.reply(err_reply)
            except Exception as final_e:
                 logger.error(f"Failed to even send stat error message: {final_e}")

    async def _handle_find_chat_id_cmd(self, event: events.NewMessage.Event, args_str: str):
        if not (self._admin_id is not None and event.sender_id == self._admin_id): return
        query = args_str.strip()
        if not query:
            await event.reply("请输入要查找的对话关键词 (名称或用户名)。")
            return

        status_msg = None
        try:
            status_msg = await event.reply(f"⏳ 正在查找包含 “{html.escape(query)}” 的对话...")
            found_ids = await self.backend.find_chat_id(query)

            if not found_ids:
                 await status_msg.edit(f"找不到名称或用户名中包含 “{html.escape(query)}” 的对话。")
                 return

            results_text = [f"找到 {len(found_ids)} 个匹配对话:"]
            tasks = [asyncio.create_task(self.backend.translate_chat_id(chat_id), name=f"translate-{chat_id}") for chat_id in found_ids]
            names = await asyncio.gather(*tasks, return_exceptions=True)

            for chat_id, name_res in zip(found_ids, names):
                 if isinstance(name_res, Exception):
                     results_text.append(f"- 对话 `{chat_id}` (获取名称出错: {type(name_res).__name__})")
                 else:
                     results_text.append(f"- {html.escape(name_res)} (`{chat_id}`)")

            final_text = "\n".join(results_text)
            if len(final_text) > 4000:
                 final_text = final_text[:3950] + "\n\n...(结果过长，已截断)"
            await status_msg.edit(final_text, parse_mode='html')

        except Exception as e:
            logger.error(f"Error in /find_chat_id handler: {e}", exc_info=True)
            err_reply = f"🆘 查找对话 ID 时发生错误: {type(e).__name__}"
            try:
                 if status_msg: await status_msg.edit(err_reply)
                 else: await event.reply(err_reply)
            except Exception as final_e:
                 logger.error(f"Failed to even send find_chat_id error message: {final_e}")


    async def _handle_usage_cmd(self, event: events.NewMessage.Event, args_str: str):
        if not (self._admin_id is not None and event.sender_id == self._admin_id): return
        if self._cfg.no_redis or isinstance(self._redis, FakeRedis):
            await event.reply("⚠️ 无法获取使用统计，因为 Redis 未启用或连接失败。统计数据可能不准确或不可用。")

        status_msg = None
        try:
            status_msg = await event.reply("⏳ 正在获取使用统计...")
            pipe = self._redis.pipeline()
            pipe.scard(self._TOTAL_USERS_KEY)
            pipe.scard(self._ACTIVE_USERS_KEY)
            results = pipe.execute()

            total_users = results[0] if isinstance(results[0], int) else 0
            active_users = results[1] if isinstance(results[1], int) else 0

            usage_text = f"""
📊 **机器人使用统计 ({self.id})**

- **总互动用户数:** {total_users}
- **最近 15 分钟活跃用户数:** {active_users}
"""
            if isinstance(self._redis, FakeRedis):
                 usage_text += "\n\n*注意: 当前使用内存缓存，统计数据在重启后会丢失。*"

            await status_msg.edit(usage_text, parse_mode='markdown')

        except (RedisConnectionError, RedisResponseError) as e:
             logger.error(f"Redis error getting usage stats: {e}")
             err_reply = "❌ 获取统计时 Redis 出错，请检查连接或配置。"
             try:
                  if status_msg: await status_msg.edit(err_reply)
                  else: await event.reply(err_reply)
             except Exception: pass
        except Exception as e:
             logger.error(f"Error handling /usage: {e}", exc_info=True)
             err_reply = f"🆘 获取使用统计时发生内部错误: {type(e).__name__}"
             try:
                  if status_msg: await status_msg.edit(err_reply)
                  else: await event.reply(err_reply)
             except Exception: pass


    async def _handle_monitor_cmd(self, event: events.NewMessage.Event, args_str: str):
        """处理 /monitor_chat 命令 (管理员)"""
        # **添加日志：进入处理函数**
        self._logger.debug(f"Entering _handle_monitor_cmd with args: '{args_str}'")
        if not (self._admin_id is not None and event.sender_id == self._admin_id):
            self._logger.warning("Monitor command called by non-admin or admin_id is invalid.")
            return

        try:
            args = self.chat_ids_parser.parse_args(shlex.split(args_str))
        except ArgumentError as e:
            await event.reply(f"❌ 参数错误: {e}\n\n用法: `/monitor_chat [对话ID/用户名/链接...]`")
            return

        target_chats_input = args.chats
        target_chat_identifiers: List[Union[int, str]] = list(target_chats_input)

        selected_chat_id: Optional[int] = None
        if not target_chat_identifiers and event.is_reply:
            replied_msg = await event.get_reply_message()
            if replied_msg and replied_msg.sender_id == self.my_id and replied_msg.text and '☑️ 已选择:' in replied_msg.text:
                 if not self._cfg.no_redis:
                     try:
                         redis_prefix = f'{self.id}:'
                         select_key = f'{redis_prefix}select_chat:{event.chat_id}:{replied_msg.id}'
                         cached_id = self._redis.get(select_key)
                         if cached_id: selected_chat_id = int(cached_id)
                     except Exception as e: self._logger.warning(f"Redis error getting chat_id for monitor: {e}")
                 if selected_chat_id is None:
                     try:
                         match = re.search(r'\(`(-?\d+)`\)', replied_msg.text)
                         if match: selected_chat_id = int(match.group(1))
                     except Exception: pass
                 if selected_chat_id:
                     target_chat_identifiers = [selected_chat_id]
                     self._logger.info(f"Monitor target set to {selected_chat_id} based on reply.")

        if not target_chat_identifiers:
            await event.reply("请指定至少一个要监控的对话的 ID、用户名、链接，或回复一个已选择的对话消息。")
            return

        status_msg = await event.reply(f"⏳ 正在处理 {len(target_chat_identifiers)} 个对话的监控请求...")
        share_ids_to_monitor = []
        parse_results = []
        processed_inputs = set()

        for chat_input in target_chat_identifiers:
            input_key = str(chat_input)
            if input_key in processed_inputs: continue
            processed_inputs.add(input_key)
            try:
                share_id = await self.backend.str_to_chat_id(chat_input)
                share_ids_to_monitor.append(share_id)
                parse_results.append((True, chat_input, share_id))
            except EntityNotFoundError:
                parse_results.append((False, chat_input, f"找不到对话"))
            except Exception as e:
                parse_results.append((False, chat_input, f"解析时出错: {type(e).__name__}"))

        if not share_ids_to_monitor:
            error_report = "无法添加监控，原因如下:\n\n" + "\n".join([f"- {html.escape(str(inp))}: {err}" for success, inp, err in parse_results if not success])
            await status_msg.edit(error_report, parse_mode='html')
            return

        try:
            added_ok, add_failed = await self.backend.add_chats_to_monitoring(share_ids_to_monitor)

            report_lines = []
            name_tasks = {} # For fetching names concurrently
            # Prepare name fetching tasks for successful parses
            for success, inp, sid in parse_results:
                if success:
                    name_tasks[sid] = asyncio.create_task(self.backend.translate_chat_id(sid), name=f"translate-{sid}")
            # Also fetch names for failed adds if they were parsed correctly
            for sid in add_failed.keys():
                 if sid not in name_tasks: # Only fetch if not already fetching
                     name_tasks[sid] = asyncio.create_task(self.backend.translate_chat_id(sid), name=f"translate-{sid}")

            name_results = await asyncio.gather(*name_tasks.values(), return_exceptions=True)
            name_map = {}
            name_idx = 0
            for sid in name_tasks.keys():
                res = name_results[name_idx]
                if isinstance(res, Exception): name_map[sid] = "(获取名称出错)"
                else: name_map[sid] = res
                name_idx += 1

            # Build report
            for success, inp, sid_or_err in parse_results:
                 if success and sid_or_err in added_ok:
                      name = name_map.get(sid_or_err, "(未知名称)")
                      report_lines.append(f"✅ 已添加监控: \"{html.escape(name)}\" ({sid_or_err})")
                 elif not success:
                      report_lines.append(f"❌ 添加失败 ({html.escape(str(inp))}): {sid_or_err}")

            for sid, reason in add_failed.items():
                 name = name_map.get(sid, "(未知名称)")
                 report_lines.append(f"⚠️ 添加失败 ({html.escape(name)} {sid}): {reason}")


            final_report = "监控请求处理完成:\n\n" + "\n".join(report_lines)
            max_report_len = 4000
            if len(final_report) > max_report_len:
                 final_report = final_report[:max_report_len - 50] + "\n\n...(报告过长，已截断)"
            await status_msg.edit(final_report, parse_mode='html')

        except Exception as e:
            logger.error(f"Error calling backend to add monitoring: {e}", exc_info=True)
            await status_msg.edit(f"🆘 添加监控到后端时发生错误: {type(e).__name__}")


    async def _handle_refresh_names_cmd(self, event: events.NewMessage.Event, args_str: str):
        """处理 /refresh_chat_names 命令 (管理员)"""
        # **添加调试日志**
        self._logger.debug(f"Entering _handle_refresh_names_cmd. Admin check: admin_id={self._admin_id}, sender_id={event.sender_id}")
        if not (self._admin_id is not None and event.sender_id == self._admin_id):
             self._logger.warning("Refresh names command called by non-admin or admin_id invalid.")
             return # 如果不是管理员或管理员ID无效，则不执行任何操作

        status_msg = None
        try:
            # **添加调试日志**
            self._logger.debug("Admin verified. Sending status message...")
            status_msg = await event.reply("⏳ 正在请求后端刷新对话名称缓存...")
            # **添加调试日志**
            self._logger.debug("Calling backend session refresh_translate_table...")
            # 调用后端 session 的刷新方法
            await self.backend.session.refresh_translate_table()
            # **添加调试日志**
            self._logger.debug("Backend refresh complete. Editing status message...")
            await status_msg.edit("✅ 后端对话名称缓存已刷新。")
        except Exception as e:
             logger.error(f"Error refreshing chat names: {e}", exc_info=True)
             err_reply = f"🆘 刷新对话名称缓存时出错: {type(e).__name__}"
             try:
                  if status_msg: await status_msg.edit(err_reply)
                  else: await event.reply(err_reply) # 如果发送初始消息失败，则回复错误
             except Exception as final_e:
                  logger.error(f"Failed to send refresh_chat_names error message: {final_e}")


    async def run_until_disconnected(self):
        """运行客户端直到断开连接"""
        logger.info(f"Frontend bot {self.id} is running...")
        await self.bot.run_until_disconnected()
