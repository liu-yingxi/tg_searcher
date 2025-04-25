# -*- coding: utf-8 -*-
import html
import re # [新增] 导入 re 用于剥离 HTML 标签
from time import time
from typing import Optional, List, Tuple, Set, Union, Any
from datetime import datetime
from traceback import format_exc
from argparse import ArgumentParser, ArgumentError # 导入 ArgumentError
import shlex
import asyncio # [新增] 导入 asyncio

import redis
import whoosh.index # 导入 whoosh.index 以便捕获 LockError
from telethon import TelegramClient, events, Button
from telethon.tl.types import BotCommand, BotCommandScopePeer, BotCommandScopeDefault, MessageEntityMentionName
from telethon.tl.custom import Message as TgMessage
from telethon.tl.functions.bots import SetBotCommandsRequest
import telethon.errors.rpcerrorlist as rpcerrorlist
from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError, ResponseError as RedisResponseError # [修改] 导入 RedisResponseError

# Assuming these imports work from your project structure
try:
    from .common import CommonBotConfig, get_logger, get_share_id, remove_first_word, brief_content
    from .backend_bot import BackendBot, EntityNotFoundError
    from .indexer import SearchResult, IndexMsg # 确保 IndexMsg 已更新
except ImportError:
    # Provide fallback or handle appropriately if run standalone
    print("Warning: Assuming relative imports fail, define fallbacks if needed.")
    # Define minimal fallbacks if necessary for the script to load without error
    class CommonBotConfig: pass
    def get_logger(name): import logging; return logging.getLogger(name)
    def get_share_id(x): return int(x) if x else 0
    def remove_first_word(s): return ' '.join(s.split()[1:]) if len(s.split()) > 1 else ''
    # [修改] 更新 brief_content 的默认值以匹配新需求，但函数调用处会覆盖
    def brief_content(s, l=35): return (s[:l] + '...') if len(s) > l else s
    class BackendBot: pass
    class EntityNotFoundError(Exception): pass
    class SearchResult: pass
    class IndexMsg: pass

# 获取日志记录器
logger = get_logger('frontend_bot')


class BotFrontendConfig:
    """存储 Frontend Bot 配置的类"""
    @staticmethod
    def _parse_redis_cfg(redis_cfg: str) -> Tuple[str, int]:
        """解析 Redis 'host:port' 配置字符串"""
        colon_idx = redis_cfg.find(':')
        if colon_idx < 0: return redis_cfg, 6379 # 无端口则使用默认 6379
        try:
            host = redis_cfg[:colon_idx] if colon_idx > 0 else 'localhost'
            port = int(redis_cfg[colon_idx + 1:])
            return host, port
        except (ValueError, TypeError): raise ValueError(f"Invalid Redis port in '{redis_cfg}'")

    def __init__(self, **kw: Any):
        """从关键字参数初始化配置"""
        try:
            self.bot_token: str = kw['bot_token']
            self.admin: Union[int, str] = kw['admin_id'] # 管理员 ID 或用户名
        except KeyError as e: raise ValueError(f"Missing required config key: {e}")

        self.page_len: int = kw.get('page_len', 10) # 搜索结果每页显示条数
        if self.page_len <= 0: logger.warning("page_len must be positive, using 10."); self.page_len = 10

        self.no_redis: bool = kw.get('no_redis', False) # 是否禁用 Redis
        self.redis_host: Optional[Tuple[str, int]] = None # Redis 主机和端口
        if not self.no_redis:
             try:
                  redis_cfg = kw.get('redis', 'localhost:6379') # 获取 Redis 配置
                  if redis_cfg: self.redis_host = self._parse_redis_cfg(redis_cfg)
                  else: logger.warning("Redis config empty. Disabling redis."); self.no_redis = True
             except ValueError as e: logger.error(f"Error parsing redis config '{kw.get('redis')}': {e}. Disabling redis."); self.no_redis = True
             except KeyError: logger.info("Redis config key 'redis' not found. Disabling redis."); self.no_redis = True

        self.private_mode: bool = kw.get('private_mode', False) # 是否开启私聊模式
        self.private_whitelist: Set[int] = set() # 私聊模式白名单 (用户ID或对话ID)
        raw_whitelist = kw.get('private_whitelist', [])
        if isinstance(raw_whitelist, list):
             for item in raw_whitelist:
                 try: self.private_whitelist.add(int(item))
                 except (ValueError, TypeError): logger.warning(f"Could not parse whitelist item '{item}' as int.")
        elif raw_whitelist: logger.warning("private_whitelist format incorrect (expected list), ignoring.")
        # 管理员 ID 会在 Bot 启动时自动加入白名单


# FakeRedis (用于无 Redis 环境的内存模拟)
class FakeRedis:
    """一个简单的内存字典，模拟部分 Redis 功能，用于在无 Redis 环境下运行"""
    def __init__(self):
        self._data = {} # 存储格式: { key: (value, expiry_timestamp_or_None) }
        self._logger = get_logger('FakeRedis')
        self._logger.warning("Using FakeRedis: Data is volatile and will be lost on restart.")
    def get(self, key):
        v = self._data.get(key)
        if v and (v[1] is None or v[1] > time()): return v[0] # 存在且未过期
        elif v and v[1] is not None and v[1] <= time(): del self._data[key] # 已过期，删除
        return None
    def set(self, key, val, ex=None):
        expiry = time() + ex if ex else None
        self._data[key] = (str(val), expiry) # 存储字符串值
    def delete(self, *keys):
        count = 0
        for k in keys:
            if k in self._data: del self._data[k]; count += 1
        return count
    def ping(self): return True # 总是认为连接正常
    def sadd(self, key, *values):
        """模拟 SADD"""
        current_set, expiry = self._data.get(key, (set(), None))
        if not isinstance(current_set, set): current_set = set()
        if expiry is not None and expiry <= time(): current_set = set(); expiry = None
        added_count = 0
        str_values = {str(v) for v in values}
        for v in str_values:
            if v not in current_set: current_set.add(v); added_count += 1
        self._data[key] = (current_set, expiry)
        return added_count
    def scard(self, key):
        """模拟 SCARD"""
        v = self._data.get(key)
        if v and isinstance(v[0], set) and (v[1] is None or v[1] > time()): return len(v[0])
        elif v and v[1] is not None and v[1] <= time(): del self._data[key]
        return 0
    def expire(self, key, seconds):
        """模拟 EXPIRE"""
        if key in self._data:
            value, _ = self._data[key]
            self._data[key] = (value, time() + seconds)
            return 1
        return 0


class BotFrontend:
    """处理用户交互、命令解析、结果展示的前端 Bot 类"""
    # 帮助文本 (保持不变)
    HELP_TEXT_USER = """
**可用命令:**
/s `关键词` - 搜索消息 (或 `/search`, `/ss`；直接发送也可)。
/chats `[关键词]` - 列出/选择已索引对话。
/random - 返回一条随机消息。
/help - 显示此帮助信息。

**使用 /chats 选择对话后:**
- 回复选择成功的消息 + 搜索词，可仅搜索该对话。
"""
    HELP_TEXT_ADMIN = """
**通用命令:**
/s `关键词` - 搜索消息 (或 `/search`, `/ss`；直接发送也可)。
/chats `[关键词]` - 列出/选择已索引对话。
/random - 返回一条随机消息。
/help - 显示此帮助信息。

**管理员命令:**
/download_chat `[选项] [对话...]` - 下载并索引对话历史。
/monitor_chat `对话...` - 将对话加入实时监听。
/clear `[对话...|all]` - 清除索引。
/stat - 查看后端状态。
/find_chat_id `关键词` - 根据名称查找对话 ID。
/refresh_chat_names - 刷新后端对话名称缓存。
/usage - 查看机器人使用统计。

**使用 /chats 选择对话后:**
- 回复选择成功的消息 + 搜索词，可仅搜索该对话。
- 回复选择成功的消息 + 管理命令 (如 /download_chat)，可对该对话执行操作。
"""
    # [新增] 文本显示最大字符数
    MAX_TEXT_DISPLAY_LENGTH = 35

    def __init__(self, common_cfg: CommonBotConfig, cfg: BotFrontendConfig, frontend_id: str, backend: BackendBot):
        self.backend = backend # 后端 Bot 实例
        self.id = frontend_id # 前端 ID (用于 Redis Key 等)
        self._common_cfg = common_cfg # 通用配置
        # 初始化 Telethon 客户端
        self.bot = TelegramClient(str(common_cfg.session_dir / f'frontend_{self.id}.session'),
                                  api_id=common_cfg.api_id, api_hash=common_cfg.api_hash, proxy=common_cfg.proxy)
        self._cfg = cfg # 前端特定配置
        # 初始化 Redis 连接或 FakeRedis
        self._redis: Union[redis.client.Redis, FakeRedis]
        if cfg.no_redis or cfg.redis_host is None:
            self._redis = FakeRedis()
        else:
            try:
                self._redis = Redis(host=cfg.redis_host[0], port=cfg.redis_host[1], decode_responses=True)
                self._redis.ping()
            except RedisConnectionError as e:
                logger.critical(f'Redis connection failed {cfg.redis_host}: {e}. Falling back to FakeRedis.')
                self._redis = FakeRedis()
                self._cfg.no_redis = True
            # [修改] 捕获 Redis 配置错误 (MISCONF)
            except RedisResponseError as e:
                logger.critical(f'Redis configuration error (MISCONF?) {cfg.redis_host}: {e}. Falling back to FakeRedis.')
                self._redis = FakeRedis()
                self._cfg.no_redis = True # 强制禁用 Redis，因为服务器配置有问题
            except Exception as e:
                logger.critical(f'Redis init error {cfg.redis_host}: {e}. Falling back to FakeRedis.')
                self._redis = FakeRedis()
                self._cfg.no_redis = True

        self._logger = logger
        self._admin_id: Optional[int] = None # 管理员 User ID
        self.username: Optional[str] = None # Bot 的用户名
        self.my_id: Optional[int] = None # Bot 自身的 User ID

        # Redis Keys for stats
        self._TOTAL_USERS_KEY = f'{self.id}:total_users'
        self._ACTIVE_USERS_KEY = f'{self.id}:active_users_15m'
        self._ACTIVE_USER_TTL = 900 # 15 minutes in seconds

        # Argument Parsers for commands
        self.download_arg_parser = ArgumentParser(prog="/download_chat", description="下载对话历史", add_help=False, exit_on_error=False)
        self.download_arg_parser.add_argument('--min', type=int, default=0, help="最小消息 ID (默认: 0)")
        self.download_arg_parser.add_argument('--max', type=int, default=0, help="最大消息 ID (0 = 无限制)")
        self.download_arg_parser.add_argument('chats', type=str, nargs='*', help="对话 ID 或用户名/链接列表")

        self.chat_ids_parser = ArgumentParser(prog="/monitor_chat | /clear", description="监控或清除对话索引", add_help=False, exit_on_error=False)
        self.chat_ids_parser.add_argument('chats', type=str, nargs='*', help="对话 ID/用户名列表, 或 'all' (仅用于 /clear)")

    async def start(self):
        """启动 Frontend Bot"""
        # 解析管理员 ID
        try:
            if not self._cfg.admin: raise ValueError("Admin ID not configured.")
            self._admin_id = await self.backend.str_to_chat_id(str(self._cfg.admin)) # 使用后端转换
            self._logger.info(f"Admin ID resolved to: {self._admin_id}")
            # 如果是私聊模式，自动将管理员加入白名单
            if self._cfg.private_mode and self._admin_id:
                self._cfg.private_whitelist.add(self._admin_id)
                self._logger.info(f"Admin added to private whitelist.")
        except EntityNotFoundError:
            self._logger.critical(f"Admin entity '{self._cfg.admin}' not found.")
            self._admin_id = None
        except (ValueError, TypeError) as e:
            self._logger.critical(f"Invalid admin config '{self._cfg.admin}': {e}")
            self._admin_id = None
        except Exception as e:
            self._logger.critical(f"Error resolving admin '{self._cfg.admin}': {e}", exc_info=True)
            self._admin_id = None

        if not self._admin_id:
            self._logger.error("Proceeding without valid admin ID. Admin commands might not work correctly.")

        # 再次检查 Redis 连接
        if not isinstance(self._redis, FakeRedis):
             try:
                 self._redis.ping()
                 self._logger.info(f"Redis connected at {self._cfg.redis_host}")
             except RedisConnectionError as e:
                 self._logger.critical(f'Redis check failed during start: {e}. Falling back to FakeRedis.')
                 self._redis = FakeRedis()
                 self._cfg.no_redis = True
             # [修改] 启动时也检查 Redis 配置错误
             except RedisResponseError as e:
                 self._logger.critical(f'Redis configuration error (MISCONF?) during start {self._cfg.redis_host}: {e}. Falling back to FakeRedis.')
                 self._redis = FakeRedis()
                 self._cfg.no_redis = True

        self._logger.info(f'Starting frontend bot {self.id}...')
        try:
            # 启动 Telethon 客户端
            await self.bot.start(bot_token=self._cfg.bot_token)
            me = await self.bot.get_me(); assert me is not None
            self.username, self.my_id = me.username, me.id
            self._logger.info(f'Bot (@{self.username}, id={self.my_id}) login ok')
            # 将 Bot 自身 ID 加入后端排除列表
            if self.my_id:
                try:
                    self.backend.excluded_chats.add(get_share_id(self.my_id))
                    self._logger.info(f"Bot ID {self.my_id} excluded from backend monitoring.")
                except Exception as e:
                    self._logger.warning(f"Failed to exclude bot id {self.my_id} from backend: {e}")
            # 注册命令和事件处理钩子
            await self._register_commands(); self._logger.info(f'Commands registered.')
            self._register_hooks()

            # 向管理员发送启动成功消息和初始状态
            if self._admin_id:
                 try:
                     status_msg = await self.backend.get_index_status(4000 - 100) # 留出更多余地
                     msg = f'✅ Bot frontend init complete ({self.id})\n\n{status_msg}'
                     await self.bot.send_message(self._admin_id, msg, parse_mode='html', link_preview=False)
                 except Exception as e:
                     self._logger.error(f"Failed get/send initial status: {e}", exc_info=True)
                     await self.bot.send_message(self._admin_id, f'⚠️ Bot ({self.id}) started, but failed get status: {e}')
            else:
                 self._logger.warning("No admin configured, skipping startup message.")
            self._logger.info(f"Frontend bot {self.id} started successfully.")
        except Exception as e:
            self._logger.critical(f"Frontend start failed: {e}", exc_info=True)
            # Consider raising the exception or exiting if start fails critically
            # raise e

    def _track_user_activity(self, user_id: Optional[int]):
        """使用 Redis Set 记录用户活动，用于统计。"""
        if not user_id or user_id == self._admin_id or user_id == self.my_id or self._cfg.no_redis: return
        try:
            user_id_str = str(user_id)
            if isinstance(self._redis, FakeRedis):
                self._redis.sadd(self._TOTAL_USERS_KEY, user_id_str)
                self._redis.sadd(self._ACTIVE_USERS_KEY, user_id_str)
                self._redis.expire(self._ACTIVE_USERS_KEY, self._ACTIVE_USER_TTL)
            else:
                pipe = self._redis.pipeline()
                pipe.sadd(self._TOTAL_USERS_KEY, user_id_str)
                pipe.sadd(self._ACTIVE_USERS_KEY, user_id_str)
                pipe.expire(self._ACTIVE_USERS_KEY, self._ACTIVE_USER_TTL)
                pipe.execute()
        # [修改] 特别处理 Redis 配置错误，避免反复记录日志
        except RedisResponseError as e:
            if "MISCONF" in str(e):
                 logger.error(f"Redis MISCONF error during usage tracking. Disabling Redis for this frontend instance. Error: {e}")
                 self._redis = FakeRedis() # Fallback to FakeRedis
                 self._cfg.no_redis = True # Mark as disabled
            else:
                 logger.warning(f"Redis usage tracking failed for user {user_id}: {e}")
        except Exception as e: logger.warning(f"Redis usage tracking failed for user {user_id}: {e}")

    async def _callback_handler(self, event: events.CallbackQuery.Event):
        """处理按钮回调"""
        try:
            self._logger.info(f'Callback: {event.sender_id} in {event.chat_id}, msg={event.message_id}, data={event.data!r}')
            self._track_user_activity(event.sender_id) # 记录活动

            if not event.data: await event.answer("无效操作。"); return
            try: query_data = event.data.decode('utf-8')
            except Exception: await event.answer("无效数据格式。"); return
            if not query_data.strip(): await event.answer("空操作。"); return

            parts = query_data.split('=', 1)
            if len(parts) != 2: await event.answer("操作格式错误。"); return
            action, value = parts[0], parts[1]
            redis_prefix = f'{self.id}:'
            bot_chat_id, result_msg_id = event.chat_id, event.message_id
            # 构造用于存储搜索上下文的 Redis Keys
            query_key = f'{redis_prefix}query_text:{bot_chat_id}:{result_msg_id}'
            chats_key = f'{redis_prefix}query_chats:{bot_chat_id}:{result_msg_id}'
            filter_key = f'{redis_prefix}query_filter:{bot_chat_id}:{result_msg_id}'

            # --- 处理翻页和筛选 ---
            if action == 'search_page' or action == 'search_filter':
                 current_filter = self._redis.get(filter_key) or "all"
                 current_chats_str = self._redis.get(chats_key)
                 current_query = self._redis.get(query_key)

                 if current_query is None: # 检查上下文是否已过期
                     try: await event.edit("搜索信息已过期，请重新搜索。")
                     except Exception: pass
                     self._redis.delete(query_key, chats_key, filter_key); await event.answer("搜索已过期。"); return

                 new_page_num, new_filter = 1, current_filter # 默认值
                 if action == 'search_page':
                      try: new_page_num = int(value); assert new_page_num > 0
                      except (ValueError, AssertionError): await event.answer("无效页码。"); return
                      # new_filter 保持 current_filter
                 else: # action == 'search_filter'
                      new_filter = value if value in ["all", "text_only", "file_only"] else "all"
                      if new_filter != current_filter: # 仅当 filter 改变时才更新并回第一页
                           self._redis.set(filter_key, new_filter, ex=3600) # 更新 filter
                           new_page_num = 1 # 筛选后回到第一页
                      else: # filter 未变，保持当前页
                          new_page_num = int(self._redis.get(f'{redis_prefix}query_page:{bot_chat_id}:{result_msg_id}') or 1) # 尝试获取当前页码


                 chats = [int(cid) for cid in current_chats_str.split(',')] if current_chats_str else None
                 self._logger.info(f'Callback Query:"{brief_content(current_query, 50)}" chats={chats} filter={new_filter} page={new_page_num}')

                 # [新增] 存储当前页码，以便在 filter 未变时恢复
                 self._redis.set(f'{redis_prefix}query_page:{bot_chat_id}:{result_msg_id}', new_page_num, ex=3600)

                 start_time = time()
                 try: result = self.backend.search(current_query, chats, self._cfg.page_len, new_page_num, file_filter=new_filter)
                 except Exception as e: self._logger.error(f"Backend search failed during callback: {e}", exc_info=True); await event.answer("后端搜索错误。"); return

                 # 重新渲染消息
                 response = await self._render_response_text(result, time() - start_time)
                 buttons = self._render_respond_buttons(result, new_page_num, current_filter=new_filter)
                 try: await event.edit(response, parse_mode='html', buttons=buttons, link_preview=False); await event.answer()
                 except rpcerrorlist.MessageNotModifiedError: await event.answer() # 消息未改变也需 answer
                 except rpcerrorlist.MessageIdInvalidError: await event.answer("消息已被删除。") # 原消息可能不在了
                 except Exception as e: self._logger.error(f"Failed to edit message during callback: {e}"); await event.answer("更新失败。")

            # --- 处理选择聊天 ---
            elif action == 'select_chat':
                 try:
                      chat_id = int(value)
                      try:
                           chat_name = await self.backend.translate_chat_id(chat_id)
                           reply_prompt = f'☑️ 已选择: **{html.escape(chat_name)}** (`{chat_id}`)\n\n请回复此消息进行操作。'
                      except EntityNotFoundError: reply_prompt = f'☑️ 已选择: `{chat_id}` (名称未知)\n\n请回复此消息进行操作。'
                      # 编辑按钮消息为提示文本
                      await event.edit(reply_prompt, parse_mode='markdown')
                      # 将选择的 chat_id 存入 Redis
                      select_key = f'{redis_prefix}select_chat:{bot_chat_id}:{result_msg_id}'
                      self._redis.set(select_key, chat_id, ex=3600) # 存储 1 小时
                      self._logger.info(f"Chat {chat_id} selected by {event.sender_id}, key {select_key}")
                      await event.answer("对话已选择")
                 except ValueError: await event.answer("无效的对话 ID。")
                 except Exception as e: self._logger.error(f"Error in select_chat callback: {e}", exc_info=True); await event.answer("选择对话时出错。")

            elif action == 'noop': # 处理占位按钮
                 await event.answer()
            else: # 未知 action
                 await event.answer("未知操作。")
        # [修改] 捕获 Redis 配置错误
        except RedisResponseError as e:
            logger.error(f"Redis MISCONF error during callback: {e}")
            if "MISCONF" in str(e) and not self._cfg.no_redis: # Check if not already fallen back
                self._redis = FakeRedis()
                self._cfg.no_redis = True
                logger.error("Falling back to FakeRedis due to MISCONF error during callback.")
            try: await event.answer("缓存服务暂时遇到问题，请稍后再试或联系管理员。", alert=True)
            except Exception: pass
        except Exception as e:
             self._logger.error(f"Exception in callback handler: {e}", exc_info=True)
             try: await event.answer("内部错误。", alert=True)
             except Exception as final_e: self._logger.error(f"Failed to answer callback after error: {final_e}")


    async def _normal_msg_handler(self, event: events.NewMessage.Event):
        """处理普通用户的消息"""
        text: str = event.raw_text.strip()
        sender_id = event.sender_id
        self._logger.info(f'User {sender_id} chat {event.chat_id}: "{brief_content(text, 100)}"')
        self._track_user_activity(sender_id) # 记录活动
        selected_chat_context = await self._get_selected_chat_from_reply(event)

        if not text or text.startswith('/start'):
            await event.reply("发送关键词进行搜索，或使用 /help 查看帮助。"); return
        elif text.startswith('/help'):
            await event.reply(self.HELP_TEXT_USER, parse_mode='markdown'); return
        elif text.startswith('/random'):
            try:
                msg = self.backend.rand_msg()
                try: chat_name = await self.backend.translate_chat_id(msg.chat_id)
                except EntityNotFoundError: chat_name = f"未知对话 ({msg.chat_id})"
                # 构建随机消息回复 (与搜索结果渲染逻辑保持一致)
                respond = f'随机消息来自 **{html.escape(chat_name)}** (`{msg.chat_id}`)\n'
                # [修改] 移除 sender name
                # if msg.sender: respond += f'发送者: {html.escape(msg.sender)}\n'
                respond += f'时间: {msg.post_time.strftime("%Y-%m-%d %H:%M")}\n'
                link_added = False
                if msg.filename and msg.url:
                    respond += f'<a href="{html.escape(msg.url)}">📎 {html.escape(msg.filename)}</a>\n'
                    link_added = True
                elif msg.filename:
                    respond += f"📎 {html.escape(msg.filename)}\n"

                if msg.content:
                    # [修改] 应用简短内容限制
                    content_display = html.escape(brief_content(msg.content, self.MAX_TEXT_DISPLAY_LENGTH))
                    if not link_added and msg.url:
                        respond += f'<a href="{html.escape(msg.url)}">跳转到消息</a>\n'
                    respond += f'{content_display}\n'
                elif not link_added and msg.url: # 既无文件也无文本
                    respond += f'<a href="{html.escape(msg.url)}">跳转到消息</a>\n'
                    # [修改] 移除 (空消息)
                    # respond += f'<i>(空消息)</i>\n'

            except IndexError: respond = '错误：索引库为空。'
            except EntityNotFoundError as e: respond = f"错误：源对话 `{e.entity}` 未找到。"
            except Exception as e: self._logger.error(f"Error handling /random: {e}", exc_info=True); respond = f"获取随机消息时出错: {type(e).__name__}"
            await event.reply(respond, parse_mode='html', link_preview=False)

        elif text.startswith('/chats'):
            kw = remove_first_word(text).strip(); buttons = []
            monitored = sorted(list(self.backend.monitored_chats)); found = 0
            if monitored:
                for cid in monitored:
                    try:
                         name = await self.backend.translate_chat_id(cid)
                         # 关键词匹配名称或 ID
                         if kw and kw.lower() not in name.lower() and str(cid) != kw: continue
                         found += 1
                         if found <= 50: buttons.append(Button.inline(f"{brief_content(name, 25)} (`{cid}`)", f'select_chat={cid}'))
                    except EntityNotFoundError: self._logger.warning(f"Chat {cid} not found when listing for /chats.")
                    except Exception as e: self._logger.error(f"Error processing chat {cid} for /chats: {e}")
                if buttons:
                    button_rows = [buttons[i:i+2] for i in range(0, len(buttons), 2)]
                    reply_text = f"请选择对话 ({found} 个结果):" if found <= 50 else f"找到 {found} 个结果, 显示前 50 个:"
                    await event.reply(reply_text, buttons=button_rows)
                else: await event.reply(f'没有找到与 "{html.escape(kw)}" 匹配的已索引对话。' if kw else '没有已索引的对话。')
            else: await event.reply('没有正在监控的对话。请先使用管理员命令添加。') # 提示用户没有监控对话

        # --- 处理搜索命令及其别名 ---
        elif text.startswith(('/s ', '/ss ', '/search ', '/s', '/ss', '/search')):
            command = text.split()[0]
            query = remove_first_word(text).strip() if len(text) > len(command) else ""
            if not query and not selected_chat_context: await event.reply("缺少关键词。用法: `/s 关键词`", parse_mode='markdown'); return
            await self._search(event, query, selected_chat_context) # 调用搜索主函数

        elif text.startswith('/'):
             # 未知命令
             await event.reply(f'未知命令: `{text.split()[0]}`。请使用 /help 查看帮助。', parse_mode='markdown')
        else:
             # 默认行为: 视为搜索关键词
             await self._search(event, text, selected_chat_context)


    async def _chat_ids_from_args(self, chats_args: List[str]) -> Tuple[List[int], List[str]]:
        """将命令行参数中的字符串（可能是ID或用户名）转换为 share_id 列表"""
        chat_ids, errors = [], []
        if not chats_args: return [], []
        for chat_arg in chats_args:
            try:
                chat_ids.append(await self.backend.str_to_chat_id(chat_arg)) # 调用后端转换
            except EntityNotFoundError:
                 errors.append(f'未找到: "{html.escape(chat_arg)}"')
            except Exception as e:
                 errors.append(f'解析 "{html.escape(chat_arg)}" 时出错: {type(e).__name__}')
        return chat_ids, errors


    async def _admin_msg_handler(self, event: events.NewMessage.Event):
        """处理管理员发送的消息和命令"""
        text: str = event.raw_text.strip()
        self._logger.info(f'Admin {event.sender_id} cmd: "{brief_content(text, 100)}"')
        selected_chat_context = await self._get_selected_chat_from_reply(event)
        selected_chat_id = selected_chat_context[0] if selected_chat_context else None
        selected_chat_name = selected_chat_context[1] if selected_chat_context else None
        self._track_user_activity(event.sender_id) # 记录管理员活动

        # --- 管理员命令处理 ---
        if text.startswith('/help'):
             await event.reply(self.HELP_TEXT_ADMIN, parse_mode='markdown'); return
        elif text.startswith('/stat'):
            try: status = await self.backend.get_index_status(); await event.reply(status, parse_mode='html', link_preview=False)
            except Exception as e: self._logger.error(f"Error handling /stat: {e}", exc_info=True); await event.reply(f"获取状态时出错: {html.escape(str(e))}\n<pre>{html.escape(format_exc())}</pre>", parse_mode='html')
        elif text.startswith('/download_chat'):
             try: args = self.download_arg_parser.parse_args(shlex.split(text)[1:])
             except (ArgumentError, Exception) as e: await event.reply(f"参数错误: {e}\n用法:\n<pre>{html.escape(self.download_arg_parser.format_help())}</pre>", parse_mode='html'); return
             min_id, max_id = args.min or 0, args.max or 0
             target_chat_ids, errors = await self._chat_ids_from_args(args.chats)
             # 处理回复上下文
             if not args.chats and selected_chat_id is not None and selected_chat_id not in target_chat_ids:
                  target_chat_ids = [selected_chat_id]; await event.reply(f"检测到回复: 正在下载 **{html.escape(selected_chat_name or str(selected_chat_id))}** (`{selected_chat_id}`)", parse_mode='markdown')
             elif not target_chat_ids and not errors: await event.reply("错误: 请指定要下载的对话或回复一个已选择的对话。"); return
             if errors: await event.reply("解析对话参数时出错:\n- " + "\n- ".join(errors))
             if not target_chat_ids: return
             # 执行下载
             s, f = 0, 0
             for cid in target_chat_ids:
                 try: await self._download_history(event, cid, min_id, max_id); s += 1
                 except Exception as dl_e: f += 1; self._logger.error(f"Download failed for chat {cid}: {dl_e}", exc_info=True); await event.reply(f"❌ 下载对话 {cid} 失败: {html.escape(str(dl_e))}", parse_mode='html')
             if len(target_chat_ids) > 1: await event.reply(f"下载任务完成: {s} 个成功, {f} 个失败。")
        elif text.startswith('/monitor_chat'):
            try: args = self.chat_ids_parser.parse_args(shlex.split(text)[1:])
            except (ArgumentError, Exception) as e: await event.reply(f"参数错误: {e}\n用法:\n<pre>{html.escape(self.chat_ids_parser.format_help())}</pre>", parse_mode='html'); return
            target_chat_ids, errors = await self._chat_ids_from_args(args.chats)
            # 处理回复上下文
            if not args.chats and selected_chat_id is not None and selected_chat_id not in target_chat_ids:
                 target_chat_ids = [selected_chat_id]; await event.reply(f"检测到回复: 正在监控 **{html.escape(selected_chat_name or str(selected_chat_id))}** (`{selected_chat_id}`)", parse_mode='markdown')
            elif not target_chat_ids and not errors: await event.reply("错误: 请指定要监控的对话或回复一个已选择的对话。"); return
            if errors: await event.reply("解析对话参数时出错:\n- " + "\n- ".join(errors))
            if not target_chat_ids: return
            # 执行监控
            results_msg, added_count, already_monitored_count = [], 0, 0
            for cid in target_chat_ids:
                if cid in self.backend.monitored_chats: already_monitored_count += 1
                else:
                    self.backend.monitored_chats.add(cid); added_count += 1
                    try: h = await self.backend.format_dialog_html(cid); results_msg.append(f"- ✅ {h} 已加入监控。")
                    except Exception as e: results_msg.append(f"- ✅ `{cid}` 已加入监控 (获取名称出错: {type(e).__name__})."); self._logger.info(f'Admin added chat {cid} to monitor.')
            if results_msg: await event.reply('\n'.join(results_msg), parse_mode='html', link_preview=False)
            status_parts = []
            if added_count > 0: status_parts.append(f"{added_count} 个对话已添加。")
            if already_monitored_count > 0: status_parts.append(f"{already_monitored_count} 个已在监控中。")
            final_status = " ".join(status_parts); await event.reply(final_status if final_status else "未做任何更改。")
        elif text.startswith('/clear'):
             try: args = self.chat_ids_parser.parse_args(shlex.split(text)[1:])
             except (ArgumentError, Exception) as e: await event.reply(f"参数错误: {e}\n用法:\n<pre>{html.escape(self.chat_ids_parser.format_help())}</pre>", parse_mode='html'); return
             # 处理 /clear all
             if len(args.chats) == 1 and args.chats[0].lower() == 'all':
                 try: self.backend.clear(None); await event.reply('✅ 所有索引数据已清除。')
                 except Exception as e: self._logger.error("Clear all index error:", exc_info=True); await event.reply(f"清除所有索引时出错: {e}")
                 return
             # 处理指定对话或回复
             target_chat_ids, errors = await self._chat_ids_from_args(args.chats)
             if not args.chats and selected_chat_id is not None and selected_chat_id not in target_chat_ids:
                  target_chat_ids = [selected_chat_id]; await event.reply(f"检测到回复: 正在清除 **{html.escape(selected_chat_name or str(selected_chat_id))}** (`{selected_chat_id}`)", parse_mode='markdown')
             elif not target_chat_ids and not errors: await event.reply("错误: 请指定要清除的对话，或回复一个已选择的对话，或使用 `/clear all`。"); return
             if errors: await event.reply("解析对话参数时出错:\n- " + "\n- ".join(errors))
             if not target_chat_ids: return
             # 执行清除
             self._logger.info(f'Admin clearing index for chats: {target_chat_ids}')
             try:
                 self.backend.clear(target_chat_ids); results_msg = []
                 for cid in target_chat_ids:
                     try: h = await self.backend.format_dialog_html(cid); results_msg.append(f"- ✅ {h} 的索引已清除。")
                     except Exception: results_msg.append(f"- ✅ `{cid}` 的索引已清除 (名称未知)。")
                 await event.reply('\n'.join(results_msg), parse_mode='html', link_preview=False)
             except Exception as e: self._logger.error(f"Clear specific chats error: {e}", exc_info=True); await event.reply(f"清除索引时出错: {e}")
        elif text.startswith('/find_chat_id'):
             q = remove_first_word(text).strip();
             if not q: await event.reply('错误: 缺少关键词。'); return
             try:
                 results = await self.backend.find_chat_id(q); sb = []
                 if results:
                      sb.append(f'找到 {len(results)} 个与 "{html.escape(q)}" 匹配的对话:\n')
                      for cid in results[:50]:
                          try: n = await self.backend.translate_chat_id(cid); sb.append(f'- {html.escape(n)}: `{cid}`\n')
                          except EntityNotFoundError: sb.append(f'- 未知对话: `{cid}`\n')
                          except Exception as e: sb.append(f'- `{cid}` (获取名称出错: {type(e).__name__})\n')
                      if len(results) > 50: sb.append("\n(仅显示前 50 个结果)")
                 else: sb.append(f'未找到与 "{html.escape(q)}" 匹配的对话。')
                 await event.reply(''.join(sb), parse_mode='html')
             except Exception as e: self._logger.error(f"Find chat ID error: {e}", exc_info=True); await event.reply(f"查找对话 ID 时出错: {e}")
        elif text.startswith('/refresh_chat_names'):
            msg: Optional[TgMessage] = None
            try:
                msg = await event.reply('⏳ 正在刷新对话名称缓存，这可能需要一些时间...')
                await self.backend.session.refresh_translate_table()
                await msg.edit('✅ 对话名称缓存已刷新。')
            except Exception as e:
                self._logger.error("Refresh chat names error:", exc_info=True)
                error_text = f'❌ 刷新缓存时出错: {html.escape(str(e))}'
                if msg:
                    try: await msg.edit(error_text)
                    except Exception: await event.reply(error_text) # 编辑失败则发送新消息
                else: await event.reply(error_text) # 初始消息发送失败
        elif text.startswith('/usage'):
             if self._cfg.no_redis: await event.reply("使用统计功能需要 Redis (当前已禁用)。"); return
             try:
                 total_count = 0; active_count = 0
                 if isinstance(self._redis, FakeRedis):
                     total_count = self._redis.scard(self._TOTAL_USERS_KEY)
                     active_count = self._redis.scard(self._ACTIVE_USERS_KEY)
                 else:
                     # [修改] 捕获 Redis 错误
                     try:
                         pipe = self._redis.pipeline()
                         pipe.scard(self._TOTAL_USERS_KEY)
                         pipe.scard(self._ACTIVE_USERS_KEY)
                         results = pipe.execute()
                         total_count = results[0] if results and len(results) > 0 else 0
                         active_count = results[1] if results and len(results) > 1 else 0
                     except RedisResponseError as e:
                         logger.error(f"Redis MISCONF error during usage check: {e}")
                         await event.reply(f"获取使用统计时出错: Redis 服务器配置错误 (MISCONF)。")
                         return # Don't proceed further
                     except RedisConnectionError as e:
                         logger.error(f"Redis connection error during usage check: {e}")
                         await event.reply(f"获取使用统计时出错: 无法连接到 Redis。")
                         return

                 await event.reply(f"📊 **使用统计**\n- 总独立用户数: {total_count}\n- 活跃用户数 (最近15分钟): {active_count}", parse_mode='markdown')
             except Exception as e: self._logger.error(f"Failed to get usage stats: {e}", exc_info=True); await event.reply(f"获取使用统计时出错: {html.escape(str(e))}")
        else:
             # 管理员输入的其他文本按普通用户处理 (即视为搜索)
             await self._normal_msg_handler(event)


    async def _search(self, event: events.NewMessage.Event, query: str, selected_chat_context: Optional[Tuple[int, str]]):
        """执行搜索的核心函数"""
        selected_chat_id = selected_chat_context[0] if selected_chat_context else None
        selected_chat_name = selected_chat_context[1] if selected_chat_context else None

        # 如果在选定对话上下文但没有查询词，则搜索全部
        if not query and selected_chat_context:
             query = '*' # 使用 '*' 匹配所有
             await event.reply(f"正在搜索 **{html.escape(selected_chat_name or str(selected_chat_id))}** (`{selected_chat_id}`) 中的所有消息", parse_mode='markdown')
        elif not query: # 全局搜索不能没有关键词
             self._logger.debug("Empty query ignored for global search.")
             return

        target_chats = [selected_chat_id] if selected_chat_id is not None else None # 设置搜索目标
        try: is_empty = self.backend.is_empty(selected_chat_id) # 检查索引是否为空
        except Exception as e: self._logger.error(f"Check index empty error: {e}"); await event.reply("检查索引状态时出错。"); return

        if is_empty:
            if selected_chat_context: await event.reply(f'对话 **{html.escape(selected_chat_name or str(selected_chat_id))}** 的索引为空。')
            else: await event.reply('全局索引为空。')
            return

        start = time(); ctx_info = f"在对话 {selected_chat_id} 中" if target_chats else "全局"
        self._logger.info(f'正在搜索 "{brief_content(query, 50)}" ({ctx_info})')
        try:
            # 执行搜索 (初始 filter 为 'all')
            result = self.backend.search(query, target_chats, self._cfg.page_len, 1, file_filter="all")
            # 渲染结果
            text = await self._render_response_text(result, time() - start)
            buttons = self._render_respond_buttons(result, 1, current_filter="all")
            # 发送回复
            msg = await event.reply(text, parse_mode='html', buttons=buttons, link_preview=False)
            # 存储上下文到 Redis
            if msg and not self._cfg.no_redis: # Check if redis is enabled
                try:
                    prefix, bcid, mid = f'{self.id}:', event.chat_id, msg.id
                    # 使用 pipeline 减少网络往返
                    pipe = self._redis.pipeline()
                    pipe.set(f'{prefix}query_text:{bcid}:{mid}', query, ex=3600)
                    pipe.set(f'{prefix}query_filter:{bcid}:{mid}', "all", ex=3600) # 存初始 filter
                    pipe.set(f'{prefix}query_page:{bcid}:{mid}', 1, ex=3600) # 存初始页码
                    if target_chats: pipe.set(f'{prefix}query_chats:{bcid}:{mid}', ','.join(map(str, target_chats)), ex=3600)
                    else: pipe.delete(f'{prefix}query_chats:{bcid}:{mid}') # 全局搜索则删除此 key
                    pipe.execute()
                # [修改] 捕获 Redis 错误并警告，但不中断流程
                except RedisResponseError as e:
                    logger.error(f"Redis MISCONF error saving search context: {e}")
                    if "MISCONF" in str(e): # Check if it's the specific error
                         self._redis = FakeRedis(); self._cfg.no_redis = True
                         logger.error("Falling back to FakeRedis due to MISCONF error.")
                         await event.reply("⚠️ 缓存服务暂时遇到问题，翻页和筛选功能可能受限。") # Notify user
                except RedisConnectionError as e:
                    logger.error(f"Redis connection error saving search context: {e}")
                    # Don't necessarily disable redis for connection errors, maybe temporary
                    await event.reply("⚠️ 无法连接到缓存服务，翻页和筛选功能可能受限。")
                except Exception as e:
                     logger.error(f"Failed to save search context to Redis: {e}")

        except whoosh.index.LockError: await event.reply('⏳ 索引当前正忙，请稍后再试。')
        except Exception as e: self._logger.error(f"Search execution error: {e}", exc_info=True); await event.reply(f'搜索时发生错误: {type(e).__name__}。')


    async def _download_history(self, event: events.NewMessage.Event, chat_id: int, min_id: int, max_id: int):
        """处理下载历史记录的逻辑，包括进度回调"""
        try: chat_html = await self.backend.format_dialog_html(chat_id)
        except Exception as e: chat_html = f"对话 `{chat_id}`"

        # 检查是否重复下载
        try:
            if min_id == 0 and max_id == 0 and not self.backend.is_empty(chat_id):
                await event.reply(f'⚠️ 警告: {chat_html} 的索引已存在。重新下载可能导致消息重复。'
                                  f'如果需要重新下载，请先使用 `/clear {chat_id}` 清除旧索引，或指定 `--min/--max` 范围。',
                                  parse_mode='html')
        except Exception as e: self._logger.error(f"Check empty error before download {chat_id}: {e}")

        # 进度更新相关变量
        prog_msg: Optional[TgMessage] = None
        last_update = time(); interval = 5; count = 0

        # 进度回调函数 (中文)
        async def cb(cur_id: int, dl_count: int):
            nonlocal prog_msg, last_update, count; count = dl_count; now = time()
            if now - last_update > interval: # 控制更新频率
                last_update = now
                txt = f'⏳ 正在下载 {chat_html}:\n已处理 {dl_count} 条，当前消息 ID: {cur_id}'
                try:
                    if prog_msg is None: prog_msg = await event.reply(txt, parse_mode='html')
                    else: await prog_msg.edit(txt, parse_mode='html')
                except rpcerrorlist.FloodWaitError as fwe: self._logger.warning(f"Flood wait ({fwe.seconds}s) during progress update for {chat_id}."); last_update += fwe.seconds # 延后下次更新
                except rpcerrorlist.MessageNotModifiedError: pass
                except rpcerrorlist.MessageIdInvalidError: prog_msg = None # 进度消息被删
                except Exception as e: self._logger.error(f"Edit progress message error {chat_id}: {e}"); prog_msg = None

        start = time()
        try:
            # 调用后端下载
            await self.backend.download_history(chat_id, min_id, max_id, cb)
            msg = f'✅ {chat_html} 下载完成，索引了 {count} 条消息，耗时 {time()-start:.2f} 秒。'
            try:
                if prog_msg: await prog_msg.edit(msg, parse_mode='html')
                else: await event.reply(msg, parse_mode='html')
                prog_msg = None # 防止 finally 再次删除
            except Exception: await self.bot.send_message(event.chat_id, msg, parse_mode='html') # 编辑/回复失败则发送
        except (EntityNotFoundError, ValueError) as e: # 捕获预期错误
            self._logger.error(f"Download failed for {chat_id}: {e}")
            error_msg = f'❌ 下载 {chat_html} 时出错: {e}'
            if prog_msg: await prog_msg.edit(error_msg, parse_mode='html'); prog_msg = None
            else: await event.reply(error_msg, parse_mode='html')
        except Exception as e: # 捕获其他错误
            self._logger.error(f"Unknown download error for {chat_id}: {e}", exc_info=True)
            error_msg = f'❌ 下载 {chat_html} 时发生未知错误: {type(e).__name__}'
            if prog_msg: await prog_msg.edit(error_msg, parse_mode='html'); prog_msg = None
            else: await event.reply(error_msg, parse_mode='html')
        finally:
            # 尝试删除最终未被编辑的进度消息
            if prog_msg:
                try: await prog_msg.delete()
                except Exception: pass


    def _register_hooks(self):
        """注册 Telethon 事件钩子"""
        # 回调查询钩子
        @self.bot.on(events.CallbackQuery())
        async def cq_handler(event: events.CallbackQuery.Event):
             is_admin = self._admin_id and event.sender_id == self._admin_id
             is_wl = event.sender_id in self._cfg.private_whitelist
             if self._cfg.private_mode and not is_admin and not is_wl: await event.answer("您没有权限执行此操作。", alert=True); return
             await self._callback_handler(event) # 调用回调处理器

        # 新消息钩子
        @self.bot.on(events.NewMessage())
        async def msg_handler(event: events.NewMessage.Event):
            # 基础检查
            if not event.message or not event.sender_id: return
            sender = await event.message.get_sender()
            if not sender or (self.my_id and sender.id == self.my_id): return # 忽略自己

            is_admin = self._admin_id and sender.id == self._admin_id

            # 判断是否需要处理 (私聊 / @ / 回复)
            mentioned, reply_to_bot = False, False
            if event.is_group or event.is_channel:
                 if self.username and f'@{self.username}' in event.raw_text: mentioned = True
                 elif event.message.mentioned and event.message.entities:
                      for entity in event.message.entities:
                          if isinstance(entity, MessageEntityMentionName) and entity.user_id == self.my_id: mentioned = True; break
                 if event.message.is_reply and event.message.reply_to_msg_id:
                      try: reply = await event.message.get_reply_message(); reply_to_bot = reply and reply.sender_id == self.my_id
                      except Exception as e: self._logger.warning(f"Could not get reply message {event.message.reply_to_msg_id} in chat {event.chat_id}: {e}")
            process = event.is_private or mentioned or reply_to_bot
            if not process: return

            # 私聊模式权限检查
            if self._cfg.private_mode and not is_admin:
                 sender_allowed = sender.id in self._cfg.private_whitelist
                 chat_allowed = False
                 if event.chat_id:
                      try: csi = get_share_id(event.chat_id); chat_allowed = csi in self._cfg.private_whitelist
                      except Exception: pass
                 if not sender_allowed and not chat_allowed:
                     if event.is_private: await event.reply('抱歉，您没有权限使用此机器人。');
                     return

            # 分发给管理员或普通用户处理器
            handler = self._admin_msg_handler if is_admin else self._normal_msg_handler
            try:
                await handler(event)
            except whoosh.index.LockError: await event.reply('⏳ 索引当前正忙，请稍后再试。')
            except EntityNotFoundError as e: await event.reply(f'❌ 未找到相关实体: {e.entity}')
            except rpcerrorlist.UserIsBlockedError: self._logger.warning(f"User {sender.id} blocked the bot.")
            except rpcerrorlist.ChatWriteForbiddenError: self._logger.warning(f"Write forbidden in chat: {event.chat_id}.")
            # [修改] 捕获 Redis 配置错误
            except RedisResponseError as e:
                 logger.error(f"Redis MISCONF error during message handling: {e}")
                 if "MISCONF" in str(e) and not self._cfg.no_redis: # Check if not already fallen back
                     self._redis = FakeRedis(); self._cfg.no_redis = True
                     logger.error("Falling back to FakeRedis due to MISCONF error during message handling.")
                     await event.reply("处理请求时遇到缓存服务问题，请稍后再试。")
                 else: # Other Redis errors
                     await event.reply(f'处理请求时发生 Redis 错误: {type(e).__name__}。')

            except Exception as e: # 通用错误处理
                 et = type(e).__name__; self._logger.error(f"Error handling message from {sender.id} in {event.chat_id}: {et}: {e}", exc_info=True)
                 try: await event.reply(f'处理您的请求时发生错误: {et}。\n如果问题持续存在，请联系管理员。')
                 except Exception as re: self._logger.error(f"Replying with error message failed: {re}")
                 # 通知管理员
                 if self._admin_id and event.chat_id != self._admin_id:
                      try:
                          error_details = f"处理来自用户 {sender.id} 在对话 {event.chat_id} 的消息时出错:\n<pre>{html.escape(format_exc())}</pre>"
                          await self.bot.send_message(self._admin_id, error_details, parse_mode='html')
                      except Exception as ne: self._logger.error(f"Notifying admin about error failed: {ne}")


    async def _get_selected_chat_from_reply(self, event: events.NewMessage.Event) -> Optional[Tuple[int, str]]:
        """检查回复上下文，获取之前选择的对话 ID 和名称"""
        if not event.message.is_reply or not event.message.reply_to_msg_id: return None
        if self._cfg.no_redis: return None # Don't check redis if disabled
        key = f'{self.id}:select_chat:{event.chat_id}:{event.message.reply_to_msg_id}'
        res = self._redis.get(key)
        if res:
            try:
                 cid = int(res)
                 name = await self.backend.translate_chat_id(cid) # 可能抛出 EntityNotFoundError
                 return cid, name
            except ValueError: self._redis.delete(key); return None # 无效数据，删除 key
            except EntityNotFoundError: return int(res), f"未知对话 ({res})" # 找到 ID 但无名称
            # [修改] 捕获 Redis 错误
            except (RedisResponseError, RedisConnectionError) as e:
                 logger.error(f"Redis error getting selected chat context: {e}")
                 return None # Treat as no context found
            except Exception as e: self._logger.error(f"Error getting selected chat name for key {key}: {e}"); return None
        return None # 没有找到对应的 key


    async def _register_commands(self):
        """注册 Telegram Bot 命令列表"""
        admin_peer = None
        if self._admin_id:
            try: admin_peer = await self.bot.get_input_entity(self._admin_id)
            except ValueError:
                 self._logger.warning(f"Could not get input entity for admin ID {self._admin_id} directly (might be username). Trying get_entity.")
                 try: admin_entity = await self.bot.get_entity(self._admin_id); admin_peer = await self.bot.get_input_entity(admin_entity)
                 except Exception as e: self._logger.error(f'Failed to get admin input entity via get_entity for {self._admin_id}: {e}')
            except Exception as e: self._logger.error(f'Failed to get admin input entity for {self._admin_id}: {e}')
        else: self._logger.warning("Admin ID invalid or not configured, skipping admin-specific command registration.")

        # 定义命令列表
        admin_commands = [ BotCommand(c, d) for c, d in [
            ("download_chat", '[选项] [对话...] 下载历史'),
            ("monitor_chat", '对话... 添加实时监控'),
            ("clear", '[对话...|all] 清除索引'),
            ("stat", '查询后端状态'),
            ("find_chat_id", '关键词 查找对话ID'),
            ("refresh_chat_names", '刷新对话名称缓存'),
            ("usage", '查看使用统计')
        ]]
        common_commands = [ BotCommand(c, d) for c, d in [
            ("s", '关键词 搜索 (或 /search /ss)'),
            ("chats", '[关键词] 列出/选择对话'),
            ("random", '随机返回一条消息'),
            ("help", '显示帮助信息')
        ]]

        # 设置命令
        if admin_peer:
            try:
                await self.bot(SetBotCommandsRequest(scope=BotCommandScopePeer(admin_peer), lang_code='', commands=admin_commands + common_commands))
                self._logger.info(f"Admin commands set successfully for peer {self._admin_id}.")
            except Exception as e:
                self._logger.error(f"Setting admin commands failed for peer {self._admin_id}: {e}")
        try:
            await self.bot(SetBotCommandsRequest(scope=BotCommandScopeDefault(), lang_code='', commands=common_commands))
            self._logger.info("Default commands set successfully.")
        except Exception as e:
            self._logger.error(f"Setting default commands failed: {e}")


    # [新增] 辅助函数：剥离 HTML 标签
    def _strip_html(self, text: str) -> str:
        """简单的 HTML 标签剥离器"""
        return re.sub('<[^>]*>', '', text)

    async def _render_response_text(self, result: SearchResult, used_time: float) -> str:
        """将搜索结果渲染为发送给用户的 HTML 文本 (优化版)"""
        if not isinstance(result, SearchResult) or not result.hits:
             return "没有找到相关的消息。"

        sb = [f'找到 {result.total_results} 条结果，耗时 {used_time:.3f} 秒:\n\n']
        for i, hit in enumerate(result.hits, 1):
            try:
                msg = hit.msg
                if not isinstance(msg, IndexMsg):
                     sb.append(f"<b>{i}.</b> 错误: 无效的消息数据。\n\n"); continue

                try: title = await self.backend.translate_chat_id(msg.chat_id)
                except EntityNotFoundError: title = f"未知对话 ({msg.chat_id})"
                except Exception as te: title = f"错误 ({msg.chat_id}): {type(te).__name__}"

                # 构建消息头
                hdr = [f"<b>{i}. {html.escape(title)}</b>"]
                # [修改] 移除 sender name
                # if msg.sender: hdr.append(f"(<u>{html.escape(msg.sender)}</u>)") # 移除
                if isinstance(msg.post_time, datetime): hdr.append(f'[{msg.post_time.strftime("%y-%m-%d %H:%M")}]')
                else: hdr.append('[无效时间]')
                sb.append(' '.join(hdr) + '\n')

                # --- 优化链接和文本显示 ---
                link_added = False
                # 1. 文件名链接优先
                if msg.filename and msg.url:
                    sb.append(f'<a href="{html.escape(msg.url)}">📎 {html.escape(msg.filename)}</a>\n')
                    link_added = True
                elif msg.filename: # 只有文件名
                     sb.append(f"📎 {html.escape(msg.filename)}\n")

                # 2. 处理高亮或后备文本，并限制长度
                display_text = ""
                if hit.highlighted:
                    # 检查高亮文本的纯文本长度
                    plain_highlighted = self._strip_html(hit.highlighted)
                    if len(plain_highlighted) <= self.MAX_TEXT_DISPLAY_LENGTH:
                        display_text = hit.highlighted # 长度合格，保留高亮
                    else:
                        # 长度超标，移除高亮，截断纯文本
                        display_text = html.escape(brief_content(plain_highlighted, self.MAX_TEXT_DISPLAY_LENGTH))
                elif msg.content: # 没有高亮，但有原文
                    display_text = html.escape(brief_content(msg.content, self.MAX_TEXT_DISPLAY_LENGTH))
                # [修改] 不再添加 (空消息) 占位符
                # else: # 既无高亮也无原文 (可能是仅文件)
                #     pass # display_text 保持空

                # 3. 如果前面没加链接，且 URL 存在，加通用链接
                if not link_added and msg.url:
                    sb.append(f'<a href="{html.escape(msg.url)}">跳转到消息</a>\n')

                # 4. 添加处理后的文本 (如果非空)
                if display_text:
                    sb.append(f"{display_text}\n")

                sb.append("\n") # 每个结果后加空行
                # --- 结束优化 ---

            except Exception as e:
                 sb.append(f"<b>{i}.</b> 渲染此条结果时出错: {type(e).__name__}\n\n")
                 self._logger.error(f"Error rendering hit (msg URL: {getattr(hit, 'msg', None) and getattr(hit.msg, 'url', 'N/A')}): {e}", exc_info=True)

        # 处理消息过长截断
        final = ''.join(sb); max_len = 4096
        if len(final) > max_len:
             cutoff_msg = "\n\n...(结果过多，仅显示部分)"
             cutoff_point = max_len - len(cutoff_msg) - 10
             last_nl = final.rfind('\n\n', 0, cutoff_point)
             if last_nl != -1: final = final[:last_nl] + cutoff_msg
             else: final = final[:max_len - len(cutoff_msg)] + cutoff_msg
        return final


    def _render_respond_buttons(self, result: SearchResult, cur_page_num: int, current_filter: str = "all") -> Optional[List[List[Button]]]:
        """生成包含中文筛选和翻页按钮的列表"""
        if not isinstance(result, SearchResult): return None
        buttons = []

        # --- 第一行：筛选按钮 (中文) ---
        filter_buttons = [
            Button.inline("【全部】" if current_filter == "all" else "全部", 'search_filter=all'),
            Button.inline("【纯文本】" if current_filter == "text_only" else "纯文本", 'search_filter=text_only'),
            Button.inline("【仅文件】" if current_filter == "file_only" else "仅文件", 'search_filter=file_only')
        ]
        buttons.append(filter_buttons)

        # --- 第二行：翻页按钮 (中文) ---
        try:
             page_len = max(1, self._cfg.page_len)
             total_pages = (result.total_results + page_len - 1) // page_len
        except Exception as e: self._logger.error(f"Error calculating total pages: {e}"); total_pages = 1

        if total_pages > 1:
            page_buttons = []
            if cur_page_num > 1: page_buttons.append(Button.inline('⬅️ 上一页', f'search_page={cur_page_num - 1}'))
            page_buttons.append(Button.inline(f'{cur_page_num}/{total_pages}', 'noop')) # 页码指示器
            if not result.is_last_page and cur_page_num < total_pages: page_buttons.append(Button.inline('下一页 ➡️', f'search_page={cur_page_num + 1}'))
            if page_buttons: buttons.append(page_buttons)

        return buttons if buttons else None
