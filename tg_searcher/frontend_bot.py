# -*- coding: utf-8 -*-
import html
from time import time
from typing import Optional, List, Tuple, Set, Union, Any
from datetime import datetime
from traceback import format_exc
from argparse import ArgumentParser, ArgumentError # 导入 ArgumentError
import shlex

import redis
import whoosh.index # 导入 whoosh.index 以便捕获 LockError
from telethon import TelegramClient, events, Button
from telethon.tl.types import BotCommand, BotCommandScopePeer, BotCommandScopeDefault, MessageEntityMentionName
from telethon.tl.custom import Message as TgMessage
from telethon.tl.functions.bots import SetBotCommandsRequest
import telethon.errors.rpcerrorlist as rpcerrorlist
from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError

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
    def brief_content(s, l=50): return (s[:l] + '...') if len(s) > l else s
    class BackendBot: pass
    class EntityNotFoundError(Exception): pass
    class SearchResult: pass
    class IndexMsg: pass

# 获取日志记录器
logger = get_logger('frontend_bot')


class BotFrontendConfig:
    @staticmethod
    def _parse_redis_cfg(redis_cfg: str) -> Tuple[str, int]:
        colon_idx = redis_cfg.find(':')
        if colon_idx < 0: return redis_cfg, 6379 # 默认端口
        try:
            host = redis_cfg[:colon_idx] if colon_idx > 0 else 'localhost'
            port = int(redis_cfg[colon_idx + 1:])
            return host, port
        except (ValueError, TypeError): raise ValueError(f"Invalid Redis port in '{redis_cfg}'")

    def __init__(self, **kw: Any):
        try:
            self.bot_token: str = kw['bot_token']
            self.admin: Union[int, str] = kw['admin_id'] # 在 start 中解析为 int
        except KeyError as e: raise ValueError(f"Missing required config key: {e}")

        self.page_len: int = kw.get('page_len', 10)
        if self.page_len <= 0: logger.warning("page_len must be positive, using 10."); self.page_len = 10

        self.no_redis: bool = kw.get('no_redis', False)
        self.redis_host: Optional[Tuple[str, int]] = None
        if not self.no_redis:
             try:
                  redis_cfg = kw.get('redis', 'localhost:6379')
                  if redis_cfg: self.redis_host = self._parse_redis_cfg(redis_cfg)
                  else: logger.warning("Redis config empty. Disabling redis."); self.no_redis = True
             except ValueError as e: logger.error(f"Error parsing redis config '{kw.get('redis')}': {e}. Disabling redis."); self.no_redis = True
             except KeyError: logger.info("Redis config key 'redis' not found. Disabling redis."); self.no_redis = True

        self.private_mode: bool = kw.get('private_mode', False)
        self.private_whitelist: Set[int] = set()
        raw_whitelist = kw.get('private_whitelist', [])
        if isinstance(raw_whitelist, list):
             for item in raw_whitelist:
                 try: self.private_whitelist.add(int(item))
                 except (ValueError, TypeError): logger.warning(f"Could not parse whitelist item '{item}' as int.")
        elif raw_whitelist: logger.warning("private_whitelist format incorrect (expected list), ignoring.")
        # admin ID 会在 start 时加入


# [修改] 增强 FakeRedis 以模拟 Set 和 expire
class FakeRedis:
    """一个简单的内存字典，模拟部分 Redis 功能，用于在无 Redis 环境下运行"""
    def __init__(self):
        self._data = {} # 存储 (value, expiry_timestamp)
        self._logger = get_logger('FakeRedis')
        self._logger.warning("Using FakeRedis: Data is volatile and will be lost on restart.")
    def get(self, key):
        v = self._data.get(key)
        # 检查是否存在且未过期
        if v and (v[1] is None or v[1] > time()):
            return v[0] # 只返回存储的值
        elif v and v[1] is not None and v[1] <= time():
            # 如果已过期，则删除
            del self._data[key]
        return None # 不存在或已过期
    def set(self, key, val, ex=None):
        expiry = time() + ex if ex else None
        self._data[key] = (str(val), expiry) # 存储为 (字符串值, 过期时间戳)
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
        """模拟 SADD 命令，将元素添加到集合"""
        current_set, expiry = self._data.get(key, (set(), None))
        if not isinstance(current_set, set): # 如果键存在但不是集合，则重置
             current_set = set()
        # 检查是否过期
        if expiry is not None and expiry <= time():
            current_set = set()
            expiry = None # 清除过期时间
        added_count = 0
        str_values = {str(v) for v in values} # 确保存储的是字符串
        for v in str_values:
            if v not in current_set:
                current_set.add(v)
                added_count += 1
        self._data[key] = (current_set, expiry) # 更新集合和过期时间
        return added_count
    def scard(self, key):
        """模拟 SCARD 命令，获取集合大小"""
        v = self._data.get(key)
        if v and isinstance(v[0], set) and (v[1] is None or v[1] > time()):
            return len(v[0])
        elif v and v[1] is not None and v[1] <= time():
            del self._data[key] # 删除过期的集合
        return 0
    def expire(self, key, seconds):
        """模拟 EXPIRE 命令，设置键的过期时间"""
        if key in self._data:
            value, _ = self._data[key]
            self._data[key] = (value, time() + seconds)
            return 1
        return 0


class BotFrontend:
    # --- 帮助文本定义 ---
    HELP_TEXT_USER = """
**可用命令:**
/s `关键词` - 搜索消息 (或 `/search`, `/ss`；直接发送也可)。
/chats `[关键词]` - 列出/选择已索引对话。
/random - 返回一条随机消息。
/help - 显示此帮助信息。

**使用 /chats 选择对话后:**
- 回复选择成功的消息 + 搜索词，可仅搜索该对话。
"""
    # [修改] 添加了 /usage 命令的说明
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

    def __init__(self, common_cfg: CommonBotConfig, cfg: BotFrontendConfig, frontend_id: str, backend: BackendBot):
        self.backend = backend
        self.id = frontend_id
        self._common_cfg = common_cfg
        self.bot = TelegramClient(str(common_cfg.session_dir / f'frontend_{self.id}.session'),
                                  api_id=common_cfg.api_id, api_hash=common_cfg.api_hash, proxy=common_cfg.proxy)
        self._cfg = cfg
        self._redis: Union[redis.client.Redis, FakeRedis]
        if cfg.no_redis or cfg.redis_host is None:
             self._redis = FakeRedis() # 如果配置禁用 Redis 或主机信息无效，则使用 FakeRedis
        else:
            try:
                 # 尝试连接真实 Redis
                 self._redis = Redis(host=cfg.redis_host[0], port=cfg.redis_host[1], decode_responses=True)
                 self._redis.ping() # 测试连接
            except RedisConnectionError as e:
                 logger.critical(f'Redis connection failed {cfg.redis_host}: {e}. Falling back to FakeRedis.')
                 self._redis = FakeRedis() # 连接失败，回退到 FakeRedis
                 self._cfg.no_redis = True
            except Exception as e:
                 logger.critical(f'Redis init error {cfg.redis_host}: {e}. Falling back to FakeRedis.')
                 self._redis = FakeRedis() # 其他初始化错误，回退到 FakeRedis
                 self._cfg.no_redis = True

        self._logger = logger
        self._admin_id: Optional[int] = None
        self.username: Optional[str] = None
        self.my_id: Optional[int] = None

        # [新增] 定义用于统计的 Redis Key
        self._TOTAL_USERS_KEY = f'{self.id}:total_users'       # 存储所有使用过机器人的用户ID (Set)
        self._ACTIVE_USERS_KEY = f'{self.id}:active_users_15m' # 存储最近15分钟活跃的用户ID (Set, 带过期时间)
        self._ACTIVE_USER_TTL = 900 # 15 minutes in seconds (活跃用户记录的过期时间)

        # 参数解析器
        self.download_arg_parser = ArgumentParser(prog="/download_chat", description="Download chat history.", add_help=False, exit_on_error=False)
        self.download_arg_parser.add_argument('--min', type=int, default=0, help="Min message ID (default: 0)")
        self.download_arg_parser.add_argument('--max', type=int, default=0, help="Max message ID (0 = no limit)")
        self.download_arg_parser.add_argument('chats', type=str, nargs='*', help="Chat IDs or usernames")

        self.chat_ids_parser = ArgumentParser(prog="/monitor_chat | /clear", description="Monitor or clear chats.", add_help=False, exit_on_error=False)
        self.chat_ids_parser.add_argument('chats', type=str, nargs='*', help="Chat IDs/usernames, or 'all' for /clear")

    async def start(self):
        try:
            if not self._cfg.admin: raise ValueError("Admin ID not configured.")
            # Assuming backend.str_to_chat_id is async and handles conversion
            self._admin_id = await self.backend.str_to_chat_id(str(self._cfg.admin))
            self._logger.info(f"Admin ID resolved to: {self._admin_id}")
            if self._cfg.private_mode and self._admin_id:
                self._cfg.private_whitelist.add(self._admin_id);
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
             self._logger.error("Proceeding without valid admin ID.")

        # 再次检查 Redis 连接（如果不是 FakeRedis）
        if not isinstance(self._redis, FakeRedis):
             try:
                  self._redis.ping()
                  self._logger.info(f"Redis connected at {self._cfg.redis_host}")
             except RedisConnectionError as e:
                  self._logger.critical(f'Redis check failed during start: {e}. Falling back to FakeRedis.')
                  self._redis = FakeRedis()
                  self._cfg.no_redis = True

        self._logger.info(f'Starting frontend bot {self.id}...')
        try:
             await self.bot.start(bot_token=self._cfg.bot_token)
             me = await self.bot.get_me(); assert me is not None
             self.username, self.my_id = me.username, me.id
             self._logger.info(f'Bot (@{self.username}, id={self.my_id}) login ok')
             # Assuming get_share_id works correctly
             if self.my_id: # 确保 my_id 已获取
                 self.backend.excluded_chats.add(get_share_id(self.my_id))
                 self._logger.info(f"Bot ID {self.my_id} excluded from backend.")
             await self._register_commands(); self._logger.info(f'Commands registered.')
             self._register_hooks()

             if self._admin_id:
                  try:
                       # Assuming backend.get_index_status exists and is async
                       status_msg = await self.backend.get_index_status(4000 - 50) # Adjusted length
                       msg = f'✅ Bot frontend init complete\n\n{status_msg}'
                       await self.bot.send_message(self._admin_id, msg, parse_mode='html', link_preview=False)
                  except Exception as e:
                       self._logger.error(f"Failed get/send initial status: {e}", exc_info=True)
                       await self.bot.send_message(self._admin_id, f'⚠️ Bot started, but failed get status: {e}')
             else:
                  self._logger.warning("No admin configured, skipping startup message.")
             self._logger.info(f"Frontend bot {self.id} started successfully.")
        except Exception as e:
             self._logger.critical(f"Frontend start failed: {e}", exc_info=True)

    # [新增] 函数：记录用户活动到 Redis
    def _track_user_activity(self, user_id: Optional[int]):
        """使用 Redis Set 记录用户活动，用于统计。"""
        # 如果没有用户ID，或者是管理员/机器人自己，或者 Redis 被禁用，则不记录
        if not user_id or user_id == self._admin_id or user_id == self.my_id or self._cfg.no_redis:
            return
        try:
            user_id_str = str(user_id) # Redis Set 存储字符串
            # 使用 pipeline 提高效率 (对真实 Redis 有效)
            if isinstance(self._redis, FakeRedis):
                # FakeRedis 不支持 pipeline，直接调用
                self._redis.sadd(self._TOTAL_USERS_KEY, user_id_str) # 添加到总用户集合
                self._redis.sadd(self._ACTIVE_USERS_KEY, user_id_str) # 添加到活跃用户集合
                self._redis.expire(self._ACTIVE_USERS_KEY, self._ACTIVE_USER_TTL) # 每次活跃都重置活跃集合的过期时间
            else: # 真实 Redis
                # 使用 pipeline 将多个命令一次性发送给 Redis 服务器
                pipe = self._redis.pipeline()
                pipe.sadd(self._TOTAL_USERS_KEY, user_id_str) # SADD: 添加元素到 Set，如果已存在则忽略
                pipe.sadd(self._ACTIVE_USERS_KEY, user_id_str) # 添加到活跃用户 Set
                pipe.expire(self._ACTIVE_USERS_KEY, self._ACTIVE_USER_TTL) # EXPIRE: 设置 Key 的过期时间（秒）
                pipe.execute() # 执行 pipeline 中的所有命令
        except Exception as e:
            # 记录失败，但不应中断正常流程
            self._logger.warning(f"Redis usage tracking failed for user {user_id}: {e}")


    async def _callback_handler(self, event: events.CallbackQuery.Event):
        try:
            self._logger.info(f'Callback: {event.sender_id} in {event.chat_id}, msg={event.message_id}, data={event.data!r}')
            # [修改] 调用用户活动追踪
            self._track_user_activity(event.sender_id)

            if not event.data: await event.answer("Invalid action."); return
            try: query_data = event.data.decode('utf-8')
            except Exception: await event.answer("Invalid data format."); return
            if not query_data.strip(): await event.answer("Empty action."); return

            parts = query_data.split('=', 1)
            if len(parts) != 2: await event.answer("Action format error."); return
            action, value = parts[0], parts[1]
            redis_prefix = f'{self.id}:'
            bot_chat_id, result_msg_id = event.chat_id, event.message_id
            query_key = f'{redis_prefix}query_text:{bot_chat_id}:{result_msg_id}'
            chats_key = f'{redis_prefix}query_chats:{bot_chat_id}:{result_msg_id}'
            filter_key = f'{redis_prefix}query_filter:{bot_chat_id}:{result_msg_id}'

            # --- 处理翻页和筛选 ---
            if action == 'search_page' or action == 'search_filter':
                 new_page_num, new_filter = 1, "all" # 默认值
                 if action == 'search_page':
                      try: new_page_num = int(value); assert new_page_num > 0
                      except (ValueError, AssertionError): await event.answer("Invalid page."); return
                      # 翻页时保持当前的筛选器
                      new_filter = self._redis.get(filter_key) or "all"
                 else: # action == 'search_filter'
                      new_filter = value if value in ["all", "text_only", "file_only"] else "all"
                      # 更改筛选器时，重置到第一页，并更新 Redis 中存储的筛选器
                      self._redis.set(filter_key, new_filter, ex=3600)
                      new_page_num = 1 # 筛选后回到第一页

                 q = self._redis.get(query_key)
                 chats_str = self._redis.get(chats_key)
                 if q is None:
                     # 搜索上下文已过期（可能 Redis key 超时）
                     try: await event.edit("Search info expired. Please search again.")
                     except Exception: pass # 编辑失败也无妨
                     self._redis.delete(query_key, chats_key, filter_key) # 清理相关 key
                     await event.answer("Search expired."); return

                 chats = [int(cid) for cid in chats_str.split(',')] if chats_str else None
                 self._logger.info(f'Callback Query:"{brief_content(q)}" chats={chats} filter={new_filter} page={new_page_num}')
                 start_time = time()
                 try:
                      # Assuming backend.search exists
                      result = self.backend.search(q, chats, self._cfg.page_len, new_page_num, file_filter=new_filter)
                 except Exception as e:
                      self._logger.error(f"Backend search failed during callback: {e}", exc_info=True)
                      await event.answer("Backend search error."); return

                 # 重新渲染消息内容和按钮
                 response = await self._render_response_text(result, time() - start_time)
                 buttons = self._render_respond_buttons(result, new_page_num, current_filter=new_filter)
                 try:
                      # 编辑原始消息
                      await event.edit(response, parse_mode='html', buttons=buttons, link_preview=False)
                      await event.answer() # 向 Telegram 确认回调已处理
                 except rpcerrorlist.MessageNotModifiedError:
                      await event.answer() # 消息未改变，也确认处理
                 except rpcerrorlist.MessageIdInvalidError:
                      await event.answer("Message deleted or inaccessible.") # 原始消息可能已被删除
                 except Exception as e:
                      self._logger.error(f"Failed to edit message during callback: {e}")
                      await event.answer("Update failed.")

            # --- 处理选择聊天 ---
            elif action == 'select_chat':
                 try:
                      chat_id = int(value)
                      try:
                           # Assuming backend.translate_chat_id exists and is async
                           chat_name = await self.backend.translate_chat_id(chat_id)
                           reply_prompt = f'☑️ 已选择: **{html.escape(chat_name)}** (`{chat_id}`)\n\n请回复此消息进行操作。'
                      except EntityNotFoundError:
                           reply_prompt = f'☑️ 已选择: `{chat_id}` (名称未知)\n\n请回复此消息进行操作。'
                      # 编辑按钮消息，显示选择结果
                      await event.edit(reply_prompt, parse_mode='markdown')
                      # 将选择的 chat_id 存入 Redis，以便后续回复时获取上下文
                      select_key = f'{redis_prefix}select_chat:{bot_chat_id}:{result_msg_id}'
                      self._redis.set(select_key, chat_id, ex=3600) # 存储 1 小时
                      self._logger.info(f"Chat {chat_id} selected by {event.sender_id}, key {select_key}")
                      await event.answer("对话已选择")
                 except ValueError: await event.answer("无效的对话 ID。")
                 except Exception as e:
                      self._logger.error(f"Error in select_chat callback: {e}", exc_info=True)
                      await event.answer("选择对话时出错。")

            elif action == 'noop': # 处理无效按钮（例如页码指示器）
                 await event.answer()
            else: # 未知操作
                 await event.answer("Unknown action.")
        except Exception as e:
             self._logger.error(f"Exception in callback handler: {e}", exc_info=True)
             try: await event.answer("Internal error occurred.") # 通知用户出错
             except Exception as final_e: self._logger.error(f"Failed to answer callback after error: {final_e}")


    async def _normal_msg_handler(self, event: events.NewMessage.Event):
        text: str = event.raw_text.strip()
        sender_id = event.sender_id # Assume sender exists based on hook logic
        self._logger.info(f'User {sender_id} chat {event.chat_id}: "{brief_content(text, 100)}"')
        # [修改] 调用用户活动追踪
        self._track_user_activity(sender_id)
        selected_chat_context = await self._get_selected_chat_from_reply(event)

        if not text or text.startswith('/start'):
            await event.reply("发送关键词进行搜索，或使用 /help 查看帮助。"); return
        elif text.startswith('/help'):
            await event.reply(self.HELP_TEXT_USER, parse_mode='markdown'); return
        elif text.startswith('/random'):
            try:
                # Assuming backend.rand_msg exists
                msg = self.backend.rand_msg()
                # 将 chat_id（int）转换为名称
                try: chat_name = await self.backend.translate_chat_id(msg.chat_id)
                except EntityNotFoundError: chat_name = f"未知对话 ({msg.chat_id})"
                # 构建回复消息
                display = f"📎 {html.escape(msg.filename)}" if msg.filename else html.escape(brief_content(msg.content))
                if msg.filename and msg.content: display += f" ({html.escape(brief_content(msg.content))})"
                respond = f'随机消息来自 **{html.escape(chat_name)}** (`{msg.chat_id}`)\n'
                if msg.sender: respond += f'发送者: {html.escape(msg.sender)}\n'
                respond += f'时间: {msg.post_time.strftime("%Y-%m-%d %H:%M")}\n'
                respond += f'内容: {display or "(空)"}\n<a href="{msg.url}">跳转到消息</a>'
            except IndexError: respond = '错误：索引库为空。'
            except EntityNotFoundError as e: respond = f"错误：源对话 `{e.entity}` 未找到。"
            except Exception as e:
                 self._logger.error(f"Error handling /random: {e}", exc_info=True)
                 respond = f"获取随机消息时出错: {type(e).__name__}"
            await event.reply(respond, parse_mode='html', link_preview=False)

        elif text.startswith('/chats'):
            kw = remove_first_word(text).strip() # 获取 /chats 后面的关键词（可能为空）
            buttons = []
            # 获取后端监控的 chat id 列表
            monitored = sorted(list(self.backend.monitored_chats))
            found = 0
            if monitored:
                for cid in monitored:
                    try:
                         # 获取 chat 名称
                         name = await self.backend.translate_chat_id(cid)
                         # 如果用户提供了关键词，则进行过滤
                         if kw and kw.lower() not in name.lower() and str(cid) != kw: continue
                         found += 1
                         # 最多显示 50 个按钮
                         if found <= 50:
                              # 创建内联按钮，文本为名称+ID，数据为 select_chat=ID
                              buttons.append(Button.inline(f"{brief_content(name, 25)} (`{cid}`)", f'select_chat={cid}'))
                    except EntityNotFoundError:
                         self._logger.warning(f"Chat {cid} not found when listing for /chats.")
                    except Exception as e:
                         self._logger.error(f"Error processing chat {cid} for /chats: {e}")
                if buttons:
                    # 将按钮两两一组排列
                    button_rows = [buttons[i:i+2] for i in range(0, len(buttons), 2)]
                    reply_text = f"请选择对话 ({found} 个结果):" if found <= 50 else f"找到 {found} 个结果, 显示前 50 个:"
                    await event.reply(reply_text, buttons=button_rows)
                else:
                    # 没有找到匹配的对话
                    await event.reply(f'没有找到与 "{html.escape(kw)}" 匹配的已索引对话。' if kw else '没有已索引的对话。')
            else:
                 await event.reply('没有正在监控的对话。请先使用 /download_chat 添加。')

        # --- 处理搜索命令及其别名 ---
        elif text.startswith(('/s ', '/ss ', '/search ', '/s', '/ss', '/search')):
            command = text.split()[0]
            # 提取命令后的查询词
            query = remove_first_word(text).strip() if len(text) > len(command) else ""
            # 如果没有查询词，并且没有通过回复选择对话，则提示用法
            if not query and not selected_chat_context:
                 await event.reply("缺少关键词。用法: `/s 关键词`", parse_mode='markdown')
                 return
            # 调用搜索函数
            await self._search(event, query, selected_chat_context)

        elif text.startswith('/'):
             # 处理未知的斜杠命令
             await event.reply(f'未知命令: `{text.split()[0]}`。请使用 /help 查看帮助。', parse_mode='markdown')
        else:
             # 默认行为：将用户输入的文本作为关键词进行搜索
             await self._search(event, text, selected_chat_context)


    async def _chat_ids_from_args(self, chats_args: List[str]) -> Tuple[List[int], List[str]]:
        """将命令行参数中的字符串（可能是ID或用户名）转换为 share_id 列表"""
        chat_ids, errors = [], []
        if not chats_args: return [], []
        for chat_arg in chats_args:
            try:
                # 调用后端的转换函数，处理数字 ID 或用户名/链接
                chat_ids.append(await self.backend.str_to_chat_id(chat_arg))
            except EntityNotFoundError:
                 errors.append(f'未找到: "{html.escape(chat_arg)}"')
            except Exception as e:
                 errors.append(f'解析 "{html.escape(chat_arg)}" 时出错: {type(e).__name__}')
        return chat_ids, errors


    async def _admin_msg_handler(self, event: events.NewMessage.Event):
        text: str = event.raw_text.strip()
        self._logger.info(f'Admin {event.sender_id} cmd: "{brief_content(text, 100)}"')
        # 检查是否是回复某个消息，如果是，尝试获取之前选择的对话上下文
        selected_chat_context = await self._get_selected_chat_from_reply(event)
        selected_chat_id = selected_chat_context[0] if selected_chat_context else None
        selected_chat_name = selected_chat_context[1] if selected_chat_context else None
        # [修改] 调用用户活动追踪 (也可以考虑不追踪管理员？但追踪了也无妨)
        self._track_user_activity(event.sender_id)

        # --- 统一使用 if/elif/else 处理管理员命令 ---
        if text.startswith('/help'):
             await event.reply(self.HELP_TEXT_ADMIN, parse_mode='markdown'); return
        elif text.startswith('/stat'):
            try:
                # Assuming backend.get_index_status exists and is async
                status = await self.backend.get_index_status()
                await event.reply(status, parse_mode='html', link_preview=False)
            except Exception as e:
                 self._logger.error(f"Error handling /stat: {e}", exc_info=True)
                 await event.reply(f"获取状态时出错: {html.escape(str(e))}\n<pre>{html.escape(format_exc())}</pre>", parse_mode='html')
        elif text.startswith('/download_chat'):
            try:
                 # 使用 shlex 分割参数，处理带引号的情况
                 args = self.download_arg_parser.parse_args(shlex.split(text)[1:])
            except (ArgumentError, Exception) as e:
                 # 参数解析失败，显示帮助信息
                 await event.reply(f"参数错误: {e}\n用法:\n<pre>{html.escape(self.download_arg_parser.format_help())}</pre>", parse_mode='html')
                 return
            min_id, max_id = args.min or 0, args.max or 0
            target_chat_ids, errors = await self._chat_ids_from_args(args.chats)

            # 如果命令中没有指定 chat，但有回复上下文，则使用回复的 chat
            if not args.chats and selected_chat_id is not None and selected_chat_id not in target_chat_ids:
                 target_chat_ids = [selected_chat_id];
                 await event.reply(f"检测到回复: 正在下载 **{html.escape(selected_chat_name or str(selected_chat_id))}** (`{selected_chat_id}`)", parse_mode='markdown')
            elif not target_chat_ids and not errors:
                 # 没有指定 chat，也没有回复上下文，则报错
                 await event.reply("错误: 请指定要下载的对话或回复一个已选择的对话。"); return

            if errors: await event.reply("解析对话参数时出错:\n- " + "\n- ".join(errors))
            if not target_chat_ids: return # 如果没有有效的 chat id，则停止

            # 逐个执行下载
            s, f = 0, 0 # 成功和失败计数
            for cid in target_chat_ids:
                try:
                     await self._download_history(event, cid, min_id, max_id)
                     s += 1
                except Exception as dl_e:
                     f += 1
                     self._logger.error(f"Download failed for chat {cid}: {dl_e}", exc_info=True)
                     # 告知管理员下载失败
                     await event.reply(f"❌ 下载对话 {cid} 失败: {html.escape(str(dl_e))}", parse_mode='html')
            # 如果下载了多个对话，最后给个总结
            if len(target_chat_ids) > 1: await event.reply(f"下载任务完成: {s} 个成功, {f} 个失败。")
        elif text.startswith('/monitor_chat'):
            try:
                 args = self.chat_ids_parser.parse_args(shlex.split(text)[1:])
            except (ArgumentError, Exception) as e:
                 await event.reply(f"参数错误: {e}\n用法:\n<pre>{html.escape(self.chat_ids_parser.format_help())}</pre>", parse_mode='html'); return
            target_chat_ids, errors = await self._chat_ids_from_args(args.chats)

            # 处理回复上下文
            if not args.chats and selected_chat_id is not None and selected_chat_id not in target_chat_ids:
                 target_chat_ids = [selected_chat_id];
                 await event.reply(f"检测到回复: 正在监控 **{html.escape(selected_chat_name or str(selected_chat_id))}** (`{selected_chat_id}`)", parse_mode='markdown')
            elif not target_chat_ids and not errors:
                 await event.reply("错误: 请指定要监控的对话或回复一个已选择的对话。"); return

            if errors: await event.reply("解析对话参数时出错:\n- " + "\n- ".join(errors))
            if not target_chat_ids: return

            # 执行监听操作
            results_msg, added_count, already_monitored_count = [], 0, 0
            for cid in target_chat_ids:
                # Assuming backend.monitored_chats is a set
                if cid in self.backend.monitored_chats:
                     already_monitored_count += 1 # 已经是监控状态
                else:
                    # 添加到后端的监控列表
                    self.backend.monitored_chats.add(cid); added_count += 1
                    try:
                         # 尝试获取格式化的对话 HTML 链接
                         h = await self.backend.format_dialog_html(cid)
                         results_msg.append(f"- ✅ {h} 已加入监控。")
                    except Exception as e:
                         results_msg.append(f"- ✅ `{cid}` 已加入监控 (获取名称出错: {type(e).__name__}).")
                         self._logger.info(f'Admin added chat {cid} to monitor.')
            if results_msg: await event.reply('\n'.join(results_msg), parse_mode='html', link_preview=False)

            # 报告最终状态
            status_parts = []
            if added_count > 0: status_parts.append(f"{added_count} 个对话已添加。")
            if already_monitored_count > 0: status_parts.append(f"{already_monitored_count} 个已在监控中。")
            final_status = " ".join(status_parts)
            await event.reply(final_status if final_status else "未做任何更改。")
        elif text.startswith('/clear'):
            try:
                 args = self.chat_ids_parser.parse_args(shlex.split(text)[1:])
            except (ArgumentError, Exception) as e:
                 await event.reply(f"参数错误: {e}\n用法:\n<pre>{html.escape(self.chat_ids_parser.format_help())}</pre>", parse_mode='html'); return

            # 处理 /clear all
            if len(args.chats) == 1 and args.chats[0].lower() == 'all':
                try:
                    # 调用后端的 clear(None) 清除所有
                    self.backend.clear(None); await event.reply('✅ 所有索引数据已清除。')
                except Exception as e:
                     self._logger.error("Clear all index error:", exc_info=True)
                     await event.reply(f"清除所有索引时出错: {e}")
                return

            # 处理指定对话或回复上下文
            target_chat_ids, errors = await self._chat_ids_from_args(args.chats)
            if not args.chats and selected_chat_id is not None and selected_chat_id not in target_chat_ids:
                 target_chat_ids = [selected_chat_id];
                 await event.reply(f"检测到回复: 正在清除 **{html.escape(selected_chat_name or str(selected_chat_id))}** (`{selected_chat_id}`)", parse_mode='markdown')
            elif not target_chat_ids and not errors:
                 await event.reply("错误: 请指定要清除的对话，或回复一个已选择的对话，或使用 `/clear all`。"); return

            if errors: await event.reply("解析对话参数时出错:\n- " + "\n- ".join(errors))
            if not target_chat_ids: return

            # 执行清除指定对话的操作
            self._logger.info(f'Admin clearing index for chats: {target_chat_ids}')
            try:
                # 调用后端的 clear(list_of_ids)
                self.backend.clear(target_chat_ids); results_msg = []
                for cid in target_chat_ids:
                    try:
                        h = await self.backend.format_dialog_html(cid)
                        results_msg.append(f"- ✅ {h} 的索引已清除。")
                    except Exception:
                        results_msg.append(f"- ✅ `{cid}` 的索引已清除 (名称未知)。")
                await event.reply('\n'.join(results_msg), parse_mode='html', link_preview=False)
            except Exception as e:
                 self._logger.error(f"Clear specific chats error: {e}", exc_info=True)
                 await event.reply(f"清除索引时出错: {e}")
        elif text.startswith('/refresh_chat_names'):
            msg = await event.reply('正在刷新对话名称缓存...')
            try:
                # Assuming backend.session.refresh_translate_table exists and is async
                await self.backend.session.refresh_translate_table()
                await msg.edit('✅ 对话名称缓存已刷新。')
            except Exception as e:
                 self._logger.error("Refresh chat names error:", exc_info=True)
                 await msg.edit(f'刷新缓存时出错: {e}')
        elif text.startswith('/find_chat_id'):
            q = remove_first_word(text).strip();
            if not q: await event.reply('错误: 缺少关键词。'); return
            try:
                # Assuming backend.find_chat_id exists and is async
                results = await self.backend.find_chat_id(q); sb = []
                if results:
                     sb.append(f'找到 {len(results)} 个与 "{html.escape(q)}" 匹配的对话:\n')
                     # 最多显示 50 个结果
                     for cid in results[:50]:
                         try:
                             n = await self.backend.translate_chat_id(cid)
                             sb.append(f'- {html.escape(n)}: `{cid}`\n')
                         except EntityNotFoundError: sb.append(f'- 未知对话: `{cid}`\n')
                         except Exception as e: sb.append(f'- `{cid}` (获取名称出错: {type(e).__name__})\n')
                     if len(results) > 50: sb.append("\n(仅显示前 50 个结果)")
                else: sb.append(f'未找到与 "{html.escape(q)}" 匹配的对话。')
                await event.reply(''.join(sb), parse_mode='html')
            except Exception as e:
                 self._logger.error(f"Find chat ID error: {e}", exc_info=True)
                 await event.reply(f"查找对话 ID 时出错: {e}")
        # [新增] 处理 /usage 命令
        elif text.startswith('/usage'):
            if self._cfg.no_redis:
                 await event.reply("使用统计功能需要 Redis (当前已禁用)。"); return
            try:
                total_count = 0
                active_count = 0
                # 从 Redis (或 FakeRedis) 获取统计数据
                if isinstance(self._redis, FakeRedis):
                    # FakeRedis 直接调用模拟的 scard
                    total_count = self._redis.scard(self._TOTAL_USERS_KEY)
                    active_count = self._redis.scard(self._ACTIVE_USERS_KEY) # 依赖 FakeRedis 的过期逻辑
                else: # 真实 Redis
                    # 使用 pipeline 一次性获取两个值
                    pipe = self._redis.pipeline()
                    pipe.scard(self._TOTAL_USERS_KEY)  # SCARD: 返回 Set 的基数（元素数量）
                    pipe.scard(self._ACTIVE_USERS_KEY)
                    results = pipe.execute()
                    total_count = results[0] if results and len(results) > 0 else 0
                    active_count = results[1] if results and len(results) > 1 else 0

                await event.reply(f"📊 **使用统计**\n"
                                  f"- 总独立用户数: {total_count}\n"
                                  f"- 活跃用户数 (最近15分钟): {active_count}", parse_mode='markdown')
            except Exception as e:
                self._logger.error(f"Failed to get usage stats: {e}", exc_info=True)
                await event.reply(f"获取使用统计时出错: {html.escape(str(e))}")
        else:
             # 如果管理员输入的不是以上任何命令，则按普通用户消息处理
             await self._normal_msg_handler(event)


    async def _search(self, event: events.NewMessage.Event, query: str, selected_chat_context: Optional[Tuple[int, str]]):
        selected_chat_id = selected_chat_context[0] if selected_chat_context else None
        selected_chat_name = selected_chat_context[1] if selected_chat_context else None

        # 如果在选定对话上下文中没有提供查询词，则搜索该对话的全部内容
        if not query and selected_chat_context:
             query = '*' # Whoosh 中 * 代表匹配所有文档
             await event.reply(f"正在搜索 **{html.escape(selected_chat_name or str(selected_chat_id))}** (`{selected_chat_id}`) 中的所有消息", parse_mode='markdown')
        elif not query:
             # 如果全局搜索且没有查询词，则忽略（避免返回所有消息）
             self._logger.debug("Empty query ignored for global search.")
             # 或者可以回复提示用户输入关键词
             # await event.reply("请输入要搜索的关键词。")
             return

        target_chats = [selected_chat_id] if selected_chat_id is not None else None # 如果有选定对话，则只搜索该对话
        try:
             # 检查索引是否为空 (全局或特定对话)
             is_empty = self.backend.is_empty(selected_chat_id)
        except Exception as e:
             self._logger.error(f"Check index empty error: {e}")
             await event.reply("检查索引状态时出错。"); return

        if is_empty:
            if selected_chat_context:
                await event.reply(f'对话 **{html.escape(selected_chat_name or str(selected_chat_id))}** 的索引为空。')
            else:
                await event.reply('全局索引为空。')
            return

        start = time(); ctx_info = f"在对话 {selected_chat_id} 中" if target_chats else "全局"
        self._logger.info(f'正在搜索 "{brief_content(query)}" ({ctx_info})')
        try:
            # 调用后端搜索，初始搜索不过滤文件类型 (file_filter="all")
            result = self.backend.search(query, target_chats, self._cfg.page_len, 1, file_filter="all")
            # 渲染回复文本
            text = await self._render_response_text(result, time() - start)
            # 生成按钮（翻页、筛选）
            buttons = self._render_respond_buttons(result, 1, current_filter="all") # 初始 filter 是 "all"
            msg = await event.reply(text, parse_mode='html', buttons=buttons, link_preview=False)

            # 如果成功发送了回复消息，将搜索上下文存入 Redis，用于后续翻页/筛选
            if msg:
                prefix, bcid, mid = f'{self.id}:', event.chat_id, msg.id # 构造 Redis key 前缀
                # 存储查询文本、筛选器状态和目标对话列表 (如果是全局搜索则删除 chats key)
                self._redis.set(f'{prefix}query_text:{bcid}:{mid}', query, ex=3600)
                self._redis.set(f'{prefix}query_filter:{bcid}:{mid}', "all", ex=3600) # 存储初始 filter
                if target_chats:
                    self._redis.set(f'{prefix}query_chats:{bcid}:{mid}', ','.join(map(str, target_chats)), ex=3600)
                else:
                    # 如果是全局搜索，确保删除 chats key (可能之前搜索留下的)
                    self._redis.delete(f'{prefix}query_chats:{bcid}:{mid}')
        except whoosh.index.LockError:
             # Whoosh 索引被锁，通常是写入操作正在进行
             await event.reply('⏳ 索引当前正忙，请稍后再试。')
        except Exception as e:
             self._logger.error(f"Search execution error: {e}", exc_info=True)
             await event.reply(f'搜索时发生错误: {type(e).__name__}。')


    async def _download_history(self, event: events.NewMessage.Event, chat_id: int, min_id: int, max_id: int):
         # chat_id is assumed to be share_id already
         try:
              # 获取对话的 HTML 格式名称，用于显示
              chat_html = await self.backend.format_dialog_html(chat_id)
         except Exception as e:
              chat_html = f"对话 `{chat_id}`" # 获取失败则用 ID

         try:
             # 如果是从头下载 (min_id=0, max_id=0) 且该对话已有索引，则发出警告
             if min_id == 0 and max_id == 0 and not self.backend.is_empty(chat_id):
                 await event.reply(f'⚠️ 警告: {chat_html} 的索引已存在。重新下载可能导致消息重复。'
                                   f'如果需要重新下载，请先使用 `/clear {chat_id}` 清除旧索引，或指定 `--min/--max` 范围。',
                                   parse_mode='html')
         except Exception as e:
             self._logger.error(f"Check empty error before download {chat_id}: {e}")

         prog_msg: Optional[TgMessage] = None # 用于存储进度消息对象
         last_update = time(); interval = 5 # 每 5 秒更新一次进度
         count = 0 # 已下载计数

         # 定义回调函数，用于在下载过程中更新进度
         async def cb(cur_id: int, dl_count: int):
             nonlocal prog_msg, last_update, count; count = dl_count; now = time()
             # 控制更新频率
             if now - last_update > interval:
                 last_update = now
                 txt = f'⏳ 正在下载 {chat_html}:\n已处理 {dl_count} 条，当前消息 ID: {cur_id}'
                 try:
                     if prog_msg is None: # 如果还没有发送过进度消息，则发送新的
                          prog_msg = await event.reply(txt, parse_mode='html')
                     else: # 否则编辑之前的进度消息
                          await prog_msg.edit(txt, parse_mode='html')
                 except rpcerrorlist.FloodWaitError as fwe:
                      # 处理 Telegram 的频率限制
                      self._logger.warning(f"Flood wait ({fwe.seconds}s) during progress update for {chat_id}.")
                      last_update += fwe.seconds # 延迟下一次更新
                 except rpcerrorlist.MessageNotModifiedError: pass # 消息内容无变化，忽略
                 except rpcerrorlist.MessageIdInvalidError: prog_msg = None # 进度消息可能被删了
                 except Exception as e:
                      self._logger.error(f"Edit progress message error {chat_id}: {e}"); prog_msg = None

         start = time()
         try:
              # 调用后端的下载函数，传入回调
              await self.backend.download_history(chat_id, min_id, max_id, cb)
              # 下载完成后的提示消息
              msg = f'✅ {chat_html} 下载完成，索引了 {count} 条消息，耗时 {time()-start:.2f} 秒。'
              try:
                   # 尝试回复原始命令消息
                   await event.reply(msg, parse_mode='html')
              except Exception:
                   # 回复失败（例如原始消息被删），则直接发送到当前聊天窗口
                   await self.bot.send_message(event.chat_id, msg, parse_mode='html')
         except (EntityNotFoundError, ValueError) as e: # 捕获已知的可预料错误
              self._logger.error(f"Download failed for {chat_id}: {e}")
              await event.reply(f'❌ 下载 {chat_html} 时出错: {e}', parse_mode='html')
         except Exception as e: # 其他未知错误
              self._logger.error(f"Unknown download error for {chat_id}: {e}", exc_info=True)
              await event.reply(f'❌ 下载 {chat_html} 时发生未知错误: {type(e).__name__}', parse_mode='html')
         finally:
              # 无论成功失败，尝试删除最后的进度消息
              if prog_msg:
                  try:
                      await prog_msg.delete()
                  except Exception: # 删除失败也无妨 (例如已经被删了)
                      pass


    def _register_hooks(self):
        # 处理按钮回调
        @self.bot.on(events.CallbackQuery())
        async def cq_handler(event: events.CallbackQuery.Event):
             # 权限检查 (私聊模式下)
             is_admin = self._admin_id and event.sender_id == self._admin_id
             is_wl = event.sender_id in self._cfg.private_whitelist
             if self._cfg.private_mode and not is_admin and not is_wl:
                   await event.answer("您没有权限执行此操作。", alert=True); return
             # 调用回调处理函数
             await self._callback_handler(event)

        # 处理新消息
        @self.bot.on(events.NewMessage())
        async def msg_handler(event: events.NewMessage.Event):
            # 基础检查：确保有消息体和发送者 ID
            if not event.message or not event.sender_id:
                 self._logger.debug("Ignoring message without body or sender.")
                 return

            sender = await event.message.get_sender() # 获取发送者对象
            # 忽略机器人自己发送的消息或无法获取发送者的消息
            if not sender or (self.my_id and sender.id == self.my_id): return

            is_admin = self._admin_id and sender.id == self._admin_id

            # 判断是否需要处理此消息 (私聊，或群组/频道中被 @ 或回复)
            mentioned, reply_to_bot = False, False
            if event.is_group or event.is_channel: # 群组/频道消息
                 # 检查是否被 @
                 if self.username and f'@{self.username}' in event.raw_text: mentioned = True
                 # 更可靠地检查实体中的提及
                 elif event.message.mentioned and event.message.entities:
                      for entity in event.message.entities:
                          if isinstance(entity, MessageEntityMentionName) and entity.user_id == self.my_id:
                              mentioned = True; break
                 # 检查是否回复了机器人的消息
                 if event.message.is_reply and event.message.reply_to_msg_id:
                      try:
                          reply = await event.message.get_reply_message()
                          # 确认回复的消息存在且是机器人发的
                          reply_to_bot = reply and reply.sender_id == self.my_id
                      except Exception as e:
                          # 获取回复消息失败（可能被删），忽略
                          self._logger.warning(f"Could not get reply message {event.message.reply_to_msg_id} in chat {event.chat_id}: {e}")
                          pass

            # 只有私聊消息，或者在群组/频道中被提及/回复的消息才需要处理
            process = event.is_private or mentioned or reply_to_bot
            if not process: return

            # 私聊模式权限检查 (如果启用了私聊模式且用户不是管理员)
            if self._cfg.private_mode and not is_admin:
                 sender_allowed = sender.id in self._cfg.private_whitelist
                 chat_allowed = False # 对话是否在白名单（例如允许特定群组使用）
                 if event.chat_id: # 仅当 chat_id 存在时检查
                      try:
                          csi = get_share_id(event.chat_id) # 获取对话的 share_id
                          chat_allowed = csi in self._cfg.private_whitelist
                      except Exception: pass # 转换 chat_id 出错则忽略

                 # 如果发送者和对话都不在白名单，则拒绝访问
                 if not sender_allowed and not chat_allowed:
                     self._logger.debug(f"Permission denied in private mode for user {sender.id} in chat {event.chat_id}")
                     # 只在私聊中回复拒绝消息，避免在群组中刷屏
                     if event.is_private: await event.reply('抱歉，您没有权限使用此机器人。')
                     return

            # 根据是否是管理员，分发到不同的处理函数
            handler = self._admin_msg_handler if is_admin else self._normal_msg_handler
            try:
                 await handler(event)
            # 捕获特定和常见的错误
            except whoosh.index.LockError:
                 await event.reply('⏳ 索引当前正忙，请稍后再试。')
            except EntityNotFoundError as e:
                 await event.reply(f'❌ 未找到相关实体: {e.entity}')
            except rpcerrorlist.UserIsBlockedError:
                 # 用户已将机器人拉黑
                 self._logger.warning(f"User {sender.id} blocked the bot.")
            except rpcerrorlist.ChatWriteForbiddenError:
                 # 机器人在此对话中被禁言
                 self._logger.warning(f"Write forbidden in chat: {event.chat_id}.")
            # 捕获其他所有异常
            except Exception as e:
                 et = type(e).__name__ # 获取异常类型名称
                 self._logger.error(f"Error handling message from {sender.id} in {event.chat_id}: {et}: {e}", exc_info=True)
                 try:
                      # 向用户报告发生错误
                      await event.reply(f'处理您的请求时发生错误: {et}。\n如果问题持续存在，请联系管理员。')
                 except Exception as re:
                      # 如果连报告错误都失败了...
                      self._logger.error(f"Replying with error message failed: {re}")
                 # 如果配置了管理员，且错误不是发生在与管理员的私聊中，则向管理员发送详细错误报告
                 if self._admin_id and event.chat_id != self._admin_id:
                      try:
                          error_details = f"处理来自用户 {sender.id} 在对话 {event.chat_id} 的消息时出错:\n"
                          error_details += f"<pre>{html.escape(format_exc())}</pre>" # 发送完整的 Traceback
                          await self.bot.send_message(self._admin_id, error_details, parse_mode='html')
                      except Exception as ne:
                          self._logger.error(f"Notifying admin about error failed: {ne}")


    async def _get_selected_chat_from_reply(self, event: events.NewMessage.Event) -> Optional[Tuple[int, str]]:
        """检查消息是否回复了之前的 /chats 选择消息，如果是，返回选择的 chat_id 和名称"""
        # 必须是回复消息，且有回复目标 ID
        if not event.message.is_reply or not event.message.reply_to_msg_id: return None

        # 构造用于存储选择上下文的 Redis key
        key = f'{self.id}:select_chat:{event.chat_id}:{event.message.reply_to_msg_id}'
        # 从 Redis 读取
        res = self._redis.get(key)
        if res:
            try:
                 cid = int(res) # 尝试将存储的值转为整数 chat_id
                 # 尝试从后端获取对话名称
                 name = await self.backend.translate_chat_id(cid)
                 return cid, name # 返回 (chat_id, chat_name)
            except ValueError:
                 # 如果 Redis 中存的值不是有效的整数 ID，删除这个无效 key
                 self._redis.delete(key); return None
            except EntityNotFoundError:
                 # 如果 chat_id 有效但后端找不到名称，返回 ID 和未知提示
                 return int(res), f"未知对话 ({res})"
            except Exception as e:
                 # 其他获取名称的错误
                 self._logger.error(f"Error getting selected chat name for key {key}: {e}"); return None
        return None # Redis 中没有对应的 key


    async def _register_commands(self):
        """注册 Telegram Bot 命令"""
        admin_peer = None
        if self._admin_id:
            try:
                 # 获取管理员的 InputPeer 对象，用于设置特定范围的命令
                 admin_peer = await self.bot.get_input_entity(self._admin_id)
            except ValueError: # 如果 admin_id 是用户名且未被 TG 缓存，直接获取会失败
                 self._logger.warning(f"Could not get input entity for admin ID {self._admin_id} directly (might be username). Trying get_entity.")
                 try:
                     admin_entity = await self.bot.get_entity(self._admin_id) # 先获取实体
                     admin_peer = await self.bot.get_input_entity(admin_entity) # 再获取 InputPeer
                 except Exception as e:
                      self._logger.error(f'Failed to get admin input entity via get_entity for {self._admin_id}: {e}')
            except Exception as e:
                 self._logger.error(f'Failed to get admin input entity for {self._admin_id}: {e}')
        else:
             self._logger.warning("Admin ID invalid or not configured, skipping admin-specific command registration.")

        # 定义管理员命令列表 (command, description)
        # [修改] 添加了 /usage 命令
        admin_commands = [ BotCommand(c, d) for c, d in [
            ("download_chat", '[选项] [对话...] 下载历史'),
            ("monitor_chat", '对话... 添加实时监控'),
            ("clear", '[对话...|all] 清除索引'),
            ("stat", '查询后端状态'),
            ("find_chat_id", '关键词 查找对话ID'),
            ("refresh_chat_names", '刷新对话名称缓存'),
            ("usage", '查看使用统计')
        ]]
        # 定义普通用户命令列表
        common_commands = [ BotCommand(c, d) for c, d in [
            ("s", '关键词 搜索 (或 /search /ss)'),
            ("chats", '[关键词] 列出/选择对话'),
            ("random", '随机返回一条消息'),
            ("help", '显示帮助信息')
        ]]

        # 为管理员设置命令 (管理员命令 + 普通用户命令)
        if admin_peer:
            try:
                await self.bot(SetBotCommandsRequest(
                    scope=BotCommandScopePeer(admin_peer), # scope 指定命令生效范围为特定用户
                    lang_code='', # lang_code 为空表示所有语言
                    commands=admin_commands + common_commands # 合并命令列表
                ))
                self._logger.info(f"Admin commands set successfully for peer {self._admin_id}.")
            except Exception as e:
                # 设置失败（例如权限问题）
                self._logger.error(f"Setting admin commands failed for peer {self._admin_id}: {e}")

        # 为所有其他用户设置默认命令 (仅普通用户命令)
        try:
            await self.bot(SetBotCommandsRequest(
                scope=BotCommandScopeDefault(), # scope=Default 表示默认范围
                lang_code='',
                commands=common_commands
            ))
            self._logger.info("Default commands set successfully.")
        except Exception as e:
            self._logger.error(f"Setting default commands failed: {e}")


    async def _render_response_text(self, result: SearchResult, used_time: float) -> str:
        """将搜索结果渲染为发送给用户的文本"""
        # 检查结果是否有效，是否有命中
        if not isinstance(result, SearchResult) or not result.hits:
             return "没有找到相关的消息。"

        sb = [f'找到 {result.total_results} 条结果，耗时 {used_time:.3f} 秒:\n\n']
        for i, hit in enumerate(result.hits, 1): # 遍历命中结果，带序号
            try:
                msg = hit.msg # 获取 IndexMsg 对象
                # 确保 msg 是有效的 IndexMsg 对象
                if not isinstance(msg, IndexMsg):
                     sb.append(f"<b>{i}.</b> 错误: 无效的消息数据。\n\n")
                     continue

                try:
                    # 尝试获取对话标题
                    title = await self.backend.translate_chat_id(msg.chat_id)
                except EntityNotFoundError:
                    title = f"未知对话 ({msg.chat_id})"
                except Exception as te:
                    # 获取标题出错
                    title = f"错误 ({msg.chat_id}): {type(te).__name__}"

                # 构建消息头：序号、标题、发送者、时间
                hdr = [f"<b>{i}. {html.escape(title)}</b>"] # 序号和加粗的标题
                if msg.sender:
                     hdr.append(f"(<u>{html.escape(msg.sender)}</u>)") # 下划线的发送者
                # 确保 post_time 是 datetime 对象再格式化
                if isinstance(msg.post_time, datetime):
                    hdr.append(f'[{msg.post_time.strftime("%y-%m-%d %H:%M")}]') # 格式化时间
                else:
                     hdr.append('[无效时间]') # 时间无效时的后备显示

                sb.append(' '.join(hdr) + '\n') # 拼接头部信息
                if msg.filename:
                     # 如果有文件名，显示文件名
                     sb.append(f"📎 文件: <b>{html.escape(msg.filename)}</b>\n")

                # [修改] 直接使用 hit.highlighted，它已包含上下文和 <b> 标签
                # 这个 highlighted 字段由 indexer.py 中的 HtmlFormatter 生成
                display_text = hit.highlighted

                # 如果 highlighted 为空（例如只有文件，或高亮失败），提供后备内容
                if not display_text:
                     if msg.content:
                          # 使用简短内容作为后备，并进行 HTML 转义
                          display_text = html.escape(brief_content(msg.content, 150))
                     elif msg.filename:
                          # 只有文件，没有文本内容
                          display_text = f"<i>(文件，无文本内容)</i>"
                     else:
                          # 消息完全为空
                          display_text = "<i>(空消息)</i>"

                # 添加跳转链接和高亮/后备文本
                if msg.url:
                     sb.append(f'<a href="{html.escape(msg.url)}">跳转到消息</a>\n{display_text}\n\n')
                else:
                     # 如果没有 URL
                     sb.append(f"{display_text} (无链接)\n\n")
            except Exception as e:
                 # 单条结果渲染出错
                 sb.append(f"<b>{i}.</b> 渲染此条结果时出错: {type(e).__name__}\n\n")
                 self._logger.error(f"Error rendering hit (msg URL: {getattr(hit, 'msg', None) and getattr(hit.msg, 'url', 'N/A')}): {e}", exc_info=True)

        final = ''.join(sb); max_len = 4096 # Telegram 消息长度限制
        # 如果生成的消息过长，进行截断
        if len(final) > max_len:
             cutoff_msg = "\n\n...(结果过多，仅显示部分)"
             cutoff_point = max_len - len(cutoff_msg) - 10 # 留一点余地
             # 尝试在最后一个双换行符处截断，保持格式美观
             last_nl = final.rfind('\n\n', 0, cutoff_point)
             if last_nl != -1:
                 final = final[:last_nl] + cutoff_msg
             else: # 无法优雅截断，直接硬截断
                 final = final[:max_len - len(cutoff_msg)] + cutoff_msg
        return final


    def _render_respond_buttons(self, result: SearchResult, cur_page_num: int, current_filter: str = "all") -> Optional[List[List[Button]]]:
        """根据搜索结果生成翻页和筛选按钮"""
        if not isinstance(result, SearchResult): return None
        buttons = [] # 存储按钮行

        # --- 第一行：筛选按钮 ---
        # 根据当前的筛选状态 (current_filter)，给对应的按钮加上【】高亮
        filter_buttons = [
            Button.inline("【全部】" if current_filter == "all" else "全部", 'search_filter=all'),
            Button.inline("【纯文本】" if current_filter == "text_only" else "纯文本", 'search_filter=text_only'),
            Button.inline("【仅文件】" if current_filter == "file_only" else "仅文件", 'search_filter=file_only')
        ]
        buttons.append(filter_buttons)

        # --- 第二行：翻页按钮 ---
        try:
             # 计算总页数
             page_len = max(1, self._cfg.page_len) # 防止 page_len 配置为 0 或负数
             total_pages = (result.total_results + page_len - 1) // page_len
        except Exception as e:
             self._logger.error(f"Error calculating total pages: {e}")
             total_pages = 1 # 计算出错时，假定只有一页

        if total_pages > 1: # 只有超过一页时才需要翻页按钮
            page_buttons = []
            # 上一页按钮 (当前页 > 1 时显示)
            if cur_page_num > 1:
                 page_buttons.append(Button.inline('⬅️ 上一页', f'search_page={cur_page_num - 1}'))

            # 当前页码指示器 (不可点击)
            page_buttons.append(Button.inline(f'{cur_page_num}/{total_pages}', 'noop')) # 'noop' 表示无操作

            # 下一页按钮 (不是最后一页时显示)
            # 使用 result.is_last_page (由后端 Indexer.search 计算得到) 判断是否是最后一页
            if not result.is_last_page and cur_page_num < total_pages: # 双重检查
                 page_buttons.append(Button.inline('下一页 ➡️', f'search_page={cur_page_num + 1}'))

            # 只有当存在翻页按钮时才添加这一行
            if page_buttons:
                 buttons.append(page_buttons)

        # 如果 buttons 列表不为空 (即至少有一行按钮)，则返回列表，否则返回 None
        return buttons if buttons else None

# Example minimal main execution block (if needed for testing)
# if __name__ == '__main__':
#     logger.info("Frontend Bot script loaded.")
#     # Add basic setup for testing if desired
