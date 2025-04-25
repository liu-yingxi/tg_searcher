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
    print("Warning: Assuming relative imports fail, define fallbacks if needed.")
    class CommonBotConfig: pass
    def get_logger(name): import logging; return logging.getLogger(name)
    def get_share_id(x): return int(x) if x else 0
    def remove_first_word(s): return ' '.join(s.split()[1:]) if len(s.split()) > 1 else ''
    def brief_content(s, l=70): return (s[:l] + '...') if len(s) > l else s # 更新默认长度
    class BackendBot: pass
    class EntityNotFoundError(Exception): pass
    class SearchResult: pass
    class IndexMsg: pass

logger = get_logger('frontend_bot')


class BotFrontendConfig:
    """存储 Frontend Bot 配置的类"""
    @staticmethod
    def _parse_redis_cfg(redis_cfg: str) -> Tuple[str, int]:
        """解析 Redis 'host:port' 配置字符串"""
        colon_idx = redis_cfg.find(':')
        if colon_idx < 0: return redis_cfg, 6379
        try:
            host = redis_cfg[:colon_idx] if colon_idx > 0 else 'localhost'; port = int(redis_cfg[colon_idx + 1:])
            return host, port
        except (ValueError, TypeError): raise ValueError(f"Invalid Redis port in '{redis_cfg}'")

    def __init__(self, **kw: Any):
        """从关键字参数初始化配置"""
        try:
            self.bot_token: str = kw['bot_token']; self.admin: Union[int, str] = kw['admin_id']
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


class FakeRedis:
    """一个简单的内存字典，模拟部分 Redis 功能，用于在无 Redis 环境下运行"""
    def __init__(self):
        self._data = {} # 存储格式: { key: (value, expiry_timestamp_or_None) }
        self._logger = get_logger('FakeRedis')
        self._logger.warning("Using FakeRedis: Data is volatile and will be lost on restart.")
    def get(self, key):
        v = self._data.get(key)
        if v and (v[1] is None or v[1] > time()): return v[0]
        elif v and v[1] is not None and v[1] <= time(): del self._data[key]
        return None
    def set(self, key, val, ex=None):
        expiry = time() + ex if ex else None
        self._data[key] = (str(val), expiry)
    def delete(self, *keys):
        count = 0; [self._data.pop(k, None) for k in keys]; return len(keys) # Simplified
    def ping(self): return True
    def sadd(self, key, *values):
        s, exp = self._data.get(key, (set(), None)); added = 0
        if not isinstance(s, set) or (exp and exp <= time()): s=set(); exp=None
        vals = {str(v) for v in values}; [ (s.add(v), added := added + 1) for v in vals if v not in s ]; self._data[key] = (s, exp); return added
    def scard(self, key):
        v = self._data.get(key); return len(v[0]) if v and isinstance(v[0], set) and (v[1] is None or v[1] > time()) else 0
    def expire(self, key, seconds):
        if key in self._data: v, _ = self._data[key]; self._data[key] = (v, time() + seconds); return 1
        return 0


class BotFrontend:
    """处理用户交互、命令解析、结果展示的前端 Bot 类"""
    # 帮助文本
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
    # 文本显示最大字符数
    MAX_TEXT_DISPLAY_LENGTH = 70
    # 高亮 HTML 安全长度限制 (防止极端情况)
    MAX_HIGHLIGHT_HTML_LENGTH = 350

    def __init__(self, common_cfg: CommonBotConfig, cfg: BotFrontendConfig, frontend_id: str, backend: BackendBot):
        """初始化 Frontend Bot"""
        self.backend = backend
        self.id = frontend_id
        self._common_cfg = common_cfg
        self.bot = TelegramClient(str(common_cfg.session_dir / f'frontend_{self.id}.session'),
                                  api_id=common_cfg.api_id, api_hash=common_cfg.api_hash, proxy=common_cfg.proxy)
        self._cfg = cfg
        # 初始化 Redis 连接或 FakeRedis，并处理错误
        if cfg.no_redis or cfg.redis_host is None:
            self._redis = FakeRedis()
        else:
            try:
                self._redis = Redis(host=cfg.redis_host[0], port=cfg.redis_host[1], decode_responses=True)
                self._redis.ping() # 尝试连接
            except RedisConnectionError as e:
                logger.critical(f'Redis connection failed {cfg.redis_host}: {e}. Falling back to FakeRedis.')
                self._redis = FakeRedis(); self._cfg.no_redis = True
            except RedisResponseError as e: # 捕获配置错误
                logger.critical(f'Redis configuration error (MISCONF?) {cfg.redis_host}: {e}. Falling back to FakeRedis.')
                self._redis = FakeRedis(); self._cfg.no_redis = True
            except Exception as e:
                logger.critical(f'Redis init error {cfg.redis_host}: {e}. Falling back to FakeRedis.')
                self._redis = FakeRedis(); self._cfg.no_redis = True

        self._logger = logger
        self._admin_id: Optional[int] = None
        self.username: Optional[str] = None
        self.my_id: Optional[int] = None

        # Redis Keys for statistics
        self._TOTAL_USERS_KEY = f'{self.id}:total_users'
        self._ACTIVE_USERS_KEY = f'{self.id}:active_users_15m'
        self._ACTIVE_USER_TTL = 900 # 15 minutes

        # 命令参数解析器
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
            self._admin_id = await self.backend.str_to_chat_id(str(self._cfg.admin))
            logger.info(f"Admin ID resolved to: {self._admin_id}")
            if self._cfg.private_mode and self._admin_id: self._cfg.private_whitelist.add(self._admin_id); logger.info("Admin added to private whitelist.")
        except EntityNotFoundError: logger.critical(f"Admin entity '{self._cfg.admin}' not found."); self._admin_id = None
        except (ValueError, TypeError) as e: logger.critical(f"Invalid admin config '{self._cfg.admin}': {e}"); self._admin_id = None
        except Exception as e: logger.critical(f"Error resolving admin '{self._cfg.admin}': {e}", exc_info=True); self._admin_id = None
        if not self._admin_id: logger.error("Proceeding without valid admin ID.")

        # 再次检查 Redis 连接
        if not isinstance(self._redis, FakeRedis):
             try: self._redis.ping(); logger.info(f"Redis connected at {self._cfg.redis_host}")
             except (RedisConnectionError, RedisResponseError) as e: logger.critical(f'Redis check failed during start: {e}. Falling back to FakeRedis.'); self._redis = FakeRedis(); self._cfg.no_redis = True

        logger.info(f'Starting frontend bot {self.id}...')
        try:
            await self.bot.start(bot_token=self._cfg.bot_token)
            me = await self.bot.get_me(); assert me is not None
            self.username, self.my_id = me.username, me.id; logger.info(f'Bot (@{self.username}, id={self.my_id}) login ok')
            if self.my_id: self.backend.excluded_chats.add(get_share_id(self.my_id)); logger.info(f"Bot ID {self.my_id} excluded from backend.")
            await self._register_commands(); logger.info('Commands registered.')
            self._register_hooks()
            # 发送启动消息给管理员
            if self._admin_id:
                 try: status_msg = await self.backend.get_index_status(4000 - 100); await self.bot.send_message(self._admin_id, f'✅ Bot frontend init complete ({self.id})\n\n{status_msg}', parse_mode='html', link_preview=False)
                 except Exception as e: logger.error(f"Failed get/send initial status: {e}", exc_info=True); await self.bot.send_message(self._admin_id, f'⚠️ Bot ({self.id}) started, but failed get status: {e}')
            logger.info(f"Frontend bot {self.id} started successfully.")
        except Exception as e: logger.critical(f"Frontend start failed: {e}", exc_info=True)

    def _track_user_activity(self, user_id: Optional[int]):
        """记录用户活动（用于 /usage 统计）"""
        # 忽略机器人自身、管理员、或者 Redis 已禁用/出错的情况
        if not user_id or user_id == self._admin_id or user_id == self.my_id or self._cfg.no_redis: return
        try:
            user_id_str = str(user_id)
            if isinstance(self._redis, FakeRedis): # FakeRedis 不支持 pipeline
                self._redis.sadd(self._TOTAL_USERS_KEY, user_id_str)
                self._redis.sadd(self._ACTIVE_USERS_KEY, user_id_str)
                self._redis.expire(self._ACTIVE_USERS_KEY, self._ACTIVE_USER_TTL)
            else: # 使用 pipeline 提高效率
                pipe = self._redis.pipeline()
                pipe.sadd(self._TOTAL_USERS_KEY, user_id_str) # 记录到总用户 set
                pipe.sadd(self._ACTIVE_USERS_KEY, user_id_str) # 记录到活跃用户 set
                pipe.expire(self._ACTIVE_USERS_KEY, self._ACTIVE_USER_TTL) # 设置活跃用户 key 的过期时间
                pipe.execute()
        except RedisResponseError as e: # 处理 Redis 写入配置错误
            if "MISCONF" in str(e) and not self._cfg.no_redis:
                 logger.error(f"Redis MISCONF error during usage tracking. Disabling Redis for this frontend instance. Error: {e}")
                 self._redis = FakeRedis() # 切换到 FakeRedis
                 self._cfg.no_redis = True # 标记为禁用
            else: logger.warning(f"Redis usage tracking failed for user {user_id}: {e}")
        except Exception as e: logger.warning(f"Redis usage tracking failed for user {user_id}: {e}")

    async def _callback_handler(self, event: events.CallbackQuery.Event):
        """处理按钮回调查询"""
        try:
            logger.info(f'Callback: {event.sender_id} in {event.chat_id}, msg={event.message_id}, data={event.data!r}')
            self._track_user_activity(event.sender_id) # 记录用户活动

            if not event.data: await event.answer("无效操作。"); return
            try: query_data = event.data.decode('utf-8')
            except Exception: await event.answer("无效数据格式。"); return
            if not query_data.strip(): await event.answer("空操作。"); return

            parts = query_data.split('=', 1)
            if len(parts) != 2: await event.answer("操作格式错误。"); return
            action, value = parts[0], parts[1]
            redis_prefix = f'{self.id}:'
            bot_chat_id, result_msg_id = event.chat_id, event.message_id
            # 缓存 Keys
            query_key = f'{redis_prefix}query_text:{bot_chat_id}:{result_msg_id}'
            chats_key = f'{redis_prefix}query_chats:{bot_chat_id}:{result_msg_id}'
            filter_key = f'{redis_prefix}query_filter:{bot_chat_id}:{result_msg_id}'
            page_key = f'{redis_prefix}query_page:{bot_chat_id}:{result_msg_id}'

            # --- 处理翻页和筛选 ---
            if action == 'search_page' or action == 'search_filter':
                 # 从 Redis 获取当前搜索上下文 (如果 Redis 可用)
                 current_filter = "all"; current_chats_str = None; current_query = None; current_page = 1
                 if not self._cfg.no_redis:
                     try:
                         pipe = self._redis.pipeline()
                         pipe.get(filter_key); pipe.get(chats_key); pipe.get(query_key); pipe.get(page_key)
                         results = pipe.execute()
                         current_filter = results[0] or "all"
                         current_chats_str = results[1]
                         current_query = results[2]
                         current_page = int(results[3] or 1)
                     except (RedisResponseError, RedisConnectionError) as e:
                         logger.error(f"Redis error getting context in callback: {e}")
                         await event.answer("缓存服务暂时遇到问题，无法处理翻页/筛选。", alert=True); return
                     except Exception as e:
                         logger.error(f"Error getting context from Redis: {e}"); return # Non-redis error

                 # 检查上下文是否有效
                 if current_query is None:
                     try: await event.edit("搜索信息已过期，请重新搜索。")
                     except Exception: pass
                     # 清理可能存在的旧 keys
                     if not self._cfg.no_redis: self._redis.delete(query_key, chats_key, filter_key, page_key)
                     await event.answer("搜索已过期。"); return

                 # 确定新的页码和过滤器
                 new_page, new_filter = current_page, current_filter
                 if action == 'search_page':
                      try: new_page = int(value); assert new_page > 0
                      except (ValueError, AssertionError): await event.answer("无效页码。"); return
                 else: # action == 'search_filter'
                      temp_filter = value if value in ["all", "text_only", "file_only"] else "all"
                      if temp_filter != current_filter: # 过滤器改变
                           new_filter = temp_filter
                           new_page = 1 # 重置到第一页
                      # 否则过滤器未变，页码保持不变

                 # 更新 Redis 中的上下文 (如果 Redis 可用且有变化)
                 if not self._cfg.no_redis and (new_page != current_page or new_filter != current_filter):
                     try:
                         pipe = self._redis.pipeline()
                         pipe.set(page_key, new_page, ex=3600) # 更新页码
                         pipe.set(filter_key, new_filter, ex=3600) # 更新过滤器
                         # Query 和 Chats 不变，只需刷新它们的 TTL
                         pipe.expire(query_key, 3600); pipe.expire(chats_key, 3600)
                         pipe.execute()
                     except (RedisResponseError, RedisConnectionError) as e:
                         logger.error(f"Redis error updating context in callback: {e}")
                         # 即使更新失败也继续尝试搜索，但可能下次翻页会出错

                 # 执行搜索
                 chats = [int(cid) for cid in current_chats_str.split(',')] if current_chats_str else None
                 logger.info(f'Callback Query:"{brief_content(current_query, 50)}" chats={chats} filter={new_filter} page={new_page}')
                 start_time = time()
                 try: result = self.backend.search(current_query, chats, self._cfg.page_len, new_page, file_filter=new_filter)
                 except Exception as e: logger.error(f"Backend search failed during callback: {e}", exc_info=True); await event.answer("后端搜索错误。"); return

                 # 渲染并编辑消息
                 response = await self._render_response_text(result, time() - start_time)
                 buttons = self._render_respond_buttons(result, new_page, current_filter=new_filter)
                 try: await event.edit(response, parse_mode='html', buttons=buttons, link_preview=False); await event.answer()
                 except rpcerrorlist.MessageNotModifiedError: await event.answer() # 消息未改变也需 answer
                 except rpcerrorlist.MessageIdInvalidError: await event.answer("消息已被删除或无法访问。")
                 except Exception as e: logger.error(f"Failed to edit message during callback: {e}"); await event.answer("更新搜索结果失败。")

            # --- 处理选择聊天 ---
            elif action == 'select_chat':
                 try:
                      chat_id = int(value)
                      try: chat_name = await self.backend.translate_chat_id(chat_id)
                      except EntityNotFoundError: chat_name = f"未知对话 ({chat_id})"
                      reply_prompt = f'☑️ 已选择: **{html.escape(chat_name)}** (`{chat_id}`)\n\n请回复此消息进行操作。'
                      await event.edit(reply_prompt, parse_mode='markdown')
                      # 存储选择到 Redis (如果可用)
                      if not self._cfg.no_redis:
                          try:
                              select_key = f'{redis_prefix}select_chat:{bot_chat_id}:{result_msg_id}'
                              self._redis.set(select_key, chat_id, ex=3600)
                              logger.info(f"Chat {chat_id} selected by {event.sender_id}, key {select_key}")
                          except (RedisResponseError, RedisConnectionError) as e:
                              logger.error(f"Redis error setting selected chat context: {e}")
                              await event.answer("选择已记录，但缓存服务暂时遇到问题。", alert=True) # 告知用户可能的问题
                      await event.answer("对话已选择")
                 except ValueError: await event.answer("无效的对话 ID。")
                 except Exception as e: logger.error(f"Error in select_chat callback: {e}", exc_info=True); await event.answer("选择对话时出错。", alert=True)

            # --- 处理占位按钮 ---
            elif action == 'noop': await event.answer()
            # --- 处理未知操作 ---
            else: await event.answer("未知操作。")
        # --- 通用错误处理 ---
        except (RedisResponseError, RedisConnectionError) as e: # 捕获 Redis 错误
            logger.error(f"Redis error during callback processing: {e}")
            if "MISCONF" in str(e) and not self._cfg.no_redis: # 处理 MISCONF 并降级
                self._redis = FakeRedis(); self._cfg.no_redis = True
                logger.error("Falling back to FakeRedis due to MISCONF error during callback.")
            try: await event.answer("缓存服务暂时遇到问题，请稍后再试或联系管理员。", alert=True)
            except Exception: pass
        except Exception as e:
             logger.error(f"Exception in callback handler: {e}", exc_info=True)
             try: await event.answer("处理回调时发生内部错误。", alert=True)
             except Exception as final_e: logger.error(f"Failed to answer callback after error: {final_e}")


    async def _normal_msg_handler(self, event: events.NewMessage.Event):
        """处理普通用户的消息"""
        text: str = event.raw_text.strip()
        sender_id = event.sender_id
        logger.info(f'User {sender_id} chat {event.chat_id}: "{brief_content(text, 100)}"')
        self._track_user_activity(sender_id)
        selected_chat_context = await self._get_selected_chat_from_reply(event)

        if not text or text.startswith('/start'): await event.reply("发送关键词进行搜索，或使用 /help 查看帮助。"); return
        elif text.startswith('/help'): await event.reply(self.HELP_TEXT_USER, parse_mode='markdown'); return
        elif text.startswith('/random'):
            # 处理 /random 命令
            try:
                msg = self.backend.rand_msg()
                try: chat_name = await self.backend.translate_chat_id(msg.chat_id)
                except EntityNotFoundError: chat_name = f"未知对话 ({msg.chat_id})"
                # 构建回复消息 (格式与搜索结果类似)
                respond = f'随机消息来自 <b>{html.escape(chat_name)}</b>\n'
                respond += f'<code>[{msg.post_time.strftime("%y-%m-%d %H:%M")}]</code>\n'
                link_added = False
                if msg.filename and msg.url: respond += f'<a href="{html.escape(msg.url)}">📎 {html.escape(msg.filename)}</a>\n'; link_added = True
                elif msg.filename: respond += f"📎 {html.escape(msg.filename)}\n"
                if msg.content:
                    content_display = html.escape(brief_content(msg.content, self.MAX_TEXT_DISPLAY_LENGTH)) # 应用长度限制
                    if not link_added and msg.url: respond += f'<a href="{html.escape(msg.url)}">跳转到消息</a>\n'
                    respond += f'{content_display}\n'
                elif not link_added and msg.url: respond += f'<a href="{html.escape(msg.url)}">跳转到消息</a>\n'
            except IndexError: respond = '错误：索引库为空。'
            except EntityNotFoundError as e: respond = f"错误：源对话 `{e.entity}` 未找到。"
            except Exception as e: logger.error(f"/random error: {e}", exc_info=True); respond = f"获取随机消息时出错: {type(e).__name__}"
            await event.reply(respond, parse_mode='html', link_preview=False)
        elif text.startswith('/chats'): await self._handle_chats_command(event, text) # 调用辅助函数
        elif text.startswith(('/s ', '/ss ', '/search ', '/s', '/ss', '/search')):
            # 处理搜索命令
            command = text.split()[0]; query = remove_first_word(text).strip() if len(text) > len(command) else ""
            if not query and not selected_chat_context: await event.reply("缺少关键词。用法: `/s 关键词`", parse_mode='markdown'); return
            await self._search(event, query, selected_chat_context) # 执行搜索
        elif text.startswith('/'): await event.reply(f'未知命令: `{text.split()[0]}`。用 /help。', parse_mode='markdown')
        else: await self._search(event, text, selected_chat_context) # 默认视为搜索


    async def _handle_chats_command(self, event, text):
        """辅助函数：处理 /chats 命令"""
        kw = remove_first_word(text).strip(); buttons = []; monitored = sorted(list(self.backend.monitored_chats)); found = 0
        if monitored:
            for cid in monitored:
                try:
                     name = await self.backend.translate_chat_id(cid)
                     if kw and kw.lower() not in name.lower() and str(cid) != kw: continue
                     found += 1
                     if found <= 50: buttons.append(Button.inline(f"{brief_content(name, 25)} (`{cid}`)", f'select_chat={cid}'))
                except EntityNotFoundError: logger.warning(f"Chat {cid} not found for /chats.")
                except Exception as e: logger.error(f"Error processing chat {cid} for /chats: {e}")
            if buttons:
                rows = [buttons[i:min(i + 2, len(buttons))] for i in range(0, len(buttons), 2)] # 最多2列
                reply_text = f"请选择对话 ({found} 个结果):" if found <= 50 else f"找到 {found} 个结果, 显示前 50 个:"
                await event.reply(reply_text, buttons=rows[:25]) # 限制总行数
            else: await event.reply(f'没有找到与 "{html.escape(kw)}" 匹配的已索引对话。' if kw else '没有已索引的对话。')
        else: await event.reply('没有正在监控的对话。')


    async def _chat_ids_from_args(self, chats_args: List[str]) -> Tuple[List[int], List[str]]:
        """辅助函数：将字符串参数列表转换为 chat_id 列表和错误列表"""
        chat_ids, errors = [], []
        if not chats_args: return [], []
        for chat_arg in chats_args:
            try: chat_ids.append(await self.backend.str_to_chat_id(chat_arg))
            except EntityNotFoundError: errors.append(f'未找到: "{html.escape(chat_arg)}"')
            except Exception as e: errors.append(f'解析 "{html.escape(chat_arg)}" 时出错: {type(e).__name__}')
        return chat_ids, errors


    async def _admin_msg_handler(self, event: events.NewMessage.Event):
        """处理管理员发送的消息和命令"""
        text: str = event.raw_text.strip()
        logger.info(f'Admin {event.sender_id} cmd: "{brief_content(text, 100)}"')
        selected_chat_context = await self._get_selected_chat_from_reply(event)
        selected_chat_id = selected_chat_context[0] if selected_chat_context else None
        selected_chat_name = selected_chat_context[1] if selected_chat_context else None
        self._track_user_activity(event.sender_id)

        # --- 管理员命令分发 ---
        if text.startswith('/help'): await event.reply(self.HELP_TEXT_ADMIN, parse_mode='markdown'); return
        elif text.startswith('/stat'): await self._handle_stat_command(event); return
        elif text.startswith('/download_chat'): await self._handle_download_command(event, text, selected_chat_id, selected_chat_name); return
        elif text.startswith('/monitor_chat'): await self._handle_monitor_command(event, text, selected_chat_id, selected_chat_name); return
        elif text.startswith('/clear'): await self._handle_clear_command(event, text, selected_chat_id, selected_chat_name); return
        elif text.startswith('/find_chat_id'): await self._handle_find_id_command(event, text); return
        elif text.startswith('/refresh_chat_names'): await self._handle_refresh_command(event); return
        elif text.startswith('/usage'): await self._handle_usage_command(event); return
        # 如果不是已知管理员命令，则按普通用户消息处理 (通常是搜索)
        else: await self._normal_msg_handler(event)


    # --- 为每个管理员命令创建独立的处理函数 ---

    async def _handle_stat_command(self, event):
        """处理 /stat 命令"""
        try:
            status = await self.backend.get_index_status()
            await event.reply(status, parse_mode='html', link_preview=False)
        except Exception as e:
            logger.error(f"Error handling /stat: {e}", exc_info=True)
            await event.reply(f"获取状态时出错: {html.escape(str(e))}\n<pre>{html.escape(format_exc())}</pre>", parse_mode='html')

    async def _handle_download_command(self, event, text, selected_chat_id, selected_chat_name):
        """处理 /download_chat 命令"""
        try:
            args = self.download_arg_parser.parse_args(shlex.split(text)[1:])
            min_id, max_id = args.min or 0, args.max or 0
            target_chat_ids, errors = await self._chat_ids_from_args(args.chats)
            # 处理回复上下文
            if not args.chats and selected_chat_id is not None and selected_chat_id not in target_chat_ids:
                target_chat_ids = [selected_chat_id]
                await event.reply(f"检测到回复: 正在下载 **{html.escape(selected_chat_name or str(selected_chat_id))}** (`{selected_chat_id}`)", parse_mode='markdown')
            elif not target_chat_ids and not errors:
                await event.reply("错误: 请指定要下载的对话或回复一个已选择的对话。"); return
            # 显示解析错误
            if errors: await event.reply("解析对话参数时出错:\n- " + "\n- ".join(errors))
            if not target_chat_ids: return
            # 执行下载
            s, f = 0, 0
            for cid in target_chat_ids:
                try: await self._download_history(event, cid, min_id, max_id); s += 1
                except Exception as dl_e: f += 1; logger.error(f"Download failed chat {cid}: {dl_e}", exc_info=True); await event.reply(f"❌ 下载对话 {cid} 失败: {html.escape(str(dl_e))}", parse_mode='html')
            if len(target_chat_ids) > 1: await event.reply(f"下载任务完成: {s} 个成功, {f} 个失败。")
        except (ArgumentError, Exception) as e:
             await event.reply(f"参数错误: {e}\n用法:\n<pre>{html.escape(self.download_arg_parser.format_help())}</pre>", parse_mode='html')

    async def _handle_monitor_command(self, event, text, selected_chat_id, selected_chat_name):
        """处理 /monitor_chat 命令"""
        try:
            args = self.chat_ids_parser.parse_args(shlex.split(text)[1:])
            target_chat_ids, errors = await self._chat_ids_from_args(args.chats)
            if not args.chats and selected_chat_id is not None and selected_chat_id not in target_chat_ids:
                target_chat_ids = [selected_chat_id]
                await event.reply(f"检测到回复: 正在监控 **{html.escape(selected_chat_name or str(selected_chat_id))}** (`{selected_chat_id}`)", parse_mode='markdown')
            elif not target_chat_ids and not errors:
                await event.reply("错误: 请指定要监控的对话或回复一个已选择的对话。"); return
            if errors: await event.reply("解析对话参数时出错:\n- " + "\n- ".join(errors))
            if not target_chat_ids: return
            # 执行监控
            results_msg, added_count, already_monitored_count = [], 0, 0
            for cid in target_chat_ids:
                if cid in self.backend.monitored_chats: already_monitored_count += 1
                else:
                    self.backend.monitored_chats.add(cid); added_count += 1
                    try: h = await self.backend.format_dialog_html(cid); results_msg.append(f"- ✅ {h} 已加入监控。")
                    except Exception as e: results_msg.append(f"- ✅ `{cid}` 已加入监控 (获取名称出错: {type(e).__name__}).")
            if results_msg: await event.reply('\n'.join(results_msg), parse_mode='html', link_preview=False)
            status_parts = []; [parts.append(f"{added_count} 个对话已添加。") if added_count else None, parts.append(f"{already_monitored_count} 个已在监控中。") if already_monitored_count else None]
            await event.reply(" ".join(status_parts) if status_parts else "未做任何更改。")
        except (ArgumentError, Exception) as e:
            await event.reply(f"参数错误: {e}\n用法:\n<pre>{html.escape(self.chat_ids_parser.format_help())}</pre>", parse_mode='html')

    async def _handle_clear_command(self, event, text, selected_chat_id, selected_chat_name):
        """处理 /clear 命令"""
        try:
            args = self.chat_ids_parser.parse_args(shlex.split(text)[1:])
            # 处理 /clear all
            if len(args.chats) == 1 and args.chats[0].lower() == 'all':
                try: self.backend.clear(None); await event.reply('✅ 所有索引数据已清除。')
                except Exception as e: logger.error("Clear all index error:", exc_info=True); await event.reply(f"清除所有索引时出错: {e}")
                return
            # 处理指定对话或回复
            target_chat_ids, errors = await self._chat_ids_from_args(args.chats)
            if not args.chats and selected_chat_id is not None and selected_chat_id not in target_chat_ids:
                target_chat_ids = [selected_chat_id]
                await event.reply(f"检测到回复: 正在清除 **{html.escape(selected_chat_name or str(selected_chat_id))}** (`{selected_chat_id}`)", parse_mode='markdown')
            elif not target_chat_ids and not errors:
                await event.reply("错误: 请指定要清除的对话，或回复一个已选择的对话，或使用 `/clear all`。"); return
            if errors: await event.reply("解析对话参数时出错:\n- " + "\n- ".join(errors))
            if not target_chat_ids: return
            # 执行清除
            logger.info(f'Admin clearing index for chats: {target_chat_ids}')
            try:
                self.backend.clear(target_chat_ids); results_msg = []
                for cid in target_chat_ids:
                    try: h = await self.backend.format_dialog_html(cid); results_msg.append(f"- ✅ {h} 的索引已清除。")
                    except Exception: results_msg.append(f"- ✅ `{cid}` 的索引已清除 (名称未知)。")
                await event.reply('\n'.join(results_msg), parse_mode='html', link_preview=False)
            except Exception as e: logger.error(f"Clear specific chats error: {e}", exc_info=True); await event.reply(f"清除索引时出错: {e}")
        except (ArgumentError, Exception) as e:
            await event.reply(f"参数错误: {e}\n用法:\n<pre>{html.escape(self.chat_ids_parser.format_help())}</pre>", parse_mode='html')

    async def _handle_find_id_command(self, event, text):
        """处理 /find_chat_id 命令"""
        q = remove_first_word(text).strip()
        if not q: await event.reply('错误: 缺少关键词。'); return
        try:
            results = await self.backend.find_chat_id(q); sb = []
            if results:
                 sb.append(f'找到 {len(results)} 个与 "{html.escape(q)}" 匹配的对话:\n')
                 for cid in results[:50]: # Limit display
                     try: n = await self.backend.translate_chat_id(cid); sb.append(f'- {html.escape(n)}: `{cid}`\n')
                     except EntityNotFoundError: sb.append(f'- 未知对话: `{cid}`\n')
                     except Exception as e: sb.append(f'- `{cid}` (获取名称出错: {type(e).__name__})\n')
                 if len(results) > 50: sb.append("\n(仅显示前 50 个结果)")
            else: sb.append(f'未找到与 "{html.escape(q)}" 匹配的对话。')
            await event.reply(''.join(sb), parse_mode='html')
        except Exception as e: logger.error(f"Find chat ID error: {e}", exc_info=True); await event.reply(f"查找对话 ID 时出错: {e}")

    async def _handle_refresh_command(self, event):
        """处理 /refresh_chat_names 命令"""
        msg: Optional[TgMessage] = None
        try:
            msg = await event.reply('⏳ 正在刷新对话名称缓存，这可能需要一些时间...')
            await self.backend.session.refresh_translate_table()
            await msg.edit('✅ 对话名称缓存已刷新。')
        except Exception as e:
            logger.error("Refresh chat names error:", exc_info=True)
            error_text = f'❌ 刷新缓存时出错: {html.escape(str(e))}'
            try: await (msg.edit(error_text) if msg else event.reply(error_text)) # Try edit, fallback reply
            except Exception: await event.reply(error_text) # Final fallback

    async def _handle_usage_command(self, event):
        """处理 /usage 命令"""
        if self._cfg.no_redis: await event.reply("使用统计功能需要 Redis (当前已禁用)。"); return
        try:
            total_count = 0; active_count = 0
            if isinstance(self._redis, FakeRedis):
                 total_count = self._redis.scard(self._TOTAL_USERS_KEY)
                 active_count = self._redis.scard(self._ACTIVE_USERS_KEY)
            else:
                 try: # Wrap Redis calls
                     pipe = self._redis.pipeline()
                     pipe.scard(self._TOTAL_USERS_KEY)
                     pipe.scard(self._ACTIVE_USERS_KEY)
                     results = pipe.execute()
                     total_count = results[0] if results and len(results) > 0 else 0
                     active_count = results[1] if results and len(results) > 1 else 0
                 except (RedisResponseError, RedisConnectionError) as e: # Handle Redis errors
                     logger.error(f"Redis error during usage check: {e}")
                     await event.reply(f"获取使用统计时出错: Redis 服务错误 ({type(e).__name__})。")
                     if "MISCONF" in str(e) and not self._cfg.no_redis: self._redis = FakeRedis(); self._cfg.no_redis = True; logger.error("Falling back to FakeRedis.")
                     return
            await event.reply(f"📊 **使用统计**\n- 总独立用户数: {total_count}\n- 活跃用户数 (最近15分钟): {active_count}", parse_mode='markdown')
        except Exception as e: logger.error(f"Failed to get usage stats: {e}", exc_info=True); await event.reply(f"获取使用统计时出错: {html.escape(str(e))}")


    async def _search(self, event: events.NewMessage.Event, query: str, selected_chat_context: Optional[Tuple[int, str]]):
        """执行搜索的核心函数"""
        selected_chat_id = selected_chat_context[0] if selected_chat_context else None
        selected_chat_name = selected_chat_context[1] if selected_chat_context else None

        # 处理空查询或上下文搜索
        if not query and selected_chat_context:
             query = '*' # 搜索所有文档
             await event.reply(f"正在搜索 **{html.escape(selected_chat_name or str(selected_chat_id))}** (`{selected_chat_id}`) 中的所有消息", parse_mode='markdown')
        elif not query:
             logger.debug("Empty query ignored for global search.")
             return # 全局搜索不能没有关键词

        target_chats = [selected_chat_id] if selected_chat_id is not None else None # 设置搜索目标
        # 检查索引是否为空
        try: is_empty = self.backend.is_empty(selected_chat_id)
        except Exception as e: logger.error(f"Check index empty error: {e}"); await event.reply("检查索引状态时出错。"); return
        if is_empty:
            await event.reply(f'对话 **{html.escape(selected_chat_name or str(selected_chat_id))}** 的索引为空。' if selected_chat_context else '全局索引为空。')
            return

        start = time(); ctx_info = f"在对话 {selected_chat_id} 中" if target_chats else "全局"
        logger.info(f'正在搜索 "{brief_content(query, 50)}" ({ctx_info})')
        try:
            # 执行搜索
            result = self.backend.search(query, target_chats, self._cfg.page_len, 1, file_filter="all")
            text = await self._render_response_text(result, time() - start)
            buttons = self._render_respond_buttons(result, 1, current_filter="all")
            msg = await event.reply(text, parse_mode='html', buttons=buttons, link_preview=False)
            # 存储搜索上下文到 Redis (如果可用)
            if msg and not self._cfg.no_redis:
                try:
                    prefix, bcid, mid = f'{self.id}:', event.chat_id, msg.id
                    pipe = self._redis.pipeline()
                    pipe.set(f'{prefix}query_text:{bcid}:{mid}', query, ex=3600)
                    pipe.set(f'{prefix}query_filter:{bcid}:{mid}', "all", ex=3600)
                    pipe.set(f'{prefix}query_page:{bcid}:{mid}', 1, ex=3600)
                    if target_chats: pipe.set(f'{prefix}query_chats:{bcid}:{mid}', ','.join(map(str, target_chats)), ex=3600)
                    else: pipe.delete(f'{prefix}query_chats:{bcid}:{mid}') # 删除 chats key
                    pipe.execute()
                except (RedisResponseError, RedisConnectionError) as e:
                    logger.error(f"Redis error saving search context: {e}")
                    # 通知用户缓存问题，但不中断搜索结果的显示
                    await event.reply("⚠️ 缓存服务暂时遇到问题，翻页和筛选功能可能受限。")
                    if "MISCONF" in str(e) and not self._cfg.no_redis: self._redis = FakeRedis(); self._cfg.no_redis = True; logger.error("Falling back to FakeRedis.")
                except Exception as e:
                     logger.error(f"Failed to save search context to Redis: {e}")
        except whoosh.index.LockError: await event.reply('⏳ 索引当前正忙，请稍后再试。')
        except Exception as e: logger.error(f"Search execution error: {e}", exc_info=True); await event.reply(f'搜索时发生错误: {type(e).__name__}。')


    async def _download_history(self, event: events.NewMessage.Event, chat_id: int, min_id: int, max_id: int):
        """处理下载历史记录的逻辑，包括中文进度回调"""
        try: chat_html = await self.backend.format_dialog_html(chat_id)
        except Exception: chat_html = f"对话 `{chat_id}`"

        # 检查重复下载警告
        try:
            if min_id == 0 and max_id == 0 and not self.backend.is_empty(chat_id):
                await event.reply(f'⚠️ 警告: {chat_html} 的索引已存在。重新下载可能导致消息重复。\n如需重下, 请先 `/clear {chat_id}` 或指定 `--min/--max`。', parse_mode='html')
        except Exception as e: logger.error(f"Check empty error before download {chat_id}: {e}")

        prog_msg: Optional[TgMessage] = None; last_update = time(); interval = 5; count = 0
        # 中文进度回调
        async def cb(cur_id: int, dl_count: int):
            nonlocal prog_msg, last_update, count; count = dl_count; now = time()
            if now - last_update > interval:
                last_update = now
                txt = f'⏳ 正在下载 {chat_html}:\n已处理 {dl_count} 条，当前消息 ID: {cur_id}'
                try:
                    if prog_msg is None: prog_msg = await event.reply(txt, parse_mode='html')
                    else: await prog_msg.edit(txt, parse_mode='html')
                except rpcerrorlist.FloodWaitError as fwe: logger.warning(f"Flood wait ({fwe.seconds}s) updating progress."); last_update += fwe.seconds
                except rpcerrorlist.MessageNotModifiedError: pass
                except rpcerrorlist.MessageIdInvalidError: prog_msg = None # 进度消息被删
                except Exception as e: logger.error(f"Edit progress message error: {e}"); prog_msg = None
        # 执行下载并处理结果
        start = time()
        final_msg_text = ""
        try:
            await self.backend.download_history(chat_id, min_id, max_id, cb)
            final_msg_text = f'✅ {chat_html} 下载完成，索引了 {count} 条消息，耗时 {time()-start:.2f} 秒。'
        except (EntityNotFoundError, ValueError) as e: final_msg_text = f'❌ 下载 {chat_html} 时出错: {html.escape(str(e))}'
        except Exception as e: logger.error(f"Unknown download error for {chat_id}: {e}", exc_info=True); final_msg_text = f'❌ 下载 {chat_html} 时发生未知错误: {type(e).__name__}'
        # 发送最终结果 (编辑或新消息)
        try:
            if prog_msg: await prog_msg.edit(final_msg_text, parse_mode='html')
            else: await event.reply(final_msg_text, parse_mode='html')
        except Exception as final_e: # 如果编辑/回复失败
            logger.error(f"Failed to send final download status: {final_e}")
            # 尝试发送新消息作为后备
            try: await self.bot.send_message(event.chat_id, final_msg_text, parse_mode='html')
            except Exception as fallback_e: logger.error(f"Fallback send message failed: {fallback_e}")
        # 如果原始进度消息还存在且未被编辑（例如出错时），尝试删除
        if prog_msg and prog_msg.text.startswith('⏳'): # Check if it's still the progress message
            try: await prog_msg.delete()
            except Exception: pass


    def _register_hooks(self):
        """注册 Telethon 事件钩子"""
        # 回调查询钩子
        @self.bot.on(events.CallbackQuery())
        async def cq_handler(event: events.CallbackQuery.Event):
             is_admin = self._admin_id and event.sender_id == self._admin_id; is_wl = event.sender_id in self._cfg.private_whitelist
             if self._cfg.private_mode and not is_admin and not is_wl: await event.answer("您没有权限执行此操作。", alert=True); return
             await self._callback_handler(event)

        # 新消息钩子
        @self.bot.on(events.NewMessage())
        async def msg_handler(event: events.NewMessage.Event):
            # 基础检查
            if not event.message or not event.sender_id: return
            sender = await event.message.get_sender()
            if not sender or (self.my_id and sender.id == self.my_id): return # 忽略自己或无效发送者

            is_admin = self._admin_id and sender.id == self._admin_id

            # 判断是否需要处理 (私聊 / @机器人 / 回复机器人)
            mentioned, reply_to_bot = False, False
            if event.is_group or event.is_channel:
                 if self.username and f'@{self.username}' in event.raw_text: mentioned = True
                 elif event.message.mentioned and event.message.entities:
                      for entity in event.message.entities:
                          if isinstance(entity, MessageEntityMentionName) and entity.user_id == self.my_id: mentioned = True; break
                 if event.message.is_reply and event.message.reply_to_msg_id:
                      try: reply = await event.message.get_reply_message(); reply_to_bot = reply and reply.sender_id == self.my_id
                      except Exception as e: logger.warning(f"Could not get reply message {event.message.reply_to_msg_id} in {event.chat_id}: {e}")
            process = event.is_private or mentioned or reply_to_bot
            if not process: return # 不处理无关消息

            # 私聊模式权限检查
            if self._cfg.private_mode and not is_admin:
                 sender_allowed = sender.id in self._cfg.private_whitelist
                 chat_allowed = False
                 if event.chat_id:
                      try: csi = get_share_id(event.chat_id); chat_allowed = csi in self._cfg.private_whitelist
                      except Exception: pass
                 if not sender_allowed and not chat_allowed:
                     if event.is_private: await event.reply('抱歉，您没有权限使用此机器人。');
                     return # 阻止未授权用户

            # 分发给管理员或普通用户处理器
            handler = self._admin_msg_handler if is_admin else self._normal_msg_handler
            try:
                await handler(event)
            # --- 特定错误处理 ---
            except whoosh.index.LockError: await event.reply('⏳ 索引当前正忙，请稍后再试。')
            except EntityNotFoundError as e: await event.reply(f'❌ 未找到相关实体: {e.entity}')
            except rpcerrorlist.UserIsBlockedError: logger.warning(f"User {sender.id} blocked the bot.")
            except rpcerrorlist.ChatWriteForbiddenError: logger.warning(f"Write forbidden in chat: {event.chat_id}.")
            except (RedisResponseError, RedisConnectionError) as e: # Redis 错误
                 logger.error(f"Redis error during message handling: {e}")
                 if "MISCONF" in str(e) and not self._cfg.no_redis: # 处理 MISCONF 并降级
                     self._redis = FakeRedis(); self._cfg.no_redis = True
                     logger.error("Falling back to FakeRedis due to MISCONF error during message handling.")
                     await event.reply("处理请求时遇到缓存服务问题，请稍后再试。")
                 else: await event.reply(f'处理请求时发生 Redis 错误: {type(e).__name__}。')
            # --- 通用错误处理 ---
            except Exception as e:
                 et = type(e).__name__; logger.error(f"Error handling message from {sender.id} in {event.chat_id}: {et}: {e}", exc_info=True)
                 try: await event.reply(f'处理您的请求时发生错误: {et}。\n如果问题持续存在，请联系管理员。')
                 except Exception as re: logger.error(f"Replying with error message failed: {re}")
                 # 向管理员发送错误通知
                 if self._admin_id and event.chat_id != self._admin_id:
                      try:
                          error_details = f"处理来自用户 {sender.id} 在对话 {event.chat_id} 的消息时出错:\n<pre>{html.escape(format_exc())}</pre>"
                          await self.bot.send_message(self._admin_id, error_details, parse_mode='html')
                      except Exception as ne: logger.error(f"Notifying admin about error failed: {ne}")


    async def _get_selected_chat_from_reply(self, event: events.NewMessage.Event) -> Optional[Tuple[int, str]]:
        """检查回复上下文，获取之前选择的对话 ID 和名称"""
        if not event.message.is_reply or not event.message.reply_to_msg_id: return None
        if self._cfg.no_redis: return None # 如果 Redis 禁用则直接返回

        key = f'{self.id}:select_chat:{event.chat_id}:{event.message.reply_to_msg_id}'
        res = None
        try:
            res = self._redis.get(key)
        except (RedisResponseError, RedisConnectionError) as e:
            logger.error(f"Redis error getting selected chat context: {e}")
            if "MISCONF" in str(e) and not self._cfg.no_redis: self._redis = FakeRedis(); self._cfg.no_redis = True; logger.error("Falling back to FakeRedis.")
            return None # Redis 出错时返回 None

        if res:
            try:
                 cid = int(res)
                 name = await self.backend.translate_chat_id(cid) # 可能抛出 EntityNotFoundError
                 return cid, name
            except ValueError: self._redis.delete(key); return None # 无效数据，删除 key
            except EntityNotFoundError: return int(res), f"未知对话 ({res})" # 找到 ID 但无名称
            except Exception as e: logger.error(f"Error getting selected chat name for key {key}: {e}"); return None
        return None # 没有找到对应的 key


    async def _register_commands(self):
        """注册 Telegram Bot 命令列表"""
        admin_peer = None
        if self._admin_id:
            try: admin_peer = await self.bot.get_input_entity(self._admin_id)
            except ValueError: logger.warning(f"Could not get input entity for admin {self._admin_id} directly. Trying get_entity.")
            try: admin_entity = await self.bot.get_entity(self._admin_id); admin_peer = await self.bot.get_input_entity(admin_entity)
            except Exception as e: logger.error(f'Failed get admin input entity via get_entity for {self._admin_id}: {e}')
            except Exception as e: logger.error(f'Failed get admin input entity for {self._admin_id}: {e}')
        else: logger.warning("Admin ID invalid or not configured, skipping admin-specific commands.")

        # 定义命令列表 (中文描述)
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

        # 为管理员和默认范围设置命令
        if admin_peer:
            try: await self.bot(SetBotCommandsRequest(scope=BotCommandScopePeer(admin_peer), lang_code='', commands=admin_commands + common_commands)); logger.info(f"Admin commands set for peer {self._admin_id}.")
            except Exception as e: logger.error(f"Setting admin commands failed for {self._admin_id}: {e}")
        try: await self.bot(SetBotCommandsRequest(scope=BotCommandScopeDefault(), lang_code='', commands=common_commands)); logger.info("Default commands set.")
        except Exception as e: logger.error(f"Setting default commands failed: {e}")


    def _strip_html(self, text: str) -> str:
        """简单的 HTML 标签剥离器"""
        return re.sub('<[^>]*>', '', text)


    async def _render_response_text(self, result: SearchResult, used_time: float) -> str:
        """
        将搜索结果渲染为发送给用户的 HTML 文本。
        - 优化标题格式
        - 优化链接显示
        - 优先保留高亮
        - 限制文本长度
        - 移除空消息占位符
        """
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

                # 构建消息头 (仅序号、粗体标题、代码块时间)
                hdr_parts = [f"<b>{i}. {html.escape(title)}</b>"]
                if isinstance(msg.post_time, datetime): hdr_parts.append(f'<code>[{msg.post_time.strftime("%y-%m-%d %H:%M")}]</code>')
                else: hdr_parts.append('<code>[无效时间]</code>')
                sb.append(' '.join(hdr_parts) + '\n')

                # --- 优化链接和文本显示 ---
                link_added = False
                # 1. 文件名链接优先
                if msg.filename and msg.url:
                    sb.append(f'<a href="{html.escape(msg.url)}">📎 {html.escape(msg.filename)}</a>\n')
                    link_added = True
                elif msg.filename: # 只有文件名，不可点击
                     sb.append(f"📎 {html.escape(msg.filename)}\n")

                # 2. 处理文本内容 (优先高亮，然后截断)
                display_text = ""
                if hit.highlighted:
                    # 优先使用 Whoosh 生成的高亮 HTML，它已经处理了上下文
                    # 增加一个安全检查，防止极端过长的原始 HTML
                    if len(hit.highlighted) < self.MAX_HIGHLIGHT_HTML_LENGTH:
                        display_text = hit.highlighted # 使用带<b>标签的片段
                    else: # 如果原始 HTML 过长，回退到截断纯文本
                        plain_highlighted = self._strip_html(hit.highlighted)
                        display_text = html.escape(brief_content(plain_highlighted, self.MAX_TEXT_DISPLAY_LENGTH))
                        logger.warning(f"Highlight HTML too long ({len(hit.highlighted)}), fallback to plain text for {msg.url}")
                elif msg.content: # 没有高亮，使用原文截断
                    display_text = html.escape(brief_content(msg.content, self.MAX_TEXT_DISPLAY_LENGTH))
                # 无需添加 (空消息)

                # 3. 通用跳转链接 (如果前面没加文件链接)
                if not link_added and msg.url:
                    sb.append(f'<a href="{html.escape(msg.url)}">跳转到消息</a>\n')

                # 4. 添加处理后的文本 (如果非空)
                if display_text:
                    sb.append(f"{display_text}\n")
                # --- 结束优化 ---

                sb.append("\n") # 每个结果后的分隔符

            except Exception as e:
                 sb.append(f"<b>{i}.</b> 渲染此条结果时出错: {type(e).__name__}\n\n")
                 # 使用更安全的属性访问方式
                 msg_url = getattr(getattr(hit, 'msg', None), 'url', 'N/A')
                 logger.error(f"Error rendering hit (msg URL: {msg_url}): {e}", exc_info=True)

        # 处理消息过长截断
        final = ''.join(sb); max_len = 4096
        if len(final) > max_len:
             cutoff_msg = "\n\n...(结果过多，仅显示部分)"
             cutoff_point = max_len - len(cutoff_msg) - 10
             last_nl = final.rfind('\n\n', 0, cutoff_point)
             final = final[:last_nl if last_nl != -1 else cutoff_point] + cutoff_msg
        return final


    def _render_respond_buttons(self, result: SearchResult, cur_page_num: int, current_filter: str = "all") -> Optional[List[List[Button]]]:
        """生成包含中文筛选和翻页按钮的列表 (中文)"""
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
        try: page_len = max(1, self._cfg.page_len); total_pages = (result.total_results + page_len - 1) // page_len
        except Exception as e: logger.error(f"Error calculating total pages: {e}"); total_pages = 1

        if total_pages > 1:
            page_buttons = []
            if cur_page_num > 1: page_buttons.append(Button.inline('⬅️ 上一页', f'search_page={cur_page_num - 1}'))
            page_buttons.append(Button.inline(f'{cur_page_num}/{total_pages}', 'noop')) # 页码指示器
            if not result.is_last_page and cur_page_num < total_pages: page_buttons.append(Button.inline('下一页 ➡️', f'search_page={cur_page_num + 1}'))
            if page_buttons: buttons.append(page_buttons)

        return buttons if buttons else None
