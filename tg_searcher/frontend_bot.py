# -*- coding: utf-8 -*-
import html
from time import time
from typing import Optional, List, Tuple, Set, Union, Any
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

from .common import CommonBotConfig, get_logger, get_share_id, remove_first_word, brief_content
from .backend_bot import BackendBot, EntityNotFoundError
from .indexer import SearchResult, IndexMsg # 确保 IndexMsg 已更新

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


class FakeRedis:
    def __init__(self): self._data = {}; self._logger = get_logger('FakeRedis'); self._logger.warning("Using FakeRedis: Data volatile.")
    def get(self, key): return self._data.get(key)
    def set(self, key, val, ex=None): self._data[key] = str(val) # 模拟字符串存储
    def delete(self, *keys): count = 0; [self._data.pop(k, None) for k in keys if k in self._data and (count := count + 1)]; return count # 修复语法并简化
    def ping(self): return True


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
        if cfg.no_redis or cfg.redis_host is None: self._redis = FakeRedis()
        else:
            try: self._redis = Redis(host=cfg.redis_host[0], port=cfg.redis_host[1], decode_responses=True); self._redis.ping()
            except RedisConnectionError as e: logger.critical(f'Redis connection failed {cfg.redis_host}: {e}. Falling back to FakeRedis.'); self._redis = FakeRedis(); self._cfg.no_redis = True
            except Exception as e: logger.critical(f'Redis init error {cfg.redis_host}: {e}. Falling back to FakeRedis.'); self._redis = FakeRedis(); self._cfg.no_redis = True

        self._logger = logger
        self._admin_id: Optional[int] = None
        self.username: Optional[str] = None
        self.my_id: Optional[int] = None

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
            self._admin_id = await self.backend.str_to_chat_id(str(self._cfg.admin))
            self._logger.info(f"Admin ID resolved to: {self._admin_id}")
            if self._cfg.private_mode and self._admin_id: self._cfg.private_whitelist.add(self._admin_id); self._logger.info(f"Admin added to private whitelist.")
        except EntityNotFoundError: self._logger.critical(f"Admin entity '{self._cfg.admin}' not found."); self._admin_id = None
        except (ValueError, TypeError) as e: self._logger.critical(f"Invalid admin config '{self._cfg.admin}': {e}"); self._admin_id = None
        except Exception as e: self._logger.critical(f"Error resolving admin '{self._cfg.admin}': {e}", exc_info=True); self._admin_id = None
        if not self._admin_id: self._logger.error("Proceeding without valid admin ID.")

        if not isinstance(self._redis, FakeRedis):
             try: self._redis.ping(); self._logger.info(f"Redis connected at {self._cfg.redis_host}")
             except RedisConnectionError as e: self._logger.critical(f'Redis check failed: {e}. Falling back.'); self._redis = FakeRedis(); self._cfg.no_redis = True

        self._logger.info(f'Starting frontend bot {self.id}...')
        try:
             await self.bot.start(bot_token=self._cfg.bot_token)
             me = await self.bot.get_me(); assert me is not None
             self.username, self.my_id = me.username, me.id
             self._logger.info(f'Bot (@{self.username}, id={self.my_id}) login ok')
             self.backend.excluded_chats.add(get_share_id(self.my_id)); self._logger.info(f"Bot ID {self.my_id} excluded from backend.")
             await self._register_commands(); self._logger.info(f'Commands registered.')
             self._register_hooks()

             if self._admin_id:
                  try:
                       msg = '✅ Bot frontend init complete\n\n' + await self.backend.get_index_status(4000 - 20)
                       await self.bot.send_message(self._admin_id, msg, parse_mode='html', link_preview=False)
                  except Exception as e: self._logger.error(f"Failed get/send initial status: {e}", exc_info=True); await self.bot.send_message(self._admin_id, f'⚠️ Bot started, but failed get status: {e}')
             else: self._logger.warning("No admin configured, skipping startup message.")
             self._logger.info(f"Frontend bot {self.id} started successfully.")
        except Exception as e: self._logger.critical(f"Frontend start failed: {e}", exc_info=True)


    async def _callback_handler(self, event: events.CallbackQuery.Event):
        try:
            self._logger.info(f'Callback: {event.sender_id} in {event.chat_id}, msg={event.message_id}, data={event.data!r}')
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
                      new_filter = self._redis.get(filter_key) or "all" # 保持 filter
                 else: # action == 'search_filter'
                      new_filter = value if value in ["all", "text_only", "file_only"] else "all"
                      self._redis.set(filter_key, new_filter, ex=3600) # 更新 filter, 回到第一页

                 q = self._redis.get(query_key)
                 chats_str = self._redis.get(chats_key)
                 if q is None:
                     try: await event.edit("Search info expired. Please search again.")
                     except Exception: pass
                     self._redis.delete(chats_key, filter_key); await event.answer("Search expired."); return

                 chats = [int(cid) for cid in chats_str.split(',')] if chats_str else None
                 self._logger.info(f'Query:"{brief_content(q)}" chats={chats} filter={new_filter} page={new_page_num}')
                 start_time = time()
                 try: result = self.backend.search(q, chats, self._cfg.page_len, new_page_num, file_filter=new_filter)
                 except Exception as e: self._logger.error(f"Backend search failed: {e}", exc_info=True); await event.answer("Backend search error."); return
                 response = await self._render_response_text(result, time() - start_time)
                 buttons = self._render_respond_buttons(result, new_page_num, current_filter=new_filter)
                 try: await event.edit(response, parse_mode='html', buttons=buttons, link_preview=False); await event.answer()
                 except rpcerrorlist.MessageNotModifiedError: await event.answer()
                 except rpcerrorlist.MessageIdInvalidError: await event.answer("Message deleted.")
                 except Exception as e: self._logger.error(f"Failed to edit message: {e}"); await event.answer("Update failed.")

            # --- 处理选择聊天 ---
            elif action == 'select_chat':
                 try:
                      chat_id = int(value)
                      try: chat_name = await self.backend.translate_chat_id(chat_id); reply_prompt = f'☑️ Selected: **{html.escape(chat_name)}** (`{chat_id}`)\n\nReply to operate.'
                      except EntityNotFoundError: reply_prompt = f'☑️ Selected: `{chat_id}` (Name unknown)\n\nReply to operate.'
                      await event.edit(reply_prompt, parse_mode='markdown')
                      select_key = f'{redis_prefix}select_chat:{bot_chat_id}:{result_msg_id}'
                      self._redis.set(select_key, chat_id, ex=3600)
                      self._logger.info(f"Chat {chat_id} selected by {event.sender_id}, key {select_key}")
                      await event.answer("Chat selected")
                 except ValueError: await event.answer("Invalid chat ID.")
                 except Exception as e: self._logger.error(f"Error in select_chat: {e}", exc_info=True); await event.answer("Error selecting chat.")

            elif action == 'noop': await event.answer()
            else: await event.answer("Unknown action.")
        except Exception as e:
             self._logger.error(f"Exception in callback handler: {e}", exc_info=True)
             try: await event.answer("Internal error.")
             except Exception as final_e: self._logger.error(f"Failed to answer callback after error: {final_e}")


    async def _normal_msg_handler(self, event: events.NewMessage.Event):
        text: str = event.raw_text.strip()
        sender_id = event.sender_id # Assume sender exists based on hook logic
        self._logger.info(f'User {sender_id} chat {event.chat_id}: "{brief_content(text, 100)}"')
        selected_chat_context = await self._get_selected_chat_from_reply(event)

        if not text or text.startswith('/start'): await event.reply("Send keywords to search, or /help."); return
        elif text.startswith('/help'): await event.reply(self.HELP_TEXT_USER, parse_mode='markdown'); return
        elif text.startswith('/random'):
            try:
                msg = self.backend.rand_msg()
                chat_name = await self.backend.translate_chat_id(msg.chat_id)
                display = f"📎 {html.escape(msg.filename)}" if msg.filename else html.escape(brief_content(msg.content))
                if msg.filename and msg.content: display += f" ({html.escape(brief_content(msg.content))})"
                respond = f'Rand msg from **{html.escape(chat_name)}** (`{msg.chat_id}`)\n'
                if msg.sender: respond += f'Sender: {html.escape(msg.sender)}\n'
                respond += f'Time: {msg.post_time.strftime("%Y-%m-%d %H:%M")}\n'
                respond += f'Content: {display or "(empty)"}\n<a href="{msg.url}">Go to message</a>'
            except IndexError: respond = 'Error: Index empty.'
            except EntityNotFoundError as e: respond = f"Error: Source chat `{e.entity}` not found."
            except Exception as e: self._logger.error(f"Error /random: {e}", exc_info=True); respond = f"Error getting random msg: {type(e).__name__}"
            await event.reply(respond, parse_mode='html', link_preview=False)

        elif text.startswith('/chats'):
            kw = remove_first_word(text); buttons = []
            monitored = sorted(list(self.backend.monitored_chats)); found = 0
            if monitored:
                for cid in monitored:
                    try:
                         name = await self.backend.translate_chat_id(cid)
                         if kw and kw.lower() not in name.lower(): continue
                         found += 1
                         if found <= 50: buttons.append(Button.inline(f"{brief_content(name, 25)} (`{cid}`)", f'select_chat={cid}'))
                    except EntityNotFoundError: self._logger.warning(f"Chat {cid} not found for /chats.")
                    except Exception as e: self._logger.error(f"Error processing chat {cid} for /chats: {e}")
                if buttons: await event.reply(f"Select chat ({found} found):" if found <= 50 else f"{found} found, showing 50:", buttons=[buttons[i:i+2] for i in range(0, len(buttons), 2)])
                else: await event.reply(f'No indexed chats found matching "{html.escape(kw)}".' if kw else 'No indexed chats found.')
            else: await event.reply('No monitored chats. Use /download_chat first.')

        # --- 处理搜索命令及其别名 ---
        elif text.startswith(('/s ', '/ss ', '/search ', '/s', '/ss', '/search')):
            command = text.split()[0]
            query = remove_first_word(text).strip() if len(text) > len(command) else ""
            if not query and not selected_chat_context: await event.reply("Keyword missing. Usage: `/s keyword`", parse_mode='markdown'); return
            await self._search(event, query, selected_chat_context)

        elif text.startswith('/'): await event.reply(f'Unknown command: `{text.split()[0]}`. Use /help.', parse_mode='markdown')
        else: await self._search(event, text, selected_chat_context) # Default to search


    async def _chat_ids_from_args(self, chats_args: List[str]) -> Tuple[List[int], List[str]]:
        # ... (代码不变，已确认返回 share_id) ...
        chat_ids, errors = [], []
        if not chats_args: return [], []
        for chat_arg in chats_args:
            try: chat_ids.append(await self.backend.str_to_chat_id(chat_arg))
            except EntityNotFoundError: errors.append(f'Not found: "{html.escape(chat_arg)}"')
            except Exception as e: errors.append(f'Error parsing "{html.escape(chat_arg)}": {type(e).__name__}')
        return chat_ids, errors


    async def _admin_msg_handler(self, event: events.NewMessage.Event):
        text: str = event.raw_text.strip()
        self._logger.info(f'Admin {event.sender_id} cmd: "{brief_content(text, 100)}"')
        selected_chat_context = await self._get_selected_chat_from_reply(event)
        selected_chat_id = selected_chat_context[0] if selected_chat_context else None
        selected_chat_name = selected_chat_context[1] if selected_chat_context else None

        # --- 统一使用 if/elif/else 处理管理员命令 ---
        if text.startswith('/help'): await event.reply(self.HELP_TEXT_ADMIN, parse_mode='markdown'); return
        elif text.startswith('/stat'):
            try: await event.reply(await self.backend.get_index_status(), parse_mode='html', link_preview=False)
            except Exception as e: self._logger.error("Error /stat: {e}", exc_info=True); await event.reply(f"Error getting status: {html.escape(str(e))}\n<pre>{html.escape(format_exc())}</pre>", parse_mode='html')
        elif text.startswith('/download_chat'):
            try: args = self.download_arg_parser.parse_args(shlex.split(text)[1:])
            except (ArgumentError, Exception) as e: await event.reply(f"Arg error: {e}\nUsage:\n<pre>{html.escape(self.download_arg_parser.format_help())}</pre>", parse_mode='html'); return
            min_id, max_id = args.min or 0, args.max or 0
            target_chat_ids, errors = await self._chat_ids_from_args(args.chats)
            if not args.chats and selected_chat_id is not None and selected_chat_id not in target_chat_ids: target_chat_ids = [selected_chat_id]; await event.reply(f"Reply detected: Downloading **{html.escape(selected_chat_name)}** (`{selected_chat_id}`)", parse_mode='markdown')
            elif not target_chat_ids and not errors: await event.reply("Error: Specify chat or reply."); return
            if errors: await event.reply("Parse errors:\n- " + "\n- ".join(errors))
            if not target_chat_ids: return
            # 执行下载
            s, f = 0, 0
            for cid in target_chat_ids:
                try: await self._download_history(event, cid, min_id, max_id); s += 1
                except Exception as dl_e: f += 1; self._logger.error(f"Download failed for {cid}: {dl_e}", exc_info=True); await event.reply(f"❌ Download {cid} failed: {html.escape(str(dl_e))}", parse_mode='html')
            if len(target_chat_ids) > 1: await event.reply(f"Downloads complete: {s} success, {f} failed.")
        elif text.startswith('/monitor_chat'):
            try: args = self.chat_ids_parser.parse_args(shlex.split(text)[1:])
            except (ArgumentError, Exception) as e: await event.reply(f"Arg error: {e}\nUsage:\n<pre>{html.escape(self.chat_ids_parser.format_help())}</pre>", parse_mode='html'); return
            target_chat_ids, errors = await self._chat_ids_from_args(args.chats)
            if not args.chats and selected_chat_id is not None and selected_chat_id not in target_chat_ids: target_chat_ids = [selected_chat_id]; await event.reply(f"Reply detected: Monitoring **{html.escape(selected_chat_name)}** (`{selected_chat_id}`)", parse_mode='markdown')
            elif not target_chat_ids and not errors: await event.reply("Error: Specify chat or reply."); return
            if errors: await event.reply("Parse errors:\n- " + "\n- ".join(errors))
            if not target_chat_ids: return
            # 执行监听
            r, a, m = [], 0, 0
            for cid in target_chat_ids:
                if cid in self.backend.monitored_chats: m += 1
                else: self.backend.monitored_chats.add(cid); a += 1; try: h = await self.backend.format_dialog_html(cid); r.append(f"- ✅ {h} added.") except Exception as e: r.append(f"- ✅ `{cid}` added (name error: {type(e).__name__})."); self._logger.info(f'Admin added {cid} monitor.')
            if r: await event.reply('\n'.join(r), parse_mode='html', link_preview=False)
            s = ([f"{c} added." for c in [a] if c > 0] + [f"{c} already monitored." for c in [m] if c > 0]); await event.reply(" ".join(s) if s else "No changes.")
        elif text.startswith('/clear'):
            try: args = self.chat_ids_parser.parse_args(shlex.split(text)[1:])
            except (ArgumentError, Exception) as e: await event.reply(f"Arg error: {e}\nUsage:\n<pre>{html.escape(self.chat_ids_parser.format_help())}</pre>", parse_mode='html'); return
            if len(args.chats) == 1 and args.chats[0].lower() == 'all':
                try: self.backend.clear(None); await event.reply('✅ All index cleared.')
                except Exception as e: self._logger.error("Clear all error:", exc_info=True); await event.reply(f"Clear all error: {e}")
                return
            target_chat_ids, errors = await self._chat_ids_from_args(args.chats)
            if not args.chats and selected_chat_id is not None and selected_chat_id not in target_chat_ids: target_chat_ids = [selected_chat_id]; await event.reply(f"Reply detected: Clearing **{html.escape(selected_chat_name)}** (`{selected_chat_id}`)", parse_mode='markdown')
            elif not target_chat_ids and not errors: await event.reply("Error: Specify chat, reply, or use `/clear all`."); return
            if errors: await event.reply("Parse errors:\n- " + "\n- ".join(errors))
            if not target_chat_ids: return
            # 执行清除
            self._logger.info(f'Admin clear index for: {target_chat_ids}')
            try:
                self.backend.clear(target_chat_ids); r = []
                for cid in target_chat_ids: try: h = await self.backend.format_dialog_html(cid); r.append(f"- ✅ {h} cleared.") except Exception: r.append(f"- ✅ `{cid}` cleared (name unknown).")
                await event.reply('\n'.join(r), parse_mode='html', link_preview=False)
            except Exception as e: self._logger.error(f"Clear error: {e}", exc_info=True); await event.reply(f"Clear error: {e}")
        elif text.startswith('/refresh_chat_names'):
            msg = await event.reply('Refreshing chat name cache...'); try: await self.backend.session.refresh_translate_table(); await msg.edit('✅ Cache refreshed.')
            except Exception as e: self._logger.error("Refresh names error:", exc_info=True); await msg.edit(f'Refresh error: {e}')
        elif text.startswith('/find_chat_id'):
            q = remove_first_word(text);
            if not q: await event.reply('Error: Keyword missing.'); return
            try:
                results = await self.backend.find_chat_id(q); sb = []
                if results:
                     sb.append(f'{len(results)} chats found matching "{html.escape(q)}":\n')
                     for cid in results[:50]: try: n=await self.backend.translate_chat_id(cid); sb.append(f'- {html.escape(n)}: `{cid}`\n') except EntityNotFoundError: sb.append(f'- Unknown: `{cid}`\n') except Exception as e: sb.append(f'- `{cid}` (name error: {type(e).__name__})\n')
                     if len(results) > 50: sb.append("\n(Showing first 50)")
                else: sb.append(f'No chats found matching "{html.escape(q)}".')
                await event.reply(''.join(sb), parse_mode='html')
            except Exception as e: self._logger.error(f"Find chat ID error: {e}", exc_info=True); await event.reply(f"Find chat ID error: {e}")
        else: await self._normal_msg_handler(event) # 管理员的其他输入按普通用户处理


    async def _search(self, event: events.NewMessage.Event, query: str, selected_chat_context: Optional[Tuple[int, str]]):
        if not query and selected_chat_context: query = '*'; await event.reply(f"Searching all in **{html.escape(selected_chat_context[1])}** (`{selected_chat_context[0]}`)", parse_mode='markdown')
        elif not query: self._logger.debug("Empty query ignored."); return

        target_chats = [selected_chat_context[0]] if selected_chat_context else None
        try: is_empty = self.backend.is_empty(target_chats[0] if target_chats else None)
        except Exception as e: self._logger.error(f"Check empty error: {e}"); await event.reply("Index check error."); return

        if is_empty: await event.reply(f'Chat **{html.escape(selected_chat_context[1])}** index empty.' if selected_chat_context else 'Global index empty.'); return

        start = time(); ctx_info = f"in chat {target_chats[0]}" if target_chats else "globally"
        self._logger.info(f'Searching "{brief_content(query)}" {ctx_info}')
        try:
            result = self.backend.search(query, target_chats, self._cfg.page_len, 1, file_filter="all") # 初始搜索不过滤
            text = await self._render_response_text(result, time() - start)
            buttons = self._render_respond_buttons(result, 1, current_filter="all")
            msg = await event.reply(text, parse_mode='html', buttons=buttons, link_preview=False)
            if msg: # 存储信息以供翻页/筛选
                prefix, bcid, mid = f'{self.id}:', event.chat_id, msg.id
                self._redis.set(f'{prefix}query_text:{bcid}:{mid}', query, ex=3600)
                self._redis.set(f'{prefix}query_filter:{bcid}:{mid}', "all", ex=3600) # 存初始 filter
                if target_chats: self._redis.set(f'{prefix}query_chats:{bcid}:{mid}', ','.join(map(str, target_chats)), ex=3600)
                else: self._redis.delete(f'{prefix}query_chats:{bcid}:{mid}')
        except whoosh.index.LockError: await event.reply('⏳ Index locked, try again.')
        except Exception as e: self._logger.error(f"Search error: {e}", exc_info=True); await event.reply(f'Search error: {type(e).__name__}.')


    async def _download_history(self, event: events.NewMessage.Event, chat_id: int, min_id: int, max_id: int):
         # chat_id is share_id
         try: chat_html = await self.backend.format_dialog_html(chat_id)
         except Exception as e: chat_html = f"对话 `{chat_id}`"
         try: # 检查是否空索引
             if min_id == 0 and max_id == 0 and not self.backend.is_empty(chat_id):
                 await event.reply(f'⚠️ Warn: {chat_html} index exists. Redownload may cause duplicates. Use `/clear {chat_id}` or specify range.', parse_mode='html')
         except Exception as e: self._logger.error(f"Check empty error {chat_id}: {e}")

         prog_msg: Optional[TgMessage] = None; last_update = time(); interval = 5; count = 0
         async def cb(cur_id: int, dl_count: int):
             nonlocal prog_msg, last_update, count; count = dl_count; now = time()
             if now - last_update > interval: last_update = now; txt = f'⏳ Downloading {chat_html}:\nProcessed {dl_count}, current ID: {cur_id}'
                 try:
                     if prog_msg is None: prog_msg = await event.reply(txt, parse_mode='html')
                     else: await prog_msg.edit(txt, parse_mode='html')
                 except rpcerrorlist.FloodWaitError as fwe: last_update += fwe.seconds
                 except rpcerrorlist.MessageNotModifiedError: pass
                 except rpcerrorlist.MessageIdInvalidError: prog_msg = None
                 except Exception as e: self._logger.error(f"Edit progress error {chat_id}: {e}"); prog_msg = None

         start = time()
         try:
              await self.backend.download_history(chat_id, min_id, max_id, cb)
              msg = f'✅ {chat_html} download complete, indexed {count} msgs, took {time()-start:.2f}s.'
              try: await event.reply(msg, parse_mode='html')
              except Exception: await self.bot.send_message(event.chat_id, msg, parse_mode='html') # 回复失败则发送
         except (EntityNotFoundError, ValueError) as e: # 捕获已知错误
              self._logger.error(f"Download failed {chat_id}: {e}"); await event.reply(f'❌ Download {chat_html} error: {e}', parse_mode='html')
         except Exception as e: # 其他错误
              self._logger.error(f"Download failed {chat_id}: {e}", exc_info=True); await event.reply(f'❌ Download {chat_html} unknown error: {type(e).__name__}', parse_mode='html')
         finally:
              if prog_msg: try: await prog_msg.delete() catch Exception: pass


    def _register_hooks(self):
        @self.bot.on(events.CallbackQuery())
        async def cq_handler(event: events.CallbackQuery.Event):
             is_admin = self._admin_id and event.sender_id == self._admin_id
             is_wl = event.sender_id in self._cfg.private_whitelist
             if self._cfg.private_mode and not is_admin and not is_wl:
                   await event.answer("Permission denied.", alert=True); return
             await self._callback_handler(event)

        @self.bot.on(events.NewMessage())
        async def msg_handler(event: events.NewMessage.Event):
            sender = await event.message.get_sender()
            if not sender or sender.id == self.my_id: return # 忽略无发送者或自己的消息
            is_admin = self._admin_id and sender.id == self._admin_id

            mentioned, reply_to_bot = False, False
            if event.is_group or event.is_channel:
                 if self.username and f'@{self.username}' in event.raw_text: mentioned = True
                 elif event.message.mentioned and event.message.entities: # 检查提及实体
                      for entity in event.message.entities:
                          if isinstance(entity, MessageEntityMentionName) and entity.user_id == self.my_id: mentioned = True; break
                 if event.message.is_reply: # 检查回复
                      try: reply = await event.message.get_reply_message(); reply_to_bot = reply and reply.sender_id == self.my_id
                      except Exception: pass # 获取回复失败则忽略

            process = event.is_private or mentioned or reply_to_bot
            if not process: return # 不处理群组中无关消息

            if self._cfg.private_mode and not is_admin: # 私人模式权限
                 try: csi = get_share_id(event.chat_id)
                 except Exception: csi = None
                 if sender.id not in self._cfg.private_whitelist and (csi is None or csi not in self._cfg.private_whitelist):
                     if event.is_private: await event.reply('Permission denied (private mode).');
                     return

            # 分发处理
            handler = self._admin_msg_handler if is_admin else self._normal_msg_handler
            try: await handler(event)
            except whoosh.index.LockError: await event.reply('⏳ Index locked, try later.')
            except EntityNotFoundError as e: await event.reply(f'❌ Not found: {e.entity}')
            except telethon.errors.rpcerrorlist.UserIsBlockedError: self._logger.warning(f"User {sender.id} blocked.")
            except telethon.errors.rpcerrorlist.ChatWriteForbiddenError: self._logger.warning(f"Write forbidden: {event.chat_id}.")
            except Exception as e:
                 et = type(e).__name__; self._logger.error(f"Handle msg error {sender.id}: {et}: {e}", exc_info=True)
                 try: await event.reply(f'Error: {et}.\nContact admin.')
                 except Exception as re: self._logger.error(f"Reply error failed: {re}")
                 if self._admin_id and event.chat_id != self._admin_id: # 通知管理员
                      try: await self.bot.send_message(self._admin_id, f"Error user {sender.id} chat {event.chat_id}:\n<pre>{html.escape(format_exc())}</pre>", parse_mode='html')
                      except Exception as ne: self._logger.error(f"Notify admin failed: {ne}")


    async def _get_selected_chat_from_reply(self, event: events.NewMessage.Event) -> Optional[Tuple[int, str]]:
        if not event.message.is_reply or not event.message.reply_to_msg_id: return None
        key = f'{self.id}:select_chat:{event.chat_id}:{event.message.reply_to_msg_id}'
        res = self._redis.get(key)
        if res:
            try: cid = int(res); name = await self.backend.translate_chat_id(cid); return cid, name
            except ValueError: self._redis.delete(key); return None # 删除无效 key
            except EntityNotFoundError: return int(res), f"Unknown ({res})" # 返回 ID 和未知名称
            except Exception as e: self._logger.error(f"Error get selected chat {key}: {e}"); return None
        return None


    async def _register_commands(self):
        admin_peer = None
        if self._admin_id: try: admin_peer = await self.bot.get_input_entity(self._admin_id)
                           except Exception as e: self._logger.error(f'Failed get admin input entity {self._admin_id}: {e}')
        else: self._logger.warning("Admin ID invalid, skip admin commands registration.")

        ac = [ BotCommand(c, d) for c, d in [ ("download_chat", '[Opts] [Chats] Download'), ("monitor_chat", 'Chats Add monitor'), ("clear", '[Chats|all] Clear index'), ("stat", 'Query status'), ("find_chat_id", 'KW Find chat ID'), ("refresh_chat_names", 'Refresh name cache')]]
        cc = [ BotCommand(c, d) for c, d in [ ("s", 'KW Search (or /search /ss)'), ("chats", '[KW] List/Select chats'), ("random", 'Random message'), ("help", 'Show help')]]

        if admin_peer: try: await self.bot(SetBotCommandsRequest(scope=BotCommandScopePeer(admin_peer), lang_code='', commands=ac+cc)); self._logger.info(f"Set admin commands ok.") catch Exception as e: self._logger.error(f"Set admin commands failed: {e}")
        try: await self.bot(SetBotCommandsRequest(scope=BotCommandScopeDefault(), lang_code='', commands=cc)); self._logger.info("Set default commands ok.")
        except Exception as e: self._logger.error(f"Set default commands failed: {e}")


    async def _render_response_text(self, result: SearchResult, used_time: float) -> str:
        if not isinstance(result, SearchResult) or result.total_results == 0: return "No relevant messages found."
        sb = [f'Found {result.total_results} results, took {used_time:.3f}s:\n\n']
        for i, hit in enumerate(result.hits, 1):
            try:
                msg = hit.msg
                try: title = await self.backend.translate_chat_id(msg.chat_id)
                except EntityNotFoundError: title = f"Unknown ({msg.chat_id})"
                hdr = [f"<b>{i}. {html.escape(title)}</b>"];
                if msg.sender: hdr.append(f"(<u>{html.escape(msg.sender)}</u>)")
                hdr.append(f'[{msg.post_time.strftime("%y-%m-%d %H:%M")}]')
                sb.append(' '.join(hdr) + '\n')
                if msg.filename: sb.append(f"📎 File: <b>{html.escape(msg.filename)}</b>\n")
                # --- 使用修复后的高亮/摘要逻辑 ---
                display_text = hit.highlighted or "" # 使用高亮片段
                if not display_text: # 无高亮时的回退逻辑
                     if msg.content: display_text = html.escape(brief_content(msg.content, 150))
                     elif msg.filename: display_text = f"<i>(File, no text content)</i>"
                     else: display_text = "<i>(Empty message)</i>"
                # --- 结束修复 ---
                if msg.url: sb.append(f'<a href="{html.escape(msg.url)}">Go to msg</a>\n{display_text}\n\n') # 固定链接文本，下方显示摘要
                else: sb.append(f"{display_text} (No link)\n\n")
            except Exception as e: sb.append(f"<b>{i}.</b> Error rendering result: {type(e).__name__}\n\n"); self._logger.error(f"Error rendering hit: {e}", exc_info=True)

        final = ''.join(sb); max_len = 4096
        if len(final) > max_len:
             cutoff = "\n\n...(Too many results, showing partial)"
             last_nl = final.rfind('\n\n', 0, max_len - len(cutoff) - 10)
             final = final[:last_nl if last_nl != -1 else max_len - len(cutoff)] + cutoff
        return final


    def _render_respond_buttons(self, result: SearchResult, cur_page_num: int, current_filter: str = "all") -> Optional[List[List[Button]]]:
        if not isinstance(result, SearchResult): return None
        buttons = []
        # 筛选按钮
        fr = [ Button.inline("【All】" if current_filter == "all" else "All", 'search_filter=all'),
               Button.inline("【Text】" if current_filter == "text_only" else "Text", 'search_filter=text_only'),
               Button.inline("【File】" if current_filter == "file_only" else "File", 'search_filter=file_only') ]
        buttons.append(fr)
        # 翻页按钮
        try: total_pages = (result.total_results + max(1, self._cfg.page_len) - 1) // max(1, self._cfg.page_len)
        except Exception: total_pages = 1
        if total_pages > 1:
            pr = []
            if cur_page_num > 1: pr.append(Button.inline('⬅️ Prev', f'search_page={cur_page_num - 1}'))
            pr.append(Button.inline(f'{cur_page_num}/{total_pages}', 'noop'))
            if not result.is_last_page and cur_page_num < total_pages: pr.append(Button.inline('Next ➡️', f'search_page={cur_page_num + 1}'))
            if pr: buttons.append(pr)
        return buttons if buttons else None
