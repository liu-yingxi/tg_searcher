# -*- coding: utf-8 -*-
import html
from time import time
from typing import Optional, List, Tuple, Set, Union
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
# 导入 SearchResult 和 IndexMsg
from .indexer import SearchResult, IndexMsg

# 获取日志记录器
logger = get_logger('frontend_bot')


class BotFrontendConfig:
    @staticmethod
    def _parse_redis_cfg(redis_cfg: str) -> Tuple[str, int]:
        colon_idx = redis_cfg.find(':')
        if colon_idx < 0: return redis_cfg, 6379
        try:
            host = redis_cfg[:colon_idx] if colon_idx > 0 else 'localhost'
            port = int(redis_cfg[colon_idx + 1:])
            return host, port
        except (ValueError, TypeError): raise ValueError(f"Invalid Redis port in '{redis_cfg}'")

    def __init__(self, **kw):
        try: self.bot_token: str = kw['bot_token']; self.admin: Union[int, str] = kw['admin_id']
        except KeyError as e: raise ValueError(f"Missing required config key: {e}")
        self.page_len: int = kw.get('page_len', 10)
        if self.page_len <= 0: logger.warning("page_len invalid, using 10."); self.page_len = 10
        self.no_redis: bool = kw.get('no_redis', False)
        self.redis_host: Optional[Tuple[str, int]] = None
        if not self.no_redis:
             try:
                  redis_cfg = kw.get('redis', 'localhost:6379')
                  if redis_cfg: self.redis_host = self._parse_redis_cfg(redis_cfg)
                  else: logger.warning("Redis config empty. Disabling redis."); self.no_redis = True
             except ValueError as e: logger.error(f"Error parsing redis config: {e}. Disabling redis."); self.no_redis = True
             except KeyError: logger.info("Redis config key 'redis' not found. Disabling redis."); self.no_redis = True
        self.private_mode: bool = kw.get('private_mode', False)
        self.private_whitelist: Set[int] = set()
        raw_wl = kw.get('private_whitelist', [])
        if isinstance(raw_wl, list):
            for item in raw_wl:
                try: self.private_whitelist.add(int(item))
                except (ValueError, TypeError): logger.warning(f"Cannot parse whitelist item '{item}'")
        elif raw_wl: logger.warning("private_whitelist format invalid, ignoring.")
        # admin ID 加入白名单在 start() 中进行

class FakeRedis:
    """内存模拟 Redis 接口"""
    def __init__(self): self._data = {}; self._logger = get_logger('FakeRedis'); self._logger.warning("Using FakeRedis")
    def get(self, key): return self._data.get(key)
    def set(self, key, val, ex=None):
        if ex: self._logger.debug(f"FakeRedis setex ignored: {key}")
        self._data[key] = str(val)
    def delete(self, *keys): count = 0; for k in keys: if k in self._data: del self._data[k]; count += 1; return count
    def ping(self): return True


class BotFrontend:
    """ TG Searcher 前端 Bot """

    # --- 帮助文本常量 ---
    HELP_TEXT_USER = """
**可用命令:**
/s `关键词` - (别名: /ss, /search) 搜索消息。直接发送关键词也可搜索。
/chats `[关键词]` - 列出/选择已索引的对话。
/random - 返回一条随机消息。
/help - 显示此帮助信息。

**使用 /chats 选择对话后:**
- 回复选择成功的消息 + 搜索词，可仅搜索该对话。
"""

    HELP_TEXT_ADMIN = """
**通用命令:**
/s `关键词` - (别名: /ss, /search) 搜索消息。直接发送关键词也可搜索。
/chats `[关键词]` - 列出/选择已索引的对话。
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
    # --- 结束帮助文本 ---

    def __init__(self, common_cfg: CommonBotConfig, cfg: BotFrontendConfig, frontend_id: str, backend: BackendBot):
        self.backend = backend
        self.id = frontend_id
        self._common_cfg = common_cfg
        self.bot = TelegramClient(str(common_cfg.session_dir / f'frontend_{self.id}.session'),
                                  api_id=common_cfg.api_id, api_hash=common_cfg.api_hash, proxy=common_cfg.proxy)
        self._cfg = cfg
        # 初始化 Redis / FakeRedis
        if cfg.no_redis or cfg.redis_host is None: self._redis = FakeRedis()
        else:
            try: self._redis = Redis(host=cfg.redis_host[0], port=cfg.redis_host[1], decode_responses=True); self._redis.ping()
            except Exception as e: logger.critical(f'Redis init failed: {e}. Falling back to FakeRedis.'); self._redis = FakeRedis(); self._cfg.no_redis = True
        self._logger = logger # 使用模块级 logger
        self._admin_id: Optional[int] = None
        self.username: Optional[str] = None
        self.my_id: Optional[int] = None

        # --- 参数解析器 ---
        self.download_arg_parser = ArgumentParser(prog="/download_chat", description="下载并索引对话历史", add_help=False, exit_on_error=False)
        self.download_arg_parser.add_argument('--min', type=int, default=0, help="起始消息ID (默认为0)")
        self.download_arg_parser.add_argument('--max', type=int, default=0, help="结束消息ID (0=无限制)")
        self.download_arg_parser.add_argument('chats', type=str, nargs='*', help="对话ID或用户名列表")

        self.chat_ids_parser = ArgumentParser(prog="/monitor_chat 或 /clear", description="处理对话列表", add_help=False, exit_on_error=False)
        self.chat_ids_parser.add_argument('chats', type=str, nargs='*', help="对话ID或用户名列表")


    async def start(self):
        # 解析管理员 ID
        try:
            if not self._cfg.admin: raise ValueError("Admin ID not configured.")
            self._admin_id = await self.backend.str_to_chat_id(str(self._cfg.admin))
            self._logger.info(f"Admin ID resolved: {self._admin_id}")
            if self._cfg.private_mode and self._admin_id: self._cfg.private_whitelist.add(self._admin_id); self._logger.info(f"Admin added to private whitelist.")
        except Exception as e: self._logger.critical(f"Failed to resolve admin '{self._cfg.admin}': {e}", exc_info=True); self._admin_id = None

        # 检查 Redis 连接
        if not isinstance(self._redis, FakeRedis):
             try: self._redis.ping(); self._logger.info(f"Redis connected: {self._cfg.redis_host}")
             except RedisConnectionError as e: self._logger.critical(f'Redis connection failed: {e}. Fallback to FakeRedis.'); self._redis = FakeRedis(); self._cfg.no_redis = True

        self._logger.info(f'Starting frontend bot {self.id}...')
        try:
             await self.bot.start(bot_token=self._cfg.bot_token)
             me = await self.bot.get_me(); assert me is not None
             self.username, self.my_id = me.username, me.id
             self._logger.info(f'Bot (@{self.username}, id={self.my_id}) login ok.')
             self.backend.excluded_chats.add(get_share_id(self.my_id)); self._logger.info(f"Added self ID {self.my_id} to backend excludes.")
             await self._register_commands(); self._logger.info(f'Bot commands registered.')
             self._register_hooks(); self._logger.info(f'Event hooks registered.')
             if self._admin_id: # 发送启动状态给 Admin
                  try:
                       msg_head = '✅ Bot 前端初始化完成\n\n'; stat_text = await self.backend.get_index_status(4000 - len(msg_head))
                       await self.bot.send_message(self._admin_id, msg_head + stat_text, parse_mode='html', link_preview=False)
                  except Exception as e: self._logger.error(f"Failed get/send initial status: {e}", exc_info=True); await self.bot.send_message(self._admin_id, f'⚠️ Bot 启动，但获取初始状态失败: {e}')
             else: self._logger.warning("Admin ID invalid, skip startup message.")
             self._logger.info(f"Frontend bot {self.id} started successfully.")
        except Exception as e: self._logger.critical(f"Failed to start frontend bot {self.id}: {e}", exc_info=True)


    async def _callback_handler(self, event: events.CallbackQuery.Event):
        try:
            self._logger.info(f'Callback: {event.sender_id} in {event.chat_id}, msg={event.message_id}, data={event.data!r}')
            if not event.data: await event.answer("无效操作 (no data)。"); return
            try: query_data = event.data.decode('utf-8')
            except Exception: await event.answer("无效操作 (bad data format)。"); return
            if not query_data.strip(): await event.answer("无效操作 (empty data)。"); return

            parts = query_data.split('=', 1)
            if len(parts) != 2: await event.answer("操作格式错误。"); return

            action, value = parts[0], parts[1]
            redis_prefix = f'{self.id}:'
            bot_chat_id, result_msg_id = event.chat_id, event.message_id
            query_key = f'{redis_prefix}query_text:{bot_chat_id}:{result_msg_id}'
            chats_key = f'{redis_prefix}query_chats:{bot_chat_id}:{result_msg_id}'
            filter_key = f'{redis_prefix}query_filter:{bot_chat_id}:{result_msg_id}' # 过滤器 Key

            if action == 'search_page' or action == 'search_filter':
                 new_page_num = 1; current_filter = self._redis.get(filter_key) or "all"
                 if action == 'search_page':
                      try: new_page_num = int(value); assert new_page_num > 0
                      except (ValueError, AssertionError): await event.answer("无效页码。"); return
                      new_filter = current_filter # 翻页保持过滤器
                 else: # action == 'search_filter'
                      new_filter = value if value in ["all", "text_only", "file_only"] else "all"
                      self._redis.set(filter_key, new_filter, ex=3600) # 更新过滤器并重置页码

                 q, chats_str = self._redis.get(query_key), self._redis.get(chats_key)
                 if q is None: await event.answer("搜索信息已过期。"); return # 简单提示

                 chats = [int(cid) for cid in chats_str.split(',')] if chats_str else None
                 self._logger.info(f'Query [{brief_content(q)}] chats={chats} filter={new_filter} page={new_page_num}')
                 start_time = time()
                 try: result = self.backend.search(q, chats, self._cfg.page_len, new_page_num, file_filter=new_filter)
                 except Exception as e: self._logger.error(f"Backend search error: {e}", exc_info=True); await event.answer("搜索后端出错。"); return
                 used_time = time() - start_time

                 response = await self._render_response_text(result, used_time)
                 buttons = self._render_respond_buttons(result, new_page_num, current_filter=new_filter) # 传递当前过滤器
                 try: await event.edit(response, parse_mode='html', buttons=buttons, link_preview=False); await event.answer()
                 except rpcerrorlist.MessageNotModifiedError: await event.answer() # 无需提示
                 except rpcerrorlist.MessageIdInvalidError: await event.answer("消息已删除。")
                 except Exception as e: self._logger.error(f"Edit error: {e}"); await event.answer("更新结果失败。")

            elif action == 'select_chat':
                 try:
                      chat_id = int(value)
                      try: chat_name = await self.backend.translate_chat_id(chat_id); prompt = f'☑️ 已选: **{html.escape(chat_name)}** (`{chat_id}`)\n\n回复本消息进行操作。'
                      except EntityNotFoundError: prompt = f'☑️ 已选: `{chat_id}` (无法获取名称)\n\n回复本消息进行操作。'
                      await event.edit(prompt, parse_mode='markdown')
                      select_key = f'{redis_prefix}select_chat:{bot_chat_id}:{result_msg_id}'; self._redis.set(select_key, chat_id, ex=3600)
                      self._logger.info(f"Chat {chat_id} selected by {event.sender_id}, key {select_key}")
                      await event.answer("对话已选择")
                 except ValueError: await event.answer("无效对话 ID。")
                 except Exception as e: self._logger.error(f"Select chat error: {e}", exc_info=True); await event.answer("处理选择出错。")

            elif action == 'noop': await event.answer() # 静默处理不可点按钮
            else: self._logger.warning(f'Unknown callback action: {action}'); await event.answer("未知操作。")
        except Exception as e:
             self._logger.error(f"Callback handler error: {e}", exc_info=True)
             try: await event.answer("内部错误。")
             except Exception as final_e: self._logger.error(f"Failed to answer callback after error: {final_e}")


    async def _normal_msg_handler(self, event: events.NewMessage.Event):
        text: str = event.raw_text.strip()
        sender = await event.message.get_sender(); sender_id = sender.id if sender else 'Unknown'
        self._logger.info(f'User {sender_id} in {event.chat_id} sends: "{brief_content(text, 100)}"')
        selected_chat_context = await self._get_selected_chat_from_reply(event)

        if not text or text.startswith('/start'): await event.reply("欢迎！发送关键词搜索，或用 /help 查看命令。"); return
        elif text.startswith('/help'): await event.reply(self.HELP_TEXT_USER, parse_mode='markdown'); return # 发送用户帮助并返回

        elif text.startswith('/random'):
            try:
                msg = self.backend.rand_msg()
                chat_name = await self.backend.translate_chat_id(msg.chat_id)
                dc = f"📎 {html.escape(msg.filename)}" + (f" ({html.escape(brief_content(msg.content))})" if msg.content else "") if msg.filename else html.escape(brief_content(msg.content))
                respond = f'随机消息来自 **{html.escape(chat_name)}** (`{msg.chat_id}`)\n'
                if msg.sender: respond += f'发送者: {html.escape(msg.sender)}\n'
                respond += f'时间: {msg.post_time.strftime("%Y-%m-%d %H:%M")}\n内容: {dc or "(空)"}\n<a href="{msg.url}">跳转</a>'
            except IndexError: respond = '错误：索引为空。'
            except EntityNotFoundError as e: respond = f"错误: 无法找到来源对话 (`{e.entity}`)。"
            except Exception as e: self._logger.error(f"Random error: {e}", exc_info=True); respond = f"获取随机消息出错: {type(e).__name__}"
            await event.reply(respond, parse_mode='html', link_preview=False)

        elif text.startswith('/chats'):
            buttons, kw = [], remove_first_word(text)
            monitored = sorted(list(self.backend.monitored_chats)); found = 0
            if monitored:
                for cid in monitored:
                    try:
                         cname = await self.backend.translate_chat_id(cid)
                         if kw and kw.lower() not in cname.lower(): continue
                         found += 1;
                         if found <= 50: buttons.append(Button.inline(f"{brief_content(cname, 25)} (`{cid}`)", f'select_chat={cid}'))
                    except EntityNotFoundError: self._logger.warning(f"Chat ID {cid} not found during /chats.")
                    except Exception as e: self._logger.error(f"Error processing chat {cid} for /chats: {e}")
                if buttons: reply_text = "请选择对话：" if found <= 50 else f"找到 {found} 个，显示前 50：" ; button_rows = [buttons[i:i + 2] for i in range(0, len(buttons), 2)]; await event.reply(reply_text, buttons=button_rows)
                else: await event.reply(f'无标题含 "{html.escape(kw)}" 的已索引对话。' if kw else '无已索引对话。')
            else: await event.reply('暂无监听对话。')

        # --- 处理搜索命令及其别名 ---
        elif text.startswith(('/s ', '/ss ', '/search ', '/s', '/ss', '/search')):
            command = text.split()[0]
            query = remove_first_word(text) if text != command else ""
            if not query and not selected_chat_context: await event.reply(f"请输入关键词。用法: `{command} 关键词`", parse_mode='markdown'); return
            await self._search(event, query, selected_chat_context)

        elif text.startswith('/'): await event.reply(f'错误：未知命令 `{text.split()[0]}`。', parse_mode='markdown')
        else: await self._search(event, text, selected_chat_context) # 默认搜索


    async def _chat_ids_from_args(self, chats_args: List[str]) -> Tuple[List[int], List[str]]:
        cids, errors = [], []
        if not chats_args: return [], []
        for arg in chats_args:
            try: cids.append(await self.backend.str_to_chat_id(arg))
            except EntityNotFoundError: errors.append(f'未找到 "{html.escape(arg)}"')
            except Exception as e: errors.append(f'解析 "{html.escape(arg)}" 出错: {type(e).__name__}')
        return cids, errors


    async def _admin_msg_handler(self, event: events.NewMessage.Event):
        text: str = event.raw_text.strip()
        self._logger.info(f'Admin {event.chat_id} cmd: "{brief_content(text, 100)}"')
        sel_chat_ctx = await self._get_selected_chat_from_reply(event)
        sel_cid, sel_cname = (sel_chat_ctx[0], sel_chat_ctx[1]) if sel_chat_ctx else (None, None)

        if text.startswith('/help'): await event.reply(self.HELP_TEXT_ADMIN, parse_mode='markdown'); return
        elif text.startswith('/stat'):
            try: await event.reply(await self.backend.get_index_status(), parse_mode='html', link_preview=False)
            except Exception as e: self._logger.error("Stat error:", exc_info=True); err_trace = html.escape(format_exc()); await event.reply(f"获取状态出错: {html.escape(str(e))}\n<pre>{err_trace}</pre>", parse_mode='html')
        elif text.startswith('/download_chat'):
            try: args = self.download_arg_parser.parse_args(shlex.split(text)[1:])
            except Exception as e: usage = self.download_arg_parser.format_help(); await event.reply(f"参数错误: {e}\n用法:{usage}"); return
            min_id, max_id = args.min or 0, args.max or 0
            target_cids, errors = await self._chat_ids_from_args(args.chats)
            if not args.chats and sel_cid and sel_cid not in target_cids: target_cids=[sel_cid]; await event.reply(f"回复模式：下载 **{html.escape(sel_cname)}** (`{sel_cid}`)", parse_mode='markdown')
            elif not target_cids and not errors: await event.reply("错误：需指定对话或回复。"); return
            if errors: await event.reply("无法解析:\n- " + "\n- ".join(errors));
            if not target_cids: return
            s_cnt, f_cnt = 0, 0
            for cid in target_cids:
                try: await self._download_history(event, cid, min_id, max_id); s_cnt += 1
                except Exception as dl_e: f_cnt += 1; self._logger.error(f"DL fail {cid}: {dl_e}", exc_info=True); await event.reply(f"❌ 下载 `{cid}` 失败: {html.escape(str(dl_e))}")
            if len(target_cids) > 1: await event.reply(f"下载完成: {s_cnt} 成功, {f_cnt} 失败。")
        elif text.startswith('/monitor_chat'):
            try: args = self.chat_ids_parser.parse_args(shlex.split(text)[1:])
            except Exception as e: usage = self.chat_ids_parser.format_help(); await event.reply(f"参数错误: {e}\n用法:{usage}"); return
            target_cids, errors = await self._chat_ids_from_args(args.chats)
            if not args.chats and sel_cid and sel_cid not in target_cids: target_cids = [sel_cid]; await event.reply(f"回复模式：监听 **{html.escape(sel_cname)}** (`{sel_cid}`)", parse_mode='markdown')
            elif not target_cids and not errors: await event.reply("错误：需指定对话或回复。"); return
            if errors: await event.reply("无法解析:\n- " + "\n- ".join(errors));
            if not target_cids: return
            replies, added, already = [], 0, 0
            for cid in target_cids:
                if cid in self.backend.monitored_chats: already += 1
                else:
                    self.backend.monitored_chats.add(cid); added += 1
                    try: html_c = await self.backend.format_dialog_html(cid); replies.append(f"- ✅ {html_c} 已加入监听。")
                    except Exception as e: replies.append(f"- ✅ `{cid}` 已加入监听 (名错: {type(e).__name__})。")
                    self._logger.info(f'Admin added {cid} monitor')
            if replies: await event.reply('\n'.join(replies), parse_mode='html', link_preview=False)
            summary = [f"{c}个已加入。" for c in [added] if c>0] + [f"{c}个已在监听。" for c in [already] if c>0]
            if summary: await event.reply(" ".join(summary))
        elif text.startswith('/clear'):
            try: args = self.chat_ids_parser.parse_args(shlex.split(text)[1:])
            except Exception as e: usage = self.chat_ids_parser.format_help(); await event.reply(f"参数错误: {e}\n用法:{usage}"); return
            if len(args.chats) == 1 and args.chats[0].lower() == 'all':
                try: self.backend.clear(None); await event.reply('✅ 全部索引已清除。')
                except Exception as e: self._logger.error("Clear all error:", exc_info=True); await event.reply(f"清除出错: {e}")
                return
            target_cids, errors = await self._chat_ids_from_args(args.chats)
            if not args.chats and sel_cid and sel_cid not in target_cids: target_cids = [sel_cid]; await event.reply(f"回复模式：清除 **{html.escape(sel_cname)}** (`{sel_cid}`)", parse_mode='markdown')
            elif not target_cids and not errors: await event.reply("错误：需指定对话或回复，或用 /clear all。"); return
            if errors: await event.reply("无法解析:\n- " + "\n- ".join(errors));
            if not target_cids: return
            try:
                self.backend.clear(target_cids); replies = []
                for cid in target_cids:
                    try: html_c = await self.backend.format_dialog_html(cid); replies.append(f"- ✅ {html_c} 索引已清除。")
                    except Exception: replies.append(f"- ✅ `{cid}` 索引已清除。")
                await event.reply('\n'.join(replies), parse_mode='html', link_preview=False)
            except Exception as e: self._logger.error(f"Clear error: {e}", exc_info=True); await event.reply(f"清除出错: {e}")
        elif text.startswith('/refresh_chat_names'):
            msg = await event.reply('刷新对话名称缓存...');
            try: await self.backend.session.refresh_translate_table(); await msg.edit('✅ 缓存刷新完成。')
            except Exception as e: self._logger.error("Refresh names error:", exc_info=True); await msg.edit(f'刷新出错: {e}')
        elif text.startswith('/find_chat_id'):
            q = remove_first_word(text)
            if not q: await event.reply('错误：请输入关键词。'); return
            try:
                results = await self.backend.find_chat_id(q); sb = []
                if results:
                     sb.append(f'找到 {len(results)} 个含 "{html.escape(q)}" 的对话:\n')
                     for cid in results[:50]:
                         try: cname = await self.backend.translate_chat_id(cid); sb.append(f'- {html.escape(cname)}: `{cid}`\n')
                         except EntityNotFoundError: sb.append(f'- 未知: `{cid}`\n')
                         except Exception as e: sb.append(f'- `{cid}` 错: {type(e).__name__}\n')
                     if len(results) > 50: sb.append("\n(仅显示前 50)")
                else: sb.append(f'未找到含 "{html.escape(q)}" 的对话。')
                await event.reply(''.join(sb), parse_mode='html')
            except Exception as e: self._logger.error(f"Find chat error: {e}", exc_info=True); await event.reply(f"查找出错: {e}")
        else: await self._normal_msg_handler(event) # 其他情况交给普通处理器


    async def _search(self, event: events.NewMessage.Event, query: str, selected_chat_context: Optional[Tuple[int, str]]):
        if not query and selected_chat_context: query = '*'; await event.reply(f"搜索对话 **{html.escape(selected_chat_context[1])}** (`{selected_chat_context[0]}`)...", parse_mode='markdown')
        elif not query: self._logger.debug("Empty search query ignored."); return

        target_cids = [selected_chat_context[0]] if selected_chat_context else None
        try: is_empty = self.backend.is_empty(chat_id=target_cids[0] if target_cids else None)
        except Exception as e: self._logger.error(f"Index check error: {e}"); await event.reply("检查索引出错。"); return
        if is_empty: await event.reply('索引为空。' if not selected_chat_context else f'对话 **{html.escape(selected_chat_context[1])}** 索引为空。', parse_mode='markdown'); return

        start_time = time()
        ctx_info = f"in chat {target_cids[0]}" if target_cids else "globally"
        self._logger.info(f'Searching "{brief_content(query, 100)}" {ctx_info}')
        try:
            result = self.backend.search(query, target_cids, self._cfg.page_len, 1, file_filter="all") # 初始搜索不过滤
            used_time = time() - start_time
            resp_text = await self._render_response_text(result, used_time)
            buttons = self._render_respond_buttons(result, 1, current_filter="all") # 初始过滤器为 "all"
            msg = await event.reply(resp_text, parse_mode='html', buttons=buttons, link_preview=False)
            if msg: # 存储上下文信息到 Redis
                prefix, bcid, mid = f'{self.id}:', event.chat_id, msg.id
                self._redis.set(f'{prefix}query_text:{bcid}:{mid}', query, ex=3600)
                self._redis.set(f'{prefix}query_filter:{bcid}:{mid}', "all", ex=3600) # 存初始过滤器
                if target_cids: self._redis.set(f'{prefix}query_chats:{bcid}:{mid}', ','.join(map(str, target_cids)), ex=3600)
                else: self._redis.delete(f'{prefix}query_chats:{bcid}:{mid}') # 确保全局搜索时清除
        except whoosh.index.LockError: await event.reply('⏳ 索引锁定中，请稍后。')
        except Exception as e: self._logger.error(f"Search error: {e}", exc_info=True); await event.reply(f'搜索出错: {type(e).__name__}。')


    async def _download_history(self, event: events.NewMessage.Event, chat_id: int, min_id: int, max_id: int):
         try: chat_html = await self.backend.format_dialog_html(chat_id)
         except Exception as e: self._logger.error(f"Format HTML error {chat_id}: {e}"); chat_html = f"`{chat_id}`"
         try:
             if min_id==0 and max_id==0 and not self.backend.is_empty(chat_id=chat_id):
                 await event.reply(f'⚠️ 警告: {chat_html} 索引已存在。\n下载全部可能导致重复。请先 `/clear {chat_id}` 或指定范围。', parse_mode='html')
         except Exception as e: self._logger.error(f"Index check error {chat_id}: {e}")

         prog_msg: Optional[TgMessage] = None; last_update = time(); interval = 5; total_dl = 0
         async def cb(cur_id: int, dl_cnt: int):
             nonlocal prog_msg, last_update, total_dl; total_dl = dl_cnt; now = time()
             if now - last_update > interval:
                 last_update = now; text = f'⏳ 下载 {chat_html}:\n已处理 {dl_cnt} 条, 当前 ID: {cur_id}'
                 try:
                     if prog_msg is None: prog_msg = await event.reply(text, parse_mode='html')
                     else: await prog_msg.edit(text, parse_mode='html')
                 except rpcerrorlist.FloodWaitError as fwe: self._logger.warning(f"Flood wait {fwe.seconds}s"); last_update += fwe.seconds
                 except rpcerrorlist.MessageNotModifiedError: pass
                 except rpcerrorlist.MessageIdInvalidError: self._logger.warning("Progress msg deleted."); prog_msg = None
                 except Exception as e: self._logger.error(f"Edit progress error: {e}"); prog_msg = None
         start = time()
         try:
              await self.backend.download_history(chat_id, min_id, max_id, cb)
              used = time() - start; comp_msg = f'✅ {chat_html} 下载完成，索引 {total_dl} 条，用时 {used:.2f} 秒。'
              try: await event.reply(comp_msg, parse_mode='html')
              except Exception: await self.bot.send_message(event.chat_id, comp_msg, parse_mode='html')
         except (EntityNotFoundError, ValueError) as e: self._logger.error(f"DL failed {chat_id}: {e}"); await event.reply(f'❌ 下载 {chat_html} 出错: {e}', parse_mode='html')
         except Exception as e: self._logger.error(f"DL failed {chat_id}:", exc_info=True); await event.reply(f'❌ 下载 {chat_html} 未知错误: {type(e).__name__}', parse_mode='html')
         finally:
              if prog_msg: try: await prog_msg.delete()
                           except Exception as e: self._logger.warning(f"Delete progress msg error: {e}")


    def _register_hooks(self):
        @self.bot.on(events.CallbackQuery())
        async def cb_handler(event: events.CallbackQuery.Event):
             sid = event.sender_id
             if self._cfg.private_mode and sid != self._admin_id and sid not in self._cfg.private_whitelist:
                  await event.answer("无权操作。", alert=True); return
             await self._callback_handler(event)

        @self.bot.on(events.NewMessage())
        async def msg_handler(event: events.NewMessage.Event):
            sender = await event.message.get_sender();
            if not sender: return
            sid = sender.id
            if not self.my_id: try: self.my_id = (await self.bot.get_me()).id; except Exception: return
            if sid == self.my_id: return

            is_admin = (self._admin_id is not None and sid == self._admin_id)
            mentioned, replied = False, False
            if event.is_group or event.is_channel:
                 if self.username and f'@{self.username}' in event.raw_text: mentioned = True
                 elif event.message.mentioned and event.message.entities:
                      for e in event.message.entities:
                          if isinstance(e, MessageEntityMentionName) and e.user_id == self.my_id: mentioned = True; break
                 if event.message.is_reply:
                      try: reply = await event.message.get_reply_message(); replied = bool(reply and reply.sender_id == self.my_id)
                      except Exception: pass # 获取回复失败

            process = event.is_private or mentioned or replied
            if not process: return

            if self._cfg.private_mode and not is_admin:
                 try: chat_sid = get_share_id(event.chat_id)
                 except Exception: chat_sid = None
                 sender_ok = sid in self._cfg.private_whitelist
                 chat_ok = chat_sid is not None and chat_sid in self._cfg.private_whitelist
                 if not sender_ok and not chat_ok:
                     if event.is_private: await event.reply('无权使用此 Bot。')
                     self._logger.info(f"Blocked private access user {sid} chat {event.chat_id}({chat_sid})")
                     return

            handler = self._admin_msg_handler if is_admin else self._normal_msg_handler
            try: await handler(event)
            except whoosh.index.LockError: await event.reply('⏳ 索引锁定中...')
            except EntityNotFoundError as e: await event.reply(f'❌ 未找到: {e.entity}')
            except telethon.errors.rpcerrorlist.UserIsBlockedError: self._logger.warning(f"User {sid} blocked bot.")
            except telethon.errors.rpcerrorlist.ChatWriteForbiddenError: self._logger.warning(f"Write forbidden in {event.chat_id}.")
            except Exception as e:
                 etype = type(e).__name__; self._logger.error(f"Handler error from {sid}: {etype}: {e}", exc_info=True)
                 try: await event.reply(f'处理出错: {etype}。\n请联系管理员。')
                 except Exception as reply_e: self._logger.error(f"Reply error msg failed: {reply_e}")
                 if self._admin_id and event.chat_id != self._admin_id:
                      try: await self.bot.send_message(self._admin_id, f"用户 {sid} (聊 {event.chat_id}) 错误:\n<pre>{html.escape(format_exc())}</pre>", parse_mode='html')
                      except Exception as admin_e: self._logger.error(f"Notify admin failed: {admin_e}")


    async def _get_selected_chat_from_reply(self, event: events.NewMessage.Event) -> Optional[Tuple[int, str]]:
        msg = event.message
        if not msg.is_reply or not msg.reply_to_msg_id: return None
        prefix, key = f'{self.id}:', f'select_chat:{event.chat_id}:{msg.reply_to_msg_id}'
        res = self._redis.get(f'{prefix}{key}')
        if res:
            try:
                cid = int(res) # share_id
                try: cname = await self.backend.translate_chat_id(cid)
                except EntityNotFoundError: cname = f"未知 ({cid})"
                self._logger.info(f"Msg from {event.sender_id} is reply to selection for {cid}")
                return cid, cname
            except ValueError: self._logger.warning(f"Invalid chat_id in Redis {key}: {res}"); self._redis.delete(f'{prefix}{key}'); return None
            except Exception as e: self._logger.error(f"Error processing selection context {key}: {e}"); return None
        else: return None


    async def _register_commands(self):
        admin_peer = None
        if self._admin_id:
             try: admin_peer = await self.bot.get_input_entity(self._admin_id)
             except Exception as e: self._logger.error(f'Get admin entity error {self._admin_id}: {e}', exc_info=True)
        else: self._logger.warning("Admin ID invalid, skip admin commands.")

        admin_cmds = [ BotCommand(c, d) for c, d in [
            ("download_chat", '[选项] [对话...] 下载索引'), ("monitor_chat", '对话... 加入监听'),
            ("clear", '[对话...|all] 清除索引'), ("stat", '查询状态'),
            ("find_chat_id", '关键词 查找对话ID'), ("refresh_chat_names", '刷新对话名称缓存')]]
        common_cmds = [ BotCommand(c, d) for c, d in [
            ("s", '(别名 /ss /search) 关键词 搜索'), # 将 /s 作为主命令显示
            ("chats", '[关键词] 列出/选择对话'), ("random", '随机消息'), ("help", '显示帮助')]]

        if admin_peer:
            try: await self.bot(SetBotCommandsRequest(scope=BotCommandScopePeer(admin_peer), lang_code='', commands=admin_cmds + common_cmds)); self._logger.info(f"Set cmds for admin {self._admin_id}.")
            except Exception as e: self._logger.error(f"Set admin cmds error: {e}", exc_info=True)
        try: await self.bot(SetBotCommandsRequest(scope=BotCommandScopeDefault(), lang_code='', commands=common_cmds)); self._logger.info("Set default cmds.")
        except Exception as e: self._logger.error(f"Set default cmds error: {e}", exc_info=True)


    async def _render_response_text(self, result: SearchResult, used_time: float) -> str:
        if not isinstance(result, SearchResult) or result.total_results == 0: return "未找到相关消息。"
        sb = [f'找到 {result.total_results} 条结果，用时 {used_time:.3f} 秒:\n\n']
        for i, hit in enumerate(result.hits, 1):
            try:
                msg: IndexMsg = hit.msg
                try: chat_title = await self.backend.translate_chat_id(msg.chat_id)
                except EntityNotFoundError: chat_title = f"未知对话 ({msg.chat_id})"
                except Exception as e: chat_title = f"对话 {msg.chat_id} (错误)"; self._logger.warning(f"Translate chat error {msg.chat_id}: {e}")

                header = [f"<b>{i}. {html.escape(chat_title)}</b>"]
                if msg.sender: header.append(f"(<u>{html.escape(msg.sender)}</u>)")
                header.append(f'[{msg.post_time.strftime("%y-%m-%d %H:%M")}]')
                sb.append(' '.join(header) + '\n')
                if msg.filename: sb.append(f"📎 文件: <b>{html.escape(msg.filename)}</b>\n")

                # --- 使用渲染好的高亮摘要 ---
                display_text = hit.highlighted if hit.highlighted else ""
                # 如果高亮为空，尝试用原始内容或文件名
                if not display_text:
                    if msg.content: display_text = html.escape(brief_content(msg.content, 150))
                    elif msg.filename: display_text = f"<i>(文件: {html.escape(brief_content(msg.filename, 50))})</i>"
                    else: display_text = "<i>(空消息)</i>"

                # 固定链接文本，下方显示摘要
                link_text = "跳转到消息"
                if msg.url: sb.append(f'<a href="{html.escape(msg.url)}">{link_text}</a>\n{display_text}\n\n')
                else: sb.append(f"{display_text} (无链接)\n\n")
            except Exception as e: sb.append(f"<b>{i}.</b> 渲染出错: {type(e).__name__}\n\n"); self._logger.error(f"Render hit error: {e}", exc_info=True)

        final = ''.join(sb)
        max_len = 4096; cutoff = "\n\n...(结果过多，仅显示部分)"
        if len(final) > max_len:
             last_nl = final.rfind('\n\n', 0, max_len - len(cutoff))
             final = final[:last_nl if last_nl != -1 else max_len - len(cutoff)] + cutoff
        return final


    # --- render_respond_buttons: 添加了 current_filter 参数 ---
    def _render_respond_buttons(self, result: SearchResult, cur_page_num: int, current_filter: str = "all") -> Optional[List[List[Button]]]:
        if not isinstance(result, SearchResult) or result.total_results == 0: return None
        try: page_len = self._cfg.page_len if self._cfg.page_len > 0 else 10; total_pages = (result.total_results + page_len - 1) // page_len
        except ZeroDivisionError: total_pages = 1

        buttons = []
        # 筛选按钮行
        filter_row = [ Button.inline("【全部】" if current_filter == "all" else "全部", 'search_filter=all'),
                       Button.inline("【仅文本】" if current_filter == "text_only" else "仅文本", 'search_filter=text_only'),
                       Button.inline("【仅文件】" if current_filter == "file_only" else "仅文件", 'search_filter=file_only')]
        buttons.append(filter_row)

        # 翻页按钮行
        if total_pages > 1:
            page_row = []
            if cur_page_num > 1: page_row.append(Button.inline('⬅️ 上页', f'search_page={cur_page_num - 1}'))
            page_row.append(Button.inline(f'{cur_page_num}/{total_pages}', 'noop'))
            if not result.is_last_page and cur_page_num < total_pages: page_row.append(Button.inline('下页 ➡️', f'search_page={cur_page_num + 1}'))
            if page_row: buttons.append(page_row) # 只有需要翻页时才添加

        return buttons if buttons else None
