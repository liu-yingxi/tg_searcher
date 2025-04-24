# -*- coding: utf-8 -*-
import html
from datetime import datetime
from typing import Optional, List, Set, Dict

import telethon.errors.rpcerrorlist
from telethon import events
from telethon.tl.patched import Message as TgMessage
from telethon.tl.types import User
from whoosh.query import Term # 导入 Term

# 导入 SearchResult 以便在出错时返回空结果
from .indexer import Indexer, IndexMsg, SearchResult
from .common import CommonBotConfig, escape_content, get_share_id, get_logger, format_entity_name, brief_content, \
    EntityNotFoundError
from .session import ClientSession

# 获取日志记录器
logger = get_logger('backend_bot') # 使用统一的 logger 名称


class BackendBotConfig:
    def __init__(self, **kw):
        self.monitor_all = kw.get('monitor_all', False)
        # 保存原始配置，以便在 start 时解析用户名
        self._raw_exclude_chats = kw.get('exclude_chats', [])
        self.excluded_chats: Set[int] = set() # 初始化为空，将在 start 中填充

        # 在 __init__ 中可以预处理整数 ID
        for chat_id_or_name in self._raw_exclude_chats:
            try:
                share_id = get_share_id(int(chat_id_or_name))
                self.excluded_chats.add(share_id)
            except (ValueError, TypeError):
                 pass # 非整数将在 start 中处理


class BackendBot:
    def __init__(self, common_cfg: CommonBotConfig, cfg: BackendBotConfig,
                 session: ClientSession, clean_db: bool, backend_id: str):
        self.id: str = backend_id
        self.session = session
        self._logger = get_logger(f'bot-backend:{backend_id}') # 使用实例特定的 logger
        self._cfg = cfg

        if clean_db: self._logger.info(f'Index cleaning requested for backend {backend_id}')
        try:
            self._indexer: Indexer = Indexer(common_cfg.index_dir / backend_id, clean_db)
        except ValueError as e:
             self._logger.critical(f"Indexer init failed for backend {backend_id}: {e}")
             raise
        except Exception as e:
             self._logger.critical(f"Unexpected error initializing Indexer for backend {backend_id}: {e}", exc_info=True)
             raise

        try:
            self.monitored_chats: Set[int] = self._indexer.list_indexed_chats()
            self._logger.info(f"Loaded {len(self.monitored_chats)} monitored chats from index.")
        except Exception as e:
            self._logger.error(f"Failed to list indexed chats on startup: {e}"); self.monitored_chats = set()

        # 使用配置中已解析的整数 ID 初始化 excluded_chats
        self.excluded_chats = cfg.excluded_chats
        self._raw_exclude_chats = cfg._raw_exclude_chats # 保存原始配置以供 start 解析
        self.newest_msg: Dict[int, IndexMsg] = dict()
        # _load_newest_messages_on_startup 移到 start()

    def _load_newest_messages_on_startup(self):
         self._logger.info("Loading newest message for each monitored chat...")
         count = 0
         for chat_id in list(self.monitored_chats): # 使用副本迭代
              if chat_id in self.excluded_chats: continue # 跳过排除的
              try:
                   result = self._indexer.search(q_str='*', in_chats=[chat_id], page_len=1, page_num=1)
                   if result.hits: self.newest_msg[chat_id] = result.hits[0].msg; count += 1
              except Exception as e: self._logger.warning(f"Failed to load newest message for chat {chat_id}: {e}")
         self._logger.info(f"Finished loading newest messages for {count} chats.")


    async def start(self):
        self._logger.info(f'Starting backend bot {self.id}...')

        # 解析配置中可能是用户名的 exclude_chats
        resolved_excludes_in_cfg = set()
        for chat_id_or_name in self._raw_exclude_chats:
            if isinstance(chat_id_or_name, str) and not chat_id_or_name.isdigit():
                 try:
                      share_id = await self.str_to_chat_id(chat_id_or_name)
                      resolved_excludes_in_cfg.add(share_id)
                      self._logger.info(f"Resolved exclude chat '{chat_id_or_name}' to ID {share_id}")
                 except EntityNotFoundError: self._logger.warning(f"Exclude chat '{chat_id_or_name}' not found, ignoring.")
                 except Exception as e: self._logger.error(f"Error resolving exclude chat '{chat_id_or_name}': {e}")
        self.excluded_chats.update(resolved_excludes_in_cfg)
        self._logger.info(f"Final excluded chats: {self.excluded_chats or 'None'}")

        # 加载最新消息
        self._load_newest_messages_on_startup()

        # 检查监控聊天
        chats_to_remove = set()
        for chat_id in list(self.monitored_chats): # 使用副本迭代
            try:
                if chat_id in self.excluded_chats:
                     self._logger.info(f"Chat {chat_id} in exclude list, removing from monitoring."); chats_to_remove.add(chat_id); continue
                # 尝试获取名称以验证可访问性
                await self.translate_chat_id(chat_id)
                # self._logger.info(f'Monitoring chat "{chat_name}" ({chat_id})') # 减少日志噪音
            except EntityNotFoundError:
                 self._logger.warning(f'Monitored chat {chat_id} not found, removing.'); chats_to_remove.add(chat_id)
            except Exception as e:
                self._logger.error(f'Error checking monitored chat {chat_id}: {e}, removing.'); chats_to_remove.add(chat_id)

        if chats_to_remove:
            try:
                 for chat_id in chats_to_remove: self.monitored_chats.discard(chat_id)
                 self._logger.info(f'Removed {len(chats_to_remove)} chats from monitoring list.')
                 # 考虑是否删除索引数据
            except Exception as e: self._logger.error(f"Error removing chats from monitoring list: {e}")

        self._register_hooks()
        self._logger.info(f"Backend bot {self.id} started successfully.")


    # --- search 方法: 接受并传递 file_filter ---
    def search(self, q: str, in_chats: Optional[List[int]], page_len: int, page_num: int, file_filter: str = "all") -> SearchResult:
        self._logger.debug(f"Backend {self.id} search: q='{brief_content(q)}', chats={in_chats}, page={page_num}, filter={file_filter}")
        try:
            result = self._indexer.search(q, in_chats, page_len, page_num, file_filter=file_filter)
            self._logger.debug(f"Search returned {result.total_results} total hits, {len(result.hits)} on page {page_num}.")
            return result
        except Exception as e:
             self._logger.error(f"Backend search execution error: {e}", exc_info=True)
             return SearchResult([], True, 0)


    def rand_msg(self) -> IndexMsg:
        try: return self._indexer.retrieve_random_document()
        except IndexError: raise IndexError("Index is empty, cannot retrieve random message.")


    def is_empty(self, chat_id=None):
        try: return self._indexer.is_empty(chat_id)
        except Exception as e: self._logger.error(f"Error checking index emptiness for {chat_id}: {e}"); return True


    async def download_history(self, chat_id: int, min_id: int, max_id: int, call_back=None):
        share_id = get_share_id(chat_id)
        self._logger.info(f'Downloading history from {share_id} ({min_id=}, {max_id=})')
        if share_id in self.excluded_chats:
             self._logger.warning(f"Skipping download for excluded chat {share_id}.")
             raise ValueError(f"对话 {share_id} 已被设置为排除，无法下载。")
        if share_id not in self.monitored_chats:
             self.monitored_chats.add(share_id); self._logger.info(f"Added chat {share_id} to monitored list.")

        msg_list = []; downloaded_count = 0; processed_count = 0
        try:
            async for tg_message in self.session.iter_messages(entity=share_id, min_id=min_id, max_id=max_id):
                processed_count += 1
                if not isinstance(tg_message, TgMessage): continue
                url = f'https://t.me/c/{share_id}/{tg_message.id}'
                sender = await self._get_sender_name(tg_message)
                post_time = tg_message.date
                if not isinstance(post_time, datetime): post_time = datetime.now() # Fallback
                msg_text, filename = '', None
                if tg_message.file and hasattr(tg_message.file, 'name') and tg_message.file.name:
                    filename = tg_message.file.name
                    if tg_message.text: msg_text = escape_content(tg_message.text.strip())
                elif tg_message.text: msg_text = escape_content(tg_message.text.strip())

                if msg_text or filename:
                    try:
                        # IndexMsg __init__ 会自动设置 has_file
                        msg = IndexMsg(content=msg_text or "", url=url, chat_id=share_id, post_time=post_time, sender=sender or "", filename=filename)
                        msg_list.append(msg); downloaded_count += 1
                    except Exception as create_e: self._logger.error(f"Error creating IndexMsg for {url}: {create_e}")
                if call_back and processed_count % 100 == 0:
                     try: await call_back(tg_message.id, downloaded_count)
                     except Exception as cb_e: self._logger.warning(f"Callback error: {cb_e}")
        # --- 更具体的错误处理 ---
        except (telethon.errors.rpcerrorlist.ChannelPrivateError, telethon.errors.rpcerrorlist.ChatAdminRequiredError) as e:
             self._logger.error(f"Permission denied for {chat_id} ({share_id}): {e}. Ensure session is member/admin.")
             self.monitored_chats.discard(share_id); raise EntityNotFoundError(f"无法访问对话 {chat_id}，请检查权限。") from e
        except (telethon.errors.rpcerrorlist.ChatIdInvalidError, telethon.errors.rpcerrorlist.PeerIdInvalidError):
            self._logger.error(f"Invalid Chat ID {chat_id} ({share_id})."); self.monitored_chats.discard(share_id); raise EntityNotFoundError(f"无效对话 ID: {chat_id}")
        except ValueError as e: # Handle potential get_input_entity errors
             if "Cannot find any entity corresponding to" in str(e): self._logger.error(f"Cannot find entity for {chat_id} ({share_id})."); self.monitored_chats.discard(share_id); raise EntityNotFoundError(f"无法找到对话实体: {chat_id}") from e
             else: self._logger.error(f"ValueError iterating messages for {chat_id} ({share_id}): {e}", exc_info=True) # Other ValueErrors
        except Exception as e: self._logger.error(f"Error iterating messages for {chat_id} ({share_id}): {e}", exc_info=True)

        self._logger.info(f'Fetching history {share_id} complete: {downloaded_count}/{processed_count} messages to index.')
        if not msg_list: self._logger.info(f"No messages to index for {share_id}."); return

        # 批量写入
        writer = self._indexer.ix.writer()
        newest_msg_in_batch = None; indexed_count_in_batch = 0
        try:
            for msg in msg_list:
                try: self._indexer.add_document(msg, writer); indexed_count_in_batch += 1
                except Exception as add_e: self._logger.error(f"Error adding doc URL {msg.url}: {add_e}")
                if newest_msg_in_batch is None or msg.post_time > newest_msg_in_batch.post_time: newest_msg_in_batch = msg
            if newest_msg_in_batch:
                 cid = int(newest_msg_in_batch.chat_id)
                 if cid not in self.newest_msg or newest_msg_in_batch.post_time > self.newest_msg[cid].post_time:
                      self.newest_msg[cid] = newest_msg_in_batch; self._logger.debug(f"Updated newest cache for {cid} to {newest_msg_in_batch.url}")
            writer.commit()
            self._logger.info(f'Write index commit ok: {indexed_count_in_batch} msgs from {share_id}')
        except Exception as e: writer.cancel(); self._logger.error(f"Error writing batch index for {share_id}: {e}")


    def clear(self, chat_ids: Optional[List[int]] = None):
        if chat_ids is not None:
            share_ids_to_clear = {get_share_id(cid) for cid in chat_ids}
            try:
                with self._indexer.ix.writer() as w:
                    for sid in share_ids_to_clear:
                        w.delete_by_term('chat_id', str(sid))
                        self.monitored_chats.discard(sid)
                        if sid in self.newest_msg: del self.newest_msg[sid]
                        self._logger.info(f'Cleared index/monitoring for chat {sid}')
            except Exception as e: self._logger.error(f"Error clearing index for {share_ids_to_clear}: {e}")
        else:
            try:
                self._indexer.clear(); self.monitored_chats.clear(); self.newest_msg.clear()
                self._logger.info('Cleared all index data and monitoring.')
            except Exception as e: self._logger.error(f"Error clearing all index data: {e}")


    async def find_chat_id(self, q: str) -> List[int]:
        try: return await self.session.find_chat_id(q) # 应返回 share_id 列表
        except Exception as e: self._logger.error(f"Error finding chat id for '{q}': {e}"); return []


    # --- get_index_status: 已修复计数 Bug ---
    async def get_index_status(self, length_limit: int = 4000):
        cur_len = 0; sb = []
        try: total_docs = self._indexer.ix.doc_count(); sb.append(f'后端 "{self.id}" (sess:"{self.session.name}") 总消息: <b>{total_docs}</b>\n\n')
        except Exception as e: self._logger.error(f"Failed get total doc count: {e}"); sb.append(f'后端 "{self.id}" (sess:"{self.session.name}") 总消息: <b>错误</b>\n\n')
        overflow_msg = f'\n\n(部分对话统计因长度限制未显示)'
        def append_msg(lst): nonlocal cur_len, sb; new_len = sum(len(s) for s in lst); if cur_len + new_len > length_limit - len(overflow_msg) - 50: return True; cur_len += new_len; sb.extend(lst); return False

        # 显示排除列表
        if self.excluded_chats:
            excluded_list = sorted(list(self.excluded_chats))
            if append_msg([f'{len(excluded_list)} 个对话被禁止索引:\n']): sb.append(overflow_msg); return ''.join(sb)
            for cid in excluded_list:
                try: chat_html = await self.format_dialog_html(cid)
                except EntityNotFoundError: chat_html = f"未知对话 ({cid})"
                except Exception as e: chat_html = f"对话 {cid} (错误: {type(e).__name__})"
                if append_msg([f'- {chat_html}\n']): sb.append(overflow_msg); return ''.join(sb)
            if sb and sb[-1] != '\n': sb.append('\n')

        # 显示监控列表
        monitored_list = sorted(list(self.monitored_chats))
        if append_msg([f'总计 {len(monitored_list)} 个对话已加入索引:\n']): sb.append(overflow_msg); return ''.join(sb)
        try:
             with self._indexer.ix.searcher() as searcher:
                 for cid in monitored_list:
                     msg_for_chat = []; num = 0
                     try: query = Term('chat_id', str(cid)); results = searcher.search(query, limit=0); num = results.estimated_length()
                     except Exception as e: self._logger.error(f"Error counting for chat {cid}: {e}")
                     try: chat_html = await self.format_dialog_html(cid); msg_for_chat.append(f'- {chat_html} 共 {num} 条\n')
                     except EntityNotFoundError: msg_for_chat.append(f'- 未知对话 (`{cid}`) 共 {num} 条\n')
                     except Exception as e: msg_for_chat.append(f'- 对话 `{cid}` (错误: {type(e).__name__}) 共 {num} 条\n')
                     if newest := self.newest_msg.get(cid):
                         dc = f"📎 {newest.filename}" + (f" ({brief_content(newest.content)})" if newest.content else "") if newest.filename else brief_content(newest.content)
                         esc_dc = html.escape(dc or "(空)")
                         msg_for_chat.append(f'  最新: <a href="{newest.url}">{esc_dc}</a> (@{newest.post_time.strftime("%y-%m-%d %H:%M")})\n')
                     if append_msg(msg_for_chat): sb.append(overflow_msg); break
        except Exception as e: self._logger.error(f"Failed to open searcher for status: {e}"); append_msg(["\n错误：无法读取索引状态。\n"])

        return ''.join(sb)


    async def translate_chat_id(self, chat_id: int) -> str:
        try: return await self.session.translate_chat_id(int(chat_id))
        except (telethon.errors.rpcerrorlist.ChannelPrivateError, telethon.errors.rpcerrorlist.ChatIdInvalidError, ValueError) as e: raise EntityNotFoundError(f"无法访问或无效 Chat ID: {chat_id}") from e
        except EntityNotFoundError: self._logger.warning(f"Entity not found for {chat_id}"); raise
        except Exception as e: self._logger.error(f"Unexpected error translating {chat_id}: {e}"); raise EntityNotFoundError(f"获取 {chat_id} 名称出错")


    async def str_to_chat_id(self, chat: str) -> int:
         try:
             try: return get_share_id(int(chat)) # 尝试直接转换 ID
             except ValueError: raw_id = await self.session.str_to_chat_id(chat); return get_share_id(raw_id) # 否则查找
         except EntityNotFoundError: self._logger.warning(f"Entity not found for '{chat}'"); raise
         except Exception as e: self._logger.error(f"Error converting '{chat}': {e}"); raise EntityNotFoundError(f"解析 '{chat}' 出错")


    async def format_dialog_html(self, chat_id: int):
        try: name = await self.translate_chat_id(int(chat_id)); escaped_name = html.escape(name); return f'<a href="https://t.me/c/{int(chat_id)}/1">{escaped_name}</a> (`{int(chat_id)}`)'
        except EntityNotFoundError: return f'未知对话 (`{chat_id}`)'
        except ValueError: return f'无效对话 ID (`{chat_id}`)'
        except Exception as e: self._logger.warning(f"Error formatting dialog {chat_id}: {e}"); return f'对话 `{chat_id}` (错误)'


    def _should_monitor(self, chat_id: int):
        try:
            sid = get_share_id(chat_id)
            if sid in self.excluded_chats: return False
            return self._cfg.monitor_all or sid in self.monitored_chats
        except Exception as e: self._logger.warning(f"Error checking monitor status for {chat_id}: {e}"); return False

    @staticmethod
    async def _get_sender_name(message: TgMessage) -> str:
        try:
            sender = await message.get_sender(); return format_entity_name(sender) if isinstance(sender, User) else getattr(sender, 'title', '')
        except Exception: return ''


    def _register_hooks(self):
        # 新消息处理器
        @self.session.on(events.NewMessage())
        async def client_message_handler(event: events.NewMessage.Event):
            if event.chat_id is None or not self._should_monitor(event.chat_id): return
            try:
                share_id = get_share_id(event.chat_id); url = f'https://t.me/c/{share_id}/{event.id}'
                sender = await self._get_sender_name(event.message); post_time=event.message.date
                msg_text, filename = '', None
                if event.message.file and hasattr(event.message.file, 'name') and event.message.file.name:
                    filename = event.message.file.name; msg_text = escape_content(event.message.text.strip()) if event.message.text else ''
                    self._logger.info(f'New file: {url} from "{sender}" file:"{filename}" cap:"{brief_content(msg_text)}"')
                elif event.message.text:
                    msg_text = escape_content(event.message.text.strip())
                    if not msg_text.strip(): return # 忽略纯空格消息
                    self._logger.info(f'New msg: {url} from "{sender}" text:"{brief_content(msg_text)}"')
                else: return # 忽略无文本无文件的消息

                msg = IndexMsg(content=msg_text or "", url=url, chat_id=share_id, post_time=post_time, sender=sender or "", filename=filename)
                if share_id not in self.newest_msg or msg.post_time > self.newest_msg[share_id].post_time: self.newest_msg[share_id] = msg; self._logger.debug(f"Updated newest cache for {share_id}")
                try: self._indexer.add_document(msg)
                except Exception as e: self._logger.error(f"Error adding doc {url}: {e}")
            except Exception as e: self._logger.error(f"Error processing new msg in {event.chat_id}: {e}", exc_info=True)

        # 编辑消息处理器
        @self.session.on(events.MessageEdited())
        async def client_message_update_handler(event: events.MessageEdited.Event):
            if event.chat_id is None or not self._should_monitor(event.chat_id): return
            try:
                share_id = get_share_id(event.chat_id); url = f'https://t.me/c/{share_id}/{event.id}'
                new_text = escape_content(event.message.text.strip()) if event.message.text else ''
                # 注意：TG 编辑可能不触发文件变化，主要处理文本编辑
                self._logger.info(f'Msg edited: {url} new_text:"{brief_content(new_text)}"')
                try:
                    old = self._indexer.get_document_fields(url=url)
                    if old:
                        new = old.copy(); new['content'] = new_text or ""
                        # 更新 has_file 状态 (虽然文件通常不变，但以防万一)
                        new['has_file'] = 1 if new.get('filename') else 0
                        self._indexer.replace_document(url=url, new_fields=new)
                        self._logger.info(f'Updated msg content in index: {url}')
                        if share_id in self.newest_msg and self.newest_msg[share_id].url == url: self.newest_msg[share_id].content = new_text; self._logger.debug(f"Updated newest cache content: {url}")
                    else: self._logger.warning(f'Edited msg not found in index: {url}. Ignoring.')
                except Exception as e: self._logger.error(f'Error updating edited msg {url}: {e}')
            except Exception as e: self._logger.error(f"Error processing edited msg in {event.chat_id}: {e}", exc_info=True)

        # 删除消息处理器
        @self.session.on(events.MessageDeleted())
        async def client_message_delete_handler(event: events.MessageDeleted.Event):
            if not hasattr(event, 'chat_id') or event.chat_id is None or not self._should_monitor(event.chat_id): return
            try:
                share_id = get_share_id(event.chat_id); deleted_count = 0
                urls = [f'https://t.me/c/{share_id}/{mid}' for mid in event.deleted_ids]
                try:
                     with self._indexer.ix.writer() as w:
                          for url in urls:
                                if share_id in self.newest_msg and self.newest_msg[share_id].url == url: del self.newest_msg[share_id]; self._logger.info(f"Removed newest cache for {share_id}.")
                                try: w.delete_by_term('url', url); deleted_count += 1; self._logger.info(f"Deleted msg from index: {url}")
                                except Exception as del_e: self._logger.error(f"Error deleting doc {url}: {del_e}")
                     if deleted_count > 0: self._logger.info(f'Finished deleting {deleted_count} msgs for {share_id}')
                except Exception as e: self._logger.error(f"Error processing msg deletions for {share_id}: {e}")
            except Exception as e: self._logger.error(f"Error processing delete event in {event.chat_id}: {e}", exc_info=True)
