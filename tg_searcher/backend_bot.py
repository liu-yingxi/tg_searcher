# -*- coding: utf-8 -*-
import html
from datetime import datetime
from typing import Optional, List, Set, Dict, Any, Union

import telethon.errors.rpcerrorlist
from telethon import events
from telethon.tl.patched import Message as TgMessage
from telethon.tl.types import User
from whoosh.query import Term # 导入 Term
# --- ADDED IMPORTS ---
from whoosh import writing
from whoosh.writing import IndexWriter, LockError
# --- END ADDED IMPORTS ---


from .indexer import Indexer, IndexMsg, SearchResult # 导入 SearchResult
from .common import CommonBotConfig, escape_content, get_share_id, get_logger, format_entity_name, brief_content, \
    EntityNotFoundError
from .session import ClientSession

# 获取日志记录器
try:
    logger = get_logger('backend_bot')
except NameError: # 如果 get_logger 未定义 (例如直接运行此文件)
    import logging
    logger = logging.getLogger('backend_bot')
    if not logger.hasHandlers():
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logger.info("Backend_bot logger initialized with basicConfig.")


class BackendBotConfig:
    def __init__(self, **kw: Any):
        self.monitor_all: bool = kw.get('monitor_all', False)
        # 保存原始配置，以便在 start 时解析用户名
        self._raw_exclude_chats: List[Union[int, str]] = kw.get('exclude_chats', [])
        self.excluded_chats: Set[int] = set() # 在 start 中填充

        # 尝试在初始化时解析整数 ID
        for chat_id_or_name in self._raw_exclude_chats:
            try:
                share_id = get_share_id(int(chat_id_or_name))
                self.excluded_chats.add(share_id)
            except (ValueError, TypeError):
                 pass # 非整数 ID 留给 start 解析


class BackendBot:
    def __init__(self, common_cfg: CommonBotConfig, cfg: BackendBotConfig,
                 session: ClientSession, clean_db: bool, backend_id: str):
        self.id: str = backend_id
        self.session = session

        self._logger = get_logger(f'bot-backend:{backend_id}')
        self._cfg = cfg
        if clean_db:
            self._logger.info(f'Index will be cleaned for backend {backend_id}')
        try:
            self._indexer: Indexer = Indexer(common_cfg.index_dir / backend_id, clean_db)
        except ValueError as e:
             self._logger.critical(f"Indexer initialization failed for backend {backend_id}: {e}")
             raise
        except Exception as e:
             self._logger.critical(f"Unexpected error initializing indexer for {backend_id}: {e}", exc_info=True)
             raise

        # 加载已索引的聊天，并初始化监控列表
        try:
            self.monitored_chats: Set[int] = self._indexer.list_indexed_chats()
            self._logger.info(f"Loaded {len(self.monitored_chats)} monitored chats from index for backend {backend_id}")
        except Exception as e:
            self._logger.error(f"Failed to list indexed chats on startup for backend {backend_id}: {e}", exc_info=True)
            self.monitored_chats = set()

        # 使用配置中已初始化的整数 ID
        self.excluded_chats: Set[int] = cfg.excluded_chats
        self._raw_exclude_chats: List[Union[int, str]] = cfg._raw_exclude_chats # 保存原始配置
        self.newest_msg: Dict[int, IndexMsg] = dict()


    def _load_newest_messages_on_startup(self):
         """启动时尝试为每个监控的聊天加载最新消息"""
         if not self.monitored_chats: return
         self._logger.info("Loading newest message for each monitored chat...")
         count = 0
         for chat_id in list(self.monitored_chats): # 迭代副本
              if chat_id in self.excluded_chats: continue
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
            if isinstance(chat_id_or_name, str) and not chat_id_or_name.lstrip('-').isdigit():
                 try:
                      share_id = await self.str_to_chat_id(chat_id_or_name)
                      resolved_excludes_in_cfg.add(share_id)
                      self._logger.info(f"Resolved exclude chat '{chat_id_or_name}' to ID {share_id}")
                 except EntityNotFoundError: self._logger.warning(f"Exclude chat '{chat_id_or_name}' not found, ignoring.")
                 except Exception as e: self._logger.error(f"Error resolving exclude chat '{chat_id_or_name}': {e}")

        self.excluded_chats.update(resolved_excludes_in_cfg)
        self._logger.info(f"Final excluded chats for backend {self.id}: {self.excluded_chats or 'None'}")

        self._load_newest_messages_on_startup()

        # 检查监控的聊天
        chats_to_remove = set()
        for chat_id in list(self.monitored_chats): # 迭代副本
            try:
                if chat_id in self.excluded_chats:
                     self._logger.info(f"Chat {chat_id} is excluded, removing from monitoring.")
                     chats_to_remove.add(chat_id); continue
                chat_name = await self.translate_chat_id(chat_id)
                self._logger.info(f'Monitoring active for "{chat_name}" ({chat_id})')
            except EntityNotFoundError:
                 self._logger.warning(f'Monitored chat_id {chat_id} not found/accessible, removing from monitor list.')
                 chats_to_remove.add(chat_id)
            except Exception as e:
                self._logger.error(f'Exception checking monitored chat {chat_id}: {e}, removing from monitor list.')
                chats_to_remove.add(chat_id)

        if chats_to_remove:
            for chat_id in chats_to_remove:
                self.monitored_chats.discard(chat_id)
                if chat_id in self.newest_msg: del self.newest_msg[chat_id]
            self._logger.info(f'Removed {len(chats_to_remove)} chats from active monitoring.')

        self._register_hooks()
        self._logger.info(f"Backend bot {self.id} started successfully.")


    def search(self, q: str, in_chats: Optional[List[int]], page_len: int, page_num: int, file_filter: str = "all") -> SearchResult:
        self._logger.debug(f"Backend {self.id} search: q='{brief_content(q)}', chats={in_chats}, page={page_num}, filter={file_filter}")
        try:
            result = self._indexer.search(q, in_chats, page_len, page_num, file_filter=file_filter)
            self._logger.debug(f"Search returned {result.total_results} total hits, {len(result.hits)} on page {page_num}.")
            return result
        except Exception as e:
             self._logger.error(f"Backend search execution failed for {self.id}: {e}", exc_info=True)
             return SearchResult([], True, 0)


    def rand_msg(self) -> IndexMsg:
        try: return self._indexer.retrieve_random_document()
        except IndexError: raise IndexError("Index is empty, cannot retrieve random message.")
        except Exception as e: self._logger.error(f"Error retrieving random document: {e}", exc_info=True); raise


    def is_empty(self, chat_id: Optional[int] = None) -> bool:
        try: return self._indexer.is_empty(chat_id)
        except Exception as e: self._logger.error(f"Error checking index emptiness for {chat_id}: {e}"); return True


    async def download_history(self, chat_id: int, min_id: int, max_id: int, call_back: Optional[callable] = None):
        try: share_id = get_share_id(chat_id)
        except Exception as e: self._logger.error(f"Invalid chat_id format for download: {chat_id}, error: {e}"); raise EntityNotFoundError(f"无效的对话 ID 格式: {chat_id}")

        self._logger.info(f'Downloading history for {share_id} (raw_id={chat_id}, min={min_id}, max={max_id})')
        if share_id in self.excluded_chats: self._logger.warning(f"Skipping download for excluded chat {share_id}."); raise ValueError(f"对话 {share_id} 已被排除，无法下载。")

        if share_id not in self.monitored_chats: self.monitored_chats.add(share_id); self._logger.info(f"Added chat {share_id} to monitored list.")

        msg_list: List[IndexMsg] = []
        downloaded_count: int = 0
        processed_count: int = 0

        try:
            async for tg_message in self.session.iter_messages(entity=share_id, min_id=min_id, max_id=max_id, limit=None):
                processed_count += 1
                if not isinstance(tg_message, TgMessage): continue

                url = f'https://t.me/c/{share_id}/{tg_message.id}'
                sender = await self._get_sender_name(tg_message)
                post_time = tg_message.date

                msg_text, filename = '', None
                if tg_message.file and hasattr(tg_message.file, 'name') and tg_message.file.name:
                    filename = tg_message.file.name
                    if tg_message.text: msg_text = escape_content(tg_message.text.strip())
                elif tg_message.text:
                    msg_text = escape_content(tg_message.text.strip())

                if msg_text or filename:
                    try: msg = IndexMsg(content=msg_text or "", url=url, chat_id=share_id, post_time=post_time, sender=sender or "", filename=filename); msg_list.append(msg); downloaded_count += 1
                    except Exception as create_e: self._logger.error(f"Error creating IndexMsg for {url}: {create_e}")

                if call_back and processed_count % 100 == 0:
                     try: await call_back(tg_message.id, downloaded_count)
                     except Exception as cb_e: self._logger.warning(f"Error in download callback: {cb_e}")

        except telethon.errors.rpcerrorlist.ChannelPrivateError as e:
             self._logger.error(f"Permission denied for chat {share_id}. Is session member? Error: {e}"); self.monitored_chats.discard(share_id); raise EntityNotFoundError(f"无法访问对话 {chat_id}，请确保后端账号是其成员。") from e
        except (telethon.errors.rpcerrorlist.ChatIdInvalidError, telethon.errors.rpcerrorlist.PeerIdInvalidError):
            self._logger.error(f"Chat ID {share_id} (raw: {chat_id}) is invalid or peer not found."); self.monitored_chats.discard(share_id); raise EntityNotFoundError(f"无效对话 ID 或无法找到 Peer: {chat_id}")
        except ValueError as e:
             if "Cannot find any entity corresponding to" in str(e) or "Could not find the input entity for" in str(e): self._logger.error(f"Cannot find entity for chat {share_id} (raw: {chat_id}). Error: {e}"); self.monitored_chats.discard(share_id); raise EntityNotFoundError(f"无法找到对话实体: {chat_id}") from e
             else: self._logger.error(f"ValueError iterating messages for {share_id}: {e}", exc_info=True); raise
        except Exception as e: self._logger.error(f"Error iterating messages for {share_id}: {e}", exc_info=True)

        self._logger.info(f'History fetch complete for {share_id}: {downloaded_count} messages to index out of {processed_count} processed.')
        if not msg_list: return

        writer: Optional[IndexWriter] = None
        newest_msg_in_batch: Optional[IndexMsg] = None
        indexed_count_in_batch: int = 0
        try:
            writer = self._indexer.ix.writer()
            for msg in msg_list:
                try:
                    self._indexer.add_document(msg, writer); indexed_count_in_batch += 1
                    if newest_msg_in_batch is None or msg.post_time > newest_msg_in_batch.post_time: newest_msg_in_batch = msg
                except Exception as add_e: self._logger.error(f"Error adding document {msg.url} to batch writer: {add_e}")
            writer.commit()
            self._logger.info(f'Write index commit ok for {indexed_count_in_batch} messages from chat {share_id}')
            if newest_msg_in_batch:
                 current_chat_id = int(newest_msg_in_batch.chat_id)
                 if current_chat_id not in self.newest_msg or newest_msg_in_batch.post_time > self.newest_msg[current_chat_id].post_time:
                      self.newest_msg[current_chat_id] = newest_msg_in_batch
                      self._logger.debug(f"Updated newest msg cache for {current_chat_id} to {newest_msg_in_batch.url}")
        except writing.LockError: logger.error("Index is locked during batch write. Downloaded messages are lost."); raise RuntimeError("Index is locked, cannot write downloaded messages.")
        except Exception as e: logger.error(f"Error writing batch index for {share_id}: {e}", exc_info=True)


    def clear(self, chat_ids: Optional[List[int]] = None):
        if chat_ids is not None:
            share_ids_to_clear = {get_share_id(cid) for cid in chat_ids}
            try:
                with self._indexer.ix.writer() as w:
                    for share_id in share_ids_to_clear:
                        deleted_count = w.delete_by_term('chat_id', str(share_id))
                        self.monitored_chats.discard(share_id)
                        if share_id in self.newest_msg: del self.newest_msg[share_id]
                        self._logger.info(f'Cleared {deleted_count} docs and stopped monitoring chat {share_id}')
            except Exception as e: self._logger.error(f"Error clearing index for chats {share_ids_to_clear}: {e}")
        else:
            try:
                self._indexer.clear(); self.monitored_chats.clear(); self.newest_msg.clear()
                self._logger.info('Cleared all index data and stopped monitoring.')
            except Exception as e: self._logger.error(f"Error clearing all index data: {e}")


    async def find_chat_id(self, q: str) -> List[int]:
        try: return await self.session.find_chat_id(q)
        except Exception as e: self._logger.error(f"Error finding chat id for '{q}': {e}"); return []


    async def get_index_status(self, length_limit: int = 4000) -> str:
        """获取后端索引状态的文本描述"""
        cur_len = 0
        sb = []
        try: total_docs = self._indexer.ix.doc_count()
        except Exception as e: total_docs = -1; self._logger.error(f"Failed get total doc count: {e}")
        sb.append(f'后端 "{self.id}" (会话: "{self.session.name}") 总消息: <b>{total_docs if total_docs >= 0 else "[获取失败]"}</b>\n\n')

        overflow_msg = f'\n\n(部分信息因长度限制未显示)'
        def append_msg(msg_list: List[str]) -> bool:
            nonlocal cur_len; new_len = sum(len(msg) for msg in msg_list)
            if cur_len + new_len > length_limit - len(overflow_msg) - 50: return True
            cur_len += new_len; sb.extend(msg_list); return False

        if self.excluded_chats:
            excluded_list = sorted(list(self.excluded_chats))
            if append_msg([f'{len(excluded_list)} 个对话被禁止索引:\n']): sb.append(overflow_msg); return ''.join(sb)
            for chat_id in excluded_list:
                try: chat_html = await self.format_dialog_html(chat_id)
                except EntityNotFoundError: chat_html = f"未知对话 (`{chat_id}`)"
                except Exception: chat_html = f"对话 `{chat_id}` (获取名称出错)"
                if append_msg([f'- {chat_html}\n']): sb.append(overflow_msg); return ''.join(sb)
            if sb and sb[-1] != '\n\n': sb.append('\n')

        monitored_chats_list = sorted(list(self.monitored_chats))
        if append_msg([f'总计 {len(monitored_chats_list)} 个对话被加入了索引:\n']): sb.append(overflow_msg); return ''.join(sb)

        try:
             with self._indexer.ix.searcher() as searcher:
                 for chat_id in monitored_chats_list:
                     msg_for_chat = []
                     num = -1 # 初始化为错误状态
                     try:
                         # [最终修正] 使用 search + estimated_length 获取数量
                         results = searcher.search(Term('chat_id', str(chat_id)), limit=0)
                         num = results.estimated_length() # 获取匹配查询的文档总数
                     except Exception as e:
                         # 如果搜索或获取数量失败，记录错误，num 保持 -1
                         self._logger.error(f"Error counting documents for chat {chat_id}: {e}", exc_info=True) # 添加 exc_info

                     try: chat_html = await self.format_dialog_html(chat_id)
                     except EntityNotFoundError: chat_html = f"未知对话 (`{chat_id}`)"
                     except Exception: chat_html = f"对话 `{chat_id}` (获取名称出错)"

                     # 使用 "[计数失败]" 作为更明确的错误提示
                     msg_for_chat.append(f'- {chat_html} 共 {"[计数失败]" if num < 0 else num} 条消息\n')

                     if newest_msg := self.newest_msg.get(chat_id):
                         display = f"📎 {newest_msg.filename}" if newest_msg.filename else brief_content(newest_msg.content)
                         if newest_msg.filename and newest_msg.content: display += f" ({brief_content(newest_msg.content)})"
                         esc_display = html.escape(display or "(空)")
                         msg_for_chat.append(f'  最新: <a href="{newest_msg.url}">{esc_display}</a> (@{newest_msg.post_time.strftime("%y-%m-%d %H:%M")})\n')

                     if append_msg(msg_for_chat): sb.append(overflow_msg); break
        except Exception as e:
             self._logger.error(f"Failed open searcher for status: {e}")
             append_msg(["\n错误：无法获取详细状态。\n"])

        return ''.join(sb)


    async def translate_chat_id(self, chat_id: int) -> str:
        try: return await self.session.translate_chat_id(int(chat_id))
        except (telethon.errors.rpcerrorlist.ChannelPrivateError, telethon.errors.rpcerrorlist.ChatIdInvalidError, ValueError, TypeError): raise EntityNotFoundError(f"无法访问或无效 Chat ID: {chat_id}")
        except EntityNotFoundError: self._logger.warning(f"Entity not found for {chat_id}"); raise
        except Exception as e: self._logger.error(f"Error translating chat_id {chat_id}: {e}"); raise EntityNotFoundError(f"获取对话 {chat_id} 名称时出错") from e


    async def str_to_chat_id(self, chat: str) -> int:
         try:
             try: raw_id = int(chat); return get_share_id(raw_id)
             except ValueError: raw_id = await self.session.str_to_chat_id(chat); return get_share_id(raw_id)
         except EntityNotFoundError: self._logger.warning(f"Entity not found for '{chat}'"); raise
         except Exception as e: self._logger.error(f"Error converting '{chat}' to chat_id: {e}"); raise EntityNotFoundError(f"解析 '{chat}' 时出错") from e


    async def format_dialog_html(self, chat_id: int):
        try: name = await self.translate_chat_id(int(chat_id)); esc_name = html.escape(name); return f'<a href="https://t.me/c/{chat_id}/1">{esc_name}</a> (`{chat_id}`)'
        except EntityNotFoundError: return f'未知对话 (`{chat_id}`)'
        except ValueError: return f'无效对话 ID (`{chat_id}`)'
        except Exception as e: self._logger.warning(f"Error formatting html for {chat_id}: {e}"); return f'对话 `{chat_id}` (获取名称出错)'


    def _should_monitor(self, chat_id: int) -> bool:
        try:
            share_id = get_share_id(chat_id)
            if share_id in self.excluded_chats: return False
            return self._cfg.monitor_all or (share_id in self.monitored_chats)
        except Exception: return False

    @staticmethod
    async def _get_sender_name(message: TgMessage) -> str:
        try:
            sender = await message.get_sender()
            if isinstance(sender, User): return format_entity_name(sender)
            else: return getattr(sender, 'title', '')
        except Exception: return ''


    def _register_hooks(self):
        @self.session.on(events.NewMessage())
        async def client_message_handler(event: events.NewMessage.Event):
            if event.chat_id is None or not self._should_monitor(event.chat_id): return
            try:
                share_id = get_share_id(event.chat_id)
                url = f'https://t.me/c/{share_id}/{event.id}'
                sender = await self._get_sender_name(event.message)
                post_time = event.message.date
                msg_text, filename = '', None
                if event.message.file and hasattr(event.message.file, 'name') and event.message.file.name:
                    filename = event.message.file.name
                    if event.message.text: msg_text = escape_content(event.message.text.strip())
                    self._logger.info(f'New file {url} from "{sender}": "{filename}" Cap:"{brief_content(msg_text)}"')
                elif event.message.text:
                    msg_text = escape_content(event.message.text.strip())
                    if not msg_text.strip() and not filename: return
                    self._logger.info(f'New msg {url} from "{sender}": "{brief_content(msg_text)}"')
                else: return

                msg = IndexMsg(content=msg_text or "", url=url, chat_id=share_id, post_time=post_time, sender=sender or "", filename=filename)
                if share_id not in self.newest_msg or msg.post_time > self.newest_msg[share_id].post_time: self.newest_msg[share_id] = msg; self._logger.debug(f"Updated newest cache for {share_id} to {url}")
                try: self._indexer.add_document(msg)
                except Exception as e: self._logger.error(f"Error adding doc {url} to index: {e}")
            except Exception as e: self._logger.error(f"Error processing new message in chat {event.chat_id}: {e}", exc_info=True)

        @self.session.on(events.MessageEdited())
        async def client_message_update_handler(event: events.MessageEdited.Event):
            if event.chat_id is None or not self._should_monitor(event.chat_id): return
            try:
                share_id = get_share_id(event.chat_id)
                url = f'https://t.me/c/{share_id}/{event.id}'
                new_msg_text = escape_content(event.message.text.strip()) if event.message.text else ''
                self._logger.info(f'Msg {url} edited. New content: "{brief_content(new_msg_text)}"')
                try:
                    old_fields = self._indexer.get_document_fields(url=url)
                    if old_fields:
                        new_fields = old_fields.copy(); new_fields['content'] = new_msg_text or ""
                        new_fields.setdefault('chat_id', str(share_id))
                        new_fields.setdefault('post_time', event.message.date)
                        new_fields.setdefault('sender', old_fields.get('sender', ''))
                        new_fields.setdefault('filename', old_fields.get('filename', None))
                        new_fields.setdefault('url', url)
                        self._indexer.replace_document(url=url, new_fields=new_fields)
                        self._logger.info(f'Updated msg content in index for {url}')
                        if share_id in self.newest_msg and self.newest_msg[share_id].url == url: self.newest_msg[share_id].content = new_msg_text; self._logger.debug(f"Updated newest cache content for {url}")
                    else:
                         self._logger.warning(f'Edited msg {url} not found in index. Adding as new message.')
                         sender = await self._get_sender_name(event.message)
                         post_time = event.message.date
                         filename = None # Assume file doesn't change on edit text
                         msg = IndexMsg(content=new_msg_text or "", url=url, chat_id=share_id, post_time=post_time, sender=sender or "", filename=filename)
                         self._indexer.add_document(msg)
                except Exception as e: self._logger.error(f'Error updating edited msg {url} in index: {e}')
            except Exception as e: self._logger.error(f"Error processing edited message in chat {event.chat_id}: {e}", exc_info=True)

        @self.session.on(events.MessageDeleted())
        async def client_message_delete_handler(event: events.MessageDeleted.Event):
            if not hasattr(event, 'chat_id') or event.chat_id is None or not self._should_monitor(event.chat_id):
                 self._logger.debug(f"Ignoring deletion event without valid/monitored chat_id. Deleted IDs: {event.deleted_ids}")
                 return
            try:
                share_id = get_share_id(event.chat_id)
                deleted_count = 0
                urls = [f'https://t.me/c/{share_id}/{mid}' for mid in event.deleted_ids]
                try:
                     with self._indexer.ix.writer() as writer:
                          for url in urls:
                               if share_id in self.newest_msg and self.newest_msg[share_id].url == url: del self.newest_msg[share_id]; self._logger.info(f"Removed newest cache for {share_id} due to deletion of {url}.")
                               try:
                                    count = writer.delete_by_term('url', url)
                                    if count > 0: deleted_count += count; self._logger.info(f"Deleted msg {url} from index.")
                               except Exception as del_e: self._logger.error(f"Error deleting doc {url} from index: {del_e}")
                     if deleted_count > 0: self._logger.info(f'Finished deleting {deleted_count} msgs from index for chat {share_id}')
                except Exception as e: self._logger.error(f"Error processing deletions batch for {share_id}: {e}")
            except Exception as e: self._logger.error(f"Error processing deleted event in chat {event.chat_id}: {e}", exc_info=True)

# --- 文件结束 ---
