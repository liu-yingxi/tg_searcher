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
logger = get_logger('backend_bot')


class BackendBotConfig:
    def __init__(self, **kw):
        self.monitor_all = kw.get('monitor_all', False)
        self.excluded_chats: Set[int] = set()
        # 确保 exclude_chats 中的 ID 都是整数形式的 share_id
        raw_exclude_chats = kw.get('exclude_chats', [])
        # 保存原始配置，以便在 start 时解析用户名
        self._raw_exclude_chats = raw_exclude_chats

        for chat_id_or_name in raw_exclude_chats:
            try:
                # 尝试将配置值直接视为整数 ID 或 share_id
                share_id = get_share_id(int(chat_id_or_name))
                self.excluded_chats.add(share_id)
            except (ValueError, TypeError):
                 # 如果不是有效的整数 ID，暂时忽略，将在 start 中尝试解析
                 logger.debug(f"Exclude chat value '{chat_id_or_name}' is not an integer ID, will try to resolve later.")
                 pass


class BackendBot:
    def __init__(self, common_cfg: CommonBotConfig, cfg: BackendBotConfig,
                 session: ClientSession, clean_db: bool, backend_id: str):
        self.id: str = backend_id
        self.session = session

        self._logger = get_logger(f'bot-backend:{backend_id}')
        self._cfg = cfg
        if clean_db:
            self._logger.info(f'Index will be cleaned for backend {backend_id}')
        # 使用更新后的 Indexer 初始化
        try:
            self._indexer: Indexer = Indexer(common_cfg.index_dir / backend_id, clean_db)
        except ValueError as e:
             # 捕获 Schema 不兼容错误并退出或记录
             self._logger.critical(f"Indexer initialization failed for backend {backend_id}: {e}")
             raise # 重新抛出让上层知道

        # on startup, all indexed chats are added to monitor list
        try:
            self.monitored_chats: Set[int] = self._indexer.list_indexed_chats()
            self._logger.info(f"Loaded {len(self.monitored_chats)} monitored chats from index for backend {backend_id}")
        except Exception as e:
            self._logger.error(f"Failed to list indexed chats on startup for backend {backend_id}: {e}")
            self.monitored_chats = set() # 初始化为空集合

        # 使用配置中已解析的整数 ID 初始化 excluded_chats
        self.excluded_chats = cfg.excluded_chats
        self._raw_exclude_chats = cfg._raw_exclude_chats # 保存原始配置以供 start 解析
        # newest_msg 仍然可以基于 post_time 跟踪，类型是 IndexMsg
        self.newest_msg: Dict[int, IndexMsg] = dict()
        # 移动 _load_newest_messages_on_startup 到 start()

    def _load_newest_messages_on_startup(self):
         """启动时尝试为每个监控的聊天加载最新消息"""
         self._logger.info("Loading newest message for each monitored chat...")
         count = 0
         # 遍历监控列表的副本，以防在加载时列表被修改
         for chat_id in list(self.monitored_chats):
              # 跳过已排除的聊天
              if chat_id in self.excluded_chats:
                   continue
              try:
                   result = self._indexer.search(q_str='*', in_chats=[chat_id], page_len=1, page_num=1)
                   if result.hits:
                        self.newest_msg[chat_id] = result.hits[0].msg
                        count += 1
              except Exception as e:
                   self._logger.warning(f"Failed to load newest message for chat {chat_id}: {e}")
         self._logger.info(f"Finished loading newest messages for {count} chats.")


    async def start(self):
        self._logger.info(f'Starting backend bot {self.id}...')

        # 解析配置中可能是用户名的 exclude_chats
        resolved_excludes_in_cfg = set()
        # 遍历原始配置列表
        for chat_id_or_name in self._raw_exclude_chats:
            if isinstance(chat_id_or_name, str) and not chat_id_or_name.isdigit(): # 只处理非数字字符串
                 try:
                      share_id = await self.str_to_chat_id(chat_id_or_name)
                      resolved_excludes_in_cfg.add(share_id)
                      self._logger.info(f"Resolved exclude chat '{chat_id_or_name}' to ID {share_id}")
                 except EntityNotFoundError:
                      self._logger.warning(f"Exclude chat '{chat_id_or_name}' not found, ignoring.")
                 except Exception as e:
                      self._logger.error(f"Error resolving exclude chat '{chat_id_or_name}': {e}")

        # 合并解析结果到 excluded_chats
        self.excluded_chats.update(resolved_excludes_in_cfg)
        self._logger.info(f"Final excluded chats for backend {self.id}: {self.excluded_chats or 'None'}")


        # 加载最新消息 (移到 start 中)
        self._load_newest_messages_on_startup()

        # 检查并记录监控的聊天
        chats_to_remove = set()
        for chat_id in list(self.monitored_chats):
            try:
                if chat_id in self.excluded_chats:
                     self._logger.info(f"Chat {chat_id} is in exclude list, removing from monitoring.")
                     chats_to_remove.add(chat_id)
                     continue

                chat_name = await self.translate_chat_id(chat_id)
                self._logger.info(f'Ready to monitor "{chat_name}" ({chat_id})')
                # 如果上面加载失败，这里可以再试一次？通常不需要
            except EntityNotFoundError:
                 self._logger.warning(f'Monitored chat_id {chat_id} not found, removing from monitor list.')
                 chats_to_remove.add(chat_id)
            except Exception as e:
                self._logger.error(f'Exception checking monitored chat (id={chat_id}): {e}, removing from monitor list.')
                chats_to_remove.add(chat_id)

        if chats_to_remove:
            try:
                 # 从 monitored_chats 集合中移除
                 for chat_id in chats_to_remove:
                      self.monitored_chats.discard(chat_id)
                      if chat_id in self.newest_msg:
                           del self.newest_msg[chat_id]
                 self._logger.info(f'Removed {len(chats_to_remove)} chats from monitoring list.')
                 # 如果需要，可以在这里也从 Whoosh 索引中删除这些 chat_id 的数据
                 # with self._indexer.ix.writer() as writer:
                 #     for chat_id in chats_to_remove:
                 #         writer.delete_by_term('chat_id', str(chat_id))
            except Exception as e:
                 self._logger.error(f"Error removing chats from monitoring list: {e}")

        self._register_hooks()
        self._logger.info(f"Backend bot {self.id} started successfully.")


    def search(self, q: str, in_chats: Optional[List[int]], page_len: int, page_num: int) -> SearchResult: # 添加类型提示
        # 直接调用更新后的 indexer search
        self._logger.debug(f"Backend {self.id} received search: q='{q}', in_chats={in_chats}, page={page_num}")
        try:
            result = self._indexer.search(q, in_chats, page_len, page_num)
            self._logger.debug(f"Search returned {result.total_results} total hits, {len(result.hits)} on page {page_num}.")
            return result
        except Exception as e:
             self._logger.error(f"Error during backend search execution for backend {self.id}: {e}", exc_info=True)
             # 返回一个空 SearchResult 对象
             return SearchResult([], True, 0)


    def rand_msg(self) -> IndexMsg:
        # 调用更新后的 indexer 方法
        try:
             return self._indexer.retrieve_random_document()
        except IndexError: # 处理空索引的情况
             raise IndexError("Index is empty, cannot retrieve random message.")


    def is_empty(self, chat_id=None):
        # 调用更新后的 indexer 方法
        try:
            return self._indexer.is_empty(chat_id)
        except Exception as e:
             self._logger.error(f"Error checking index emptiness for chat {chat_id}: {e}")
             return True # 出错时保守地认为它是空的


    async def download_history(self, chat_id: int, min_id: int, max_id: int, call_back=None):
        share_id = get_share_id(chat_id) # 确保是 share_id
        self._logger.info(f'Downloading history from {share_id} ({min_id=}, {max_id=})')
        # 检查是否在排除列表
        if share_id in self.excluded_chats:
             self._logger.warning(f"Skipping download for excluded chat {share_id}.")
             raise ValueError(f"对话 {share_id} 已被设置为排除，无法下载。")

        # 确保添加到监控列表
        if share_id not in self.monitored_chats:
             self.monitored_chats.add(share_id)
             self._logger.info(f"Added chat {share_id} to monitored list during download.")

        msg_list = []
        downloaded_count = 0
        processed_count = 0

        try:
            # 使用 entity=share_id 可能更健壮
            async for tg_message in self.session.iter_messages(entity=share_id, min_id=min_id, max_id=max_id):
                processed_count += 1
                if not isinstance(tg_message, TgMessage):
                     self._logger.warning(f"Skipping non-message object received in iter_messages for chat {share_id}")
                     continue

                url = f'https://t.me/c/{share_id}/{tg_message.id}'
                sender = await self._get_sender_name(tg_message)
                # 确保 post_time 是 datetime 对象
                post_time = tg_message.date # date 本身就是 datetime
                if not isinstance(post_time, datetime):
                     # 如果不是，尝试转换（理论上不应该发生）
                     try: post_time = datetime.fromtimestamp(post_time.timestamp())
                     except Exception: post_time = datetime.now() # 最终回退

                msg_text = ''
                filename = None

                if tg_message.file and hasattr(tg_message.file, 'name') and tg_message.file.name:
                    filename = tg_message.file.name
                    if tg_message.text:
                        msg_text = escape_content(tg_message.text.strip())
                elif tg_message.text:
                    msg_text = escape_content(tg_message.text.strip())

                if msg_text or filename:
                    try:
                        msg = IndexMsg(
                            content=msg_text or "",
                            url=url,
                            chat_id=share_id,
                            post_time=post_time,
                            sender=sender or "",
                            filename=filename
                        )
                        msg_list.append(msg)
                        downloaded_count += 1
                    except Exception as create_e:
                         self._logger.error(f"Error creating IndexMsg for message {url}: {create_e}")

                # 降低回调频率
                if call_back and processed_count % 100 == 0:
                     try: await call_back(tg_message.id, downloaded_count)
                     except Exception as cb_e: self._logger.warning(f"Error in download history callback: {cb_e}")

        except telethon.errors.rpcerrorlist.ChannelPrivateError as e:
             self._logger.error(f"Permission denied for chat {chat_id} ({share_id}). Cannot download history. Is the session member of the chat? Error: {e}")
             self.monitored_chats.discard(share_id)
             raise EntityNotFoundError(f"无法访问对话 {chat_id}，请确保后端账号是其成员。") from e
        except (telethon.errors.rpcerrorlist.ChatIdInvalidError, telethon.errors.rpcerrorlist.PeerIdInvalidError):
            self._logger.error(f"Chat ID {chat_id} ({share_id}) is invalid or peer cannot be found.")
            self.monitored_chats.discard(share_id)
            raise EntityNotFoundError(f"无效的对话 ID 或无法找到 Peer: {chat_id}")
        except ValueError as e:
             if "Cannot find any entity corresponding to" in str(e):
                  self._logger.error(f"Cannot find entity for chat {chat_id} ({share_id}). Error: {e}")
                  self.monitored_chats.discard(share_id)
                  raise EntityNotFoundError(f"无法找到对话实体: {chat_id}") from e
             else:
                  self._logger.error(f"ValueError during message iteration for chat {chat_id} ({share_id}): {e}", exc_info=True)
                  # 选择不抛出，让部分下载完成
        except Exception as e:
             self._logger.error(f"Error iterating messages for chat {chat_id} ({share_id}): {e}", exc_info=True)
             # 选择不抛出

        self._logger.info(f'Fetching history from {share_id} complete, {downloaded_count} messages qualified for indexing out of {processed_count} processed. Start writing index.')
        if not msg_list:
             self._logger.info(f"No messages to index for chat {share_id}.")
             return # 如果没有消息要索引，直接返回

        writer = self._indexer.ix.writer()
        newest_msg_in_batch = None
        indexed_count_in_batch = 0
        try:
            for msg in msg_list:
                try:
                    self._indexer.add_document(msg, writer)
                    indexed_count_in_batch += 1
                    if newest_msg_in_batch is None or msg.post_time > newest_msg_in_batch.post_time:
                         newest_msg_in_batch = msg
                except Exception as add_e:
                     self._logger.error(f"Error adding document (URL: {msg.url}) to index batch: {add_e}")
            if newest_msg_in_batch:
                 current_chat_id = int(newest_msg_in_batch.chat_id)
                 # 仅当新消息确实比当前缓存新时才更新
                 if current_chat_id not in self.newest_msg or newest_msg_in_batch.post_time > self.newest_msg[current_chat_id].post_time:
                      self.newest_msg[current_chat_id] = newest_msg_in_batch
                      self._logger.debug(f"Updated newest message cache for chat {current_chat_id} to msg URL {newest_msg_in_batch.url}")
            writer.commit()
            self._logger.info(f'Write index commit ok for {indexed_count_in_batch} messages from chat {share_id}')
        except Exception as e:
            writer.cancel()
            self._logger.error(f"Error writing batch index for chat {share_id}: {e}")
            # 考虑是否需要重新抛出异常


    def clear(self, chat_ids: Optional[List[int]] = None):
        if chat_ids is not None:
            # 确认 chat_ids 是 share_id
            share_ids_to_clear = {get_share_id(cid) for cid in chat_ids} # 再次确保
            try:
                with self._indexer.ix.writer() as w:
                    for share_id in share_ids_to_clear:
                        w.delete_by_term('chat_id', str(share_id))
                        self.monitored_chats.discard(share_id)
                        if share_id in self.newest_msg:
                            del self.newest_msg[share_id]
                        self._logger.info(f'Cleared index and stopped monitoring for chat {share_id}')
            except Exception as e:
                 self._logger.error(f"Error clearing index for chats {share_ids_to_clear}: {e}")
        else:
            try:
                self._indexer.clear()
                self.monitored_chats.clear()
                self.newest_msg.clear()
                self._logger.info('Cleared all index data and stopped monitoring all chats.')
            except Exception as e:
                 self._logger.error(f"Error clearing all index data: {e}")


    async def find_chat_id(self, q: str) -> List[int]:
        try:
            # session.find_chat_id 应该返回 share_id 列表
            return await self.session.find_chat_id(q)
        except Exception as e:
             self._logger.error(f"Error finding chat id for query '{q}': {e}")
             return []


    # --- get_index_status 已修复 Bug ---
    async def get_index_status(self, length_limit: int = 4000):
        cur_len = 0
        sb = []
        try:
             total_docs = self._indexer.ix.doc_count()
             sb.append(f'后端 "{self.id}"（session: "{self.session.name}"）总消息数: <b>{total_docs}</b>\n\n')
        except Exception as e:
             self._logger.error(f"Failed to get total document count: {e}")
             sb.append(f'后端 "{self.id}"（session: "{self.session.name}"）总消息数: <b>获取失败</b>\n\n')

        overflow_msg = f'\n\n(部分对话统计信息因长度限制未显示)'

        def append_msg(msg_list: List[str]):
            nonlocal cur_len, sb
            new_len = sum(len(msg) for msg in msg_list)
            if cur_len + new_len > length_limit - len(overflow_msg) - 50:
                return True
            else:
                cur_len += new_len
                sb.extend(msg_list)
                return False

        if self.excluded_chats:
            excluded_list = sorted(list(self.excluded_chats))
            if append_msg([f'{len(excluded_list)} 个对话被禁止索引:\n']):
                 sb.append(overflow_msg)
                 return ''.join(sb)
            for chat_id in excluded_list:
                try: chat_html = await self.format_dialog_html(chat_id)
                except EntityNotFoundError: chat_html = f"未知对话 ({chat_id})"
                except Exception as e: chat_html = f"对话 {chat_id} (获取名称出错: {type(e).__name__})"
                if append_msg([f'- {chat_html}\n']):
                     sb.append(overflow_msg)
                     return ''.join(sb)
            if sb and sb[-1] != '\n': sb.append('\n') # 确保列表后有空行

        monitored_chats_list = sorted(list(self.monitored_chats))
        if append_msg([f'总计 {len(monitored_chats_list)} 个对话被加入了索引:\n']):
             sb.append(overflow_msg)
             return ''.join(sb)

        try:
             with self._indexer.ix.searcher() as searcher:
                 for chat_id in monitored_chats_list:
                     msg_for_chat = []
                     num = 0
                     try:
                         query = Term('chat_id', str(chat_id))
                         num = searcher.doc_count(query=query) # 使用修复后的方式获取数量
                     except Exception as e:
                         self._logger.error(f"Error counting documents for chat {chat_id}: {e}")

                     try:
                         chat_html = await self.format_dialog_html(chat_id)
                         msg_for_chat.append(f'- {chat_html} 共 {num} 条消息\n')
                     except EntityNotFoundError: msg_for_chat.append(f'- 未知对话 (`{chat_id}`) 共 {num} 条消息\n') # 保持 ID 显示
                     except Exception as e: msg_for_chat.append(f'- 对话 `{chat_id}` (获取名称出错: {type(e).__name__}) 共 {num} 条消息\n')

                     if newest_msg := self.newest_msg.get(chat_id):
                         display_content = newest_msg.filename if newest_msg.filename else newest_msg.content
                         if newest_msg.filename: display_content = f"📎 {newest_msg.filename}" + (f" ({brief_content(newest_msg.content)})" if newest_msg.content else "")
                         else: display_content = brief_content(newest_msg.content)
                         escaped_display_content = html.escape(display_content)
                         msg_for_chat.append(f'  最新: <a href="{newest_msg.url}">{escaped_display_content}</a> (@{newest_msg.post_time.strftime("%y-%m-%d %H:%M")})\n')

                     if append_msg(msg_for_chat):
                         sb.append(overflow_msg)
                         break
        except Exception as e:
             self._logger.error(f"Failed to open searcher for getting index status: {e}")
             if append_msg(["\n错误：无法打开索引读取器以获取详细状态。\n"]):
                  sb.append(overflow_msg)

        return ''.join(sb)
    # --- 结束修复 get_index_status ---


    async def translate_chat_id(self, chat_id: int) -> str:
        try:
            chat_id_int = int(chat_id) # 确保是整数
            return await self.session.translate_chat_id(chat_id_int)
        except (telethon.errors.rpcerrorlist.ChannelPrivateError, telethon.errors.rpcerrorlist.ChatIdInvalidError, ValueError):
             raise EntityNotFoundError(f"无法访问或无效的 Chat ID: {chat_id}")
        except EntityNotFoundError:
             self._logger.warning(f"translate_chat_id: Entity not found for {chat_id}")
             raise
        except Exception as e:
             self._logger.error(f"Unexpected error translating chat_id {chat_id}: {e}")
             raise EntityNotFoundError(f"获取 Chat ID {chat_id} 名称时出错")


    async def str_to_chat_id(self, chat: str) -> int:
         try:
             # 尝试直接将输入转为 int (可能是 ID)
             try:
                  raw_id = int(chat)
                  return get_share_id(raw_id)
             except ValueError:
                  # 如果不是数字，则调用 session 的查找方法
                  raw_id = await self.session.str_to_chat_id(chat)
                  return get_share_id(raw_id)
         except EntityNotFoundError:
             self._logger.warning(f"str_to_chat_id: Entity not found for '{chat}'")
             raise
         except Exception as e:
             self._logger.error(f"Error converting '{chat}' to chat_id: {e}")
             raise EntityNotFoundError(f"解析 '{chat}' 为 Chat ID 时出错")


    async def format_dialog_html(self, chat_id: int):
        try:
             chat_id_int = int(chat_id) # 确保是整数
             name = await self.translate_chat_id(chat_id_int)
             escaped_name = html.escape(name)
             return f'<a href="https://t.me/c/{chat_id_int}/1">{escaped_name}</a> (`{chat_id_int}`)'
        except EntityNotFoundError:
             return f'未知对话 (`{chat_id}`)'
        except ValueError:
             return f'无效对话 ID (`{chat_id}`)'
        except Exception as e:
             self._logger.warning(f"Error formatting dialog html for {chat_id}: {e}")
             return f'对话 `{chat_id}` (获取名称出错)'


    def _should_monitor(self, chat_id: int):
        try:
            share_id = get_share_id(chat_id)
            if share_id in self.excluded_chats: return False
            if self._cfg.monitor_all: return True
            else: return share_id in self.monitored_chats
        except Exception as e:
             self._logger.warning(f"Error checking if chat {chat_id} should be monitored: {e}")
             return False

    @staticmethod
    async def _get_sender_name(message: TgMessage) -> str:
        try:
            sender = await message.get_sender()
            if isinstance(sender, User): return format_entity_name(sender)
            elif hasattr(sender, 'title'): return sender.title # 返回频道名
            else: return ''
        except Exception: return ''


    def _register_hooks(self):
        @self.session.on(events.NewMessage())
        async def client_message_handler(event: events.NewMessage.Event):
            if event.chat_id is None or not self._should_monitor(event.chat_id): return
            try:
                share_id = get_share_id(event.chat_id)
                url = f'https://t.me/c/{share_id}/{event.id}'
                sender = await self._get_sender_name(event.message)
                post_time=event.message.date # 直接使用 datetime 对象
                msg_text, filename = '', None
                if event.message.file and hasattr(event.message.file, 'name') and event.message.file.name:
                    filename = event.message.file.name
                    if event.message.text: msg_text = escape_content(event.message.text.strip())
                    self._logger.info(f'New file {url} from "{sender}": "{filename}" Caption: "{brief_content(msg_text)}"')
                elif event.message.text:
                    msg_text = escape_content(event.message.text.strip())
                    if not msg_text.strip() and not filename: return
                    self._logger.info(f'New msg {url} from "{sender}": "{brief_content(msg_text)}"')
                else: return

                msg = IndexMsg(content=msg_text or "", url=url, chat_id=share_id, post_time=post_time, sender=sender or "", filename=filename)
                # 更新最新消息缓存
                if share_id not in self.newest_msg or msg.post_time > self.newest_msg[share_id].post_time:
                     self.newest_msg[share_id] = msg
                     self._logger.debug(f"Updated newest msg cache for chat {share_id} to {url}")
                # 添加到索引
                try: self._indexer.add_document(msg)
                except Exception as e: self._logger.error(f"Error adding document {url} to index: {e}")
            except Exception as e: self._logger.error(f"Error processing new message in chat {event.chat_id}: {e}", exc_info=True)

        @self.session.on(events.MessageEdited())
        async def client_message_update_handler(event: events.MessageEdited.Event):
            if event.chat_id is None or not self._should_monitor(event.chat_id): return
            try:
                share_id = get_share_id(event.chat_id)
                url = f'https://t.me/c/{share_id}/{event.id}'
                new_msg_text = escape_content(event.message.text.strip()) if event.message.text else ''
                self._logger.info(f'Message {url} edited. New content: "{brief_content(new_msg_text)}"')
                try:
                    old_doc_fields = self._indexer.get_document_fields(url=url)
                    if old_doc_fields:
                        # 创建新字段字典用于替换，确保类型正确
                        new_fields = old_doc_fields.copy() # 复制旧字段
                        new_fields['content'] = new_msg_text or "" # 更新内容
                        # 保留或设置其他字段的默认值
                        new_fields.setdefault('chat_id', str(share_id))
                        new_fields.setdefault('post_time', event.message.date) # 使用编辑时间？或保持不变？
                        new_fields.setdefault('sender', old_doc_fields.get('sender', ''))
                        new_fields.setdefault('filename', old_doc_fields.get('filename', None))
                        new_fields.setdefault('url', url) # 确保 URL 正确

                        self._indexer.replace_document(url=url, new_fields=new_fields)
                        self._logger.info(f'Updated message content in index for {url}')
                        # 更新最新消息缓存（如果适用）
                        if share_id in self.newest_msg and self.newest_msg[share_id].url == url:
                             self.newest_msg[share_id].content = new_msg_text # 只更新文本内容
                             self._logger.debug(f"Updated newest msg cache content for {url}")
                    else:
                        self._logger.warning(f'Edited message {url} not found in index. Ignoring edit.')
                except Exception as e: self._logger.error(f'Error updating edited message {url} in index: {e}')
            except Exception as e: self._logger.error(f"Error processing edited message in chat {event.chat_id}: {e}", exc_info=True)

        @self.session.on(events.MessageDeleted())
        async def client_message_delete_handler(event: events.MessageDeleted.Event):
            if not hasattr(event, 'chat_id') or event.chat_id is None or not self._should_monitor(event.chat_id): return
            try:
                share_id = get_share_id(event.chat_id)
                deleted_count = 0
                urls_to_delete = [f'https://t.me/c/{share_id}/{msg_id}' for msg_id in event.deleted_ids]
                try:
                     with self._indexer.ix.writer() as writer:
                          for url in urls_to_delete:
                                # 检查并移除最新消息缓存
                                if share_id in self.newest_msg and self.newest_msg[share_id].url == url:
                                     del self.newest_msg[share_id]
                                     self._logger.info(f"Removed newest message cache for chat {share_id} due to deletion.")
                                try:
                                     writer.delete_by_term('url', url)
                                     deleted_count += 1
                                     self._logger.info(f"Deleted message {url} from index.")
                                except Exception as del_e: self._logger.error(f"Error deleting document with url {url}: {del_e}")
                     if deleted_count > 0: self._logger.info(f'Finished deleting {deleted_count} messages from index for chat {share_id}')
                except Exception as e: self._logger.error(f"Error processing message deletions for chat {share_id}: {e}")
            except Exception as e: self._logger.error(f"Error processing deleted message event in chat {event.chat_id}: {e}", exc_info=True)
