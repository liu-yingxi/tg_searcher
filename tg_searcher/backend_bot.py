# -*- coding: utf-8 -*-
import html
import asyncio # 用于异步操作，如 sleep
from datetime import datetime
from typing import Optional, List, Set, Dict, Any, Union

import telethon.errors.rpcerrorlist
from telethon import events
from telethon.tl.patched import Message as TgMessage
from telethon.tl.types import User
from whoosh.query import Term # 用于构建查询
from whoosh import writing, searching # 导入 Whoosh 相关模块
from whoosh.writing import IndexWriter, LockError # 写入和锁错误

# 项目内导入
from .indexer import Indexer, IndexMsg, SearchResult
from .common import CommonBotConfig, escape_content, get_share_id, get_logger, format_entity_name, brief_content, \
    EntityNotFoundError
from .session import ClientSession

# 日志记录器
logger = get_logger('backend_bot')


class BackendBotConfig:
    """存储 Backend Bot 配置的类"""
    def __init__(self, **kw: Any):
        self.monitor_all: bool = kw.get('monitor_all', False) # 是否监控所有加入的对话
        # 原始排除列表，可能包含用户名或 ID
        self._raw_exclude_chats: List[Union[int, str]] = kw.get('exclude_chats', [])
        # 解析后的排除列表 (仅含 share_id)
        self.excluded_chats: Set[int] = set()
        # 初始化时尝试解析整数 ID
        for chat_id_or_name in self._raw_exclude_chats:
            try: self.excluded_chats.add(get_share_id(int(chat_id_or_name)))
            except (ValueError, TypeError): pass # 非整数留给 start() 解析


class BackendBot:
    """处理索引、下载、后台监控的核心 Bot 类"""
    def __init__(self, common_cfg: CommonBotConfig, cfg: BackendBotConfig,
                 session: ClientSession, clean_db: bool, backend_id: str):
        """初始化 Backend Bot"""
        self.id: str = backend_id # 后端实例 ID
        self.session = session # 底层的 Telethon 客户端会话

        self._logger = get_logger(f'bot-backend:{backend_id}')
        self._cfg = cfg # 后端特定配置
        # 初始化 Indexer
        if clean_db: self._logger.info(f'Index will be cleaned for backend {backend_id}')
        try:
            self._indexer: Indexer = Indexer(common_cfg.index_dir / backend_id, clean_db)
        except ValueError as e: self._logger.critical(f"Indexer initialization failed: {e}"); raise
        except Exception as e: self._logger.critical(f"Unexpected error initializing indexer: {e}", exc_info=True); raise

        # 加载已监控的对话列表
        try:
            self.monitored_chats: Set[int] = self._indexer.list_indexed_chats()
            self._logger.info(f"Loaded {len(self.monitored_chats)} monitored chats from index")
        except Exception as e: self._logger.error(f"Failed to list indexed chats on startup: {e}", exc_info=True); self.monitored_chats = set()

        # 存储最终的排除列表 (包括启动时解析的)
        self.excluded_chats: Set[int] = cfg.excluded_chats
        self._raw_exclude_chats: List[Union[int, str]] = cfg._raw_exclude_chats # 保留原始配置
        # 缓存每个监控对话的最新消息
        self.newest_msg: Dict[int, IndexMsg] = dict()


    def _load_newest_messages_on_startup(self):
         """启动时为每个监控的对话加载最新消息到缓存"""
         if not self.monitored_chats: return
         self._logger.info("Loading newest message for each monitored chat...")
         count = 0
         # 迭代副本以防修改
         for chat_id in list(self.monitored_chats):
              if chat_id in self.excluded_chats: continue # 跳过排除的对话
              try:
                   # 搜索该对话的最新一条消息 (按时间倒序，取第一条)
                   result = self._indexer.search(q_str='*', in_chats=[chat_id], page_len=1, page_num=1, file_filter="all")
                   if result.hits: self.newest_msg[chat_id] = result.hits[0].msg; count += 1
              except Exception as e: self._logger.warning(f"Failed to load newest message for chat {chat_id}: {e}")
         self._logger.info(f"Finished loading newest messages for {count} chats.")


    async def start(self):
        """启动 Backend Bot"""
        self._logger.info(f'Starting backend bot {self.id}...')

        # 解析配置中可能是用户名的 exclude_chats
        resolved_excludes_in_cfg = set()
        for chat_id_or_name in self._raw_exclude_chats:
            # 只处理非数字字符串
            if isinstance(chat_id_or_name, str) and not chat_id_or_name.lstrip('-').isdigit():
                 try:
                      share_id = await self.str_to_chat_id(chat_id_or_name) # 尝试解析
                      resolved_excludes_in_cfg.add(share_id)
                      self._logger.info(f"Resolved exclude chat '{chat_id_or_name}' to ID {share_id}")
                 except EntityNotFoundError: self._logger.warning(f"Exclude chat '{chat_id_or_name}' not found, ignoring.")
                 except Exception as e: self._logger.error(f"Error resolving exclude chat '{chat_id_or_name}': {e}")

        # 更新最终的排除列表
        self.excluded_chats.update(resolved_excludes_in_cfg)
        self._logger.info(f"Final excluded chats for backend {self.id}: {self.excluded_chats or 'None'}")

        # 加载最新消息缓存
        self._load_newest_messages_on_startup()

        # 启动时检查监控的聊天是否仍然可访问，并移除无效的
        chats_to_remove = set()
        for chat_id in list(self.monitored_chats): # 迭代副本
            try:
                if chat_id in self.excluded_chats: # 如果在排除列表，则移除监控
                     self._logger.info(f"Chat {chat_id} is excluded, removing from monitoring.")
                     chats_to_remove.add(chat_id); continue
                # 尝试获取名称以检查可访问性
                chat_name = await self.translate_chat_id(chat_id)
                self._logger.info(f'Monitoring active for "{chat_name}" ({chat_id})')
            except EntityNotFoundError: # 无法找到或访问
                 self._logger.warning(f'Monitored chat_id {chat_id} not found/accessible, removing from monitor list.')
                 chats_to_remove.add(chat_id)
            except Exception as e: # 其他错误
                self._logger.error(f'Exception checking monitored chat {chat_id}: {e}, removing from monitor list.')
                chats_to_remove.add(chat_id)

        # 执行移除
        if chats_to_remove:
            for chat_id in chats_to_remove:
                self.monitored_chats.discard(chat_id)
                self.newest_msg.pop(chat_id, None) # 从缓存中移除
            self._logger.info(f'Removed {len(chats_to_remove)} chats from active monitoring.')

        # 注册 Telethon 事件钩子
        self._register_hooks()
        self._logger.info(f"Backend bot {self.id} started successfully.")


    def search(self, q: str, in_chats: Optional[List[int]], page_len: int, page_num: int, file_filter: str = "all") -> SearchResult:
        """将搜索请求转发给 Indexer"""
        self._logger.debug(f"Backend {self.id} search: q='{brief_content(q)}', chats={in_chats}, page={page_num}, filter={file_filter}")
        try:
            # 调用 Indexer 的 search 方法
            result = self._indexer.search(q, in_chats, page_len, page_num, file_filter=file_filter)
            self._logger.debug(f"Search returned {result.total_results} total hits, {len(result.hits)} on page {page_num}.")
            return result
        except Exception as e:
             self._logger.error(f"Backend search execution failed for {self.id}: {e}", exc_info=True)
             return SearchResult([], True, 0) # 返回空结果


    def rand_msg(self) -> IndexMsg:
        """从 Indexer 获取随机消息"""
        try: return self._indexer.retrieve_random_document()
        except IndexError: raise IndexError("Index is empty, cannot retrieve random message.")
        except Exception as e: self._logger.error(f"Error retrieving random document: {e}", exc_info=True); raise


    def is_empty(self, chat_id: Optional[int] = None) -> bool:
        """检查索引或特定对话是否为空"""
        try: return self._indexer.is_empty(chat_id)
        except Exception as e: self._logger.error(f"Error checking index emptiness for {chat_id}: {e}"); return True # 出错时认为空


    async def download_history(self, chat_id: int, min_id: int, max_id: int, call_back: Optional[callable] = None):
        """
        下载指定对话的历史记录并添加到索引。

        :param chat_id: 原始对话 ID (将被转换为 share_id)。
        :param min_id: 要下载的最小消息 ID (不包括)。
        :param max_id: 要下载的最大消息 ID (不包括)。
        :param call_back: 可选的回调函数，用于报告进度 (接收 cur_id, dl_count)。
        """
        try: share_id = get_share_id(chat_id) # 转换为 share_id
        except Exception as e: logger.error(f"Invalid chat_id format for download: {chat_id}, error: {e}"); raise EntityNotFoundError(f"无效的对话 ID 格式: {chat_id}")

        logger.info(f'Downloading history for {share_id} (raw_id={chat_id}, min={min_id}, max={max_id})')
        # 跳过已排除的对话
        if share_id in self.excluded_chats: logger.warning(f"Skipping download for excluded chat {share_id}."); raise ValueError(f"对话 {share_id} 已被排除，无法下载。")
        # 如果尚未监控，则添加到监控列表
        if share_id not in self.monitored_chats: self.monitored_chats.add(share_id); logger.info(f"Added chat {share_id} to monitored list.")

        msg_list: List[IndexMsg] = [] # 存储待索引的消息
        downloaded_count: int = 0 # 实际准备索引的消息数
        processed_count: int = 0 # Telethon 返回的总消息数

        try:
            # 异步迭代消息历史
            async for tg_message in self.session.iter_messages(entity=share_id, min_id=min_id, max_id=max_id, limit=None):
                processed_count += 1
                if not isinstance(tg_message, TgMessage): continue # 跳过非消息类型

                # 构造消息 URL
                url = f'https://t.me/c/{share_id}/{tg_message.id}'
                # 获取发送者名称
                sender = await self._get_sender_name(tg_message)
                # 获取发送时间 (确保是 datetime)
                post_time = tg_message.date
                if not isinstance(post_time, datetime): post_time = datetime.now()

                # 提取文本和文件名
                msg_text, filename = '', None
                if tg_message.file and hasattr(tg_message.file, 'name') and tg_message.file.name:
                    filename = tg_message.file.name
                    if tg_message.text: msg_text = escape_content(tg_message.text.strip()) # 文件可能附带标题
                elif tg_message.text:
                    msg_text = escape_content(tg_message.text.strip())

                # 只有包含文本或文件名时才创建 IndexMsg
                if msg_text or filename:
                    try: msg = IndexMsg(content=msg_text or "", url=url, chat_id=share_id, post_time=post_time, sender=sender or "", filename=filename); msg_list.append(msg); downloaded_count += 1
                    except Exception as create_e: logger.error(f"Error creating IndexMsg for {url}: {create_e}")

                # 定期调用回调函数报告进度
                if call_back and processed_count % 100 == 0:
                     try: await call_back(tg_message.id, downloaded_count)
                     except Exception as cb_e: logger.warning(f"Error in download callback: {cb_e}")
                # 定期释放事件循环，防止长时间阻塞
                if processed_count % 500 == 0: await asyncio.sleep(0.01)

        # 处理 Telethon 可能抛出的错误
        except telethon.errors.rpcerrorlist.ChannelPrivateError as e: logger.error(f"Permission denied for chat {share_id}. Error: {e}"); self.monitored_chats.discard(share_id); raise EntityNotFoundError(f"无法访问对话 {chat_id}，请确保后端账号是其成员。") from e
        except (telethon.errors.rpcerrorlist.ChatIdInvalidError, telethon.errors.rpcerrorlist.PeerIdInvalidError): logger.error(f"Chat ID {share_id} (raw: {chat_id}) invalid."); self.monitored_chats.discard(share_id); raise EntityNotFoundError(f"无效对话 ID 或无法找到 Peer: {chat_id}")
        except ValueError as e: # 处理无法找到实体错误
             if "Cannot find any entity corresponding to" in str(e) or "Could not find the input entity for" in str(e): logger.error(f"Cannot find entity for chat {share_id} (raw: {chat_id}). Error: {e}"); self.monitored_chats.discard(share_id); raise EntityNotFoundError(f"无法找到对话实体: {chat_id}") from e
             else: logger.error(f"ValueError iterating messages for {share_id}: {e}", exc_info=True); raise
        except Exception as e: logger.error(f"Error iterating messages for {share_id}: {e}", exc_info=True); raise RuntimeError(f"下载对话 {share_id} 时发生未知错误") # 抛出通用运行时错误

        logger.info(f'History fetch complete for {share_id}: {downloaded_count} messages to index out of {processed_count} processed.')
        if not msg_list: return # 如果没有可索引的消息，直接返回

        # --- 批量写入索引 ---
        writer: Optional[IndexWriter] = None
        newest_msg_in_batch: Optional[IndexMsg] = None
        indexed_count_in_batch: int = 0
        try:
            writer = self._indexer.ix.writer() # 获取 writer
            for i, msg in enumerate(msg_list):
                try:
                    self._indexer.add_document(msg, writer); indexed_count_in_batch += 1
                    # 记录本批次中最新的消息
                    if newest_msg_in_batch is None or msg.post_time > newest_msg_in_batch.post_time: newest_msg_in_batch = msg
                    # 批量写入时也避免阻塞
                    if i % 1000 == 0: await asyncio.sleep(0.01)
                except Exception as add_e: logger.error(f"Error adding document {msg.url} to batch writer: {add_e}")
            writer.commit() # 提交写入
            logger.info(f'Write index commit ok for {indexed_count_in_batch} messages from chat {share_id}')
            # 更新该对话的最新消息缓存
            if newest_msg_in_batch:
                 current_chat_id = int(newest_msg_in_batch.chat_id)
                 if current_chat_id not in self.newest_msg or newest_msg_in_batch.post_time > self.newest_msg[current_chat_id].post_time:
                      self.newest_msg[current_chat_id] = newest_msg_in_batch
                      logger.debug(f"Updated newest msg cache for {current_chat_id} to {newest_msg_in_batch.url}")
        except writing.LockError: logger.error("Index is locked during batch write. Downloaded messages are lost."); raise RuntimeError("Index is locked, cannot write downloaded messages.")
        except Exception as e: logger.error(f"Error writing batch index for {share_id}: {e}", exc_info=True); raise RuntimeError(f"写入索引时出错 for {share_id}")


    def clear(self, chat_ids: Optional[List[int]] = None):
        """
        清除索引数据。

        :param chat_ids: 可选，要清除的 chat_id 列表。如果为 None，则清除所有索引。
        """
        if chat_ids is not None:
            # 清除指定对话的数据
            share_ids_to_clear = {get_share_id(cid) for cid in chat_ids} # 转换为 share_id 集合
            try:
                with self._indexer.ix.writer() as w:
                    for share_id in share_ids_to_clear:
                        # 按 chat_id 删除文档
                        deleted_count = w.delete_by_term('chat_id', str(share_id))
                        # 停止监控并清理缓存
                        self.monitored_chats.discard(share_id)
                        self.newest_msg.pop(share_id, None) # 从缓存移除
                        logger.info(f'Cleared {deleted_count} docs and stopped monitoring chat {share_id}')
            except Exception as e: logger.error(f"Error clearing index for chats {share_ids_to_clear}: {e}")
        else:
            # 清除所有索引数据
            try:
                self._indexer.clear(); self.monitored_chats.clear(); self.newest_msg.clear()
                logger.info('Cleared all index data and stopped monitoring.')
            except Exception as e: logger.error(f"Error clearing all index data: {e}")


    async def find_chat_id(self, q: str) -> List[int]:
        """使用会话查找匹配关键词的对话 ID"""
        try: return await self.session.find_chat_id(q)
        except Exception as e: logger.error(f"Error finding chat id for '{q}': {e}"); return []


    async def get_index_status(self, length_limit: int = 4000) -> str:
        """获取后端索引状态的文本描述 (修正计数和错误处理逻辑)"""
        cur_len = 0; sb = []; searcher = None # 初始化 searcher
        try: total_docs = self._indexer.ix.doc_count()
        except Exception as e: total_docs = -1; logger.error(f"Failed get total doc count: {e}")
        sb.append(f'后端 "{self.id}" (会话: "{self.session.name}") 总消息: <b>{total_docs if total_docs >= 0 else "[获取失败]"}</b>\n\n')

        overflow_msg = f'\n\n(部分信息因长度限制未显示)'
        def append_msg(msg_list: List[str]) -> bool: # 辅助函数，检查长度限制
            nonlocal cur_len; new_len = sum(len(msg) for msg in msg_list)
            if cur_len + new_len > length_limit - len(overflow_msg) - 50: return True
            cur_len += new_len; sb.extend(msg_list); return False

        # 显示排除列表
        if self.excluded_chats:
            excluded_list = sorted(list(self.excluded_chats))
            if append_msg([f'{len(excluded_list)} 个对话被禁止索引:\n']): sb.append(overflow_msg); return ''.join(sb)
            for chat_id in excluded_list:
                try: chat_html = await self.format_dialog_html(chat_id)
                except Exception: chat_html = f"对话 `{chat_id}` (获取名称出错)"
                if append_msg([f'- {chat_html}\n']): sb.append(overflow_msg); return ''.join(sb)
            if sb and sb[-1] != '\n\n': sb.append('\n')

        # 显示监控列表和计数
        monitored_chats_list = sorted(list(self.monitored_chats))
        if append_msg([f'总计 {len(monitored_chats_list)} 个对话被加入了索引:\n']): sb.append(overflow_msg); return ''.join(sb)

        # --- 获取每个监控对话的详细信息 ---
        try:
             searcher = self._indexer.ix.searcher() # 在循环外打开 searcher
             for chat_id in monitored_chats_list:
                 msg_for_chat = []
                 num = -1 # 初始化计数为错误状态
                 chat_id_str = str(chat_id)
                 # 尝试计数
                 try:
                     query = Term('chat_id', chat_id_str)
                     num = searcher.doc_count(query=query) # 使用 doc_count
                 except searching.SearchError as search_e: logger.error(f"Whoosh SearchError counting docs for chat {chat_id_str}: {search_e}", exc_info=True)
                 except Exception as e: logger.error(f"Unexpected error counting docs for chat {chat_id_str}: {e}", exc_info=True)
                 # 尝试获取名称
                 try: chat_html = await self.format_dialog_html(chat_id)
                 except Exception as name_e: logger.error(f"Error getting name for chat {chat_id}: {name_e}"); chat_html = f"对话 `{chat_id}` (获取名称出错)"
                 # 组合信息
                 count_str = "[计数失败]" if num < 0 else str(num)
                 msg_for_chat.append(f'- {chat_html} 共 {count_str} 条消息\n')
                 # 添加最新消息信息
                 if newest_msg := self.newest_msg.get(chat_id):
                     display = f"📎 {newest_msg.filename}" if newest_msg.filename else brief_content(newest_msg.content)
                     if newest_msg.filename and newest_msg.content: display += f" ({brief_content(newest_msg.content)})"
                     esc_display = html.escape(display or "(空)")
                     msg_for_chat.append(f'  最新: <a href="{newest_msg.url}">{esc_display}</a> (@{newest_msg.post_time.strftime("%y-%m-%d %H:%M")})\n')
                 # 检查长度并添加
                 if append_msg(msg_for_chat): sb.append(overflow_msg); break
        except writing.LockError: # 处理打开 searcher 时的锁错误
             logger.error(f"Index locked, failed to open searcher for status.")
             if append_msg(["\n错误：索引被锁定，无法获取详细状态。\n"]): sb.append(overflow_msg)
        except Exception as e: # 处理打开 searcher 或其他外部错误
             logger.error(f"Failed to open searcher or process chats for status: {e}", exc_info=True)
             if append_msg(["\n错误：无法获取详细状态。\n"]): sb.append(overflow_msg)
        finally:
            if searcher: searcher.close() # 确保 searcher 被关闭
        # --- 结束详细信息获取 ---
        return ''.join(sb)


    async def translate_chat_id(self, chat_id: int) -> str:
        """使用会话将 Chat ID 翻译为名称"""
        try: return await self.session.translate_chat_id(int(chat_id))
        except (telethon.errors.rpcerrorlist.ChannelPrivateError, telethon.errors.rpcerrorlist.ChatIdInvalidError, ValueError, TypeError): raise EntityNotFoundError(f"无法访问或无效 Chat ID: {chat_id}")
        except EntityNotFoundError: logger.warning(f"Entity not found for {chat_id}"); raise
        except Exception as e: logger.error(f"Error translating chat_id {chat_id}: {e}"); raise EntityNotFoundError(f"获取对话 {chat_id} 名称时出错") from e


    async def str_to_chat_id(self, chat: str) -> int:
        """将字符串（用户名、链接或 ID）转换为 share_id"""
        try:
            try: return get_share_id(int(chat)) # 尝试直接转换整数 ID
            except ValueError: return get_share_id(await self.session.str_to_chat_id(chat)) # 使用会话解析
        except EntityNotFoundError: logger.warning(f"Entity not found for '{chat}'"); raise
        except Exception as e: logger.error(f"Error converting '{chat}' to chat_id: {e}"); raise EntityNotFoundError(f"解析 '{chat}' 时出错") from e


    async def format_dialog_html(self, chat_id: int) -> str:
        """格式化对话的 HTML 链接和名称"""
        try: name = await self.translate_chat_id(int(chat_id)); esc_name = html.escape(name); return f'<a href="https://t.me/c/{chat_id}/1">{esc_name}</a> (`{chat_id}`)'
        except EntityNotFoundError: return f'未知对话 (`{chat_id}`)' # 实体未找到
        except ValueError: return f'无效对话 ID (`{chat_id}`)' # ID 格式无效
        except Exception as e: logger.warning(f"Error formatting html for {chat_id}: {e}"); return f'对话 `{chat_id}` (获取名称出错)'


    def _should_monitor(self, chat_id: int) -> bool:
        """判断是否应该监控此对话的消息"""
        try:
            share_id = get_share_id(chat_id)
            if share_id in self.excluded_chats: return False # 在排除列表则不监控
            # 如果配置了 monitor_all，或者该 chat_id 在监控列表里，则监控
            return self._cfg.monitor_all or (share_id in self.monitored_chats)
        except Exception: return False # 出错则不监控


    @staticmethod
    async def _get_sender_name(message: TgMessage) -> str:
        """获取消息发送者的名称（用户或频道标题）"""
        try:
            sender = await message.get_sender()
            if isinstance(sender, User): return format_entity_name(sender) # 用户则格式化名称
            else: return getattr(sender, 'title', '') # 频道/群组则取标题
        except Exception: return '' # 出错返回空


    def _register_hooks(self):
        """注册 Telethon 事件钩子，用于实时接收和处理消息"""

        # 处理新消息
        @self.session.on(events.NewMessage())
        async def client_message_handler(event: events.NewMessage.Event):
            # 检查是否需要监控此对话
            if not hasattr(event, 'chat_id') or event.chat_id is None or not self._should_monitor(event.chat_id): return
            try:
                share_id = get_share_id(event.chat_id); url = f'https://t.me/c/{share_id}/{event.id}'
                sender = await self._get_sender_name(event.message); post_time = event.message.date
                if not isinstance(post_time, datetime): post_time = datetime.now()

                # 提取文本和文件名
                msg_text, filename = '', None
                if event.message.file and hasattr(event.message.file, 'name') and event.message.file.name:
                    filename = event.message.file.name
                    if event.message.text: msg_text = escape_content(event.message.text.strip())
                    logger.info(f'New file {url} from "{sender}": "{filename}" Cap:"{brief_content(msg_text)}"')
                elif event.message.text:
                    msg_text = escape_content(event.message.text.strip())
                    if not msg_text.strip(): return # 忽略纯空白消息
                    logger.info(f'New msg {url} from "{sender}": "{brief_content(msg_text)}"')
                else: return # 忽略既无文本也无文件的消息

                # 创建 IndexMsg 并添加到索引
                msg = IndexMsg(content=msg_text or "", url=url, chat_id=share_id, post_time=post_time, sender=sender or "", filename=filename)
                # 更新最新消息缓存
                if share_id not in self.newest_msg or msg.post_time > self.newest_msg[share_id].post_time: self.newest_msg[share_id] = msg; logger.debug(f"Updated newest cache {share_id} to {url}")
                try: self._indexer.add_document(msg) # 添加到索引
                except Exception as e: logger.error(f"Error adding doc {url} to index: {e}")
            except Exception as e: logger.error(f"Error processing new message in chat {event.chat_id}: {e}", exc_info=True)

        # 处理消息编辑
        @self.session.on(events.MessageEdited())
        async def client_message_update_handler(event: events.MessageEdited.Event):
            if not hasattr(event, 'chat_id') or event.chat_id is None or not self._should_monitor(event.chat_id): return
            try:
                share_id = get_share_id(event.chat_id); url = f'https://t.me/c/{share_id}/{event.id}'
                new_msg_text = escape_content(event.message.text.strip()) if event.message.text else ''
                logger.info(f'Msg {url} edited. Checking for update...')
                try:
                    old_fields = self._indexer.get_document_fields(url=url)
                    if old_fields:
                        # 如果内容未变，则跳过
                        if old_fields.get('content') == new_msg_text: logger.debug(f"Edit event {url} same content, skipping."); return
                        # 准备更新的字段
                        new_fields = old_fields.copy(); new_fields['content'] = new_msg_text or ""
                        # 确保其他字段存在 (尽量使用旧值)
                        new_fields.setdefault('chat_id', str(share_id))
                        new_fields.setdefault('post_time', old_fields.get('post_time', event.message.date or datetime.now()))
                        new_fields.setdefault('sender', old_fields.get('sender', await self._get_sender_name(event.message) or ''))
                        new_fields.setdefault('filename', old_fields.get('filename', None))
                        new_fields.setdefault('url', url)
                        new_fields['has_file'] = 1 if new_fields.get('filename') else 0 # 重新计算
                        # 替换文档
                        self._indexer.replace_document(url=url, new_fields=new_fields)
                        logger.info(f'Updated msg content in index for {url}')
                        # 如果是最新消息，更新缓存
                        if share_id in self.newest_msg and self.newest_msg[share_id].url == url:
                             try: self.newest_msg[share_id] = IndexMsg(**new_fields); logger.debug(f"Updated newest cache content for {url}")
                             except Exception as cache_e: logger.error(f"Error reconstructing IndexMsg for cache update {url}: {cache_e}")
                    else:
                         # 如果编辑的消息不在索引中，则作为新消息添加
                         logger.warning(f'Edited msg {url} not found in index. Adding as new message.')
                         sender = await self._get_sender_name(event.message); post_time = event.message.date or datetime.now(); filename = None # 假设编辑不改变文件
                         if not isinstance(post_time, datetime): post_time = datetime.now()
                         msg = IndexMsg(content=new_msg_text or "", url=url, chat_id=share_id, post_time=post_time, sender=sender or "", filename=filename)
                         self._indexer.add_document(msg)
                         # 更新最新消息缓存
                         if share_id not in self.newest_msg or msg.post_time > self.newest_msg[share_id].post_time: self.newest_msg[share_id] = msg; logger.debug(f"Added edited msg {url} as newest cache for {share_id}")
                except Exception as e: logger.error(f'Error updating edited msg {url} in index: {e}', exc_info=True)
            except Exception as e: logger.error(f"Error processing edited message in chat {event.chat_id}: {e}", exc_info=True)

        # 处理消息删除
        @self.session.on(events.MessageDeleted())
        async def client_message_delete_handler(event: events.MessageDeleted.Event):
            if not hasattr(event, 'chat_id') or event.chat_id is None or not self._should_monitor(event.chat_id):
                 logger.debug(f"Ignoring deletion event without valid/monitored chat_id. Deleted IDs: {event.deleted_ids}")
                 return
            try:
                share_id = get_share_id(event.chat_id)
                deleted_count = 0
                urls = [f'https://t.me/c/{share_id}/{mid}' for mid in event.deleted_ids]
                try:
                     # 批量删除
                     with self._indexer.ix.writer() as writer:
                          for url in urls:
                               # 如果删除的是最新消息，清理缓存
                               if share_id in self.newest_msg and self.newest_msg[share_id].url == url:
                                    del self.newest_msg[share_id]
                                    logger.info(f"Removed newest cache for {share_id} due to deletion of {url}.")
                               try:
                                    count = writer.delete_by_term('url', url) # 按 URL 删除
                                    if count > 0: deleted_count += count; logger.info(f"Deleted msg {url} from index.")
                               except Exception as del_e: logger.error(f"Error deleting doc {url} from index: {del_e}")
                     if deleted_count > 0: logger.info(f'Finished deleting {deleted_count} msgs from index for chat {share_id}')
                except Exception as e: logger.error(f"Error processing deletions batch for {share_id}: {e}")
            except Exception as e: logger.error(f"Error processing deleted event in chat {event.chat_id}: {e}", exc_info=True)
