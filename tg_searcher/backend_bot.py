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
from whoosh import writing, searching # 导入 Whoosh 相关模块 (确保 searching 被导入)
from whoosh.writing import IndexWriter, LockError # 写入和锁错误

# 项目内导入
from .indexer import Indexer, IndexMsg, SearchResult
from .common import CommonBotConfig, escape_content, get_share_id, get_logger, format_entity_name, brief_content, \
    EntityNotFoundError
from .session import ClientSession

# 日志记录器
# 注意：这里的 get_logger 返回的是已配置的 logger 实例
logger = get_logger('backend_bot') # logger 在模块级别定义，所有实例共享


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

        # 使用特定于此后端实例的 logger
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
            # 从索引中加载已监控的对话 ID
            self.monitored_chats: Set[int] = self._indexer.list_indexed_chats()
            self._logger.info(f"Loaded {len(self.monitored_chats)} monitored chats from index")
        except Exception as e:
            # 如果加载失败，初始化为空集合并记录错误
            self._logger.error(f"Failed to list indexed chats on startup: {e}", exc_info=True)
            self.monitored_chats = set()

        # 存储最终的排除列表 (包括启动时解析的)
        self.excluded_chats: Set[int] = cfg.excluded_chats
        self._raw_exclude_chats: List[Union[int, str]] = cfg._raw_exclude_chats # 保留原始配置
        # 缓存每个监控对话的最新消息 {chat_id: IndexMsg}
        self.newest_msg: Dict[int, IndexMsg] = dict()
        # 跟踪后台任务，例如下载历史记录
        self._background_tasks: Set[asyncio.Task] = set()


    def _load_newest_messages_on_startup(self):
         """启动时为每个监控的对话加载最新消息到缓存"""
         if not self.monitored_chats:
             self._logger.info("No monitored chats found in index, skipping loading newest messages.")
             return
         self._logger.info("Loading newest message for each monitored chat...")
         count = 0
         # 迭代 monitored_chats 的副本，以防在加载期间列表被修改
         for chat_id in list(self.monitored_chats):
              # 跳过已在排除列表中的对话
              if chat_id in self.excluded_chats:
                  self._logger.debug(f"Skipping loading newest message for excluded chat {chat_id}.")
                  continue
              try:
                   # 搜索该对话的最新一条消息
                   # 使用 q_str='*' 匹配所有文档，按时间倒序，只取第一条
                   result = self._indexer.search(q_str='*', in_chats=[chat_id], page_len=1, page_num=1, file_filter="all")
                   if result.hits:
                       self.newest_msg[chat_id] = result.hits[0].msg
                       count += 1
                       self._logger.debug(f"Loaded newest message for chat {chat_id}: {result.hits[0].msg.url}")
                   else:
                       self._logger.debug(f"No messages found in index for chat {chat_id} to load as newest.")
              except Exception as e:
                  # 记录加载特定对话最新消息时的错误，但不中断整个过程
                  self._logger.warning(f"Failed to load newest message for chat {chat_id}: {e}")
         self._logger.info(f"Finished loading newest messages for {count} monitored (and not excluded) chats.")


    async def start(self):
        """启动 Backend Bot"""
        self._logger.info(f'Starting backend bot {self.id}...')

        # 解析配置中可能是用户名的 exclude_chats
        resolved_excludes_in_cfg = set()
        for chat_id_or_name in self._raw_exclude_chats:
            # 只处理非数字字符串，尝试将其解析为 share_id
            if isinstance(chat_id_or_name, str) and not chat_id_or_name.lstrip('-').isdigit():
                 try:
                      share_id = await self.str_to_chat_id(chat_id_or_name) # 尝试解析
                      resolved_excludes_in_cfg.add(share_id)
                      self._logger.info(f"Resolved exclude chat '{chat_id_or_name}' to ID {share_id}")
                 except EntityNotFoundError:
                     # 如果找不到实体，记录警告并忽略
                     self._logger.warning(f"Exclude chat '{chat_id_or_name}' not found, ignoring.")
                 except Exception as e:
                     # 记录解析过程中的其他错误
                     self._logger.error(f"Error resolving exclude chat '{chat_id_or_name}': {e}")

        # 更新最终的排除列表，合并来自配置的解析结果
        self.excluded_chats.update(resolved_excludes_in_cfg)
        self._logger.info(f"Final excluded chats for backend {self.id}: {self.excluded_chats or 'None'}")

        # 加载最新消息缓存 (在处理排除列表和验证监控列表之前)
        self._load_newest_messages_on_startup()

        # 启动时检查监控的聊天是否仍然可访问，并移除无效的或被排除的
        chats_to_remove = set()
        # 迭代 monitored_chats 的副本进行检查
        for chat_id in list(self.monitored_chats):
            try:
                # 如果对话在最终的排除列表中，将其标记为移除
                if chat_id in self.excluded_chats:
                     self._logger.info(f"Chat {chat_id} is excluded, removing from monitoring.")
                     chats_to_remove.add(chat_id)
                     continue
                # 尝试获取对话名称以检查可访问性
                chat_name = await self.translate_chat_id(chat_id)
                self._logger.info(f'Monitoring active for "{chat_name}" ({chat_id})')
            except EntityNotFoundError:
                 # 如果找不到实体或无权访问，标记为移除
                 self._logger.warning(f'Monitored chat_id {chat_id} not found/accessible, removing from monitor list.')
                 chats_to_remove.add(chat_id)
            except Exception as e:
                # 处理检查过程中的其他异常，同样标记为移除
                self._logger.error(f'Exception checking monitored chat {chat_id}: {e}, removing from monitor list.')
                chats_to_remove.add(chat_id)

        # 执行移除操作
        if chats_to_remove:
            for chat_id in chats_to_remove:
                self.monitored_chats.discard(chat_id) # 从监控集合中移除
                self.newest_msg.pop(chat_id, None) # 从最新消息缓存中移除
            self._logger.info(f'Removed {len(chats_to_remove)} chats from active monitoring.')

        # 注册 Telethon 事件钩子以接收实时消息
        self._register_hooks()
        self._logger.info(f"Backend bot {self.id} started successfully.")


    def search(self, q: str, in_chats: Optional[List[int]], page_len: int, page_num: int, file_filter: str = "all") -> SearchResult:
        """将搜索请求转发给 Indexer"""
        # 记录搜索请求的基本信息
        self._logger.debug(f"Backend {self.id} search: q='{brief_content(q)}', chats={in_chats}, page={page_num}, filter={file_filter}")
        try:
            # 调用 Indexer 的 search 方法执行搜索
            result = self._indexer.search(q, in_chats, page_len, page_num, file_filter=file_filter)
            # 记录搜索结果的基本信息
            self._logger.debug(f"Search returned {result.total_results} total hits, {len(result.hits)} on page {page_num}.")
            return result
        except Exception as e:
             # 记录后端搜索执行失败的错误
             self._logger.error(f"Backend search execution failed for {self.id}: {e}", exc_info=True)
             # 返回一个空的 SearchResult 对象，表示搜索失败
             return SearchResult([], True, 0)


    def rand_msg(self) -> IndexMsg:
        """从 Indexer 获取随机消息"""
        try:
            # 调用 Indexer 的方法来检索随机文档
            return self._indexer.retrieve_random_document()
        except IndexError:
            # 如果索引为空，则抛出特定的 IndexError
            self._logger.warning("Cannot retrieve random message: Index is empty.")
            raise IndexError("Index is empty, cannot retrieve random message.")
        except Exception as e:
            # 记录检索随机文档时发生的其他错误
            self._logger.error(f"Error retrieving random document: {e}", exc_info=True)
            # 重新抛出异常，让调用者处理
            raise


    def is_empty(self, chat_id: Optional[int] = None) -> bool:
        """检查索引或特定对话是否为空"""
        try:
            # 调用 Indexer 的 is_empty 方法
            return self._indexer.is_empty(chat_id)
        except Exception as e:
            # 记录检查索引是否为空时发生的错误
            self._logger.error(f"Error checking index emptiness for {chat_id}: {e}")
            # 在出错的情况下，保守地返回 True（认为索引为空）
            return True


    # *************************************************************************
    # * FUNCTION MODIFIED BELOW                                               *
    # *************************************************************************
    async def download_history(self, chat_id: int, min_id: int, max_id: int, call_back: Optional[callable] = None):
        """
        下载指定对话的历史记录并添加到索引。

        :param chat_id: 原始对话 ID (将被转换为 share_id)。
        :param min_id: 要下载的最小消息 ID (不包括)。
        :param max_id: 要下载的最大消息 ID (0 表示无上限，会获取比 min_id 更新的所有消息)。
        :param call_back: 可选的回调函数，用于报告进度 (接收 cur_id, dl_count)。
        """
        task_name = f"DownloadHistory-{chat_id}"
        self._logger.info(f"Starting task: {task_name} (min={min_id}, max={max_id})")
        share_id = -1 # 初始化为无效值
        try:
            share_id = get_share_id(chat_id) # 转换为 share_id
        except Exception as e:
            self._logger.error(f"Invalid chat_id format for download: {chat_id}, error: {e}")
            raise EntityNotFoundError(f"无效的对话 ID 格式: {chat_id}") # 抛出特定的错误类型

        task_name = f"DownloadHistory-{share_id}" # 更新任务名
        self._logger.info(f'Downloading history for {share_id} (raw_id={chat_id}, min={min_id}, max={max_id})')
        # 检查对话是否在排除列表中
        if share_id in self.excluded_chats:
            self._logger.warning(f"Skipping download for excluded chat {share_id}.")
            raise ValueError(f"对话 {share_id} 已被排除，无法下载。") # 抛出 ValueError 表示操作不允许

        # --- 监控反馈逻辑 ---
        is_newly_monitored = False
        if share_id not in self.monitored_chats:
            is_newly_monitored = True
            self.monitored_chats.add(share_id)
            # 添加到监控列表的日志反馈
            self._logger.info(f"[Monitoring] Added chat {share_id} to monitored list during download request.")
            # **建议**: 前端在调用此函数成功后，可以向用户发送一条明确的确认消息，例如:
            # await event.reply(f"✅ 对话 {chat_name} ({share_id}) 已成功添加到监控列表。")

        msg_list: List[IndexMsg] = [] # 存储从 Telegram 获取并准备索引的消息
        downloaded_count: int = 0 # 实际构造了 IndexMsg 的消息数量
        processed_count: int = 0 # Telethon `iter_messages` 返回的总项目数
        newest_msg_in_batch: Optional[IndexMsg] = None # 记录此批次中最新的消息
        indexed_count_in_batch: int = 0

        try:
            # 使用 Telethon 异步迭代指定对话的消息历史
            async for tg_message in self.session.iter_messages(entity=share_id, min_id=min_id, max_id=max_id, limit=None, reverse=True): # reverse=True 确保从旧到新处理，便于确定 newest_msg
                processed_count += 1
                if not isinstance(tg_message, TgMessage): continue

                url = f'https://t.me/c/{share_id}/{tg_message.id}'
                sender = await self._get_sender_name(tg_message)
                post_time = tg_message.date
                if not isinstance(post_time, datetime):
                    self._logger.warning(f"Message {url} has invalid date type {type(post_time)}, using current time.")
                    post_time = datetime.now()

                msg_text, filename = '', None
                if tg_message.file and hasattr(tg_message.file, 'name') and tg_message.file.name:
                    filename = tg_message.file.name
                    if tg_message.text: msg_text = escape_content(tg_message.text.strip())
                elif tg_message.text:
                    msg_text = escape_content(tg_message.text.strip())

                if msg_text or filename:
                    try:
                        msg = IndexMsg(content=msg_text or "", url=url, chat_id=share_id, post_time=post_time, sender=sender or "", filename=filename)
                        msg_list.append(msg)
                        downloaded_count += 1
                        # 更新此批次中遇到的最新消息
                        newest_msg_in_batch = msg
                    except Exception as create_e:
                        self._logger.error(f"Error creating IndexMsg for {url}: {create_e}")

                if call_back and processed_count % 100 == 0:
                     try: await call_back(tg_message.id, downloaded_count)
                     except Exception as cb_e: self._logger.warning(f"Error in download callback: {cb_e}")
                if processed_count % 500 == 0:
                    await asyncio.sleep(0.01) # 释放事件循环

            # --- 处理下载错误 ---
            # ... (错误处理部分保持不变) ...
        except telethon.errors.rpcerrorlist.ChannelPrivateError as e:
            self._logger.error(f"Permission denied for chat {share_id}. Is the backend account a member? Error: {e}")
            self.monitored_chats.discard(share_id) # 移除无法访问的对话
            if is_newly_monitored: self._logger.info(f"[Monitoring] Removed newly added chat {share_id} due to access error.")
            raise EntityNotFoundError(f"无法访问对话 {chat_id} ({share_id})，请确保后端账号是其成员。") from e
        except (telethon.errors.rpcerrorlist.ChatIdInvalidError, telethon.errors.rpcerrorlist.PeerIdInvalidError):
            self._logger.error(f"Chat ID {share_id} (raw: {chat_id}) invalid or peer not found.")
            self.monitored_chats.discard(share_id)
            if is_newly_monitored: self._logger.info(f"[Monitoring] Removed newly added chat {share_id} due to invalid ID.")
            raise EntityNotFoundError(f"无效对话 ID 或无法找到 Peer: {chat_id} ({share_id})")
        except ValueError as e:
             if "Cannot find any entity corresponding to" in str(e) or "Could not find the input entity for" in str(e):
                 self._logger.error(f"Cannot find entity for chat {share_id} (raw: {chat_id}). Error: {e}")
                 self.monitored_chats.discard(share_id)
                 if is_newly_monitored: self._logger.info(f"[Monitoring] Removed newly added chat {share_id} due to entity not found.")
                 raise EntityNotFoundError(f"无法找到对话实体: {chat_id} ({share_id})") from e
             else:
                 self._logger.error(f"ValueError iterating messages for {share_id}: {e}", exc_info=True)
                 raise # 重新抛出
        except Exception as e:
            self._logger.error(f"Error iterating messages for {share_id}: {e}", exc_info=True)
            # 如果在下载过程中出错，也考虑移除（如果刚添加的话）
            if is_newly_monitored:
                self.monitored_chats.discard(share_id)
                self._logger.info(f"[Monitoring] Removed newly added chat {share_id} due to download error.")
            raise RuntimeError(f"下载对话 {share_id} 时发生未知错误")

        # --- 批量写入索引 ---
        self._logger.info(f'History fetch complete for {share_id}: {downloaded_count} messages to index out of {processed_count} processed.')
        if not msg_list:
            self._logger.info(f"No indexable messages found for chat {share_id} in the specified range.")
            # 如果是新监控的但没下载到消息，仍然保留在监控列表
            return

        writer: Optional[IndexWriter] = None
        try:
            writer = self._indexer.ix.writer()
            for i, msg in enumerate(msg_list):
                try:
                    self._indexer.add_document(msg, writer)
                    indexed_count_in_batch += 1
                    if i > 0 and i % 1000 == 0:
                        self._logger.debug(f"Batch write progress for {share_id}: {i} messages added...")
                        await asyncio.sleep(0.01)
                except Exception as add_e:
                    self._logger.error(f"Error adding document {msg.url} to batch writer: {add_e}")
            writer.commit()
            self._logger.info(f'Write index commit ok for {indexed_count_in_batch} messages from chat {share_id}')
            # 更新该对话的最新消息缓存
            if newest_msg_in_batch:
                 current_chat_id = int(newest_msg_in_batch.chat_id)
                 if current_chat_id not in self.newest_msg or newest_msg_in_batch.post_time > self.newest_msg[current_chat_id].post_time:
                      self.newest_msg[current_chat_id] = newest_msg_in_batch
                      self._logger.debug(f"Updated newest msg cache for {current_chat_id} to {newest_msg_in_batch.url}")
        except writing.LockError:
            self._logger.error("Index is locked during batch write. Downloaded messages are lost.")
            if writer and not writer.is_closed:
                try: writer.cancel()
                except Exception as cancel_e: self._logger.error(f"Error cancelling writer after lock: {cancel_e}")
            # 如果是因为锁错误导致写入失败，并且是刚添加的监控，则移除
            if is_newly_monitored:
                 self.monitored_chats.discard(share_id)
                 self._logger.info(f"[Monitoring] Removed newly added chat {share_id} due to index lock during initial write.")
            raise RuntimeError("Index is locked, cannot write downloaded messages.")
        except Exception as e:
            self._logger.error(f"Error writing batch index for {share_id}: {e}", exc_info=True)
            if writer and not writer.is_closed:
                try: writer.cancel()
                except Exception as cancel_e: self._logger.error(f"Error cancelling writer after general error: {cancel_e}")
            # 如果是因为写入错误导致写入失败，并且是刚添加的监控，则移除
            if is_newly_monitored:
                 self.monitored_chats.discard(share_id)
                 self._logger.info(f"[Monitoring] Removed newly added chat {share_id} due to index write error during initial write.")
            raise RuntimeError(f"写入索引时出错 for {share_id}")
        finally:
             self._logger.info(f"Finished task: {task_name}")

    def clear(self, chat_ids: Optional[List[int]] = None):
        """
        清除索引数据。

        :param chat_ids: 可选，要清除的 chat_id 列表 (原始ID或share_id皆可)。如果为 None，则清除所有索引。
        """
        if chat_ids is not None:
            # 清除指定对话的数据
            share_ids_to_clear = set()
            invalid_inputs = []
            for cid in chat_ids:
                try:
                    # 尝试直接作为int或str获取share_id
                    share_ids_to_clear.add(get_share_id(cid))
                except Exception:
                    invalid_inputs.append(str(cid))

            if invalid_inputs:
                self._logger.warning(f"Invalid chat IDs provided for clearing: {', '.join(invalid_inputs)}")
            if not share_ids_to_clear:
                self._logger.warning("No valid share IDs to clear.")
                return # 如果没有有效的 ID，则不执行任何操作

            self._logger.info(f"Attempting to clear index data for chats: {share_ids_to_clear}")
            try:
                # 使用 Whoosh writer 按 'chat_id' 字段删除文档
                with self._indexer.ix.writer() as w:
                    total_deleted = 0
                    for share_id in share_ids_to_clear:
                        deleted_count = w.delete_by_term('chat_id', str(share_id))
                        total_deleted += deleted_count
                        # 从监控列表和最新消息缓存中移除
                        if self.monitored_chats.discard(share_id): # discard 不会抛错
                           self._logger.info(f'[Monitoring] Chat {share_id} removed from monitoring due to /clear command.')
                        if self.newest_msg.pop(share_id, None):
                           self._logger.debug(f'Removed newest msg cache for cleared chat {share_id}')
                        self._logger.info(f'Cleared {deleted_count} docs for chat {share_id}')
                    self._logger.info(f"Total {total_deleted} documents deleted for specified chats.")
            except writing.LockError:
                self._logger.error(f"Index locked. Failed to clear index for chats {share_ids_to_clear}.")
            except Exception as e:
                self._logger.error(f"Error clearing index for chats {share_ids_to_clear}: {e}", exc_info=True)
        else:
            # 清除所有索引数据
            self._logger.warning('Attempting to clear ALL index data.')
            try:
                # 调用 Indexer 的 clear 方法
                self._indexer.clear()
                # 清空监控列表和最新消息缓存
                if self.monitored_chats:
                    self._logger.info(f"[Monitoring] Removing all {len(self.monitored_chats)} chats from monitoring due to /clear all.")
                    self.monitored_chats.clear()
                if self.newest_msg:
                    self._logger.debug(f"Clearing newest message cache for {len(self.newest_msg)} chats.")
                    self.newest_msg.clear()
                self._logger.info('Cleared all index data and stopped monitoring all chats.')
            except writing.LockError:
                self._logger.error("Index locked. Failed to clear all index data.")
            except Exception as e:
                self._logger.error(f"Error clearing all index data: {e}", exc_info=True)


    async def find_chat_id(self, q: str) -> List[int]:
        """使用会话查找匹配关键词的对话 ID (返回 share_id 列表)"""
        try:
            # 调用 session 的方法查找对话 ID
            return await self.session.find_chat_id(q)
        except Exception as e:
            # 记录查找对话 ID 时的错误
            self._logger.error(f"Error finding chat id for '{q}': {e}")
            return [] # 返回空列表表示查找失败

    async def get_index_status(self, length_limit: int = 4000) -> str:
        """获取后端索引状态的文本描述 (修正计数和错误处理逻辑, 增加日志)"""
        cur_len = 0
        sb = [] # 使用列表存储字符串片段，最后 join
        searcher = None # 初始化 searcher 变量

        # 1. 获取总文档数
        total_docs = -1 # 标记获取失败
        try:
            self._logger.debug("Attempting to get total document count from index...")
            total_docs = self._indexer.ix.doc_count()
            self._logger.debug(f"Successfully retrieved total document count: {total_docs}")
        except Exception as e:
            self._logger.error(f"Failed get total doc count: {e}", exc_info=True) # Log with traceback
        # 添加头部信息 (后端 ID, 会话名, 总消息数)
        sb.append(f'后端 "{self.id}" (会话: "{self.session.name}") 总消息: <b>{total_docs if total_docs >= 0 else "[获取失败]"}</b>\n\n')

        # 定义超出长度限制时的提示信息
        overflow_msg = f'\n\n(部分信息因长度限制未显示)'

        # 辅助函数：检查添加新内容是否会超出长度限制
        def append_msg(msg_list: List[str]) -> bool:
            nonlocal cur_len
            new_len = sum(len(msg) for msg in msg_list)
            if cur_len + new_len > length_limit - len(overflow_msg) - 50:
                return True # 返回 True 表示超出限制
            cur_len += new_len
            sb.extend(msg_list)
            return False # 返回 False 表示未超出限制

        # 2. 显示排除列表
        if self.excluded_chats:
            excluded_list = sorted(list(self.excluded_chats))
            if append_msg([f'{len(excluded_list)} 个对话被禁止索引:\n']):
                sb.append(overflow_msg); return ''.join(sb)
            for chat_id in excluded_list:
                try: chat_html = await self.format_dialog_html(chat_id)
                except Exception: chat_html = f"对话 `{chat_id}` (获取名称出错)"
                if append_msg([f'- {chat_html}\n']):
                    sb.append(overflow_msg); return ''.join(sb)
            if sb and sb[-1] != '\n\n': sb.append('\n')

        # 3. 显示监控列表和计数
        monitored_chats_list = sorted(list(self.monitored_chats))
        if append_msg([f'总计 {len(monitored_chats_list)} 个对话被加入了索引:\n']):
            sb.append(overflow_msg); return ''.join(sb)

        # 4. 获取每个监控对话的详细信息
        detailed_status_error = None
        self._logger.debug(f"Getting status for {len(monitored_chats_list)} monitored chats.")
        try:
             self._logger.debug("Attempting to open index searcher for chat counts...")
             searcher = self._indexer.ix.searcher() # 在循环外打开 searcher
             self._logger.debug("Index searcher opened successfully.")
             for chat_id in monitored_chats_list:
                 msg_for_chat = []
                 num = -1 # 初始化计数为错误状态 (-1)
                 chat_id_str = str(chat_id)

                 # 尝试获取该对话的文档计数
                 try:
                     query = Term('chat_id', chat_id_str)
                     self._logger.debug(f"Counting documents for chat {chat_id_str} with query: {query}")
                     num = searcher.doc_count(query=query)
                     self._logger.debug(f"Count for chat {chat_id_str}: {num}")
                 except searching.SearchError as search_e:
                     self._logger.error(f"Whoosh SearchError counting docs for chat {chat_id_str}: {search_e}", exc_info=True)
                     if not detailed_status_error: detailed_status_error = "部分对话计数失败 (SearchError)"
                 except Exception as e:
                     self._logger.error(f"Unexpected error counting docs for chat {chat_id_str}: {e}", exc_info=True)
                     if not detailed_status_error: detailed_status_error = "部分对话计数失败 (未知错误)"

                 # 尝试获取对话名称
                 try:
                     chat_html = await self.format_dialog_html(chat_id)
                 except Exception as name_e:
                     self._logger.error(f"Error getting name for chat {chat_id}: {name_e}")
                     chat_html = f"对话 `{chat_id}` (获取名称出错)"

                 # 组合对话信息和计数结果
                 count_str = "[计数失败]" if num < 0 else str(num)
                 msg_for_chat.append(f'- {chat_html} 共 {count_str} 条消息\n')

                 # 添加该对话的最新消息信息
                 if newest_msg := self.newest_msg.get(chat_id):
                     display_parts = []
                     if newest_msg.filename: display_parts.append(f"📎 {newest_msg.filename}")
                     if newest_msg.content: display_parts.append(brief_content(newest_msg.content))
                     display = " ".join(display_parts) if display_parts else "(空消息)"
                     esc_display = html.escape(display)
                     time_str = newest_msg.post_time.strftime("%y-%m-%d %H:%M") if isinstance(newest_msg.post_time, datetime) else "[未知时间]"
                     msg_for_chat.append(f'  最新: <a href="{html.escape(newest_msg.url)}">{esc_display}</a> (@{time_str})\n')

                 # 检查长度并尝试添加
                 if append_msg(msg_for_chat):
                     sb.append(overflow_msg); break # 超出则跳出循环

             if detailed_status_error and not (sb and sb[-1].endswith(overflow_msg)):
                 if append_msg([f"\n警告: {detailed_status_error}\n"]):
                     sb.append(overflow_msg)

        except writing.LockError:
             self._logger.error(f"Index locked, failed to open searcher for status.")
             if append_msg(["\n错误：索引被锁定，无法获取详细对话状态。\n"]):
                 sb.append(overflow_msg)
        except Exception as e:
             self._logger.error(f"Failed to get detailed status (outside chat loop): {e}", exc_info=True)
             if append_msg(["\n错误：无法获取详细状态。\n"]):
                 sb.append(overflow_msg)
        finally:
            # 确保 searcher 对象在使用后被关闭
            if searcher:
                searcher.close()
                self._logger.debug("Searcher closed after getting index status.")
        # --- 结束详细信息获取 ---

        return ''.join(sb)

    async def translate_chat_id(self, chat_id: int) -> str:
        """使用会话将 Chat ID (share_id) 翻译为名称"""
        try:
            return await self.session.translate_chat_id(int(chat_id))
        except (telethon.errors.rpcerrorlist.ChannelPrivateError, telethon.errors.rpcerrorlist.ChatIdInvalidError, ValueError, TypeError) as e:
            self._logger.warning(f"Could not translate chat_id {chat_id}: {type(e).__name__}")
            raise EntityNotFoundError(f"无法访问或无效 Chat ID: {chat_id}")
        except EntityNotFoundError:
            self._logger.warning(f"Entity not found for {chat_id} during translation.")
            raise
        except Exception as e:
            self._logger.error(f"Error translating chat_id {chat_id}: {e}")
            raise EntityNotFoundError(f"获取对话 {chat_id} 名称时出错") from e


    async def str_to_chat_id(self, chat: str) -> int:
        """将字符串（用户名、链接或 ID）转换为 share_id"""
        try:
            return get_share_id(int(chat))
        except ValueError:
            try:
                return get_share_id(await self.session.str_to_chat_id(chat))
            except EntityNotFoundError:
                self._logger.warning(f"Entity not found for '{chat}' using session.")
                raise
            except Exception as e_inner:
                self._logger.error(f"Error converting '{chat}' to chat_id via session: {e_inner}")
                raise EntityNotFoundError(f"解析 '{chat}' 时出错") from e_inner
        except Exception as e_outer:
            self._logger.error(f"Error converting '{chat}' to chat_id directly: {e_outer}")
            raise EntityNotFoundError(f"解析 '{chat}' 时出错") from e_outer


    async def format_dialog_html(self, chat_id: int) -> str:
        """格式化对话的 HTML 链接和名称，包含 share_id"""
        try:
            name = await self.translate_chat_id(int(chat_id))
            esc_name = html.escape(name)
            # 创建指向对话第一条消息的链接 (通常用于跳转到对话)
            return f'<a href="https://t.me/c/{chat_id}/1">{esc_name}</a> (`{chat_id}`)'
        except EntityNotFoundError: return f'未知对话 (`{chat_id}`)'
        except ValueError: return f'无效对话 ID (`{chat_id}`)'
        except Exception as e:
            self._logger.warning(f"Error formatting html for {chat_id}: {e}")
            return f'对话 `{chat_id}` (获取名称出错)'


    def _should_monitor(self, chat_id: int) -> bool:
        """判断是否应该监控此对话的消息 (基于配置和监控列表)"""
        try:
            share_id = get_share_id(chat_id)
            if share_id in self.excluded_chats: return False
            # 如果配置了 monitor_all=True，或者该对话在当前的监控列表中，则监控
            should = self._cfg.monitor_all or (share_id in self.monitored_chats)
            # self._logger.debug(f"Should monitor {share_id}? monitor_all={self._cfg.monitor_all}, in_list={share_id in self.monitored_chats} -> {should}")
            return should
        except Exception as e:
            self._logger.warning(f"Error determining monitor status for chat {chat_id}: {e}")
            return False


    @staticmethod
    async def _get_sender_name(message: TgMessage) -> str:
        """获取消息发送者的名称（用户或频道/群组标题）"""
        sender_name = ''
        try:
            sender = await message.get_sender()
            if isinstance(sender, User):
                sender_name = format_entity_name(sender)
            elif hasattr(sender, 'title'): # Channels, Chats
                sender_name = sender.title
            elif hasattr(sender, 'username'): # Fallback for users without full name?
                sender_name = f"@{sender.username}"
        except Exception as e:
            logger.debug(f"Could not get sender name for message {getattr(message, 'id', 'N/A')}: {e}")
        return sender_name or '' # Ensure non-None return

    def _register_hooks(self):
        """注册 Telethon 事件钩子，用于实时接收和处理消息"""
        self._logger.info("Registering Telethon event handlers...")
        # 用于跟踪哪些 chat_id 已经被记录为“首次监控到”
        _first_monitor_logged: Set[int] = set()

        # --- 处理新消息 ---
        @self.session.on(events.NewMessage())
        async def client_message_handler(event: events.NewMessage.Event):
            # 基础检查：确保有 chat_id
            if not hasattr(event, 'chat_id') or event.chat_id is None:
                self._logger.debug("Ignoring event with no chat_id.")
                return

            try:
                share_id = get_share_id(event.chat_id)
                # --- 监控检查和反馈 ---
                if not self._should_monitor(share_id):
                    # self._logger.debug(f"Ignoring message from non-monitored chat {share_id}.")
                    return # 不处理不监控的对话

                # 如果是首次处理这个监控对话的消息（且未在日志中记录过），添加日志
                if share_id not in _first_monitor_logged:
                     # 检查它是否确实在监控列表或 monitor_all=True
                     if share_id in self.monitored_chats or self._cfg.monitor_all:
                         self._logger.info(f"[Monitoring] First message processed from monitored chat {share_id}.")
                         _first_monitor_logged.add(share_id)
                     # else: 理论上不应发生，因为 _should_monitor 已检查

                # --- 消息处理逻辑 (保持不变) ---
                url = f'https://t.me/c/{share_id}/{event.id}'
                sender = await self._get_sender_name(event.message)
                post_time = event.message.date
                if not isinstance(post_time, datetime):
                    self._logger.warning(f"New message {url} has invalid date type {type(post_time)}, using current time.")
                    post_time = datetime.now()

                msg_text, filename = '', None
                if event.message.file and hasattr(event.message.file, 'name') and event.message.file.name:
                    filename = event.message.file.name
                    if event.message.text: msg_text = escape_content(event.message.text.strip())
                    self._logger.info(f'New file {url} from "{sender}" in chat {share_id}: "{filename}" Caption:"{brief_content(msg_text)}"')
                elif event.message.text:
                    msg_text = escape_content(event.message.text.strip())
                    if not msg_text: self._logger.debug(f"Ignoring empty/whitespace message {url} in {share_id}."); return
                    self._logger.info(f'New msg {url} from "{sender}" in chat {share_id}: "{brief_content(msg_text)}"')
                else:
                    self._logger.debug(f"Ignoring message {url} with no text or file in {share_id}.")
                    return

                msg = IndexMsg(content=msg_text or "", url=url, chat_id=share_id, post_time=post_time, sender=sender or "", filename=filename)
                if share_id not in self.newest_msg or msg.post_time >= self.newest_msg[share_id].post_time:
                    self.newest_msg[share_id] = msg
                    self._logger.debug(f"Updated newest cache for {share_id} to {url}")
                try:
                    self._indexer.add_document(msg)
                except Exception as e:
                    self._logger.error(f"Error adding doc {url} to index: {e}", exc_info=True)
            except Exception as e:
                chat_id_repr = getattr(event, 'chat_id', 'N/A')
                self._logger.error(f"Error processing new message in chat {chat_id_repr}: {e}", exc_info=True)

        # --- 处理消息编辑 ---
        @self.session.on(events.MessageEdited())
        async def client_message_update_handler(event: events.MessageEdited.Event):
            if not hasattr(event, 'chat_id') or event.chat_id is None: return
            try:
                share_id = get_share_id(event.chat_id)
                if not self._should_monitor(share_id): return

                # 编辑处理逻辑 (保持不变)
                # ...
                url = f'https://t.me/c/{share_id}/{event.id}'
                new_msg_text = escape_content(event.message.text.strip()) if event.message.text else ''
                self._logger.info(f'Msg {url} edited in chat {share_id}. Checking for update...')

                try:
                    old_fields = self._indexer.get_document_fields(url=url)
                    if old_fields:
                        if old_fields.get('content') == new_msg_text:
                            self._logger.debug(f"Edit event {url} has same content, skipping index update.")
                            return
                        new_fields = old_fields.copy(); new_fields['content'] = new_msg_text
                        new_fields.setdefault('chat_id', str(share_id))
                        old_time = old_fields.get('post_time')
                        new_fields['post_time'] = old_time if isinstance(old_time, datetime) else (event.message.date or datetime.now())
                        if not isinstance(new_fields['post_time'], datetime): new_fields['post_time'] = datetime.now()
                        new_fields.setdefault('sender', old_fields.get('sender', await self._get_sender_name(event.message) or ''))
                        new_fields.setdefault('filename', old_fields.get('filename', None))
                        new_fields.setdefault('url', url)
                        new_fields['has_file'] = 1 if new_fields.get('filename') else 0

                        self._indexer.replace_document(url=url, new_fields=new_fields)
                        self._logger.info(f'Updated msg content in index for {url}')

                        if share_id in self.newest_msg and self.newest_msg[share_id].url == url:
                             try:
                                 # 使用 Whoosh 存储的字段类型重建 IndexMsg
                                 # 注意： Whoosh 存 chat_id 为 str, post_time 为 datetime, has_file 为 int
                                 rebuilt_msg = IndexMsg(
                                     content=new_fields['content'], url=new_fields['url'],
                                     chat_id=int(new_fields['chat_id']), # 转回 int
                                     post_time=new_fields['post_time'], # 已经是 datetime
                                     sender=new_fields['sender'], filename=new_fields['filename']
                                 )
                                 self.newest_msg[share_id] = rebuilt_msg
                                 self._logger.debug(f"Updated newest cache content for {url}")
                             except (ValueError, KeyError, TypeError) as cache_e:
                                 self._logger.error(f"Error reconstructing IndexMsg for cache update {url}: {cache_e}. Fields: {new_fields}")
                    else:
                         self._logger.warning(f'Edited msg {url} not found in index. Adding as new message.')
                         sender = await self._get_sender_name(event.message)
                         post_time = event.message.date or datetime.now()
                         if not isinstance(post_time, datetime): post_time = datetime.now()
                         filename = None # 假设编辑不改变文件信息
                         if new_msg_text:
                             msg = IndexMsg(content=new_msg_text, url=url, chat_id=share_id, post_time=post_time, sender=sender or "", filename=filename)
                             self._indexer.add_document(msg)
                             if share_id not in self.newest_msg or msg.post_time >= self.newest_msg[share_id].post_time:
                                 self.newest_msg[share_id] = msg
                                 self._logger.debug(f"Added edited msg {url} as newest cache for {share_id}")
                         else:
                             self._logger.debug(f"Ignoring edited message {url} with empty content and not found in index.")
                except Exception as e:
                    self._logger.error(f'Error updating/adding edited msg {url} in index: {e}', exc_info=True)
            except Exception as e:
                chat_id_repr = getattr(event, 'chat_id', 'N/A')
                self._logger.error(f"Error processing edited message in chat {chat_id_repr}: {e}", exc_info=True)

        # --- 处理消息删除 ---
        @self.session.on(events.MessageDeleted())
        async def client_message_delete_handler(event: events.MessageDeleted.Event):
            if not hasattr(event, 'chat_id') or event.chat_id is None:
                self._logger.debug(f"Ignoring deletion event with no chat_id. Deleted IDs: {event.deleted_ids}")
                return
            try:
                share_id = get_share_id(event.chat_id)
                if not self._should_monitor(share_id):
                    self._logger.debug(f"Ignoring deletion event from non-monitored chat {share_id}. Deleted IDs: {event.deleted_ids}")
                    return

                # 删除处理逻辑 (保持不变)
                # ...
                deleted_count_in_batch = 0
                urls_to_delete = [f'https://t.me/c/{share_id}/{mid}' for mid in event.deleted_ids]
                self._logger.info(f"Processing deletion of {len(urls_to_delete)} message(s) in chat {share_id}: {event.deleted_ids}")

                try:
                     with self._indexer.ix.writer() as writer:
                          for url in urls_to_delete:
                               if share_id in self.newest_msg and self.newest_msg[share_id].url == url:
                                    del self.newest_msg[share_id]
                                    self._logger.info(f"Removed newest cache for {share_id} due to deletion of {url}.")
                               try:
                                    count = writer.delete_by_term('url', url)
                                    if count > 0:
                                        deleted_count_in_batch += count
                                        self._logger.info(f"Deleted msg {url} from index.")
                                    else:
                                        self._logger.debug(f"Message {url} requested for deletion not found in index.")
                               except Exception as del_e:
                                    self._logger.error(f"Error deleting doc {url} from index: {del_e}")
                     if deleted_count_in_batch > 0:
                         self._logger.info(f'Finished deleting {deleted_count_in_batch} msgs from index for chat {share_id}')
                     else:
                         self._logger.info(f"No matching messages found in index to delete for chat {share_id} batch.")
                except writing.LockError:
                    self._logger.error(f"Index locked. Could not process deletions batch for {share_id}: {urls_to_delete}")
                except Exception as e:
                    self._logger.error(f"Error processing deletions batch for {share_id}: {e}", exc_info=True)
            except Exception as e:
                chat_id_repr = getattr(event, 'chat_id', 'N/A')
                self._logger.error(f"Error processing deleted event in chat {chat_id_repr}: {e}", exc_info=True)

        self._logger.info("Telethon event handlers registered.")

                self._logger.error(f"Error processing deleted event in chat {chat_id_repr}: {e}", exc_info=True)

        self._logger.info("Telethon event handlers registered.")

