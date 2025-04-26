# -*- coding: utf-8 -*-
import html
import asyncio # 用于异步操作，如 sleep
from datetime import datetime
from typing import Optional, List, Set, Dict, Any, Union, Tuple # 添加 Any, Tuple

import telethon.errors.rpcerrorlist
from telethon import events
from telethon.tl.patched import Message as TgMessage
from telethon.tl.types import User
from whoosh.query import Term # 用于构建查询
# 移除 searching 导入，因为它没有 SearchError
from whoosh import writing, index as whoosh_index
from whoosh.writing import IndexWriter, LockError # 写入和锁错误

# 项目内导入 - 使用包含文件索引的 Indexer
from .indexer import Indexer, IndexMsg, SearchResult
from .common import CommonBotConfig, escape_content, get_share_id, get_logger, format_entity_name, brief_content, \
    EntityNotFoundError
from .session import ClientSession


# 日志记录器
logger = get_logger('backend_bot') # logger 在模块级别定义，所有实例共享


class BackendBotConfig:
    """存储 Backend Bot 配置的类 - 基于你提供的文件结构"""
    def __init__(self, **kw: Any):
        self.monitor_all: bool = kw.get('monitor_all', False) # 是否监控所有加入的对话
        # 原始排除列表，可能包含用户名或 ID
        self._raw_exclude_chats: List[Union[int, str]] = kw.get('exclude_chats', [])
        # 解析后的排除列表 (仅含 share_id)
        self.excluded_chats: Set[int] = set()
        # 初始化时尝试解析整数 ID
        for chat_id_or_name in self._raw_exclude_chats:
            try:
                # 确保输入是数字或可转换为数字的字符串
                numeric_id = int(chat_id_or_name)
                self.excluded_chats.add(get_share_id(numeric_id))
            except (ValueError, TypeError):
                 # 非数字留给 start() 解析 (如果是用户名)
                 pass


class BackendBot:
    """处理索引、下载、后台监控的核心 Bot 类 - 包含文件索引逻辑和修复"""
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
            # 使用包含文件字段的 Indexer
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
        # 这里将配置中的整数ID解析结果与启动时可能解析的用户名结果合并
        # 注意：_raw_exclude_chats 主要用于启动时解析用户名
        self.excluded_chats: Set[int] = cfg.excluded_chats
        self._raw_exclude_chats: List[Union[int, str]] = cfg._raw_exclude_chats # 保留原始配置，用于start解析
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
             return SearchResult([], True, 0, page_num)


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


    async def download_history(self, chat_id: Union[int, str], min_id: int, max_id: int, call_back: Optional[callable] = None):
        """
        下载指定对话的历史记录并添加到索引。

        :param chat_id: 原始对话 ID 或用户名/链接。
        :param min_id: 要下载的最小消息 ID (不包括)。
        :param max_id: 要下载的最大消息 ID (0 表示无上限，会获取比 min_id 更新的所有消息)。
        :param call_back: 可选的回调函数，用于报告进度 (接收 cur_id, dl_count)。
        """
        task_name = f"DownloadHistory-{chat_id}"
        self._logger.info(f"Starting task: {task_name} (min={min_id}, max={max_id})")
        share_id = -1 # 初始化为无效值
        entity = None # 初始化实体
        try:
            # 尝试使用原始输入获取实体和 share_id
            entity = await self.session.get_entity(chat_id)
            share_id = get_share_id(entity.id)
        except ValueError as e: # get_entity 可能抛出 ValueError
             self._logger.error(f"Could not find entity for '{chat_id}'. Error: {e}")
             raise EntityNotFoundError(f"无法找到对话实体: {chat_id}") from e
        except Exception as e:
            self._logger.error(f"Error resolving chat '{chat_id}' or getting share_id: {e}", exc_info=True)
            raise EntityNotFoundError(f"解析对话时出错: {chat_id}") from e

        task_name = f"DownloadHistory-{share_id}" # 更新任务名
        self._logger.info(f'Downloading history for {share_id} (input="{chat_id}", min={min_id}, max={max_id})')
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

        msg_list: List[IndexMsg] = [] # 存储从 Telegram 获取并准备索引的消息
        downloaded_count: int = 0 # 实际构造了 IndexMsg 的消息数量
        processed_count: int = 0 # Telethon `iter_messages` 返回的总项目数
        newest_msg_in_batch: Optional[IndexMsg] = None # 记录此批次中最新的消息
        indexed_count_in_batch: int = 0

        try:
            # 使用 Telethon 异步迭代指定对话的消息历史
            # 传递获取到的 entity 给 iter_messages
            async for tg_message in self.session.iter_messages(entity=entity, min_id=min_id, max_id=max_id, limit=None, reverse=True): # reverse=True 确保从旧到新处理，便于确定 newest_msg
                processed_count += 1
                if not isinstance(tg_message, TgMessage): continue

                # 使用 share_id 构建 URL 和 IndexMsg
                url = f'https://t.me/c/{share_id}/{tg_message.id}'
                sender = await self._get_sender_name(tg_message)
                post_time = tg_message.date
                if not isinstance(post_time, datetime):
                    self._logger.warning(f"Message {url} has invalid date type {type(post_time)}, using current time.")
                    post_time = datetime.now()

                msg_text, filename = '', None
                # 包含文件索引逻辑
                if tg_message.file and hasattr(tg_message.file, 'name') and tg_message.file.name:
                    filename = tg_message.file.name
                    # 同时获取可能的文件标题/说明
                    if tg_message.text: msg_text = escape_content(tg_message.text.strip())
                elif tg_message.text:
                    msg_text = escape_content(tg_message.text.strip())

                # 只有当有文本内容或文件名时才索引
                if msg_text or filename:
                    try:
                        # IndexMsg 使用 share_id，并包含 filename
                        msg = IndexMsg(content=msg_text or "", url=url, chat_id=share_id, post_time=post_time, sender=sender or "", filename=filename)
                        msg_list.append(msg)
                        downloaded_count += 1
                        # 更新此批次中遇到的最新消息
                        newest_msg_in_batch = msg
                    except Exception as create_e:
                        self._logger.error(f"Error creating IndexMsg for {url}: {create_e}")
                # else: 忽略没有文本和文件名的消息

                # 进度回调和事件循环释放
                if call_back and processed_count % 100 == 0:
                     try: await call_back(tg_message.id, downloaded_count)
                     except Exception as cb_e: self._logger.warning(f"Error in download callback: {cb_e}")
                if processed_count % 500 == 0:
                    self._logger.debug(f"Download progress for {share_id}: Processed {processed_count}, Indexable {downloaded_count}")
                    await asyncio.sleep(0.01) # 释放事件循环

            # --- 处理下载错误 ---
        except telethon.errors.rpcerrorlist.ChannelPrivateError as e:
            self._logger.error(f"Permission denied for chat '{chat_id}' ({share_id}). Is the backend account a member? Error: {e}")
            self.monitored_chats.discard(share_id) # 移除无法访问的对话
            if is_newly_monitored: self._logger.info(f"[Monitoring] Removed newly added chat {share_id} due to access error.")
            raise EntityNotFoundError(f"无法访问对话 '{chat_id}' ({share_id})，请确保后端账号是其成员。") from e
        except (telethon.errors.rpcerrorlist.ChatIdInvalidError, telethon.errors.rpcerrorlist.PeerIdInvalidError):
            self._logger.error(f"Chat ID '{chat_id}' ({share_id}) invalid or peer not found.")
            self.monitored_chats.discard(share_id)
            if is_newly_monitored: self._logger.info(f"[Monitoring] Removed newly added chat {share_id} due to invalid ID.")
            raise EntityNotFoundError(f"无效对话 ID 或无法找到 Peer: '{chat_id}' ({share_id})")
        except ValueError as e:
             # Telethon 的 get_entity 或 iter_messages 可能在找不到实体时抛出 ValueError
             if "Cannot find any entity corresponding to" in str(e) or "Could not find the input entity for" in str(e):
                 self._logger.error(f"Cannot find entity for chat '{chat_id}' ({share_id}). Error: {e}")
                 self.monitored_chats.discard(share_id)
                 if is_newly_monitored: self._logger.info(f"[Monitoring] Removed newly added chat {share_id} due to entity not found.")
                 raise EntityNotFoundError(f"无法找到对话实体: '{chat_id}' ({share_id})") from e
             else:
                 # 其他类型的 ValueError
                 self._logger.error(f"ValueError iterating messages for {share_id}: {e}", exc_info=True)
                 raise RuntimeError(f"下载对话 {share_id} 时发生值错误") from e
        except Exception as e:
            self._logger.error(f"Error iterating messages for {share_id}: {e}", exc_info=True)
            # 如果在下载过程中出错，也考虑移除（如果刚添加的话）
            if is_newly_monitored:
                self.monitored_chats.discard(share_id)
                self._logger.info(f"[Monitoring] Removed newly added chat {share_id} due to download error.")
            raise RuntimeError(f"下载对话 {share_id} 时发生未知错误") from e

        # --- 批量写入索引 ---
        self._logger.info(f'History fetch complete for {share_id}: {downloaded_count} messages to index out of {processed_count} processed.')
        if not msg_list:
            self._logger.info(f"No indexable messages found for chat {share_id} in the specified range.")
            # 如果是新监控的但没下载到消息，仍然保留在监控列表
            return

        writer: Optional[IndexWriter] = None
        try:
            # 使用优化后的批量写入模式
            self._logger.info(f"Starting batch write for {len(msg_list)} messages from chat {share_id}...")
            # 一次性获取 writer
            writer = self._indexer.ix.writer()
            for i, msg in enumerate(msg_list):
                try:
                    # 直接调用 add_document，传递 writer
                    self._indexer.add_document(msg, writer=writer)
                    indexed_count_in_batch += 1
                    # 减少日志频率，避免刷屏
                    if i > 0 and (i + 1) % 5000 == 0:
                        self._logger.debug(f"Batch write progress for {share_id}: {i+1}/{len(msg_list)} messages added...")
                        await asyncio.sleep(0.01) # 短暂释放
                except Exception as add_e:
                    self._logger.error(f"Error adding document {msg.url} to batch writer: {add_e}")
            # 循环结束后提交
            writer.commit()
            self._logger.info(f'Write index commit successful for {indexed_count_in_batch} messages from chat {share_id}')
            # 更新该对话的最新消息缓存
            if newest_msg_in_batch:
                 # newest_msg_in_batch.chat_id 已经是 share_id
                 current_chat_id = newest_msg_in_batch.chat_id
                 # 检查缓存中是否已有记录，以及新消息是否更新
                 if current_chat_id not in self.newest_msg or newest_msg_in_batch.post_time > self.newest_msg[current_chat_id].post_time:
                      self.newest_msg[current_chat_id] = newest_msg_in_batch
                      self._logger.debug(f"Updated newest msg cache for {current_chat_id} to {newest_msg_in_batch.url}")

        except writing.LockError:
            self._logger.error("Index is locked during batch write. Downloaded messages are lost for this batch.")
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
             # 确保 writer 被关闭（即使在 commit() 之后也需要）
             if writer and not writer.is_closed:
                 try:
                     # 如果出现异常，commit 可能未执行，cancel 是更安全的选择
                     writer.cancel() # 或者根据是否有异常决定 commit/cancel
                 except Exception as final_cancel_e:
                     self._logger.error(f"Error ensuring writer closure: {final_cancel_e}")
             self._logger.info(f"Finished task: {task_name}")

    def clear(self, chat_ids: Optional[List[int]] = None):
        """
        清除索引数据。

        :param chat_ids: 可选，要清除的 chat_id 列表 (接收 share_id)。如果为 None，则清除所有索引。
        """
        if chat_ids is not None:
            # 清除指定对话的数据
            share_ids_to_clear = set(chat_ids) # 假设传入的就是 share_id 列表

            if not share_ids_to_clear:
                self._logger.warning("No valid share IDs to clear.")
                return # 如果没有有效的 ID，则不执行任何操作

            self._logger.info(f"Attempting to clear index data for chats: {share_ids_to_clear}")
            try:
                # 使用 Whoosh writer 按 'chat_id' 字段删除文档
                with self._indexer.ix.writer() as w:
                    total_deleted = 0
                    for share_id in share_ids_to_clear:
                        # 确保使用字符串形式的 share_id 进行 Term 查询
                        deleted_count = w.delete_by_term('chat_id', str(share_id))
                        total_deleted += deleted_count
                        # 从监控列表和最新消息缓存中移除
                        if share_id in self.monitored_chats:
                           self.monitored_chats.discard(share_id)
                           self._logger.info(f'[Monitoring] Chat {share_id} removed from monitoring due to /clear command.')
                        if share_id in self.newest_msg:
                           del self.newest_msg[share_id]
                           self._logger.debug(f'Removed newest msg cache for cleared chat {share_id}')
                        if deleted_count > 0:
                            self._logger.info(f'Cleared {deleted_count} docs for chat {share_id}')
                        else:
                            self._logger.debug(f'No docs found to clear for chat {share_id}')
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
            # 调用 session 的方法查找对话 ID (它应该返回 share_id)
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
            # 确保索引存在且可读
            if self._indexer and self._indexer.ix and not self._indexer.ix.is_empty():
                # 尝试打开 searcher 来获取计数
                with self._indexer.ix.searcher() as s:
                    total_docs = s.doc_count_all() # 获取所有文档数
                self._logger.debug(f"Successfully retrieved total document count: {total_docs}")
            elif self._indexer and self._indexer.ix and self._indexer.ix.is_empty():
                total_docs = 0 # 索引存在但是空的
                self._logger.debug("Index exists but is empty.")
            else:
                self._logger.error("Indexer or index object is not available.")
        except writing.LockError:
            self._logger.error(f"Index locked, failed get total doc count.")
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
            # 调整检查逻辑，确保在接近限制时停止
            if cur_len + new_len > length_limit - len(overflow_msg) - 100: # 增加预留空间
                return True # 返回 True 表示超出限制
            cur_len += new_len
            sb.extend(msg_list)
            return False # 返回 False 表示未超出限制

        # 2. 显示排除列表
        # 确保 self.excluded_chats 存在且非空
        if hasattr(self, 'excluded_chats') and self.excluded_chats:
            excluded_list = sorted(list(self.excluded_chats))
            if append_msg([f'{len(excluded_list)} 个对话被禁止索引:\n']):
                sb.append(overflow_msg); return ''.join(sb)
            # 使用 asyncio.gather 并发获取名称
            tasks = [self.format_dialog_html(chat_id) for chat_id in excluded_list]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for res in results:
                 if isinstance(res, Exception):
                     # 记录错误，但列表中可能难以对应回 chat_id
                     self._logger.warning(f"Error formatting dialog HTML for excluded chat: {res}")
                     # 可以添加一个通用错误提示，或者忽略格式化失败的项
                     if append_msg([f"- [获取名称出错]\n"]):
                         sb.append(overflow_msg); return ''.join(sb)
                 elif isinstance(res, str):
                      if append_msg([f'- {res}\n']):
                         sb.append(overflow_msg); return ''.join(sb)

            if sb and not sb[-1].endswith('\n\n'): sb.append('\n') # 确保段落间有空行

        # 3. 显示监控列表和计数
        # 确保 self.monitored_chats 存在
        monitored_chats_list = []
        if hasattr(self, 'monitored_chats'):
            # 只包括未被排除的监控对话
            monitored_chats_list = sorted(list(self.monitored_chats - self.excluded_chats))

        if append_msg([f'总计 {len(monitored_chats_list)} 个对话被加入了索引 (且未被排除):\n']):
            sb.append(overflow_msg); return ''.join(sb)

        # 4. 获取每个监控对话的详细信息
        detailed_status_error = None
        if monitored_chats_list: # 仅当有监控对话时才尝试打开 searcher
            self._logger.debug(f"Getting status for {len(monitored_chats_list)} monitored chats.")
            try:
                 self._logger.debug("Attempting to open index searcher for chat counts...")
                 searcher = self._indexer.ix.searcher() # 在循环外打开 searcher
                 self._logger.debug("Index searcher opened successfully.")

                 # 并发获取名称
                 name_tasks = {}
                 for chat_id in monitored_chats_list:
                     name_tasks[chat_id] = asyncio.create_task(self.format_dialog_html(chat_id))

                 # 等待名称获取完成
                 name_results = await asyncio.gather(*name_tasks.values(), return_exceptions=True)
                 chat_html_map = {}
                 name_idx = 0
                 for chat_id in monitored_chats_list:
                      res = name_results[name_idx]
                      if isinstance(res, Exception):
                          chat_html_map[chat_id] = f"对话 `{chat_id}` (获取名称出错)"
                      else:
                          chat_html_map[chat_id] = res
                      name_idx += 1

                 # 依次获取计数并组合消息
                 for chat_id in monitored_chats_list:
                     msg_for_chat = []
                     num = -1 # 初始化计数为错误状态 (-1)
                     chat_id_str = str(chat_id) # 确保是字符串
                     query = Term('chat_id', chat_id_str) # 构建 Term 查询

                     # 尝试获取该对话的文档计数
                     try:
                         # **使用修复后的方法获取计数**
                         num = self._indexer.count_by_query(query=query)
                         self._logger.debug(f"Count for chat {chat_id_str} (query={query}): {num}")
                     except Exception as e: # 捕获其他可能的错误
                         self._logger.error(f"Unexpected error counting docs for chat {chat_id_str} (query={query}): {e}", exc_info=True)
                         if not detailed_status_error: detailed_status_error = f"部分对话计数失败 ({type(e).__name__}, e.g., chat {chat_id_str})"

                     # 获取预先格式化好的 HTML 名称
                     chat_html = chat_html_map.get(chat_id, f"对话 `{chat_id}` (未知)")

                     # 组合对话信息和计数结果
                     count_str = "[计数失败]" if num < 0 else str(num)
                     msg_for_chat.append(f'- {chat_html} 共 {count_str} 条消息\n')

                     # 添加该对话的最新消息信息
                     if newest_msg := self.newest_msg.get(chat_id):
                         display_parts = []
                         if newest_msg.filename: display_parts.append(f"📎 {html.escape(brief_content(newest_msg.filename, 30))}") # 限制文件名长度
                         if newest_msg.content: display_parts.append(html.escape(brief_content(newest_msg.content, 50))) # 限制内容长度
                         display = " ".join(display_parts) if display_parts else "(空消息)"
                         time_str = newest_msg.post_time.strftime("%y-%m-%d %H:%M") if isinstance(newest_msg.post_time, datetime) else "[未知时间]"
                         msg_for_chat.append(f'  最新: <a href="{html.escape(newest_msg.url)}">{display}</a> (@{time_str})\n')

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
                 self._logger.error(f"Failed to get detailed status (outside chat loop): {type(e).__name__}: {e}", exc_info=True)
                 if append_msg(["\n错误：无法获取详细状态。\n"]):
                     sb.append(overflow_msg)
            finally:
                # 确保 searcher 对象在使用后被关闭
                if searcher:
                    try:
                        searcher.close()
                        self._logger.debug("Searcher closed after getting index status.")
                    except Exception as close_e:
                        self._logger.error(f"Error closing searcher: {close_e}")
        # --- 结束详细信息获取 ---

        return ''.join(sb).strip() # 返回前移除末尾空白


    async def translate_chat_id(self, chat_id: int) -> str:
        """使用会话将 Chat ID (share_id) 翻译为名称"""
        try:
            # 确保传入的是整数
            chat_id_int = int(chat_id)
            return await self.session.translate_chat_id(chat_id_int)
        except (ValueError, TypeError):
             # 处理 chat_id 无法转换为整数的情况
             self._logger.warning(f"Invalid chat_id type for translation: {chat_id} ({type(chat_id)})")
             raise EntityNotFoundError(f"无效的 Chat ID 格式: {chat_id}")
        except (telethon.errors.rpcerrorlist.ChannelPrivateError, telethon.errors.rpcerrorlist.ChatIdInvalidError) as e:
            self._logger.warning(f"Could not translate chat_id {chat_id}: {type(e).__name__}")
            raise EntityNotFoundError(f"无法访问或无效 Chat ID: {chat_id}")
        except EntityNotFoundError:
            self._logger.warning(f"Entity not found for {chat_id} during translation.")
            raise
        except Exception as e:
            self._logger.error(f"Error translating chat_id {chat_id}: {e}", exc_info=True)
            raise EntityNotFoundError(f"获取对话 {chat_id} 名称时出错") from e


    async def str_to_chat_id(self, chat: Union[str, int]) -> int:
        """将字符串（用户名、链接或 ID）或整数 ID 转换为 share_id"""
        # 首先处理整数输入
        if isinstance(chat, int):
            try:
                # 假设整数已经是 peer_id 或 share_id，直接用 get_share_id 处理
                return get_share_id(chat)
            except Exception as e_int:
                self._logger.error(f"Error converting int '{chat}' to share_id directly: {e_int}")
                raise EntityNotFoundError(f"解析整数 ID '{chat}' 时出错") from e_int

        # 处理字符串输入
        elif isinstance(chat, str):
            chat_str = chat.strip()
            # 尝试直接将字符串转为整数 ID 处理
            try:
                return get_share_id(int(chat_str))
            except ValueError:
                # 如果不能直接转为整数，则使用 session 的方法解析用户名、链接等
                try:
                    # session.str_to_chat_id 应该返回 peer_id
                    peer_id = await self.session.str_to_chat_id(chat_str)
                    # 将获取的 peer_id 转换为 share_id
                    return get_share_id(peer_id)
                except EntityNotFoundError:
                    self._logger.warning(f"Entity not found for '{chat_str}' using session.")
                    raise # 直接重新抛出 EntityNotFoundError
                except Exception as e_inner:
                    self._logger.error(f"Error converting '{chat_str}' to chat_id via session: {e_inner}", exc_info=True)
                    raise EntityNotFoundError(f"解析 '{chat_str}' 时出错") from e_inner
        else:
             # 处理无效输入类型
             raise TypeError(f"Invalid input type for str_to_chat_id: {type(chat)}")


    async def format_dialog_html(self, chat_id: int) -> str:
        """格式化对话的 HTML 链接和名称，包含 share_id"""
        try:
            # 确保 chat_id 是整数
            chat_id_int = int(chat_id)
            name = await self.translate_chat_id(chat_id_int)
            esc_name = html.escape(name)
            # 创建指向对话第一条消息的链接 (通常用于跳转到对话)
            return f'<a href="https://t.me/c/{chat_id_int}/1">{esc_name}</a> (`{chat_id_int}`)'
        except EntityNotFoundError:
            # 如果无法翻译名称，仍然显示 ID
            return f'未知对话 (`{chat_id}`)'
        except (ValueError, TypeError):
            # 如果 chat_id 格式无效
            return f'无效对话 ID (`{chat_id}`)'
        except Exception as e:
            # 其他获取名称时的错误
            self._logger.warning(f"Error formatting html for {chat_id}: {e}")
            return f'对话 `{chat_id}` (获取名称出错)'


    def _should_monitor(self, chat_id: int) -> bool:
        """判断是否应该监控此对话的消息 (基于配置和监控列表)"""
        try:
            # 传入的可能是 peer_id，需要转为 share_id
            share_id = get_share_id(chat_id)
            if share_id in self.excluded_chats: return False
            # 如果配置了 monitor_all=True，或者该对话在当前的监控列表中，则监控
            should = self._cfg.monitor_all or (share_id in self.monitored_chats)
            # self._logger.debug(f"Should monitor {share_id}? monitor_all={self._cfg.monitor_all}, in_list={share_id in self.monitored_chats} -> {should}")
            return should
        except Exception as e:
            self._logger.warning(f"Error determining monitor status for input chat {chat_id}: {e}")
            return False


    @staticmethod
    async def _get_sender_name(message: TgMessage) -> str:
        """获取消息发送者的名称（用户或频道/群组标题）"""
        sender_name = ''
        try:
            # 尝试获取发送者实体
            sender = await message.get_sender()
            if isinstance(sender, User):
                # 如果是用户，格式化名称
                sender_name = format_entity_name(sender)
            elif hasattr(sender, 'title'): # 适用于频道、群组等
                sender_name = sender.title
            elif hasattr(sender, 'username') and sender.username: # 最后的备选：用户名
                sender_name = f"@{sender.username}"
            # 可以添加更多对不同 Peer 类型的处理
        except Exception as e:
            # 记录获取发送者名称失败的调试信息
            logger.debug(f"Could not get sender name for message {getattr(message, 'id', 'N/A')} in chat {getattr(message, 'chat_id', 'N/A')}: {e}")
        # 确保返回字符串，即使获取失败也返回空字符串
        return sender_name or ''

    def _register_hooks(self):
        """注册 Telethon 事件钩子，用于实时接收和处理消息"""
        self._logger.info("Registering Telethon event handlers...")
        # 用于跟踪哪些 chat_id 已经被记录为“首次监控到”
        _first_monitor_logged: Set[int] = set()

        # --- 处理新消息 ---
        @self.session.on(events.NewMessage())
        async def client_message_handler(event: events.NewMessage.Event):
            # 基础检查：确保有 chat_id 和 message 对象
            message = event.message
            if not hasattr(event, 'chat_id') or event.chat_id is None or not message:
                self._logger.debug("Ignoring event with no chat_id or message object.")
                return

            try:
                # 使用 event.chat_id (通常是 peer_id) 来判断是否监控
                if not self._should_monitor(event.chat_id):
                    # self._logger.debug(f"Ignoring message from non-monitored chat {event.chat_id}.")
                    return # 不处理不监控的对话

                # 使用 get_share_id 转换为 share_id 用于存储和 URL
                share_id = get_share_id(event.chat_id)

                # 如果是首次处理这个监控对话的消息（且未在日志中记录过），添加日志
                if share_id not in _first_monitor_logged:
                     # 检查它是否确实在监控列表或 monitor_all=True
                     if share_id in self.monitored_chats or self._cfg.monitor_all:
                         self._logger.info(f"[Monitoring] First message processed from monitored chat {share_id} (Peer ID: {event.chat_id}).")
                         _first_monitor_logged.add(share_id)

                # --- 消息处理逻辑 (包含文件) ---
                url = f'https://t.me/c/{share_id}/{message.id}' # URL 使用 share_id
                sender = await self._get_sender_name(message)
                post_time = message.date
                if not isinstance(post_time, datetime):
                    self._logger.warning(f"New message {url} has invalid date type {type(post_time)}, using current time.")
                    post_time = datetime.now()

                msg_text, filename = '', None
                if message.file and hasattr(message.file, 'name') and message.file.name:
                    filename = message.file.name
                    if message.text: msg_text = escape_content(message.text.strip())
                    self._logger.info(f'New file {url} from "{sender}" in chat {share_id}: "{filename}" Caption:"{brief_content(msg_text)}"')
                elif message.text:
                    msg_text = escape_content(message.text.strip())
                    # 忽略纯空白消息
                    if not msg_text: self._logger.debug(f"Ignoring empty/whitespace message {url} in {share_id}."); return
                    self._logger.info(f'New msg {url} from "{sender}" in chat {share_id}: "{brief_content(msg_text)}"')
                else:
                    # 忽略既无文本也无有效文件名的消息
                    self._logger.debug(f"Ignoring message {url} with no text or file in {share_id}.")
                    return

                # IndexMsg 使用 share_id 并包含 filename
                msg = IndexMsg(content=msg_text or "", url=url, chat_id=share_id, post_time=post_time, sender=sender or "", filename=filename)
                # 更新最新消息缓存 (使用 share_id 作为 key)
                if share_id not in self.newest_msg or msg.post_time >= self.newest_msg[share_id].post_time:
                    self.newest_msg[share_id] = msg
                    self._logger.debug(f"Updated newest cache for {share_id} to {url}")
                try:
                    # 添加文档到索引
                    self._indexer.add_document(msg)
                except Exception as e:
                    self._logger.error(f"Error adding doc {url} to index: {e}", exc_info=True)
            except Exception as e:
                # 顶层异常处理
                chat_id_repr = getattr(event, 'chat_id', 'N/A')
                self._logger.error(f"Error processing new message in chat {chat_id_repr}: {e}", exc_info=True)

        # --- 处理消息编辑 ---
        @self.session.on(events.MessageEdited())
        async def client_message_update_handler(event: events.MessageEdited.Event):
            message = event.message
            if not hasattr(event, 'chat_id') or event.chat_id is None or not message: return

            try:
                # 检查是否监控此 chat_id
                if not self._should_monitor(event.chat_id): return
                # 获取 share_id
                share_id = get_share_id(event.chat_id)

                # 编辑处理逻辑
                url = f'https://t.me/c/{share_id}/{message.id}' # URL 使用 share_id
                new_msg_text = escape_content(message.text.strip()) if message.text else ''
                self._logger.info(f'Msg {url} edited in chat {share_id}. Checking for update...')

                try:
                    # 使用 URL (唯一标识) 查询旧文档
                    old_fields = self._indexer.get_document_fields(url=url)
                    if old_fields:
                        # 检查内容是否实际改变 (忽略文件变化)
                        if old_fields.get('content') == new_msg_text:
                            self._logger.debug(f"Edit event {url} has same text content, skipping index update.")
                            return

                        # 准备更新的字段
                        new_fields = old_fields.copy()
                        new_fields['content'] = new_msg_text
                        # 确保关键字段存在且类型正确
                        new_fields['chat_id'] = str(share_id) # 更新为当前 share_id (以防万一)
                        old_time = old_fields.get('post_time')
                        # 保留原始发帖时间，除非无法获取或类型错误
                        new_fields['post_time'] = old_time if isinstance(old_time, datetime) else (message.date or datetime.now())
                        if not isinstance(new_fields['post_time'], datetime): new_fields['post_time'] = datetime.now() # 再次确保是 datetime
                        # 尝试保留原始发送者，否则重新获取
                        new_fields.setdefault('sender', old_fields.get('sender', await self._get_sender_name(message) or ''))
                        # 保留文件名等信息（重要：编辑事件不更新文件信息）
                        new_fields['filename'] = old_fields.get('filename') # 保持旧的文件名
                        new_fields['has_file'] = old_fields.get('has_file', 0) # 保持旧的文件状态
                        new_fields['url'] = url # 确保 URL 正确

                        # 执行替换操作
                        self._indexer.replace_document(url=url, new_fields=new_fields)
                        self._logger.info(f'Updated msg content in index for {url}')

                        # 更新最新消息缓存（如果被编辑的是最新消息）
                        if share_id in self.newest_msg and self.newest_msg[share_id].url == url:
                             try:
                                 # 使用更新后的字段重建 IndexMsg 用于缓存
                                 rebuilt_msg = IndexMsg(
                                     content=new_fields['content'], url=new_fields['url'],
                                     chat_id=share_id, # 直接使用 share_id
                                     post_time=new_fields['post_time'], # 已经是 datetime
                                     sender=new_fields['sender'], filename=new_fields['filename']
                                 )
                                 self.newest_msg[share_id] = rebuilt_msg
                                 self._logger.debug(f"Updated newest cache content for {url}")
                             except (ValueError, KeyError, TypeError) as cache_e:
                                 self._logger.error(f"Error reconstructing IndexMsg for cache update {url}: {cache_e}. Fields: {new_fields}")
                    else:
                         # 如果旧文档不存在，视为新消息添加（仅当有内容时）
                         self._logger.warning(f'Edited msg {url} not found in index. Adding as new message.')
                         if new_msg_text: # 确保编辑后有文本内容才添加
                             sender = await self._get_sender_name(message)
                             post_time = message.date or datetime.now()
                             if not isinstance(post_time, datetime): post_time = datetime.now()
                             # 编辑事件通常不带文件信息，设为 None
                             filename = None

                             # 使用 share_id 创建新消息
                             msg = IndexMsg(content=new_msg_text, url=url, chat_id=share_id, post_time=post_time, sender=sender or "", filename=filename)
                             self._indexer.add_document(msg)
                             # 更新最新消息缓存
                             if share_id not in self.newest_msg or msg.post_time >= self.newest_msg[share_id].post_time:
                                 self.newest_msg[share_id] = msg
                                 self._logger.debug(f"Added edited msg {url} as newest cache for {share_id}")
                         else:
                             self._logger.debug(f"Ignoring edited message {url} with empty content and not found in index.")
                except Exception as e:
                    # 处理更新/添加过程中的错误
                    self._logger.error(f'Error updating/adding edited msg {url} in index: {e}', exc_info=True)
            except Exception as e:
                # 处理编辑事件顶层错误
                chat_id_repr = getattr(event, 'chat_id', 'N/A')
                self._logger.error(f"Error processing edited message in chat {chat_id_repr}: {e}", exc_info=True)

        # --- 处理消息删除 ---
        @self.session.on(events.MessageDeleted())
        async def client_message_delete_handler(event: events.MessageDeleted.Event):
            # 检查 chat_id
            if not hasattr(event, 'chat_id') or event.chat_id is None:
                self._logger.debug(f"Ignoring deletion event with no chat_id. Deleted IDs: {event.deleted_ids}")
                return
            # 检查是否有删除的 ID
            if not event.deleted_ids:
                 self._logger.debug(f"Ignoring deletion event with empty deleted_ids list in chat {event.chat_id}.")
                 return

            try:
                # 检查是否监控
                if not self._should_monitor(event.chat_id):
                    self._logger.debug(f"Ignoring deletion event from non-monitored chat {event.chat_id}. Deleted IDs: {event.deleted_ids}")
                    return
                # 获取 share_id
                share_id = get_share_id(event.chat_id)

                # 删除处理逻辑
                deleted_count_in_batch = 0
                # 使用 share_id 构建 URL
                urls_to_delete = [f'https://t.me/c/{share_id}/{mid}' for mid in event.deleted_ids]
                self._logger.info(f"Processing deletion of {len(urls_to_delete)} message(s) in chat {share_id}: IDs {event.deleted_ids}")

                try:
                     # 使用批量写入/删除模式
                     with self._indexer.ix.writer() as writer:
                          for url in urls_to_delete:
                               # 更新最新消息缓存
                               if share_id in self.newest_msg and self.newest_msg[share_id].url == url:
                                    del self.newest_msg[share_id]
                                    self._logger.info(f"Removed newest cache for {share_id} due to deletion of {url}.")
                               # 执行删除
                               try:
                                    # 使用 URL 删除
                                    count = writer.delete_by_term('url', url)
                                    if count > 0:
                                        deleted_count_in_batch += count
                                        self._logger.debug(f"Deleted msg {url} from index (count: {count}).")
                                    # else: 消息本就不在索引中，无需记录
                               except Exception as del_e:
                                    self._logger.error(f"Error deleting doc {url} from index within writer: {del_e}")
                     # 提交批量删除
                     if deleted_count_in_batch > 0:
                         self._logger.info(f'Finished deleting {deleted_count_in_batch} msgs from index for chat {share_id}')
                     else:
                         self._logger.info(f"No matching messages found in index to delete for chat {share_id} batch (URLs: {urls_to_delete}).")
                except writing.LockError:
                    # 处理索引锁定错误
                    self._logger.error(f"Index locked. Could not process deletions batch for {share_id}: {urls_to_delete}")
                except Exception as e:
                    # 处理其他批量删除错误
                    self._logger.error(f"Error processing deletions batch for {share_id}: {e}", exc_info=True)
            except Exception as e:
                # 处理删除事件顶层错误
                chat_id_repr = getattr(event, 'chat_id', 'N/A')
                self._logger.error(f"Error processing deleted event in chat {chat_id_repr}: {e}", exc_info=True)

        # 标记事件处理器注册完成（确保只在方法末尾执行一次）
        self._logger.info("Telethon event handlers registered.")


    async def add_chats_to_monitoring(self, chat_ids: List[int]) -> Tuple[Set[int], Dict[int, str]]:
        """
        将指定的对话 ID (share_id) 添加到实时监控列表。
        注意：此方法仅在内存中添加，重启后若对话无索引消息，监控会丢失。

        :param chat_ids: 需要添加到监控的 share_id 列表。
        :return: 一个元组，包含:
                 - 成功添加到监控列表的 share_id 集合。
                 - 一个字典，键为添加失败的 share_id，值为失败原因字符串。
        """
        added_ok = set()
        add_failed = {}

        if not chat_ids:
            return added_ok, add_failed

        for chat_id in chat_ids:
            if not isinstance(chat_id, int):
                add_failed[chat_id] = "无效的 ID 类型" # 技术上 chat_id 传入是 int，但保持检查
                continue

            if chat_id in self.excluded_chats:
                add_failed[chat_id] = "对话已被排除"
                self._logger.info(f"[Monitoring] Ignoring request to monitor excluded chat {chat_id}")
                continue

            if chat_id in self.monitored_chats:
                add_failed[chat_id] = "已在监控中"
                self._logger.debug(f"[Monitoring] Chat {chat_id} is already monitored.")
                continue

            # 尝试添加到内存中的监控列表
            self.monitored_chats.add(chat_id)
            added_ok.add(chat_id)
            self._logger.info(f"[Monitoring] Added chat {chat_id} to monitoring list via /monitor_chat.")
            # 检查此 chat 是否存在于 newest_msg 缓存中，如果不存在，尝试加载
            if chat_id not in self.newest_msg:
                 self._logger.debug(f"Chat {chat_id} added to monitoring, checking for newest message in index...")
                 try:
                      result = self._indexer.search(q_str='*', in_chats=[chat_id], page_len=1, page_num=1, file_filter="all")
                      if result.hits:
                           self.newest_msg[chat_id] = result.hits[0].msg
                           self._logger.debug(f"Loaded newest message for newly monitored chat {chat_id}: {result.hits[0].msg.url}")
                 except Exception as e:
                      self._logger.warning(f"Failed to load newest message for newly monitored chat {chat_id}: {e}")


        return added_ok, add_failed
