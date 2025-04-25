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


    async def download_history(self, chat_id: int, min_id: int, max_id: int, call_back: Optional[callable] = None):
        """
        下载指定对话的历史记录并添加到索引。

        :param chat_id: 原始对话 ID (将被转换为 share_id)。
        :param min_id: 要下载的最小消息 ID (不包括)。
        :param max_id: 要下载的最大消息 ID (0 表示无上限，会获取比 min_id 更新的所有消息)。
        :param call_back: 可选的回调函数，用于报告进度 (接收 cur_id, dl_count)。
        """
        try:
            share_id = get_share_id(chat_id) # 转换为 share_id
        except Exception as e:
            self._logger.error(f"Invalid chat_id format for download: {chat_id}, error: {e}")
            raise EntityNotFoundError(f"无效的对话 ID 格式: {chat_id}") # 抛出特定的错误类型

        self._logger.info(f'Downloading history for {share_id} (raw_id={chat_id}, min={min_id}, max={max_id})')
        # 检查对话是否在排除列表中
        if share_id in self.excluded_chats:
            self._logger.warning(f"Skipping download for excluded chat {share_id}.")
            raise ValueError(f"对话 {share_id} 已被排除，无法下载。") # 抛出 ValueError 表示操作不允许

        # 如果该对话不在当前的监控列表中，则添加它
        if share_id not in self.monitored_chats:
            self.monitored_chats.add(share_id)
            self._logger.info(f"Added chat {share_id} to monitored list during download.")

        msg_list: List[IndexMsg] = [] # 存储从 Telegram 获取并准备索引的消息
        downloaded_count: int = 0 # 实际构造了 IndexMsg 的消息数量
        processed_count: int = 0 # Telethon `iter_messages` 返回的总项目数

        try:
            # 使用 Telethon 异步迭代指定对话的消息历史
            # limit=None 表示尽可能获取所有匹配的消息
            # max_id=0 表示没有上限ID，min_id=0 表示没有下限ID (获取最新到最旧)
            # 如果 max_id > 0, 获取 ID < max_id 的消息
            # 如果 min_id > 0, 获取 ID > min_id 的消息
            async for tg_message in self.session.iter_messages(entity=share_id, min_id=min_id, max_id=max_id, limit=None, reverse=True): # reverse=True 确保从旧到新处理，便于确定 newest_msg
                processed_count += 1
                # 确保处理的是有效的 Message 对象
                if not isinstance(tg_message, TgMessage): continue

                # 构造消息的永久链接
                url = f'https://t.me/c/{share_id}/{tg_message.id}'
                # 获取发送者名称
                sender = await self._get_sender_name(tg_message)
                # 获取发送时间，确保是 datetime 对象
                post_time = tg_message.date
                if not isinstance(post_time, datetime):
                    self._logger.warning(f"Message {url} has invalid date type {type(post_time)}, using current time.")
                    post_time = datetime.now()

                # 提取消息文本和文件名
                msg_text, filename = '', None
                # 如果消息包含文件且文件名有效
                if tg_message.file and hasattr(tg_message.file, 'name') and tg_message.file.name:
                    filename = tg_message.file.name
                    # 文件也可能附带文本标题/描述
                    if tg_message.text:
                        msg_text = escape_content(tg_message.text.strip())
                # 如果消息只有文本
                elif tg_message.text:
                    msg_text = escape_content(tg_message.text.strip())

                # 只有当消息包含有效文本或有效文件名时，才创建 IndexMsg
                if msg_text or filename:
                    try:
                        msg = IndexMsg(content=msg_text or "", url=url, chat_id=share_id, post_time=post_time, sender=sender or "", filename=filename)
                        msg_list.append(msg)
                        downloaded_count += 1
                    except Exception as create_e:
                        # 记录创建 IndexMsg 时的错误
                        self._logger.error(f"Error creating IndexMsg for {url}: {create_e}")

                # 定期调用回调函数报告进度 (例如每处理 100 条消息)
                if call_back and processed_count % 100 == 0:
                     try:
                         # 使用 await 调用可能异步的回调函数
                         await call_back(tg_message.id, downloaded_count)
                     except Exception as cb_e:
                         # 记录回调函数执行中的错误
                         self._logger.warning(f"Error in download callback: {cb_e}")
                # 定期释放事件循环，防止长时间下载阻塞其他异步任务
                if processed_count % 500 == 0:
                    await asyncio.sleep(0.01)

        # --- 处理下载过程中可能发生的特定 Telethon 错误 ---
        except telethon.errors.rpcerrorlist.ChannelPrivateError as e:
            self._logger.error(f"Permission denied for chat {share_id}. Is the backend account a member? Error: {e}")
            self.monitored_chats.discard(share_id) # 移除无法访问的对话
            raise EntityNotFoundError(f"无法访问对话 {chat_id}，请确保后端账号是其成员。") from e
        except (telethon.errors.rpcerrorlist.ChatIdInvalidError, telethon.errors.rpcerrorlist.PeerIdInvalidError):
            self._logger.error(f"Chat ID {share_id} (raw: {chat_id}) invalid or peer not found.")
            self.monitored_chats.discard(share_id)
            raise EntityNotFoundError(f"无效对话 ID 或无法找到 Peer: {chat_id}")
        except ValueError as e:
             # Telethon 在找不到实体时可能抛出 ValueError
             if "Cannot find any entity corresponding to" in str(e) or "Could not find the input entity for" in str(e):
                 self._logger.error(f"Cannot find entity for chat {share_id} (raw: {chat_id}). Error: {e}")
                 self.monitored_chats.discard(share_id)
                 raise EntityNotFoundError(f"无法找到对话实体: {chat_id}") from e
             else:
                 # 其他类型的 ValueError
                 self._logger.error(f"ValueError iterating messages for {share_id}: {e}", exc_info=True)
                 raise # 重新抛出，可能是其他问题
        except Exception as e:
            # 捕获并记录其他未预料到的异常
            self._logger.error(f"Error iterating messages for {share_id}: {e}", exc_info=True)
            raise RuntimeError(f"下载对话 {share_id} 时发生未知错误") # 抛出通用运行时错误

        self._logger.info(f'History fetch complete for {share_id}: {downloaded_count} messages to index out of {processed_count} processed.')
        # 如果没有下载到任何可索引的消息，则提前返回
        if not msg_list:
            self._logger.info(f"No indexable messages found for chat {share_id} in the specified range.")
            return

        # --- 批量写入索引 ---
        writer: Optional[IndexWriter] = None
        newest_msg_in_batch: Optional[IndexMsg] = None # 记录此批次中最新的消息
        indexed_count_in_batch: int = 0
        try:
            # 获取 Whoosh IndexWriter 用于批量写入
            writer = self._indexer.ix.writer()
            # 迭代已下载的消息列表
            for i, msg in enumerate(msg_list):
                try:
                    # 调用 indexer 的 add_document 方法，传入 writer
                    self._indexer.add_document(msg, writer)
                    indexed_count_in_batch += 1
                    # 更新此批次中遇到的最新消息 (因为是从旧到新处理，最后一个就是最新的)
                    newest_msg_in_batch = msg
                    # 批量写入时也避免长时间阻塞事件循环
                    if i > 0 and i % 1000 == 0:
                        self._logger.debug(f"Batch write progress for {share_id}: {i} messages added...")
                        await asyncio.sleep(0.01)
                except Exception as add_e:
                    # 记录添加单个文档到 writer 时的错误
                    self._logger.error(f"Error adding document {msg.url} to batch writer: {add_e}")
            # 提交整个批次的写入操作
            writer.commit()
            self._logger.info(f'Write index commit ok for {indexed_count_in_batch} messages from chat {share_id}')
            # 更新该对话的最新消息缓存
            if newest_msg_in_batch:
                 current_chat_id = int(newest_msg_in_batch.chat_id) # 获取 share_id
                 # 只有当新消息比缓存中的更新时才更新缓存
                 if current_chat_id not in self.newest_msg or newest_msg_in_batch.post_time > self.newest_msg[current_chat_id].post_time:
                      self.newest_msg[current_chat_id] = newest_msg_in_batch
                      self._logger.debug(f"Updated newest msg cache for {current_chat_id} to {newest_msg_in_batch.url}")
        except writing.LockError:
            # 如果在获取 writer 或提交时索引被锁定
            self._logger.error("Index is locked during batch write. Downloaded messages are lost.")
            # 最好显式取消 writer (如果已获取)
            if writer and not writer.is_closed:
                try: writer.cancel()
                except Exception as cancel_e: self._logger.error(f"Error cancelling writer after lock: {cancel_e}")
            raise RuntimeError("Index is locked, cannot write downloaded messages.")
        except Exception as e:
            # 记录批量写入过程中发生的其他错误
            self._logger.error(f"Error writing batch index for {share_id}: {e}", exc_info=True)
            if writer and not writer.is_closed:
                try: writer.cancel()
                except Exception as cancel_e: self._logger.error(f"Error cancelling writer after general error: {cancel_e}")
            raise RuntimeError(f"写入索引时出错 for {share_id}")


    def clear(self, chat_ids: Optional[List[int]] = None):
        """
        清除索引数据。

        :param chat_ids: 可选，要清除的 chat_id 列表 (原始ID)。如果为 None，则清除所有索引。
        """
        if chat_ids is not None:
            # 清除指定对话的数据
            # 将原始 chat_id 转换为 share_id 集合，并处理可能的转换错误
            share_ids_to_clear = set()
            invalid_inputs = []
            for cid in chat_ids:
                try: share_ids_to_clear.add(get_share_id(cid))
                except Exception: invalid_inputs.append(str(cid))

            if invalid_inputs:
                self._logger.warning(f"Invalid chat IDs provided for clearing: {', '.join(invalid_inputs)}")
            if not share_ids_to_clear:
                self._logger.warning("No valid share IDs to clear.")
                return

            self._logger.info(f"Attempting to clear index data for chats: {share_ids_to_clear}")
            try:
                # 使用 Whoosh writer 按 'chat_id' 字段删除文档
                with self._indexer.ix.writer() as w:
                    total_deleted = 0
                    for share_id in share_ids_to_clear:
                        # Whoosh 的 delete_by_term 需要字段名和要匹配的值 (字符串形式)
                        deleted_count = w.delete_by_term('chat_id', str(share_id))
                        total_deleted += deleted_count
                        # 从监控列表和最新消息缓存中移除
                        self.monitored_chats.discard(share_id)
                        self.newest_msg.pop(share_id, None)
                        self._logger.info(f'Cleared {deleted_count} docs and stopped monitoring chat {share_id}')
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
                self.monitored_chats.clear()
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
        """获取后端索引状态的文本描述 (修正计数和错误处理逻辑)"""
        cur_len = 0
        sb = [] # 使用列表存储字符串片段，最后 join
        searcher = None # 初始化 searcher 变量

        # 1. 获取总文档数
        try:
            total_docs = self._indexer.ix.doc_count()
        except Exception as e:
            total_docs = -1 # 标记获取失败
            self._logger.error(f"Failed get total doc count: {e}")
        # 添加头部信息 (后端 ID, 会话名, 总消息数)
        sb.append(f'后端 "{self.id}" (会话: "{self.session.name}") 总消息: <b>{total_docs if total_docs >= 0 else "[获取失败]"}</b>\n\n')

        # 定义超出长度限制时的提示信息
        overflow_msg = f'\n\n(部分信息因长度限制未显示)'

        # 辅助函数：检查添加新内容是否会超出长度限制
        def append_msg(msg_list: List[str]) -> bool:
            nonlocal cur_len
            new_len = sum(len(msg) for msg in msg_list)
            # 检查当前长度加上新内容长度是否超过限制 (留有余地)
            if cur_len + new_len > length_limit - len(overflow_msg) - 50:
                return True # 返回 True 表示超出限制
            # 如果未超出，则增加当前长度并添加内容
            cur_len += new_len
            sb.extend(msg_list)
            return False # 返回 False 表示未超出限制

        # 2. 显示排除列表
        if self.excluded_chats:
            excluded_list = sorted(list(self.excluded_chats)) # 排序以保持一致性
            # 尝试添加排除列表的标题
            if append_msg([f'{len(excluded_list)} 个对话被禁止索引:\n']):
                sb.append(overflow_msg); return ''.join(sb) # 超出则添加提示并返回
            # 逐个添加被排除的对话信息
            for chat_id in excluded_list:
                try:
                    # 格式化对话的 HTML 表示
                    chat_html = await self.format_dialog_html(chat_id)
                except Exception:
                    # 获取名称出错时的后备显示
                    chat_html = f"对话 `{chat_id}` (获取名称出错)"
                # 尝试添加单个对话的信息
                if append_msg([f'- {chat_html}\n']):
                    sb.append(overflow_msg); return ''.join(sb) # 超出则添加提示并返回
            # 添加一个空行分隔
            if sb and sb[-1] != '\n\n': sb.append('\n')

        # 3. 显示监控列表和计数
        monitored_chats_list = sorted(list(self.monitored_chats)) # 排序
        # 尝试添加监控列表的标题
        if append_msg([f'总计 {len(monitored_chats_list)} 个对话被加入了索引:\n']):
            sb.append(overflow_msg); return ''.join(sb) # 超出则添加提示并返回

        # 4. 获取每个监控对话的详细信息
        detailed_status_error = None # 用于存储获取详细状态期间遇到的第一个错误信息
        try:
             # 在循环外打开 searcher 以提高效率，并确保关闭
             searcher = self._indexer.ix.searcher()
             # 迭代监控的对话列表
             for chat_id in monitored_chats_list:
                 msg_for_chat = [] # 存储当前对话的输出片段
                 num = -1 # 初始化计数为错误状态 (-1)
                 chat_id_str = str(chat_id) # Whoosh Term 需要字符串

                 # 尝试获取该对话的文档计数
                 try:
                     query = Term('chat_id', chat_id_str) # 构建查询
                     # 使用 searcher.doc_count 获取匹配文档数
                     num = searcher.doc_count(query=query)
                 except searching.SearchError as search_e:
                     # 记录 Whoosh 特定的搜索错误
                     self._logger.error(f"Whoosh SearchError counting docs for chat {chat_id_str}: {search_e}", exc_info=True)
                     # 如果是第一个错误，记录错误类型
                     if not detailed_status_error: detailed_status_error = "部分对话计数失败 (SearchError)"
                 except Exception as e:
                     # 记录其他计数错误
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

                 # 添加该对话的最新消息信息（如果存在于缓存中）
                 if newest_msg := self.newest_msg.get(chat_id):
                     # 准备显示内容：优先文件名，然后是简短内容
                     display_parts = []
                     if newest_msg.filename: display_parts.append(f"📎 {newest_msg.filename}")
                     if newest_msg.content: display_parts.append(brief_content(newest_msg.content)) # 使用 brief_content 限制长度
                     display = " ".join(display_parts) if display_parts else "(空消息)"
                     esc_display = html.escape(display) # HTML 转义显示内容
                     # 安全地格式化时间
                     time_str = newest_msg.post_time.strftime("%y-%m-%d %H:%M") if isinstance(newest_msg.post_time, datetime) else "[未知时间]"
                     # 组合最新消息的 HTML 行
                     msg_for_chat.append(f'  最新: <a href="{html.escape(newest_msg.url)}">{esc_display}</a> (@{time_str})\n')

                 # 检查长度并尝试添加当前对话的所有信息
                 if append_msg(msg_for_chat):
                     sb.append(overflow_msg); break # 超出则添加提示并跳出循环

             # 如果循环正常结束，但期间发生了计数错误，尝试添加错误提示
             if detailed_status_error and not (sb and sb[-1].endswith(overflow_msg)):
                 if append_msg([f"\n警告: {detailed_status_error}\n"]):
                     sb.append(overflow_msg)

        except writing.LockError: # 处理打开 searcher 时的锁错误
             self._logger.error(f"Index locked, failed to open searcher for status.")
             # 添加特定的锁定错误信息
             if append_msg(["\n错误：索引被锁定，无法获取详细对话状态。\n"]):
                 sb.append(overflow_msg)
        except Exception as e: # 处理打开 searcher 或其他外部错误
             self._logger.error(f"Failed to get detailed status: {e}", exc_info=True)
             # 添加通用的详细状态获取错误信息
             if append_msg(["\n错误：无法获取详细状态。\n"]):
                 sb.append(overflow_msg)
        finally:
            # 确保 searcher 对象在使用后被关闭，释放资源
            if searcher:
                searcher.close()
                self._logger.debug("Searcher closed after getting index status.")
        # --- 结束详细信息获取 ---

        # 将所有片段连接成最终的字符串并返回
        return ''.join(sb)


    async def translate_chat_id(self, chat_id: int) -> str:
        """使用会话将 Chat ID (share_id) 翻译为名称"""
        try:
            # 调用 session 的方法进行翻译
            return await self.session.translate_chat_id(int(chat_id))
        except (telethon.errors.rpcerrorlist.ChannelPrivateError, telethon.errors.rpcerrorlist.ChatIdInvalidError, ValueError, TypeError) as e:
            # 处理无法访问、无效 ID 或类型错误
            self._logger.warning(f"Could not translate chat_id {chat_id}: {type(e).__name__}")
            raise EntityNotFoundError(f"无法访问或无效 Chat ID: {chat_id}") # 抛出特定错误
        except EntityNotFoundError:
            # 如果 session 层直接抛出 EntityNotFoundError
            self._logger.warning(f"Entity not found for {chat_id} during translation.")
            raise # 重新抛出
        except Exception as e:
            # 处理翻译过程中的其他未知错误
            self._logger.error(f"Error translating chat_id {chat_id}: {e}")
            raise EntityNotFoundError(f"获取对话 {chat_id} 名称时出错") from e


    async def str_to_chat_id(self, chat: str) -> int:
        """将字符串（用户名、链接或 ID）转换为 share_id"""
        try:
            # 尝试直接将字符串转换为整数 ID，然后获取 share_id
            return get_share_id(int(chat))
        except ValueError:
            # 如果不是纯数字 ID，则使用 session 的方法进行解析
            try:
                return get_share_id(await self.session.str_to_chat_id(chat))
            except EntityNotFoundError:
                self._logger.warning(f"Entity not found for '{chat}' using session.")
                raise # 重新抛出 EntityNotFoundError
            except Exception as e_inner:
                self._logger.error(f"Error converting '{chat}' to chat_id via session: {e_inner}")
                raise EntityNotFoundError(f"解析 '{chat}' 时出错") from e_inner
        except Exception as e_outer:
            # 处理 get_share_id 或 int() 可能的其他错误
            self._logger.error(f"Error converting '{chat}' to chat_id directly: {e_outer}")
            raise EntityNotFoundError(f"解析 '{chat}' 时出错") from e_outer


    async def format_dialog_html(self, chat_id: int) -> str:
        """格式化对话的 HTML 链接和名称，包含 share_id"""
        try:
            # 获取对话名称
            name = await self.translate_chat_id(int(chat_id))
            esc_name = html.escape(name) # 对名称进行 HTML 转义
            # 创建指向对话第一条消息的链接 (通常用于跳转到对话)
            # 同时显示转义后的名称和原始 share_id
            return f'<a href="https://t.me/c/{chat_id}/1">{esc_name}</a> (`{chat_id}`)'
        except EntityNotFoundError:
            # 如果找不到对话实体
            return f'未知对话 (`{chat_id}`)'
        except ValueError:
            # 如果传入的 chat_id 格式无效
            return f'无效对话 ID (`{chat_id}`)'
        except Exception as e:
            # 处理获取名称或格式化过程中的其他错误
            self._logger.warning(f"Error formatting html for {chat_id}: {e}")
            return f'对话 `{chat_id}` (获取名称出错)'


    def _should_monitor(self, chat_id: int) -> bool:
        """判断是否应该监控此对话的消息 (基于配置和监控列表)"""
        try:
            share_id = get_share_id(chat_id) # 转换为 share_id
            # 如果对话在排除列表中，则不监控
            if share_id in self.excluded_chats:
                return False
            # 如果配置了 monitor_all=True，或者该对话在当前的监控列表中，则监控
            return self._cfg.monitor_all or (share_id in self.monitored_chats)
        except Exception as e:
            # 处理获取 share_id 可能的错误，出错则不监控
            self._logger.warning(f"Error determining monitor status for chat {chat_id}: {e}")
            return False


    @staticmethod
    async def _get_sender_name(message: TgMessage) -> str:
        """获取消息发送者的名称（用户或频道/群组标题）"""
        try:
            # 异步获取发送者实体
            sender = await message.get_sender()
            if isinstance(sender, User):
                # 如果是普通用户，使用 format_entity_name 格式化名称
                return format_entity_name(sender)
            else:
                # 如果是频道或群组，尝试获取其标题
                return getattr(sender, 'title', '') # 使用 getattr 避免属性不存在错误
        except Exception as e:
            # 获取发送者可能失败（例如匿名管理员、机器人等）
            logger.debug(f"Could not get sender name for message {getattr(message, 'id', 'N/A')}: {e}")
            return '' # 出错时返回空字符串


    def _register_hooks(self):
        """注册 Telethon 事件钩子，用于实时接收和处理消息"""
        self._logger.info("Registering Telethon event handlers...")

        # --- 处理新消息 ---
        @self.session.on(events.NewMessage())
        async def client_message_handler(event: events.NewMessage.Event):
            # 基础检查：确保有 chat_id 且需要监控
            if not hasattr(event, 'chat_id') or event.chat_id is None or not self._should_monitor(event.chat_id):
                return # 不处理不监控的对话

            try:
                share_id = get_share_id(event.chat_id) # 获取 share_id
                # 构造消息链接
                url = f'https://t.me/c/{share_id}/{event.id}'
                # 获取发送者名称和发送时间
                sender = await self._get_sender_name(event.message)
                post_time = event.message.date
                # 再次确保 post_time 是 datetime 对象
                if not isinstance(post_time, datetime):
                    self._logger.warning(f"New message {url} has invalid date type {type(post_time)}, using current time.")
                    post_time = datetime.now()

                # 提取文本和文件名
                msg_text, filename = '', None
                # 处理带文件的消息
                if event.message.file and hasattr(event.message.file, 'name') and event.message.file.name:
                    filename = event.message.file.name
                    # 文件可能附带标题
                    if event.message.text:
                        msg_text = escape_content(event.message.text.strip())
                    # 记录收到的文件信息
                    self._logger.info(f'New file {url} from "{sender}" in chat {share_id}: "{filename}" Caption:"{brief_content(msg_text)}"')
                # 处理纯文本消息
                elif event.message.text:
                    msg_text = escape_content(event.message.text.strip())
                    # 忽略完全是空白字符的消息
                    if not msg_text:
                        self._logger.debug(f"Ignoring empty/whitespace message {url} in {share_id}.")
                        return
                    # 记录收到的文本消息信息
                    self._logger.info(f'New msg {url} from "{sender}" in chat {share_id}: "{brief_content(msg_text)}"')
                else:
                    # 忽略既无文本也无有效文件的消息 (例如贴纸、服务消息等)
                    self._logger.debug(f"Ignoring message {url} with no text or file in {share_id}.")
                    return

                # 创建 IndexMsg 对象
                msg = IndexMsg(content=msg_text or "", url=url, chat_id=share_id, post_time=post_time, sender=sender or "", filename=filename)
                # 更新该对话的最新消息缓存
                if share_id not in self.newest_msg or msg.post_time >= self.newest_msg[share_id].post_time: # Use >= to handle same-time edits?
                    self.newest_msg[share_id] = msg
                    self._logger.debug(f"Updated newest cache for {share_id} to {url}")
                # 将新消息添加到索引
                try:
                    self._indexer.add_document(msg)
                except Exception as e:
                    self._logger.error(f"Error adding doc {url} to index: {e}", exc_info=True)
            except Exception as e:
                # 记录处理新消息过程中的顶层错误
                chat_id_repr = getattr(event, 'chat_id', 'N/A') # 安全获取 chat_id
                self._logger.error(f"Error processing new message in chat {chat_id_repr}: {e}", exc_info=True)

        # --- 处理消息编辑 ---
        @self.session.on(events.MessageEdited())
        async def client_message_update_handler(event: events.MessageEdited.Event):
            # 基础检查
            if not hasattr(event, 'chat_id') or event.chat_id is None or not self._should_monitor(event.chat_id):
                return

            try:
                share_id = get_share_id(event.chat_id)
                url = f'https://t.me/c/{share_id}/{event.id}'
                # 编辑事件只关心文本内容的变化，假定文件不会在编辑时改变
                new_msg_text = escape_content(event.message.text.strip()) if event.message.text else ''
                self._logger.info(f'Msg {url} edited in chat {share_id}. Checking for update...')

                try:
                    # 尝试从索引中获取旧的文档字段
                    old_fields = self._indexer.get_document_fields(url=url)
                    if old_fields:
                        # 如果文本内容没有变化，则跳过更新
                        if old_fields.get('content') == new_msg_text:
                            self._logger.debug(f"Edit event {url} has same content, skipping index update.")
                            return
                        # 内容有变化，准备更新字段
                        new_fields = old_fields.copy()
                        new_fields['content'] = new_msg_text # 更新 content 字段

                        # 确保其他必要字段存在并类型正确 (尽量使用旧值)
                        new_fields.setdefault('chat_id', str(share_id)) # 确保是字符串
                        # 处理 post_time，如果旧值无效或不存在，使用编辑事件的时间
                        old_time = old_fields.get('post_time')
                        new_fields['post_time'] = old_time if isinstance(old_time, datetime) else (event.message.date or datetime.now())
                        if not isinstance(new_fields['post_time'], datetime): new_fields['post_time'] = datetime.now() # Final fallback
                        new_fields.setdefault('sender', old_fields.get('sender', await self._get_sender_name(event.message) or ''))
                        new_fields.setdefault('filename', old_fields.get('filename', None)) # 保留旧文件名
                        new_fields.setdefault('url', url) # 确保 URL 正确
                        # 根据 filename 是否存在重新计算 has_file 标志
                        new_fields['has_file'] = 1 if new_fields.get('filename') else 0

                        # 使用 Indexer 的 replace_document 方法更新文档
                        self._indexer.replace_document(url=url, new_fields=new_fields)
                        self._logger.info(f'Updated msg content in index for {url}')

                        # 如果被编辑的是缓存中的最新消息，更新缓存
                        if share_id in self.newest_msg and self.newest_msg[share_id].url == url:
                             try:
                                 # 使用更新后的字段重新构造 IndexMsg 对象更新缓存
                                 self.newest_msg[share_id] = IndexMsg(**new_fields)
                                 self._logger.debug(f"Updated newest cache content for {url}")
                             except Exception as cache_e:
                                 # 记录更新缓存时的错误
                                 self._logger.error(f"Error reconstructing IndexMsg for cache update {url}: {cache_e}")
                    else:
                         # 如果编辑的消息在索引中找不到（可能发生在启动时未完全索引或中途添加监控）
                         # 将其作为新消息添加到索引
                         self._logger.warning(f'Edited msg {url} not found in index. Adding as new message.')
                         sender = await self._get_sender_name(event.message)
                         post_time = event.message.date or datetime.now()
                         if not isinstance(post_time, datetime): post_time = datetime.now()
                         filename = None # 假设编辑不改变文件信息

                         # 只在有新文本内容时添加
                         if new_msg_text:
                             msg = IndexMsg(content=new_msg_text, url=url, chat_id=share_id, post_time=post_time, sender=sender or "", filename=filename)
                             self._indexer.add_document(msg)
                             # 尝试更新最新消息缓存
                             if share_id not in self.newest_msg or msg.post_time >= self.newest_msg[share_id].post_time:
                                 self.newest_msg[share_id] = msg
                                 self._logger.debug(f"Added edited msg {url} as newest cache for {share_id}")
                         else:
                             self._logger.debug(f"Ignoring edited message {url} with empty content and not found in index.")

                except Exception as e:
                    # 记录更新或添加编辑消息到索引时的错误
                    self._logger.error(f'Error updating/adding edited msg {url} in index: {e}', exc_info=True)
            except Exception as e:
                # 记录处理编辑事件的顶层错误
                chat_id_repr = getattr(event, 'chat_id', 'N/A')
                self._logger.error(f"Error processing edited message in chat {chat_id_repr}: {e}", exc_info=True)

        # --- 处理消息删除 ---
        @self.session.on(events.MessageDeleted())
        async def client_message_delete_handler(event: events.MessageDeleted.Event):
            # 基础检查：确保有 chat_id 且需要监控
            # 注意：MessageDeleted.Event 可能 chat_id 为 None，如果删除发生在私聊或未知上下文
            if not hasattr(event, 'chat_id') or event.chat_id is None or not self._should_monitor(event.chat_id):
                 # 记录忽略的删除事件信息
                 self._logger.debug(f"Ignoring deletion event (Chat ID: {getattr(event, 'chat_id', 'None')}, "
                                    f"Monitored: {self._should_monitor(getattr(event, 'chat_id', 0))}). " # Check monitor status safely
                                    f"Deleted IDs: {event.deleted_ids}")
                 return

            try:
                share_id = get_share_id(event.chat_id)
                deleted_count_in_batch = 0
                # 为每个被删除的消息 ID 构造 URL
                urls_to_delete = [f'https://t.me/c/{share_id}/{mid}' for mid in event.deleted_ids]
                self._logger.info(f"Processing deletion of {len(urls_to_delete)} message(s) in chat {share_id}: {event.deleted_ids}")

                try:
                     # 使用 Whoosh writer 批量删除
                     with self._indexer.ix.writer() as writer:
                          for url in urls_to_delete:
                               # 检查被删除的消息是否是缓存中的最新消息
                               if share_id in self.newest_msg and self.newest_msg[share_id].url == url:
                                    # 如果是，从缓存中移除
                                    del self.newest_msg[share_id]
                                    self._logger.info(f"Removed newest cache for {share_id} due to deletion of {url}.")
                                    # 注意：这里没有重新加载第二新的消息，缓存会暂时为空，直到新消息到来或重启加载

                               # 尝试按 URL 删除文档
                               try:
                                    # delete_by_term 返回实际删除的文档数 (对于 unique 字段通常是 0 或 1)
                                    count = writer.delete_by_term('url', url)
                                    if count > 0:
                                        deleted_count_in_batch += count
                                        self._logger.info(f"Deleted msg {url} from index.")
                                    else:
                                        self._logger.debug(f"Message {url} requested for deletion not found in index.")
                               except Exception as del_e:
                                    # 记录删除单个文档时的错误
                                    self._logger.error(f"Error deleting doc {url} from index: {del_e}")
                     # 记录批量删除操作的结果
                     if deleted_count_in_batch > 0:
                         self._logger.info(f'Finished deleting {deleted_count_in_batch} msgs from index for chat {share_id}')
                     else:
                         self._logger.info(f"No matching messages found in index to delete for chat {share_id} batch.")
                except writing.LockError:
                    self._logger.error(f"Index locked. Could not process deletions batch for {share_id}: {urls_to_delete}")
                except Exception as e:
                    # 记录处理删除批次时的其他错误
                    self._logger.error(f"Error processing deletions batch for {share_id}: {e}", exc_info=True)
            except Exception as e:
                # 记录处理删除事件的顶层错误
                chat_id_repr = getattr(event, 'chat_id', 'N/A')
                self._logger.error(f"Error processing deleted event in chat {chat_id_repr}: {e}", exc_info=True)

        self._logger.info("Telethon event handlers registered.")
