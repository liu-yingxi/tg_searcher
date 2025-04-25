# -*- coding: utf-8 -*-
import html
from datetime import datetime
from typing import Optional, List, Set, Dict, Any, Union # 添加 Union

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
        # _load_newest_messages_on_startup 移到 start()


    def _load_newest_messages_on_startup(self):
         """启动时尝试为每个监控的聊天加载最新消息"""
         if not self.monitored_chats: return # 没有监控的聊天则跳过
         self._logger.info("Loading newest message for each monitored chat...")
         count = 0
         for chat_id in list(self.monitored_chats): # 迭代副本
              if chat_id in self.excluded_chats: continue
              try:
                   # 搜索该 chat_id 下按时间排序的第一条消息
                   result = self._indexer.search(q_str='*', in_chats=[chat_id], page_len=1, page_num=1)
                   if result.hits:
                        self.newest_msg[chat_id] = result.hits[0].msg
                        count += 1
              except Exception as e:
                   # 加载最新消息失败通常不严重，记录警告即可
                   self._logger.warning(f"Failed to load newest message for chat {chat_id}: {e}")
         self._logger.info(f"Finished loading newest messages for {count} chats.")


    async def start(self):
        self._logger.info(f'Starting backend bot {self.id}...')

        # 解析配置中可能是用户名的 exclude_chats
        resolved_excludes_in_cfg = set()
        for chat_id_or_name in self._raw_exclude_chats:
            # 只解析非整数的字符串
            if isinstance(chat_id_or_name, str) and not chat_id_or_name.lstrip('-').isdigit(): # 检查是否是纯数字字符串
                 try:
                      # 将用户名/链接等转换为 share_id
                      share_id = await self.str_to_chat_id(chat_id_or_name)
                      resolved_excludes_in_cfg.add(share_id)
                      self._logger.info(f"Resolved exclude chat '{chat_id_or_name}' to ID {share_id}")
                 except EntityNotFoundError:
                      # 如果找不到对应的实体，记录警告
                      self._logger.warning(f"Exclude chat '{chat_id_or_name}' not found, ignoring.")
                 except Exception as e:
                      # 其他解析错误
                      self._logger.error(f"Error resolving exclude chat '{chat_id_or_name}': {e}")

        # 合并解析结果 (整数 ID 在初始化时已加入)
        self.excluded_chats.update(resolved_excludes_in_cfg)
        self._logger.info(f"Final excluded chats for backend {self.id}: {self.excluded_chats or 'None'}")


        # 加载最新消息 (确保在检查监控列表前完成)
        self._load_newest_messages_on_startup()

        # 检查监控的聊天是否仍然有效和可访问
        chats_to_remove = set()
        for chat_id in list(self.monitored_chats): # 迭代副本，因为可能在循环中修改集合
            try:
                # 如果在排除列表里，则移除监控
                if chat_id in self.excluded_chats:
                     self._logger.info(f"Chat {chat_id} is excluded, removing from monitoring.")
                     chats_to_remove.add(chat_id); continue
                # 尝试获取名称验证聊天是否可访问
                chat_name = await self.translate_chat_id(chat_id)
                self._logger.info(f'Monitoring active for "{chat_name}" ({chat_id})')
            except EntityNotFoundError:
                 # 如果找不到或无法访问，也移除监控
                 self._logger.warning(f'Monitored chat_id {chat_id} not found/accessible, removing from monitor list.')
                 chats_to_remove.add(chat_id)
            except Exception as e:
                # 其他异常也移除监控，防止后续出错
                self._logger.error(f'Exception checking monitored chat {chat_id}: {e}, removing from monitor list.')
                chats_to_remove.add(chat_id)

        # 从监控列表中移除无效或排除的聊天
        if chats_to_remove:
            for chat_id in chats_to_remove:
                self.monitored_chats.discard(chat_id)
                # 如果最新消息缓存中有，也一并移除
                if chat_id in self.newest_msg:
                     del self.newest_msg[chat_id]
            self._logger.info(f'Removed {len(chats_to_remove)} chats from active monitoring.')
            # 注意：这里不从 Whoosh 索引中删除历史数据，除非用户明确执行 /clear

        self._register_hooks() # 注册消息处理钩子
        self._logger.info(f"Backend bot {self.id} started successfully.")


    def search(self, q: str, in_chats: Optional[List[int]], page_len: int, page_num: int, file_filter: str = "all") -> SearchResult:
        self._logger.debug(f"Backend {self.id} search: q='{brief_content(q)}', chats={in_chats}, page={page_num}, filter={file_filter}")
        try:
            # 直接调用 Indexer 的 search 方法
            result = self._indexer.search(q, in_chats, page_len, page_num, file_filter=file_filter)
            self._logger.debug(f"Search returned {result.total_results} total hits, {len(result.hits)} on page {page_num}.")
            return result
        except Exception as e:
             # 捕获 Indexer 可能抛出的异常
             self._logger.error(f"Backend search execution failed for {self.id}: {e}", exc_info=True)
             return SearchResult([], True, 0) # 返回空结果


    def rand_msg(self) -> IndexMsg:
        try:
             # 调用 Indexer 获取随机文档
             return self._indexer.retrieve_random_document()
        except IndexError:
             # 如果索引为空，IndexError 会被抛出
             raise IndexError("Index is empty, cannot retrieve random message.")
        except Exception as e:
             # 捕获其他可能的错误
             self._logger.error(f"Error retrieving random document: {e}", exc_info=True)
             raise # 重新抛出，让调用者处理


    def is_empty(self, chat_id: Optional[int] = None) -> bool:
        try:
             # 调用 Indexer 判断索引是否为空（全局或特定 chat）
             return self._indexer.is_empty(chat_id)
        except Exception as e:
             # 捕获 Indexer 可能抛出的异常
             self._logger.error(f"Error checking index emptiness for {chat_id}: {e}")
             return True # 出错时，保守地认为索引是空的或不可用


    async def download_history(self, chat_id: int, min_id: int, max_id: int, call_back: Optional[callable] = None):
        try:
            # 确保 chat_id 是 share_id (处理 -100 前缀)
            share_id = get_share_id(chat_id)
        except Exception as e:
            self._logger.error(f"Invalid chat_id format for download: {chat_id}, error: {e}")
            raise EntityNotFoundError(f"无效的对话 ID 格式: {chat_id}") # 抛出特定错误

        self._logger.info(f'Downloading history for {share_id} (raw_id={chat_id}, min={min_id}, max={max_id})')
        # 检查是否在排除列表
        if share_id in self.excluded_chats:
             self._logger.warning(f"Skipping download for excluded chat {share_id}.")
             raise ValueError(f"对话 {share_id} 已被排除，无法下载。") # 明确告知原因

        # 如果不在监控列表，则添加到监控列表
        if share_id not in self.monitored_chats:
             self.monitored_chats.add(share_id)
             self._logger.info(f"Added chat {share_id} to monitored list.")

        msg_list: List[IndexMsg] = [] # 存储待索引的消息
        downloaded_count: int = 0 # 实际下载并准备索引的消息数
        processed_count: int = 0 # 处理的总消息数 (包括跳过的)

        try:
            # 使用 Telethon 的 iter_messages 迭代消息
            # entity 可以是 share_id (int)
            # min_id / max_id 控制范围 (0 表示无限制)
            # limit=None 表示获取所有符合条件的消息
            async for tg_message in self.session.iter_messages(entity=share_id, min_id=min_id, max_id=max_id, limit=None):
                processed_count += 1
                # 确保是 Message 类型，跳过其他如 MessageService
                if not isinstance(tg_message, TgMessage): continue

                # 构造消息 URL
                url = f'https://t.me/c/{share_id}/{tg_message.id}'
                # 获取发送者名称
                sender = await self._get_sender_name(tg_message)
                # 获取消息时间 (UTC)
                post_time = tg_message.date

                msg_text, filename = '', None
                # 处理带文件的消息
                if tg_message.file and hasattr(tg_message.file, 'name') and tg_message.file.name:
                    filename = tg_message.file.name
                    # 如果文件有附带文本，也提取并转义
                    if tg_message.text: msg_text = escape_content(tg_message.text.strip())
                elif tg_message.text:
                    # 处理纯文本消息
                    msg_text = escape_content(tg_message.text.strip())

                # 只有当有文本内容或文件名时，才创建 IndexMsg
                if msg_text or filename:
                    try:
                        # 创建 IndexMsg 对象
                        msg = IndexMsg(content=msg_text or "", url=url, chat_id=share_id, post_time=post_time, sender=sender or "", filename=filename)
                        msg_list.append(msg); downloaded_count += 1
                    except Exception as create_e:
                         # 创建 IndexMsg 失败通常是数据问题，记录错误并继续
                         self._logger.error(f"Error creating IndexMsg for {url}: {create_e}")

                # 调用回调函数 (如果提供了)，用于更新进度
                # 降低回调频率，避免过于频繁
                if call_back and processed_count % 100 == 0:
                     try:
                          # 回调函数接收当前消息 ID 和已下载计数
                          await call_back(tg_message.id, downloaded_count)
                     except Exception as cb_e:
                          # 回调出错不应中断下载
                          self._logger.warning(f"Error in download callback: {cb_e}")

        # 捕获特定且常见的 Telethon 错误
        except telethon.errors.rpcerrorlist.ChannelPrivateError as e:
             self._logger.error(f"Permission denied for chat {share_id}. Is session member? Error: {e}")
             self.monitored_chats.discard(share_id) # 从监控列表移除
             raise EntityNotFoundError(f"无法访问对话 {chat_id}，请确保后端账号是其成员。") from e
        except (telethon.errors.rpcerrorlist.ChatIdInvalidError, telethon.errors.rpcerrorlist.PeerIdInvalidError):
            self._logger.error(f"Chat ID {share_id} (raw: {chat_id}) is invalid or peer not found.")
            self.monitored_chats.discard(share_id)
            raise EntityNotFoundError(f"无效对话 ID 或无法找到 Peer: {chat_id}")
        except ValueError as e: # 捕获 get_input_entity 可能的 ValueError
             # Telethon 在找不到实体时可能抛出 ValueError
             if "Cannot find any entity corresponding to" in str(e) or "Could not find the input entity for" in str(e):
                  self._logger.error(f"Cannot find entity for chat {share_id} (raw: {chat_id}). Error: {e}")
                  self.monitored_chats.discard(share_id)
                  raise EntityNotFoundError(f"无法找到对话实体: {chat_id}") from e
             else:
                  # 其他 ValueError，可能是内部问题，记录并抛出
                  self._logger.error(f"ValueError iterating messages for {share_id}: {e}", exc_info=True); raise
        except Exception as e: # 捕获其他迭代错误
             self._logger.error(f"Error iterating messages for {share_id}: {e}", exc_info=True)
             # 选择不重新抛出，让已下载的部分能够被索引

        self._logger.info(f'History fetch complete for {share_id}: {downloaded_count} messages to index out of {processed_count} processed.')
        if not msg_list: return # 没有消息需要索引，直接返回

        # 批量写入索引
        writer: Optional[IndexWriter] = None
        newest_msg_in_batch: Optional[IndexMsg] = None # 记录这一批中最新的消息
        indexed_count_in_batch: int = 0 # 记录成功添加到 writer 的消息数
        try:
            # 获取 IndexWriter 用于批量写入
            writer = self._indexer.ix.writer()
            for msg in msg_list:
                try:
                    # 调用 Indexer 的 add_document (传入 writer)
                    self._indexer.add_document(msg, writer)
                    indexed_count_in_batch += 1
                    # 更新批次内最新消息
                    if newest_msg_in_batch is None or msg.post_time > newest_msg_in_batch.post_time:
                         newest_msg_in_batch = msg
                except Exception as add_e:
                    # 添加单条消息到批次失败，记录错误但继续处理下一条
                    self._logger.error(f"Error adding document {msg.url} to batch writer: {add_e}")
            # 提交写入
            writer.commit()
            self._logger.info(f'Write index commit ok for {indexed_count_in_batch} messages from chat {share_id}')

            # 更新最新消息缓存 (如果批次中有新消息)
            if newest_msg_in_batch:
                 current_chat_id = int(newest_msg_in_batch.chat_id)
                 # 只有当新消息比缓存中的更新时才更新
                 if current_chat_id not in self.newest_msg or newest_msg_in_batch.post_time > self.newest_msg[current_chat_id].post_time:
                      self.newest_msg[current_chat_id] = newest_msg_in_batch
                      self._logger.debug(f"Updated newest msg cache for {current_chat_id} to {newest_msg_in_batch.url}")
        except writing.LockError: # 捕获写锁错误
             logger.error("Index is locked during batch write. Downloaded messages are lost.")
             if writer: writer.cancel() # 尝试取消写入
             # 抛出运行时错误，告知调用者写入失败
             raise RuntimeError("Index is locked, cannot write downloaded messages.")
        except Exception as e:
            # 其他写入错误
            logger.error(f"Error writing batch index for {share_id}: {e}", exc_info=True)
            if writer: writer.cancel() # 尝试取消
            # 选择不重新抛出，避免完全失败，但前端可能不知道部分消息丢失


    def clear(self, chat_ids: Optional[List[int]] = None):
        """清除索引数据。如果提供了 chat_ids，则只清除这些对话的数据，否则清除所有数据。"""
        if chat_ids is not None:
            # 清除指定对话
            share_ids_to_clear = {get_share_id(cid) for cid in chat_ids} # 转换为 share_id 集合去重
            try:
                # 使用 IndexWriter 删除指定 chat_id 的文档
                with self._indexer.ix.writer() as w:
                    for share_id in share_ids_to_clear:
                        # delete_by_term 会删除 'chat_id' 字段值为指定 share_id 的所有文档
                        deleted_count = w.delete_by_term('chat_id', str(share_id))
                        # 从监控列表和最新消息缓存中移除
                        self.monitored_chats.discard(share_id)
                        if share_id in self.newest_msg: del self.newest_msg[share_id]
                        self._logger.info(f'Cleared {deleted_count} docs and stopped monitoring chat {share_id}')
            except Exception as e:
                 self._logger.error(f"Error clearing index for chats {share_ids_to_clear}: {e}")
        else:
            # 清除所有对话
            try:
                self._indexer.clear() # 调用 Indexer 的 clear 方法
                self.monitored_chats.clear() # 清空内存中的监控列表
                self.newest_msg.clear() # 清空最新消息缓存
                self._logger.info('Cleared all index data and stopped monitoring.')
            except Exception as e:
                 self._logger.error(f"Error clearing all index data: {e}")


    async def find_chat_id(self, q: str) -> List[int]:
        """根据关键词查找对话 ID (返回 share_id 列表)"""
        try:
             # 调用 Session 的方法查找对话
             return await self.session.find_chat_id(q)
        except Exception as e:
             self._logger.error(f"Error finding chat id for '{q}': {e}")
             return [] # 出错时返回空列表


    # --- get_index_status 已修复 ---
    async def get_index_status(self, length_limit: int = 4000) -> str:
        """获取后端索引状态的文本描述"""
        cur_len = 0 # 当前已生成文本长度
        sb = [] # 用于拼接字符串
        try:
            # 获取总文档数
            total_docs = self._indexer.ix.doc_count()
        except Exception as e:
             total_docs = -1 # 获取失败
             self._logger.error(f"Failed get total doc count: {e}")

        # 报告头信息
        sb.append(f'后端 "{self.id}" (会话: "{self.session.name}") 总消息: <b>{total_docs if total_docs >= 0 else "[获取失败]"}</b>\n\n')

        # 用于超出长度限制时的提示
        overflow_msg = f'\n\n(部分信息因长度限制未显示)'

        # 辅助函数：尝试添加消息，如果超出长度则返回 True
        def append_msg(msg_list: List[str]) -> bool:
            nonlocal cur_len
            new_len = sum(len(msg) for msg in msg_list) # 计算要添加的文本总长
            # 预留空间给溢出提示
            if cur_len + new_len > length_limit - len(overflow_msg) - 50:
                return True # 超出限制
            cur_len += new_len; sb.extend(msg_list); return False

        # 显示被排除的对话
        if self.excluded_chats:
            excluded_list = sorted(list(self.excluded_chats))
            # 尝试添加标题，检查长度
            if append_msg([f'{len(excluded_list)} 个对话被禁止索引:\n']):
                 sb.append(overflow_msg); return ''.join(sb) # 超长则直接返回
            # 逐个添加被排除的对话信息
            for chat_id in excluded_list:
                try: chat_html = await self.format_dialog_html(chat_id)
                except EntityNotFoundError: chat_html = f"未知对话 (`{chat_id}`)"
                except Exception: chat_html = f"对话 `{chat_id}` (获取名称出错)"
                # 尝试添加单条信息，检查长度
                if append_msg([f'- {chat_html}\n']):
                     sb.append(overflow_msg); return ''.join(sb) # 超长则直接返回
            if sb and sb[-1] != '\n\n': sb.append('\n') # 添加空行分隔

        # 显示被监控的对话
        monitored_chats_list = sorted(list(self.monitored_chats))
        # 尝试添加标题，检查长度
        if append_msg([f'总计 {len(monitored_chats_list)} 个对话被加入了索引:\n']):
             sb.append(overflow_msg); return ''.join(sb)

        try:
             # 使用 searcher 来获取每个对话的文档数，避免多次打开关闭索引
             with self._indexer.ix.searcher() as searcher:
                 for chat_id in monitored_chats_list:
                     msg_for_chat = [] # 存储当前对话的信息行
                     num = -1 # 初始化为错误状态
                     try:
                         # 使用 Term 查询精确匹配 chat_id
                         # [修复] 使用 searcher.doc_count 获取数量
                         num = searcher.doc_count(query=Term('chat_id', str(chat_id)))
                     except Exception as e:
                         # 获取数量失败，num 保持 -1
                         self._logger.error(f"Error counting for chat {chat_id}: {e}")

                     try: chat_html = await self.format_dialog_html(chat_id)
                     except EntityNotFoundError: chat_html = f"未知对话 (`{chat_id}`)"
                     except Exception: chat_html = f"对话 `{chat_id}` (获取名称出错)"

                     # [修改] 将 "错误" 改为更明确的 "[计数失败]"
                     msg_for_chat.append(f'- {chat_html} 共 {"[计数失败]" if num < 0 else num} 条消息\n')

                     # 如果缓存中有最新消息，则显示
                     if newest_msg := self.newest_msg.get(chat_id):
                         # 优先显示文件名，否则显示内容摘要
                         display = f"📎 {newest_msg.filename}" if newest_msg.filename else brief_content(newest_msg.content)
                         # 如果既有文件名又有内容，补充显示内容摘要
                         if newest_msg.filename and newest_msg.content:
                              display += f" ({brief_content(newest_msg.content)})"
                         # HTML 转义，防止 XSS
                         esc_display = html.escape(display or "(空)")
                         # 添加最新消息行
                         msg_for_chat.append(f'  最新: <a href="{newest_msg.url}">{esc_display}</a> (@{newest_msg.post_time.strftime("%y-%m-%d %H:%M")})\n')

                     # 尝试添加当前对话的所有信息行，检查长度
                     if append_msg(msg_for_chat):
                          sb.append(overflow_msg); break # 超长则停止并添加溢出提示
        except Exception as e:
             # 打开 searcher 失败
             self._logger.error(f"Failed open searcher for status: {e}")
             append_msg(["\n错误：无法获取详细状态。\n"])

        return ''.join(sb) # 返回拼接好的状态字符串
    # --- 结束修复 get_index_status ---


    async def translate_chat_id(self, chat_id: int) -> str:
        """将 share_id 转换为可读的对话名称"""
        try:
             # 调用 Session 的方法进行转换
             return await self.session.translate_chat_id(int(chat_id))
        except (telethon.errors.rpcerrorlist.ChannelPrivateError, telethon.errors.rpcerrorlist.ChatIdInvalidError, ValueError, TypeError): # 添加 TypeError
             # 处理无效 ID 或无权访问的情况
             raise EntityNotFoundError(f"无法访问或无效 Chat ID: {chat_id}")
        except EntityNotFoundError:
             # Session 层找不到时，直接抛出
             self._logger.warning(f"Entity not found for {chat_id}"); raise
        except Exception as e:
             # 其他转换错误
             self._logger.error(f"Error translating chat_id {chat_id}: {e}")
             raise EntityNotFoundError(f"获取对话 {chat_id} 名称时出错") from e


    async def str_to_chat_id(self, chat: str) -> int:
         """将字符串（可能是数字ID、用户名、邀请链接等）转换为 share_id"""
         try:
             # 尝试直接将输入视为整数 ID，并转换为 share_id
             try: raw_id = int(chat); return get_share_id(raw_id)
             except ValueError:
                  # 如果不是纯数字，则调用 Session 的方法进行解析
                  raw_id = await self.session.str_to_chat_id(chat)
                  return get_share_id(raw_id)
         except EntityNotFoundError:
              # Session 层找不到实体
              self._logger.warning(f"Entity not found for '{chat}'"); raise
         except Exception as e:
              # 其他解析错误
              self._logger.error(f"Error converting '{chat}' to chat_id: {e}")
              raise EntityNotFoundError(f"解析 '{chat}' 时出错") from e


    async def format_dialog_html(self, chat_id: int):
        """将 share_id 格式化为带链接的 HTML 字符串"""
        try:
            # 获取名称并进行 HTML 转义
            name = await self.translate_chat_id(int(chat_id)); esc_name = html.escape(name)
            # 返回格式：<a href="对话链接">名称</a> (`ID`)
            # 对话链接使用 t.me/c/share_id/1 指向对话的第一条消息（通常存在）
            return f'<a href="https://t.me/c/{chat_id}/1">{esc_name}</a> (`{chat_id}`)'
        except EntityNotFoundError:
             return f'未知对话 (`{chat_id}`)'
        except ValueError:
             return f'无效对话 ID (`{chat_id}`)'
        except Exception as e:
             # 其他获取名称错误
             self._logger.warning(f"Error formatting html for {chat_id}: {e}")
             return f'对话 `{chat_id}` (获取名称出错)'


    def _should_monitor(self, chat_id: int) -> bool:
        """判断是否应该监控（处理）来自某个 chat_id 的消息"""
        try:
            share_id = get_share_id(chat_id) # 转换为 share_id
            # 如果在排除列表，则不监控
            if share_id in self.excluded_chats: return False
            # 如果配置了 monitor_all，或者该 share_id 在监控列表里，则监控
            return self._cfg.monitor_all or (share_id in self.monitored_chats)
        except Exception:
             # 无效 ID 等情况，不监控
             return False

    @staticmethod
    async def _get_sender_name(message: TgMessage) -> str:
        """获取消息发送者的名称"""
        try:
            sender = await message.get_sender() # 获取发送者实体
            if isinstance(sender, User):
                 # 如果是用户，使用 format_entity_name 格式化名称
                 return format_entity_name(sender)
            else:
                 # 否则（可能是频道），尝试获取 title 属性
                 return getattr(sender, 'title', '')
        except Exception:
             # 获取失败返回空字符串
             return ''


    def _register_hooks(self):
        """注册 Telethon 事件钩子，处理新消息、编辑消息、删除消息"""

        # 处理新消息
        @self.session.on(events.NewMessage())
        async def client_message_handler(event: events.NewMessage.Event):
            # 检查是否需要监控此对话
            if event.chat_id is None or not self._should_monitor(event.chat_id): return
            try:
                share_id = get_share_id(event.chat_id)
                url = f'https://t.me/c/{share_id}/{event.id}'
                sender = await self._get_sender_name(event.message)
                post_time = event.message.date
                msg_text, filename = '', None

                # 处理文件消息
                if event.message.file and hasattr(event.message.file, 'name') and event.message.file.name:
                    filename = event.message.file.name
                    if event.message.text: msg_text = escape_content(event.message.text.strip())
                    self._logger.info(f'New file {url} from "{sender}": "{filename}" Cap:"{brief_content(msg_text)}"')
                # 处理文本消息
                elif event.message.text:
                    msg_text = escape_content(event.message.text.strip())
                    # 跳过纯空格或空文本（无文件）
                    if not msg_text.strip() and not filename: return
                    self._logger.info(f'New msg {url} from "{sender}": "{brief_content(msg_text)}"')
                else:
                     # 无文本无文件则跳过
                     return

                # 创建 IndexMsg 并添加到索引
                msg = IndexMsg(content=msg_text or "", url=url, chat_id=share_id, post_time=post_time, sender=sender or "", filename=filename)
                # 更新最新消息缓存
                if share_id not in self.newest_msg or msg.post_time > self.newest_msg[share_id].post_time:
                     self.newest_msg[share_id] = msg; self._logger.debug(f"Updated newest cache for {share_id} to {url}")
                try:
                     self._indexer.add_document(msg) # 添加到 Whoosh 索引
                except Exception as e:
                     # 添加索引失败
                     self._logger.error(f"Error adding doc {url} to index: {e}")
            except Exception as e:
                 # 处理新消息的整体流程出错
                 self._logger.error(f"Error processing new message in chat {event.chat_id}: {e}", exc_info=True)

        # 处理编辑消息
        @self.session.on(events.MessageEdited())
        async def client_message_update_handler(event: events.MessageEdited.Event):
            if event.chat_id is None or not self._should_monitor(event.chat_id): return
            try:
                share_id = get_share_id(event.chat_id)
                url = f'https://t.me/c/{share_id}/{event.id}'
                # 获取编辑后的新文本并转义
                new_msg_text = escape_content(event.message.text.strip()) if event.message.text else ''
                self._logger.info(f'Msg {url} edited. New content: "{brief_content(new_msg_text)}"')
                try:
                    # 从索引中获取旧文档的字段
                    old_fields = self._indexer.get_document_fields(url=url)
                    if old_fields:
                        # 如果找到了旧文档
                        new_fields = old_fields.copy() # 复制旧字段
                        new_fields['content'] = new_msg_text or "" # 更新 content 字段
                        # 确保其他必需字段存在，以防万一旧文档缺少字段
                        new_fields.setdefault('chat_id', str(share_id))
                        new_fields.setdefault('post_time', event.message.date) # 可以考虑是否更新时间戳？目前使用编辑时间
                        new_fields.setdefault('sender', old_fields.get('sender', ''))
                        new_fields.setdefault('filename', old_fields.get('filename', None))
                        new_fields.setdefault('url', url)
                        # 调用 Indexer 的 replace_document 方法更新文档
                        self._indexer.replace_document(url=url, new_fields=new_fields)
                        self._logger.info(f'Updated msg content in index for {url}')
                        # 如果编辑的是缓存中的最新消息，也同步更新缓存
                        if share_id in self.newest_msg and self.newest_msg[share_id].url == url:
                             self.newest_msg[share_id].content = new_msg_text
                             self._logger.debug(f"Updated newest cache content for {url}")
                    else:
                        # 如果索引中找不到要编辑的消息 (可能发生在消息编辑前未被索引的情况)
                         self._logger.warning(f'Edited msg {url} not found in index. Adding as new message.')
                         # 可以选择作为新消息添加，如果需要的话
                         sender = await self._get_sender_name(event.message)
                         post_time = event.message.date # 使用编辑时间作为 post_time？
                         filename = None # 编辑事件通常不包含文件信息，假设文件不变
                         # 如果需要获取文件名，需要额外逻辑或假设
                         msg = IndexMsg(content=new_msg_text or "", url=url, chat_id=share_id, post_time=post_time, sender=sender or "", filename=filename)
                         self._indexer.add_document(msg)

                except Exception as e:
                     # 更新索引失败
                     self._logger.error(f'Error updating edited msg {url} in index: {e}')
            except Exception as e:
                 # 处理编辑消息的整体流程出错
                 self._logger.error(f"Error processing edited message in chat {event.chat_id}: {e}", exc_info=True)

        # 处理删除消息
        @self.session.on(events.MessageDeleted())
        async def client_message_delete_handler(event: events.MessageDeleted.Event):
            # 检查事件是否包含 chat_id，以及是否需要监控
            # 注意：MessageDeletedEvent 可能没有 chat_id (例如在 "Saved Messages" 中删除)
            if not hasattr(event, 'chat_id') or event.chat_id is None or not self._should_monitor(event.chat_id):
                 # 记录但不处理无 chat_id 或不需监控的删除事件
                 self._logger.debug(f"Ignoring deletion event without valid/monitored chat_id. Deleted IDs: {event.deleted_ids}")
                 return

            try:
                share_id = get_share_id(event.chat_id)
                deleted_count = 0
                # 构建被删除消息的 URL 列表
                urls = [f'https://t.me/c/{share_id}/{mid}' for mid in event.deleted_ids]
                try:
                     # 使用 IndexWriter 批量删除
                     with self._indexer.ix.writer() as writer:
                          for url in urls:
                               # 如果删除的是缓存中的最新消息，清除缓存
                               if share_id in self.newest_msg and self.newest_msg[share_id].url == url:
                                    del self.newest_msg[share_id]
                                    self._logger.info(f"Removed newest cache for {share_id} due to deletion of {url}.")
                               try:
                                    # 按 URL 删除文档
                                    count = writer.delete_by_term('url', url)
                                    if count > 0: deleted_count += count; self._logger.info(f"Deleted msg {url} from index.")
                               except Exception as del_e:
                                    # 删除单条失败
                                    self._logger.error(f"Error deleting doc {url} from index: {del_e}")
                     if deleted_count > 0:
                          self._logger.info(f'Finished deleting {deleted_count} msgs from index for chat {share_id}')
                except Exception as e:
                     # 获取 writer 或提交时出错
                     self._logger.error(f"Error processing deletions batch for {share_id}: {e}")
            except Exception as e:
                 # 处理删除事件的整体流程出错
                 self._logger.error(f"Error processing deleted event in chat {event.chat_id}: {e}", exc_info=True)
