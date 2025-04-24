# -*- coding: utf-8 -*-
import html
from datetime import datetime
from typing import Optional, List, Set, Dict

import telethon.errors.rpcerrorlist
from telethon import events
from telethon.tl.patched import Message as TgMessage
from telethon.tl.types import User

from .indexer import Indexer, IndexMsg # 确保 IndexMsg 已更新
from .common import CommonBotConfig, escape_content, get_share_id, get_logger, format_entity_name, brief_content, \
    EntityNotFoundError
from .session import ClientSession


class BackendBotConfig:
    def __init__(self, **kw):
        self.monitor_all = kw.get('monitor_all', False)
        self.excluded_chats: Set[int] = set(get_share_id(chat_id)
                                            for chat_id in kw.get('exclude_chats', []))


class BackendBot:
    def __init__(self, common_cfg: CommonBotConfig, cfg: BackendBotConfig,
                 session: ClientSession, clean_db: bool, backend_id: str):
        self.id: str = backend_id
        self.session = session

        self._logger = get_logger(f'bot-backend:{backend_id}')
        self._cfg = cfg
        if clean_db:
            self._logger.info(f'Index will be cleaned')
        # 使用更新后的 Indexer 初始化
        self._indexer: Indexer = Indexer(common_cfg.index_dir / backend_id, clean_db)

        # on startup, all indexed chats are added to monitor list
        self.monitored_chats: Set[int] = self._indexer.list_indexed_chats()
        self.excluded_chats = cfg.excluded_chats
        # newest_msg 仍然可以基于 post_time 跟踪，类型是 IndexMsg
        self.newest_msg: Dict[int, IndexMsg] = dict()
        # 可以在启动时尝试加载最新的消息
        self._load_newest_messages_on_startup()

    def _load_newest_messages_on_startup(self):
         """启动时尝试为每个监控的聊天加载最新消息"""
         self._logger.info("Loading newest message for each monitored chat...")
         for chat_id in self.monitored_chats:
              try:
                   # 搜索该聊天的最新一条消息
                   result = self._indexer.search(q_str='*', # 搜索所有文档
                                                 in_chats=[chat_id],
                                                 page_len=1,
                                                 page_num=1)
                   if result.hits:
                        self.newest_msg[chat_id] = result.hits[0].msg
              except Exception as e:
                   self._logger.error(f"Failed to load newest message for chat {chat_id}: {e}")
         self._logger.info("Finished loading newest messages.")


    async def start(self):
        self._logger.info(f'Init backend bot')

        # 检查并记录监控的聊天
        chats_to_remove = set()
        for chat_id in self.monitored_chats:
            try:
                chat_name = await self.translate_chat_id(chat_id)
                self._logger.info(f'Ready to monitor "{chat_name}" ({chat_id})')
                # 加载最新的消息记录 (如果启动时未加载)
                if chat_id not in self.newest_msg:
                     result = self._indexer.search(q_str='*', in_chats=[chat_id], page_len=1, page_num=1)
                     if result.hits:
                          self.newest_msg[chat_id] = result.hits[0].msg

            except EntityNotFoundError:
                 self._logger.error(f'Monitored chat_id {chat_id} not found, removing from monitor list and index.')
                 chats_to_remove.add(chat_id)
            except Exception as e:
                self._logger.error(f'Exception on get monitored chat (id={chat_id}): {e}, removing from monitor list.')
                # 不一定需要清除索引，可能只是临时问题
                chats_to_remove.add(chat_id)

        if chats_to_remove:
            with self._indexer.ix.writer() as writer:
                 for chat_id in chats_to_remove:
                      self.monitored_chats.discard(chat_id) # 使用 discard 避免 KeyErrot
                      if chat_id in self.newest_msg:
                           del self.newest_msg[chat_id]
                      # 考虑是否真的要删除索引，或者只是停止监控
                      # writer.delete_by_term('chat_id', str(chat_id))
                      self._logger.info(f'Removed chat {chat_id} from monitoring.')
            # writer.commit() # 如果有删除操作则需要 commit

        self._register_hooks()

    def search(self, q: str, in_chats: Optional[List[int]], page_len: int, page_num: int):
        # 直接调用更新后的 indexer search
        return self._indexer.search(q, in_chats, page_len, page_num)

    def rand_msg(self) -> IndexMsg:
        # 调用更新后的 indexer 方法
        try:
             return self._indexer.retrieve_random_document()
        except IndexError: # 处理空索引的情况
             raise IndexError("Index is empty, cannot retrieve random message.")


    def is_empty(self, chat_id=None):
        # 调用更新后的 indexer 方法
        return self._indexer.is_empty(chat_id)

    # --- 修改 download_history ---
    async def download_history(self, chat_id: int, min_id: int, max_id: int, call_back=None):
        share_id = get_share_id(chat_id)
        self._logger.info(f'Downloading history from {share_id} ({min_id=}, {max_id=})')
        self.monitored_chats.add(share_id)
        msg_list = []
        downloaded_count = 0 # 用于回调计数

        async for tg_message in self.session.iter_messages(chat_id, min_id=min_id, max_id=max_id):
            # 提取消息信息
            url = f'https://t.me/c/{share_id}/{tg_message.id}'
            sender = await self._get_sender_name(tg_message)
            post_time = datetime.fromtimestamp(tg_message.date.timestamp())

            msg_text = ''
            filename = None

            # 检查文件
            if tg_message.file and hasattr(tg_message.file, 'name'):
                filename = tg_message.file.name
                if tg_message.text: # 获取文件标题
                    msg_text = escape_content(tg_message.text.strip())
            # 检查文本
            elif tg_message.text:
                msg_text = escape_content(tg_message.text.strip())

            # 只有包含文本或文件名时才索引
            if msg_text or filename:
                msg = IndexMsg(
                    content=msg_text,
                    url=url,
                    chat_id=share_id, # 确保使用 share_id
                    post_time=post_time,
                    sender=sender,
                    filename=filename
                )
                msg_list.append(msg)
                downloaded_count += 1 # 增加计数

            if call_back:
                 # 注意：这里传递的是消息的原始 ID
                 await call_back(tg_message.id, downloaded_count)

        self._logger.info(f'Fetching history from {share_id} complete, {len(msg_list)} messages to index, start writing index')
        # 批量写入索引
        writer = self._indexer.ix.writer()
        newest_msg_in_batch = None
        try:
            for msg in msg_list:
                self._indexer.add_document(msg, writer)
                newest_msg_in_batch = msg # 记录最后添加的消息
            if newest_msg_in_batch:
                self.newest_msg[share_id] = newest_msg_in_batch # 更新最新消息记录
            writer.commit()
            self._logger.info(f'Write index commit ok for {len(msg_list)} messages')
        except Exception as e:
            writer.cancel() # 写入失败时取消
            self._logger.error(f"Error writing batch index for chat {share_id}: {e}")
            raise # 重新抛出异常，让调用者知道出错了
    # --- 结束修改 ---

    def clear(self, chat_ids: Optional[List[int]] = None):
        if chat_ids is not None:
            # 确认 chat_ids 是 share_id
            share_ids_to_clear = {get_share_id(cid) for cid in chat_ids}
            with self._indexer.ix.writer() as w:
                for share_id in share_ids_to_clear:
                    w.delete_by_term('chat_id', str(share_id))
                    self.monitored_chats.discard(share_id)
                    if share_id in self.newest_msg:
                        del self.newest_msg[share_id]
                    self._logger.info(f'Cleared index and stopped monitoring for chat {share_id}')
        else:
            self._indexer.clear() # 清除整个索引
            self.monitored_chats.clear()
            self.newest_msg.clear()
            self._logger.info('Cleared all index data and stopped monitoring all chats.')

    async def find_chat_id(self, q: str) -> List[int]:
        # 这个方法不需要修改，它依赖 session 的功能
        return await self.session.find_chat_id(q)

    async def get_index_status(self, length_limit: int = 4000):
        # 基本不变，但显示最新消息时可能需要考虑文件名
        cur_len = 0
        sb = [
            f'后端 "{self.id}"（session: "{self.session.name}"）总消息数: <b>{self._indexer.ix.doc_count()}</b>\n\n'
        ]
        overflow_msg = f'\n\n(部分对话统计信息因长度限制未显示)'

        def append_msg(msg_list: List[str]):
            nonlocal cur_len, sb
            total_len = sum(len(msg) for msg in msg_list)
            # 调整长度判断，为 overflow_msg 留出空间
            if cur_len + total_len > length_limit - len(overflow_msg) - 20: # 留一点余量
                return True
            else:
                cur_len += total_len
                sb.extend(msg_list) # 使用 extend 简化
                return False

        if self._cfg.monitor_all:
            if append_msg([f'{len(self.excluded_chats)} 个对话被禁止索引:\n']): return ''.join(sb) + overflow_msg
            for chat_id in self.excluded_chats:
                # 尝试获取名称，失败则显示 ID
                try:
                     chat_html = await self.format_dialog_html(chat_id)
                except EntityNotFoundError:
                     chat_html = f"未知对话 ({chat_id})"
                if append_msg([f'- {chat_html}\n']): return ''.join(sb) + overflow_msg
            sb.append('\n')

        monitored_chats_list = sorted(list(self.monitored_chats)) # 排序以获得一致的输出
        if append_msg([f'总计 {len(monitored_chats_list)} 个对话被加入了索引:\n']): return ''.join(sb) + overflow_msg

        for chat_id in monitored_chats_list:
            msg_for_chat = []
            try:
                num = self._indexer.count_by_query(chat_id=str(chat_id))
                chat_html = await self.format_dialog_html(chat_id)
                msg_for_chat.append(f'- {chat_html} 共 {num} 条消息\n')

                # 显示最新消息
                if newest_msg := self.newest_msg.get(chat_id):
                    display_content = newest_msg.filename if newest_msg.filename else newest_msg.content
                    if newest_msg.filename:
                         display_content = f"📎 {newest_msg.filename}" + (f" ({brief_content(newest_msg.content)})" if newest_msg.content else "")
                    else:
                         display_content = brief_content(newest_msg.content)

                    # 转义 display_content
                    escaped_display_content = html.escape(display_content)
                    msg_for_chat.append(f'  最新: <a href="{newest_msg.url}">{escaped_display_content}</a> (@{newest_msg.post_time.strftime("%y-%m-%d %H:%M")})\n')

                if append_msg(msg_for_chat):
                    sb.append(overflow_msg)
                    break # 跳出循环
            except EntityNotFoundError:
                 # 如果在循环中 chat_id 突然找不到了
                 msg_for_chat = [f'- 未知对话 ({chat_id}) 的信息无法获取\n']
                 if append_msg(msg_for_chat):
                     sb.append(overflow_msg)
                     break
            except Exception as e:
                 # 记录其他错误
                 self._logger.error(f"Error getting status for chat {chat_id}: {e}")
                 msg_for_chat = [f'- 对话 {chat_id} 状态获取失败\n']
                 if append_msg(msg_for_chat):
                     sb.append(overflow_msg)
                     break

        return ''.join(sb)


    async def translate_chat_id(self, chat_id: int) -> str:
        try:
            return await self.session.translate_chat_id(chat_id)
        except telethon.errors.rpcerrorlist.ChannelPrivateError:
            return '[无法获取名称]'
        except EntityNotFoundError: # 从 session 层捕获
             # 可以选择记录日志并返回一个占位符
             self._logger.warning(f"translate_chat_id: Entity not found for {chat_id}")
             raise # 或者重新抛出，让调用者处理

    async def str_to_chat_id(self, chat: str) -> int:
         # 确保返回的是 share_id
         raw_id = await self.session.str_to_chat_id(chat) # session 应该处理查找和返回 ID
         return get_share_id(raw_id) # 确保总是转换为 share_id

    async def format_dialog_html(self, chat_id: int):
        # 尝试获取名称，如果失败则显示 ID
        try:
             name = await self.translate_chat_id(chat_id)
             escaped_name = html.escape(name)
             # 链接到频道的任意高位消息 ID 通常可以打开频道信息
             return f'<a href="https://t.me/c/{chat_id}/99999999">{escaped_name}</a> ({chat_id})'
        except EntityNotFoundError:
             return f'未知对话 ({chat_id})'


    def _should_monitor(self, chat_id: int):
        share_id = get_share_id(chat_id)
        if self._cfg.monitor_all:
            return share_id not in self.excluded_chats
        else:
            return share_id in self.monitored_chats

    # 不再需要 _extract_text，直接在 handler 中处理
    # @staticmethod
    # def _extract_text(event): ...

    @staticmethod
    async def _get_sender_name(message: TgMessage) -> str:
        sender = await message.get_sender()
        if isinstance(sender, User):
            return format_entity_name(sender)
        else:
            # 对于频道消息，sender 可能是频道本身，这里返回空字符串可能更合适
            return ''

    def _register_hooks(self):
        # --- 修改 NewMessage handler ---
        @self.session.on(events.NewMessage())
        async def client_message_handler(event: events.NewMessage.Event):
            if not self._should_monitor(event.chat_id):
                return

            share_id = get_share_id(event.chat_id)
            url = f'https://t.me/c/{share_id}/{event.id}'
            sender = await self._get_sender_name(event.message)
            post_time=datetime.fromtimestamp(event.date.timestamp())

            msg_text = ''
            filename = None

            # 检查文件
            if event.message.file and hasattr(event.message.file, 'name'):
                filename = event.message.file.name
                if event.message.text: # 获取文件标题
                    msg_text = escape_content(event.message.text.strip())
                self._logger.info(f'New file {url} from "{sender}": "{filename}" Caption: "{brief_content(msg_text)}"')
            # 检查文本
            elif event.message.text:
                msg_text = escape_content(event.message.text.strip())
                # 如果文本为空或只有空格，也跳过（除非有文件名）
                if not msg_text.strip() and not filename:
                     return
                self._logger.info(f'New msg {url} from "{sender}": "{brief_content(msg_text)}"')
            else:
                # 没有文本也没有文件，跳过
                return

            # 创建 IndexMsg 对象
            msg = IndexMsg(
                content=msg_text,
                url=url,
                chat_id=share_id,
                post_time=post_time,
                sender=sender,
                filename=filename # 传递 filename
            )

            # 更新最新消息记录并添加文档
            self.newest_msg[share_id] = msg
            try:
                 self._indexer.add_document(msg)
            except Exception as e:
                 self._logger.error(f"Error adding document {url} to index: {e}")
        # --- 结束修改 ---

        # --- 修改 MessageEdited handler ---
        @self.session.on(events.MessageEdited())
        async def client_message_update_handler(event: events.MessageEdited.Event):
            if not self._should_monitor(event.chat_id):
                return

            share_id = get_share_id(event.chat_id)
            url = f'https://t.me/c/{share_id}/{event.id}'

            # 获取编辑后的文本内容
            new_msg_text = ''
            if event.message.text:
                new_msg_text = escape_content(event.message.text.strip())

            self._logger.info(f'Message {url} edited. New content: "{brief_content(new_msg_text)}"')

            # 使用 get + replace 的方式更新，以保留 filename 等其他字段
            try:
                old_doc_fields = self._indexer.get_document_fields(url=url)
                if old_doc_fields:
                    # 更新 content 字段
                    old_doc_fields['content'] = new_msg_text
                    # 其他字段（如 filename, sender 等）保持不变
                    self._indexer.replace_document(url=url, new_fields=old_doc_fields)
                    self._logger.info(f'Updated message content in index for {url}')
                    # 如果更新的是最新消息，也更新缓存
                    if chat_id in self.newest_msg and self.newest_msg[chat_id].url == url:
                         self.newest_msg[chat_id].content = new_msg_text
                else:
                    # 如果索引中没有找到这条消息（可能发生在编辑非常旧的消息或索引尚未完全建立时）
                    # 可以选择忽略，或者尝试添加它（如果需要的话）
                    self._logger.warning(f'Edited message {url} not found in index. Ignoring edit.')
            except Exception as e:
                self._logger.error(f'Error updating edited message {url} in index: {e}')
        # --- 结束修改 ---


        @self.session.on(events.MessageDeleted())
        async def client_message_delete_handler(event: events.MessageDeleted.Event):
             # 这个 handler 不需要修改，因为它基于 URL 删除
            if not hasattr(event, 'chat_id') or event.chat_id is None:
                # 有些删除事件可能没有 chat_id，忽略它们
                # self._logger.warning(f"MessageDeleted event without chat_id: {event.deleted_ids}")
                return
            if self._should_monitor(event.chat_id):
                share_id = get_share_id(event.chat_id)
                deleted_count = 0
                # 尝试批量删除
                urls_to_delete = [f'https://t.me/c/{share_id}/{msg_id}' for msg_id in event.deleted_ids]

                try:
                     with self._indexer.ix.writer() as writer:
                          for url in urls_to_delete:
                                # 检查这条消息是否是缓存的最新消息
                                if share_id in self.newest_msg and self.newest_msg[share_id].url == url:
                                     del self.newest_msg[share_id] # 从缓存中移除
                                     # 可以尝试加载次新的消息，但这可能比较复杂，暂时移除即可
                                writer.delete_by_term('url', url)
                                deleted_count += 1
                     self._logger.info(f'Deleted {deleted_count} messages from index for chat {share_id}')
                except Exception as e:
                     self._logger.error(f"Error deleting messages for chat {share_id}: {e}")
