# -*- coding: utf-8 -*-
import html
from time import time
from typing import Optional, List, Tuple, Set, Union
from traceback import format_exc
from argparse import ArgumentParser
import shlex

import redis
import whoosh.index # 导入 whoosh.index 以便捕获 LockError
from telethon import TelegramClient, events, Button
from telethon.tl.types import BotCommand, BotCommandScopePeer, BotCommandScopeDefault
from telethon.tl.custom import Message as TgMessage
from telethon.tl.functions.bots import SetBotCommandsRequest
import telethon.errors.rpcerrorlist as rpcerrorlist
from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError

from .common import CommonBotConfig, get_logger, get_share_id, remove_first_word, brief_content
from .backend_bot import BackendBot, EntityNotFoundError
from .indexer import SearchResult, IndexMsg # 确保 IndexMsg 已更新


class BotFrontendConfig:
    @staticmethod
    def _parse_redis_cfg(redis_cfg: str) -> Tuple[str, int]:
        colon_idx = redis_cfg.find(':') # 使用 find 避免 ValueError
        if colon_idx < 0:
            # 假设默认端口 6379
            return redis_cfg, 6379
            # raise ValueError("No colon in redis host config and no default provided")
        return redis_cfg[:colon_idx], int(redis_cfg[colon_idx + 1:])

    def __init__(self, **kw):
        self.bot_token: str = kw['bot_token']
        self.admin: Union[int, str] = kw['admin_id']
        self.page_len: int = kw.get('page_len', 10)
        self.no_redis: bool = kw.get('no_redis', False)
        self.redis_host: Optional[Tuple[str, int]] = None # 初始设为 None

        if not self.no_redis:
             try:
                  redis_cfg = kw.get('redis', 'localhost:6379')
                  self.redis_host = self._parse_redis_cfg(redis_cfg)
             except ValueError as e:
                  # 处理 redis 配置解析错误
                  # 可以选择记录日志并禁用 redis 或退出
                  print(f"Error parsing redis config '{redis_cfg}': {e}. Disabling redis.")
                  self.no_redis = True
             except KeyError:
                  # 如果配置中完全没有 'redis' 键
                  print(f"Redis config key 'redis' not found. Disabling redis.")
                  self.no_redis = True


        self.private_mode: bool = kw.get('private_mode', False)
        # 白名单应该只存整数 ID
        self.private_whitelist: Set[int] = set()
        raw_whitelist = kw.get('private_whitelist', [])
        if raw_whitelist:
             # 这里假设白名单里已经是整数 ID 或可以转为整数的字符串
             try:
                  self.private_whitelist = {int(uid) for uid in raw_whitelist}
             except (ValueError, TypeError) as e:
                  print(f"Warning: Could not parse private_whitelist: {raw_whitelist}. Error: {e}. Whitelist might be incomplete.")
        # admin 自动加入白名单的操作移到 start() 中，确保 admin ID 已解析


class FakeRedis:
    """
    用内存字典模拟 Redis 接口，用于无 Redis 的轻量部署。
    """
    def __init__(self):
        self._data = {}
        self._logger = get_logger('FakeRedis')
        self._logger.warning("Using FakeRedis: Data will not persist across restarts.")


    def get(self, key):
        return self._data.get(key)

    def set(self, key, val, ex=None): # 添加 ex 参数以兼容 Redis 的 set
        # 注意：FakeRedis 不支持真正的过期时间 (ex)
        if ex:
            self._logger.debug(f"FakeRedis received set with ex={ex} for key {key}, but expiration is ignored.")
        self._data[key] = str(val) # 模拟 Redis 的字符串存储

    def delete(self, *keys): # 模拟删除
         deleted_count = 0
         for key in keys:
              if key in self._data:
                   del self._data[key]
                   deleted_count += 1
         return deleted_count


    def ping(self):
        # FakeRedis 总是可用的
        return True


class BotFrontend:
    """
    Redis 数据协议 (keys):
    - {frontend_id}:query_text:{bot_chat_id}:{result_msg_id} -> 搜索结果对应的查询文本
    - {frontend_id}:query_chats:{bot_chat_id}:{result_msg_id} -> 搜索结果对应的聊天筛选 (逗号分隔的 chat_id)
    - {frontend_id}:select_chat:{bot_chat_id}:{selection_msg_id} -> 用户通过按钮选择的 chat_id
    """

    def __init__(self, common_cfg: CommonBotConfig, cfg: BotFrontendConfig, frontend_id: str, backend: BackendBot):
        self.backend = backend
        self.id = frontend_id
        self._common_cfg = common_cfg # 保存 common_cfg
        self.bot = TelegramClient(
            # 使用 Path 对象拼接路径
            str(common_cfg.session_dir / f'frontend_{self.id}.session'),
            api_id=common_cfg.api_id,
            api_hash=common_cfg.api_hash,
            proxy=common_cfg.proxy
        )
        self._cfg = cfg
        self._redis: Union[redis.client.Redis, FakeRedis]
        if cfg.no_redis or cfg.redis_host is None:
            self._redis = FakeRedis()
        else:
            try:
                 self._redis = Redis(host=cfg.redis_host[0], port=cfg.redis_host[1], decode_responses=True)
                 self._redis.ping() # 尝试连接
            except RedisConnectionError as e:
                 get_logger(f'bot-frontend:{frontend_id}').critical(
                      f'Cannot connect to Redis server {cfg.redis_host}: {e}. Falling back to FakeRedis.'
                 )
                 self._redis = FakeRedis()
                 self._cfg.no_redis = True # 标记为不使用 Redis
            except Exception as e: # 捕获其他可能的 Redis 初始化错误
                 get_logger(f'bot-frontend:{frontend_id}').critical(
                      f'Error initializing Redis client {cfg.redis_host}: {e}. Falling back to FakeRedis.'
                 )
                 self._redis = FakeRedis()
                 self._cfg.no_redis = True

        self._logger = get_logger(f'bot-frontend:{frontend_id}')
        self._admin_id = None  # 在 start() 中初始化为 int
        self.username = None

        # 下载命令参数解析器
        self.download_arg_parser = ArgumentParser(prog="/download_chat", add_help=False, exit_on_error=False) # add_help=False 避免冲突, exit_on_error=False 让我们可以捕获错误
        self.download_arg_parser.add_argument('--min', type=int, default=0, help="Minimum message ID to download") # 默认0表示从头
        self.download_arg_parser.add_argument('--max', type=int, default=0, help="Maximum message ID to download (0 means no limit)") # 默认0表示不限制
        self.download_arg_parser.add_argument('chats', type=str, nargs='*', help="Chat IDs or usernames")

        # 聊天 ID 参数解析器 (用于 /monitor_chat, /clear)
        self.chat_ids_parser = ArgumentParser(prog="/monitor_chat or /clear", add_help=False, exit_on_error=False)
        self.chat_ids_parser.add_argument('chats', type=str, nargs='*', help="Chat IDs or usernames")

    async def start(self):
        # 解析管理员 ID
        try:
            # 确保 self._cfg.admin 存在且不为空
            if not self._cfg.admin:
                 raise ValueError("Admin ID is not configured.")
            # str_to_chat_id 应该返回 share_id (int)
            self._admin_id = await self.backend.str_to_chat_id(str(self._cfg.admin)) # 确保是字符串
            self._logger.info(f"Admin ID resolved to: {self._admin_id}")
            # 将解析后的 admin ID 加入白名单（如果是 private mode）
            if self._cfg.private_mode and self._admin_id:
                 self._cfg.private_whitelist.add(self._admin_id)
                 self._logger.info(f"Admin {self._admin_id} added to private whitelist.")

        except EntityNotFoundError:
             self._logger.critical(f"Admin entity '{self._cfg.admin}' not found by backend session. Please check the admin_id/username and ensure the backend session can find it.")
             self._admin_id = None # 标记管理员无效
             self._logger.error("Proceeding without a valid admin ID. Admin commands will not work correctly.")
        except (ValueError, TypeError) as e: # 处理配置错误或类型错误
             self._logger.critical(f"Invalid admin configuration '{self._cfg.admin}': {e}")
             self._admin_id = None
             self._logger.error("Proceeding without a valid admin ID.")
        except Exception as e:
             self._logger.critical(f"Error resolving admin entity '{self._cfg.admin}': {e}", exc_info=True)
             self._admin_id = None
             self._logger.error("Proceeding without a valid admin ID.")


        # 再次检查 Redis 连接 (以防初始化时回退到 FakeRedis)
        if not isinstance(self._redis, FakeRedis):
             try:
                  self._redis.ping()
                  self._logger.info(f"Successfully connected to Redis at {self._cfg.redis_host}")
             except RedisConnectionError as e:
                  self._logger.critical(f'Redis connection failed after init: {e}. Falling back to FakeRedis.')
                  self._redis = FakeRedis()
                  self._cfg.no_redis = True

        self._logger.info(f'Start init frontend bot {self.id}')
        try:
             await self.bot.start(bot_token=self._cfg.bot_token)
             me = await self.bot.get_me()
             if me is None:
                  raise RuntimeError("Failed to get bot info (get_me() returned None)")
             self.username = me.username
             bot_id = me.id
             self._logger.info(f'Bot (@{self.username}, id={bot_id}) account login ok')

             # 将机器人自身的 ID 加入后端的排除列表
             self.backend.excluded_chats.add(get_share_id(bot_id))
             self._logger.info(f"Added bot ID {bot_id} to backend's excluded chats.")

             # 注册命令
             await self._register_commands()
             self._logger.info(f'Register bot commands ok')
             self._register_hooks() # 注册消息处理钩子

             # 发送启动消息给管理员 (如果管理员 ID 有效)
             if self._admin_id:
                  try:
                       msg_head = '✅ Bot 前端初始化完成\n\n'
                       stat_text = await self.backend.get_index_status(length_limit=4000 - len(msg_head))
                       await self.bot.send_message(self._admin_id, msg_head + stat_text, parse_mode='html', link_preview=False)
                  except Exception as e:
                       # 发送启动状态时出错也尝试通知管理员
                       error_msg = f'⚠️ Bot 启动，但获取初始状态失败: {e}'
                       self._logger.error(f"Failed to get initial status: {e}", exc_info=True)
                       try:
                           await self.bot.send_message(self._admin_id, error_msg)
                       except Exception as final_e:
                            self._logger.error(f"Failed to send startup status and error message to admin {self._admin_id}: {final_e}")
             else:
                  self._logger.warning("Admin ID not configured or invalid, skipping startup message.")

             self._logger.info(f"Frontend bot {self.id} started successfully.")

        except Exception as e:
             self._logger.critical(f"Failed to start frontend bot: {e}", exc_info=True)
             # 可能需要退出或进行其他错误处理
             # exit(1)


    # --- _callback_handler 已更新 ---
    async def _callback_handler(self, event: events.CallbackQuery.Event):
        # 使用 try-except 包装以捕获处理中的错误
        try:
            self._logger.info(f'Callback query ({event.message_id}) from {event.sender_id} in chat {event.chat_id}, data={event.data}')
            # 检查按钮数据是否为空
            if not event.data:
                await event.answer("无效操作 (no data)。")
                return
            try:
                 query_data = event.data.decode('utf-8')
            except (UnicodeDecodeError, AttributeError):
                 await event.answer("无效操作 (bad data format)。")
                 return

            if not query_data.strip():
                 await event.answer("无效操作 (empty data)。")
                 return

            # 使用更健壮的方式解析数据，例如 '=' 分割
            parts = query_data.split('=', 1)
            if len(parts) != 2:
                self._logger.warning(f"Invalid callback data format: {query_data}")
                await event.answer("操作格式错误。")
                return

            action, value = parts[0], parts[1]
            redis_prefix = f'{self.id}:' # Redis key 前缀
            bot_chat_id = event.chat_id
            result_msg_id = event.message_id

            if action == 'search_page':
                 try:
                      page_num = int(value)
                      if page_num <= 0: raise ValueError("Page number must be positive")
                 except ValueError:
                      self._logger.warning(f"Invalid page number in callback: {value}")
                      await event.answer("无效页码。")
                      return

                 # 从 Redis 获取查询信息
                 # 使用更安全的 key 格式
                 query_key = f'{redis_prefix}query_text:{bot_chat_id}:{result_msg_id}'
                 chats_key = f'{redis_prefix}query_chats:{bot_chat_id}:{result_msg_id}'

                 q = self._redis.get(query_key)
                 chats_str = self._redis.get(chats_key)

                 if q is None: # 检查 q 是否存在
                     self._logger.warning(f"Query text not found in Redis for {query_key}")
                     # 尝试编辑消息提示用户
                     try:
                         await event.edit("抱歉，无法找到此搜索的原始查询信息（可能已过期），请重新搜索。")
                     except rpcerrorlist.MessageNotModifiedError:
                         pass
                     except Exception as edit_e:
                         self._logger.error(f"Failed to edit message to show expired query error: {edit_e}")
                     # 删除可能存在的 chats key
                     if chats_str is not None: self._redis.delete(chats_key)
                     await event.answer("搜索信息已过期。") # 必须 answer callback
                     return

                 chats = None
                 if chats_str:
                     try:
                         # 过滤掉空的 chat_id 字符串
                         chats = [int(chat_id) for chat_id in chats_str.split(',') if chat_id.strip()]
                     except ValueError:
                          self._logger.warning(f"Invalid chat IDs in Redis for {chats_key}: {chats_str}")
                          # 可以选择忽略 chat filter 或报错
                          chats = None # 出错时忽略 filter

                 self._logger.info(f'Query [{q}] (chats={chats}) turned to page {page_num}')

                 start_time = time()
                 try:
                      result = self.backend.search(q, chats, self._cfg.page_len, page_num)
                 except Exception as e:
                      self._logger.error(f"Backend search failed for query '{q}' page {page_num}: {e}", exc_info=True)
                      await event.answer("搜索后端出错，请稍后再试。")
                      return
                 used_time = time() - start_time

                 response = await self._render_response_text(result, used_time)
                 buttons = self._render_respond_buttons(result, page_num)
                 # 使用 try-except 编辑消息，处理消息未修改等错误
                 try:
                     await event.edit(response, parse_mode='html', buttons=buttons, link_preview=False)
                 except rpcerrorlist.MessageNotModifiedError:
                      self._logger.info("Message not modified on page turn (likely same content).")
                      await event.answer() # 即使未修改也要 answer
                 except rpcerrorlist.MessageIdInvalidError:
                      self._logger.warning(f"Message {result_msg_id} for page turn seems to be deleted.")
                      await event.answer("无法编辑消息（可能已被删除）。")
                 except Exception as e:
                      self._logger.error(f"Failed to edit message {result_msg_id} for page turn: {e}")
                      await event.answer("更新搜索结果失败。")
                      # return # 编辑失败也需要 answer，下面有统一的 answer

                 # 无论成功与否，如果前面没有 return，这里 answer
                 await event.answer() # 成功时 answer

            elif action == 'select_chat':
                 try:
                      chat_id = int(value)
                      # 尝试获取 chat_name 用于提示，如果失败则只显示 ID
                      try:
                          chat_name = await self.backend.translate_chat_id(chat_id)
                          display_name = html.escape(chat_name) # 转义 HTML
                          reply_prompt = f'☑️ 已选择对话: **{display_name}** (`{chat_id}`)\n\n回复本条消息可对此对话执行操作 (如 /download_chat, /clear, 或直接搜索此对话)。'
                      except EntityNotFoundError:
                          self._logger.warning(f"Chat ID {value} not found by backend for select_chat display name.")
                          reply_prompt = f'☑️ 已选择对话: `{chat_id}` (无法获取名称)\n\n回复本条消息可对此对话执行操作。'

                      await event.edit(reply_prompt, parse_mode='markdown') # 使用 Markdown

                      # 将选择的 chat_id 存入 Redis，与选择按钮所在的消息 ID 关联
                      select_key = f'{redis_prefix}select_chat:{bot_chat_id}:{result_msg_id}'
                      # 设置一个过期时间，例如 1 小时 (3600 秒)，避免 Redis 无限积累
                      self._redis.set(select_key, chat_id, ex=3600)
                      self._logger.info(f"Chat {chat_id} selected by user {event.sender_id}, stored in Redis key {select_key}")
                      await event.answer("对话已选择") # 给用户一个反馈

                 except ValueError:
                      self._logger.warning(f"Invalid chat ID in select_chat callback: {value}")
                      await event.answer("无效的对话 ID。")
                 except Exception as e:
                      self._logger.error(f"Error processing select_chat callback for value {value}: {e}", exc_info=True)
                      await event.answer("处理选择对话时出错。")
                 # 此处不再需要额外的 await event.answer()，因为上面已经处理了

            elif action == 'noop': # 处理不可点的按钮
                 await event.answer()

            else:
                self._logger.warning(f'Unknown callback action: {action}')
                await event.answer("未知操作。")

        except Exception as e:
             # 捕获处理 callback 过程中的任何未预期错误
             self._logger.error(f"Exception in callback handler for data {event.data!r}: {e}", exc_info=True)
             try:
                  # 尝试向用户发送一个通用的错误提示
                  await event.answer("处理您的请求时发生内部错误。")
             except Exception as final_e:
                  # 如果连 answer 都失败了，就没办法了
                  self._logger.error(f"Failed to even answer callback after an error: {final_e}")

    # --- 结束修改 ---


    async def _normal_msg_handler(self, event: events.NewMessage.Event):
        text: str = event.raw_text.strip()
        sender_entity = await event.message.get_sender()
        sender_id = sender_entity.id if sender_entity else 'Unknown'
        self._logger.info(f'User {sender_id} (in chat {event.chat_id}) sends: "{brief_content(text, 100)}"') # 限制日志长度

        # 检查是否是回复机器人的“选择对话”消息
        selected_chat_context = await self._get_selected_chat_from_reply(event) # (chat_id, chat_name) or None

        if not text or text.startswith('/start'):
            # 可以发送一个欢迎或帮助信息
            await event.reply("欢迎使用 TG Searcher Bot！\n发送关键词进行搜索，或使用 /help 查看可用命令。")
            return

        elif text.startswith('/help'):
             # 提供帮助信息
             # TODO: 可以根据是否是管理员显示不同的帮助
             help_text = """
**可用命令:**
/search `关键词` - 搜索消息 (直接发送关键词也可)。
/chats `[关键词]` - 列出并选择已索引的对话。
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
             await event.reply(help_text, parse_mode='markdown')

        # --- _normal_msg_handler 中 /random 的 Bug Fix 应用 ---
        elif text.startswith('/random'):
            try:
                msg = self.backend.rand_msg()
                chat_name = await self.backend.translate_chat_id(msg.chat_id)
                # 正确处理 content 和 filename 的显示
                display_content = ""
                if msg.filename:
                     display_content += f"📎 {html.escape(msg.filename)}"
                if msg.content:
                     content_brief = html.escape(brief_content(msg.content))
                     if msg.filename:
                          display_content += f" ({content_brief})"
                     else:
                          display_content = content_brief # 如果只有文本

                respond = f'随机消息来自 **{html.escape(chat_name)}** (`{msg.chat_id}`)\n'
                if msg.sender: # 检查 sender 是否为空
                    respond += f'发送者: {html.escape(msg.sender)}\n'
                respond += f'时间: {msg.post_time.strftime("%Y-%m-%d %H:%M")}\n'
                respond += f'内容: {display_content}\n' # 使用处理过的 display_content
                respond += f'<a href="{msg.url}">跳转到消息</a>'

            except IndexError:
                respond = '错误：索引为空，无法获取随机消息。'
            except EntityNotFoundError as e:
                 self._logger.error(f"Error getting random message: Chat ID {e.entity} not found.")
                 respond = f"获取随机消息时出错: 无法找到来源对话。"
            except Exception as e:
                 self._logger.error(f"Error getting random message: {e}", exc_info=True)
                 respond = f"获取随机消息时出错: {type(e).__name__}"
            await event.reply(respond, parse_mode='html', link_preview=False)
        # --- 结束 Bug Fix ---

        elif text.startswith('/chats'):
            buttons = []
            kw = remove_first_word(text)
            monitored_chats_list = sorted(list(self.backend.monitored_chats)) # 排序

            chats_found = 0
            if monitored_chats_list:
                for chat_id in monitored_chats_list:
                    try:
                         chat_name = await self.backend.translate_chat_id(chat_id)
                         # 如果提供了关键词，进行过滤 (忽略大小写)
                         if kw and kw.lower() not in chat_name.lower():
                             continue
                         chats_found += 1
                         # 限制按钮数量，防止消息过长
                         if chats_found <= 50: # 最多显示 50 个
                              # 限制名称长度，避免按钮文本过长
                              display_name = f"{brief_content(chat_name, 25)} (`{chat_id}`)"
                              buttons.append([Button.inline(display_name, f'select_chat={chat_id}')])
                    except EntityNotFoundError:
                         # 如果 chat_id 找不到了，跳过
                         self._logger.warning(f"Chat ID {chat_id} from monitored list not found during /chats command.")
                         continue
                    except Exception as e:
                         self._logger.error(f"Error processing chat {chat_id} for /chats command: {e}")
                         continue # 跳过这个错误的聊天

                if buttons:
                     reply_text = "请选择一个对话进行操作：" if chats_found <= 50 else f"找到 {chats_found} 个对话，显示前 50 个："
                     # 将按钮分成多行，每行最多 N 个 (例如 2 个)
                     button_rows = [buttons[i:i + 2] for i in range(0, len(buttons), 2)]
                     await event.reply(reply_text, buttons=button_rows)
                else:
                     await event.reply(f'没有找到标题包含 "{html.escape(kw)}" 的已索引对话。' if kw else '没有已索引的对话。')

            else:
                await event.reply('暂无监听或索引的对话，请管理员使用 /download_chat 或 /monitor_chat 添加。')

        elif text.startswith('/search'):
            # 处理 /search 命令，与直接发送关键词逻辑合并
            query = remove_first_word(text)
            if not query and not selected_chat_context: # 如果命令后没跟关键词，并且不是回复选择对话的消息
                 await event.reply("请输入要搜索的关键词。用法: `/search 关键词`", parse_mode='markdown')
                 return
            await self._search(event, query, selected_chat_context) # query 可能为空

        elif text.startswith('/'):
            # 处理未知命令
            command = text.split()[0]
            await event.reply(f'错误：未知命令 `{command}`。请使用 /help 查看可用命令。', parse_mode='markdown')

        else:
            # 默认行为：将用户输入视为搜索关键词
            await self._search(event, text, selected_chat_context) # text 是完整的用户输入


    async def _chat_ids_from_args(self, chats_args: List[str]) -> Tuple[List[int], List[str]]:
        """从命令参数解析 chat id 列表 (支持名称或 ID)，返回成功列表和错误列表"""
        chat_ids = []
        errors = []
        if not chats_args:
            return [], [] # 返回空列表和空错误列表

        for chat_arg in chats_args:
            try:
                # session.str_to_chat_id 应该返回原始 ID 或 share_id，这里统一用 share_id
                chat_id = await self.backend.str_to_chat_id(chat_arg) # backend.str_to_chat_id 已处理 share_id
                chat_ids.append(chat_id) # 添加 share_id
            except EntityNotFoundError:
                errors.append(f'未找到对话 "{html.escape(chat_arg)}"')
            except Exception as e:
                 errors.append(f'解析对话 "{html.escape(chat_arg)}" 时出错: {type(e).__name__}')
        return chat_ids, errors


    async def _admin_msg_handler(self, event: events.NewMessage.Event):
        text: str = event.raw_text.strip()
        self._logger.info(f'Admin {event.chat_id} sends command: "{brief_content(text, 100)}"')

        # 检查是否是回复机器人的“选择对话”消息
        selected_chat_context = await self._get_selected_chat_from_reply(event) # (chat_id, chat_name) or None
        selected_chat_id = selected_chat_context[0] if selected_chat_context else None
        selected_chat_name = selected_chat_context[1] if selected_chat_context else None

        # --- _admin_msg_handler 中 /stat 的 Bug Fix 应用 ---
        if text.startswith('/stat'):
            try:
                 status_msg = await self.backend.get_index_status()
                 await event.reply(status_msg, parse_mode='html', link_preview=False)
            except Exception as e:
                 self._logger.error("Error getting backend status:", exc_info=True)
                 # 将错误信息发送给管理员
                 error_trace = html.escape(format_exc())
                 await event.reply(f"获取后端状态时出错: {html.escape(str(e))}\n<pre>{error_trace}</pre>", parse_mode='html')
        # --- 结束 Bug Fix ---


        elif text.startswith('/download_chat'):
            try:
                 # 使用 shlex 分割参数，处理带引号的情况
                 args_list = shlex.split(text)[1:]
                 args = self.download_arg_parser.parse_args(args_list)
            except Exception as e: # 捕获参数解析错误 (包括 SystemExit)
                 # ArgumentParser 在出错时可能会调用 sys.exit()，需要捕获
                 usage = self.download_arg_parser.format_help() # 获取用法信息
                 await event.reply(f"参数解析错误: {e}\n用法:\n<pre>{html.escape(usage)}</pre>", parse_mode='html')
                 return

            min_id = args.min if args.min > 0 else 0 # 0 表示从头开始
            max_id = args.max if args.max > 0 else 0 # 0 表示不限制

            # 优先使用命令参数中的 chats
            target_chat_ids, errors = await self._chat_ids_from_args(args.chats)

            # 如果命令参数没有指定 chats，并且是回复选择对话的消息，则使用选择的对话
            if not args.chats and selected_chat_id is not None:
                 # 检查通过回复选择的对话是否已在解析错误列表中（不太可能，但以防万一）
                 if selected_chat_id not in [cid for cid in target_chat_ids]: # 应该检查原始列表？
                     target_chat_ids = [selected_chat_id]
                     await event.reply(f"检测到回复，将对选择的对话 **{html.escape(selected_chat_name)}** (`{selected_chat_id}`) 执行下载。", parse_mode='markdown')
                 else:
                      # 如果回复的 chat id 解析也出错了，提示用户
                      await event.reply(f"检测到回复选择的对话 `{selected_chat_id}`，但解析时遇到问题。请检查错误信息。")

            elif not target_chat_ids and not errors: # 没有参数，没有回复，也没有错误
                 await event.reply(f"错误：请指定至少一个对话的 ID 或名称，或者回复一条通过 /chats 选择对话的消息。")
                 return

            # 如果解析参数时出错
            if errors:
                 await event.reply("以下对话无法解析:\n- " + "\n- ".join(errors))
                 # 可以选择继续处理成功的部分，或者直接返回
                 if not target_chat_ids: return # 如果一个都没成功，就返回

            # 对每个目标 chat_id 执行下载
            success_count = 0
            fail_count = 0
            for chat_id in target_chat_ids: # 这里应该是 share_id 列表了
                 self._logger.info(f'Admin triggered download history for {chat_id} (min={min_id}, max={max_id})')
                 try:
                      await self._download_history(event, chat_id, min_id, max_id)
                      success_count += 1
                      self._logger.info(f'Finished download task for {chat_id} (min={min_id}, max={max_id})')
                 except Exception as dl_e:
                      fail_count += 1
                      self._logger.error(f"Download failed for chat {chat_id}: {dl_e}", exc_info=True)
                      # 可以尝试给管理员发个失败提示
                      try:
                           chat_html = await self.backend.format_dialog_html(chat_id)
                           await event.reply(f"❌ 下载 {chat_html} 失败: {html.escape(str(dl_e))}", parse_mode='html')
                      except Exception: # 获取名字也可能失败
                           await event.reply(f"❌ 下载对话 `{chat_id}` 失败: {html.escape(str(dl_e))}", parse_mode='html')

            # 可以在所有任务完成后给一个总结
            if len(target_chat_ids) > 1:
                 await event.reply(f"所有下载任务完成：{success_count} 成功, {fail_count} 失败。")


        elif text.startswith('/monitor_chat'):
            try:
                 args_list = shlex.split(text)[1:]
                 args = self.chat_ids_parser.parse_args(args_list)
            except Exception as e:
                 usage = self.chat_ids_parser.format_help()
                 await event.reply(f"参数解析错误: {e}\n用法:\n<pre>{html.escape(usage)}</pre>", parse_mode='html')
                 return

            target_chat_ids, errors = await self._chat_ids_from_args(args.chats)

            if not args.chats and selected_chat_id is not None:
                 if selected_chat_id not in [cid for cid in target_chat_ids]:
                      target_chat_ids = [selected_chat_id]
                      await event.reply(f"检测到回复，将对选择的对话 **{html.escape(selected_chat_name)}** (`{selected_chat_id}`) 加入监听。", parse_mode='markdown')
                 else:
                      await event.reply(f"检测到回复选择的对话 `{selected_chat_id}`，但解析时遇到问题。请检查错误信息。")
            elif not target_chat_ids and not errors:
                 await event.reply(f"错误：请指定至少一个对话的 ID 或名称，或者回复一条通过 /chats 选择对话的消息。")
                 return

            if errors:
                 await event.reply("以下对话无法解析:\n- " + "\n- ".join(errors))
                 if not target_chat_ids: return

            replies = []
            added_count = 0
            already_monitored = 0
            for chat_id in target_chat_ids: # 这里是 share_id
                 if chat_id in self.backend.monitored_chats:
                      already_monitored += 1
                 else:
                      self.backend.monitored_chats.add(chat_id)
                      added_count += 1
                      # 尝试获取对话名称用于回复
                      try:
                           chat_html = await self.backend.format_dialog_html(chat_id)
                           replies.append(f"- ✅ {chat_html} 已成功加入监听列表。")
                      except Exception as e:
                           replies.append(f"- ✅ 对话 `{chat_id}` 已加入监听列表 (无法获取名称: {type(e).__name__})。")
                      self._logger.info(f'Admin added {chat_id} to monitored_chats')

            if replies: # 只在有新添加时发送列表
                 await event.reply('\n'.join(replies), parse_mode='html', link_preview=False)

            # 发送总结信息
            summary = []
            if added_count > 0: summary.append(f"{added_count} 个对话已加入监听。")
            if already_monitored > 0: summary.append(f"{already_monitored} 个对话已在监听列表中。")
            if summary: await event.reply(" ".join(summary))


        elif text.startswith('/clear'):
             try:
                 args_list = shlex.split(text)[1:]
                 args = self.chat_ids_parser.parse_args(args_list)
             except Exception as e:
                 usage = self.chat_ids_parser.format_help()
                 await event.reply(f"参数解析错误: {e}\n用法:\n<pre>{html.escape(usage)}</pre>", parse_mode='html')
                 return

             # 处理 '/clear all'
             if len(args.chats) == 1 and args.chats[0].lower() == 'all':
                 self._logger.warning(f'Admin triggered CLEAR ALL index')
                 try:
                      self.backend.clear(chat_ids=None) # None 表示清除全部
                      await event.reply('✅ 全部索引已成功清除。')
                 except Exception as e:
                      self._logger.error("Error during clear all:", exc_info=True)
                      await event.reply(f"清除全部索引时出错: {e}")
                 return # 清除全库后结束

             # 处理指定对话或回复的情况
             target_chat_ids, errors = await self._chat_ids_from_args(args.chats)

             if not args.chats and selected_chat_id is not None:
                  if selected_chat_id not in [cid for cid in target_chat_ids]:
                      target_chat_ids = [selected_chat_id]
                      await event.reply(f"检测到回复，将清除选择的对话 **{html.escape(selected_chat_name)}** (`{selected_chat_id}`) 的索引。", parse_mode='markdown')
                  else:
                      await event.reply(f"检测到回复选择的对话 `{selected_chat_id}`，但解析时遇到问题。请检查错误信息。")

             elif not target_chat_ids and not errors:
                  await event.reply(
                      "错误：请指定要清除索引的对话 ID 或名称，或使用 `/clear all` 清除全部索引，"
                      "或回复一条通过 /chats 选择对话的消息。", parse_mode='html')
                  return

             if errors:
                 await event.reply("以下对话无法解析:\n- " + "\n- ".join(errors))
                 if not target_chat_ids: return

             # 执行清除
             # share_ids_to_clear = [get_share_id(cid) for cid in target_chat_ids] # _chat_ids_from_args 已返回 share_id
             share_ids_to_clear = target_chat_ids
             self._logger.info(f'Admin triggered clear index for chats: {share_ids_to_clear}')
             try:
                  self.backend.clear(chat_ids=share_ids_to_clear) # 传递 share_id 列表
                  replies = []
                  for chat_id in share_ids_to_clear: # 仍然用 share_id 操作
                       try:
                           chat_html = await self.backend.format_dialog_html(chat_id)
                           replies.append(f"- ✅ {chat_html} 的索引已清除。")
                       except Exception:
                            replies.append(f"- ✅ 对话 `{chat_id}` 的索引已清除 (无法获取名称)。")
                  await event.reply('\n'.join(replies), parse_mode='html', link_preview=False)
             except Exception as e:
                  self._logger.error(f"Error clearing index for chats {share_ids_to_clear}:", exc_info=True)
                  await event.reply(f"清除指定对话索引时出错: {e}")


        elif text.startswith('/refresh_chat_names'):
            msg = await event.reply('正在刷新后端的对话名称缓存...')
            try:
                await self.backend.session.refresh_translate_table()
                await msg.edit('✅ 对话名称缓存刷新完成。')
            except Exception as e:
                 self._logger.error("Error refreshing chat names:", exc_info=True)
                 await msg.edit(f'刷新对话名称缓存时出错: {e}')


        elif text.startswith('/find_chat_id'):
            q = remove_first_word(text) # 获取命令后的所有内容作为关键词
            if not q:
                await event.reply('错误：请输入要查找的对话名称关键词。用法: `/find_chat_id 关键词`')
                return

            try:
                # find_chat_id 返回的是 share_id 列表
                chat_results = await self.backend.find_chat_id(q)
                sb = []
                if chat_results:
                     sb.append(f'找到 {len(chat_results)} 个标题中包含 "{html.escape(q)}" 的对话:\n')
                     # 限制显示数量
                     for chat_id in chat_results[:50]: # 最多显示 50 个
                         try:
                              # chat_id 已经是 share_id
                              chat_name = await self.backend.translate_chat_id(chat_id)
                              sb.append(f'- {html.escape(chat_name)}: `{chat_id}`\n') # 显示 share_id
                         except EntityNotFoundError:
                              sb.append(f'- 未知对话: `{chat_id}` (可能已离开或被删除)\n')
                         except Exception as e:
                              sb.append(f'- 对话 `{chat_id}` 获取名称失败: {type(e).__name__}\n')
                     if len(chat_results) > 50:
                          sb.append("\n(仅显示前 50 个结果)")
                else:
                     sb.append(f'未找到标题中包含 "{html.escape(q)}" 的对话。')

                await event.reply(''.join(sb), parse_mode='html')
            except Exception as e:
                 self._logger.error(f"Error finding chat ID for query '{q}':", exc_info=True)
                 await event.reply(f"查找对话 ID 时出错: {e}")

        else:
            # 如果管理员发送的不是以上命令，也按普通消息处理（允许管理员搜索等）
            await self._normal_msg_handler(event)


    async def _search(self, event: events.NewMessage.Event, query: str, selected_chat_context: Optional[Tuple[int, str]]):
        """执行搜索"""
        if not query and selected_chat_context:
             # 如果是回复选择对话的消息但没有提供额外查询词，可以提示或搜索该对话的全部内容
             query = '*' # 搜索全部，但限定在该对话
             await event.reply(f"将搜索对话 **{html.escape(selected_chat_context[1])}** (`{selected_chat_context[0]}`) 中的所有已索引消息。", parse_mode='markdown')
        elif not query:
             # 如果既没有查询词，也不是回复上下文，不执行搜索
             self._logger.debug("Empty search query received, ignoring.")
             return

        # 检查索引是否为空
        is_target_empty = False
        target_chat_id_list = [selected_chat_context[0]] if selected_chat_context else None
        try:
            if target_chat_id_list:
                 is_target_empty = self.backend.is_empty(chat_id=target_chat_id_list[0])
            elif not selected_chat_context: # 搜索全部时，检查全局索引
                 is_target_empty = self.backend.is_empty()
        except Exception as e:
             self._logger.error(f"Error checking index emptiness: {e}")
             await event.reply("检查索引状态时出错，请稍后重试。")
             return


        if is_target_empty:
             if selected_chat_context:
                  await event.reply(f'对话 **{html.escape(selected_chat_context[1])}** (`{selected_chat_context[0]}`) 的索引为空，请先使用 /download_chat 添加。', parse_mode='markdown')
             else:
                  await event.reply('当前全局索引为空，请先使用 /download_chat 添加对话。')
             return

        start_time = time()
        search_context_info = f"in chat {selected_chat_context[0]}" if selected_chat_context else "globally"
        self._logger.info(f'Searching "{query}" {search_context_info}')

        try:
            result = self.backend.search(query, in_chats=target_chat_id_list, page_len=self._cfg.page_len, page_num=1)
            used_time = time() - start_time

            respond_text = await self._render_response_text(result, used_time)
            buttons = self._render_respond_buttons(result, 1)

            # 发送结果
            msg: Optional[TgMessage] = await event.reply(respond_text, parse_mode='html', buttons=buttons, link_preview=False)

            # 存储查询信息到 Redis 以支持翻页 (只有成功发送消息才存储)
            if msg:
                redis_prefix = f'{self.id}:'
                bot_chat_id = event.chat_id
                result_msg_id = msg.id
                query_key = f'{redis_prefix}query_text:{bot_chat_id}:{result_msg_id}'
                chats_key = f'{redis_prefix}query_chats:{bot_chat_id}:{result_msg_id}'
                # 设置过期时间，例如 1 小时
                self._redis.set(query_key, query, ex=3600)
                if target_chat_id_list:
                     # 确保列表非空再 join
                     chats_str = ','.join(map(str, target_chat_id_list)) if target_chat_id_list else ''
                     if chats_str:
                          self._redis.set(chats_key, chats_str, ex=3600)
                     else:
                          self._redis.delete(chats_key) # 确保空列表不存储
                else:
                     # 如果是全局搜索，删除可能存在的旧 key
                     self._redis.delete(chats_key)
            else:
                 self._logger.error("Failed to send search result message.")


        except whoosh.index.LockError:
             # Whoosh 写入锁冲突
             self._logger.warning("Index lock error during search.")
             await event.reply('⏳ 索引当前正在写入中，请稍后再试。')
        except Exception as e:
             self._logger.error(f"Error during search for query '{query}':", exc_info=True)
             await event.reply(f'搜索时发生错误: {type(e).__name__}。请检查日志或联系管理员。')


    async def _download_history(self, event: events.NewMessage.Event, chat_id: int, min_id: int, max_id: int):
         # chat_id 应该是 share_id 了
         try:
             chat_html = await self.backend.format_dialog_html(chat_id) # 使用 share_id 获取名称和链接
         except Exception as e:
              self._logger.error(f"Failed to format chat html for {chat_id}: {e}")
              chat_html = f"对话 `{chat_id}`" # 回退显示

         # 检查是否重复下载全部历史
         # is_empty 检查的是 share_id
         try:
             if min_id == 0 and max_id == 0 and not self.backend.is_empty(chat_id=chat_id):
                 await event.reply(
                     f'⚠️ 警告: {chat_html} 的索引已存在。\n'
                     f'重新下载全部历史 (min=0, max=0) **可能导致消息重复**。\n'
                     f'如需增量更新，请使用 `--min` 指定上次结束的消息 ID。\n'
                     f'如确认要重新下载，请先使用 `/clear {chat_id}` 清除现有索引。',
                     parse_mode='html')
                 # return # 暂时不阻止，仅警告
         except Exception as e:
             self._logger.error(f"Error checking index emptiness before download for chat {chat_id}: {e}")
             # 无法检查，继续下载但可能有风险

         # 使用 nonlocal 变量在回调中更新状态
         prog_msg: Optional[TgMessage] = None
         last_update_time = time()
         update_interval = 5 # 每 5 秒更新一次进度
         total_downloaded_count = 0 # 用于最终统计的回调计数

         async def call_back(current_msg_id: int, downloaded_count: int):
             nonlocal prog_msg, last_update_time, total_downloaded_count
             total_downloaded_count = downloaded_count # 更新最终计数
             now = time()
             # 限制进度更新频率
             if now - last_update_time > update_interval:
                 last_update_time = now
                 prog_text = f'⏳ 正在下载 {chat_html}:\n已处理 {downloaded_count} 条，当前 ID: {current_msg_id}'

                 # 使用 try-except 更新消息
                 try:
                     if prog_msg is None:
                         # 第一次发送进度
                         prog_msg = await event.reply(prog_text, parse_mode='html')
                     else:
                         # 尝试编辑现有进度消息
                         await prog_msg.edit(prog_text, parse_mode='html')
                 except rpcerrorlist.FloodWaitError as fwe:
                      self._logger.warning(f"Flood wait ({fwe.seconds}s) encountered while updating download progress for {chat_id}. Skipping update.")
                      # 增加等待时间，避免立即重试
                      last_update_time += fwe.seconds
                 except rpcerrorlist.MessageNotModifiedError:
                      pass # 消息未改变，忽略
                 except rpcerrorlist.MessageIdInvalidError:
                       self._logger.warning(f"Progress message for chat {chat_id} seems to be deleted. Cannot update progress.")
                       prog_msg = None # 标记为无效，不再尝试编辑
                 except Exception as e:
                      self._logger.error(f"Failed to edit progress message for {chat_id}: {e}")
                      # 也许禁用后续更新？
                      prog_msg = None # 标记为无效

         # 开始下载
         start_time = time()
         try:
              await self.backend.download_history(chat_id, min_id, max_id, call_back)
              # 使用回调中更新的 total_downloaded_count 作为最终数量
              used_time = time() - start_time
              completion_msg = f'✅ {chat_html} 下载完成，共索引 {total_downloaded_count} 条消息，用时 {used_time:.2f} 秒。'
              # 尝试回复原始命令消息，如果失败则发送新消息
              try:
                  await event.reply(completion_msg, parse_mode='html')
              except Exception: # 可能原始消息被删等
                   await self.bot.send_message(event.chat_id, completion_msg, parse_mode='html')

         except EntityNotFoundError as e:
              self._logger.error(f"Failed to download history for {chat_id}: {e}")
              await event.reply(f'❌ 下载 {chat_html} 时出错: {e}', parse_mode='html') # 显示 EntityNotFoundError 的消息
              # 可能需要从监控列表移除
              self.backend.monitored_chats.discard(chat_id)
         except Exception as e:
              self._logger.error(f"Failed to download history for {chat_id}:", exc_info=True)
              await event.reply(f'❌ 下载 {chat_html} 时发生错误: {type(e).__name__}', parse_mode='html')
         finally:
              # 删除进度消息
              if prog_msg:
                   try:
                        await prog_msg.delete()
                   except Exception as e:
                        self._logger.warning(f"Failed to delete progress message for {chat_id}: {e}")

    # _get_last_download_count_from_log 不再需要


    def _register_hooks(self):
        @self.bot.on(events.CallbackQuery())
        async def callback_query_handler(event: events.CallbackQuery.Event):
             # 权限检查
             sender_id = event.sender_id
             # 确保白名单检查逻辑正确
             is_whitelisted = sender_id in self._cfg.private_whitelist
             if self._cfg.private_mode and not is_whitelisted and sender_id != self._admin_id:
                   self._logger.warning(f"Blocked callback query from non-whitelisted user {sender_id}.")
                   await event.answer("抱歉，您无权使用此按钮。", alert=True)
                   return
             await self._callback_handler(event)


        @self.bot.on(events.NewMessage())
        async def bot_message_handler(event: events.NewMessage.Event):
            sender = await event.message.get_sender()
            if not sender:
                 self._logger.debug("Ignoring message with no sender info.")
                 return # 无法获取发送者信息

            sender_id = sender.id
            my_id = (await self.bot.get_me()).id

            # 忽略自己的消息
            if sender_id == my_id:
                 return

            # 检查是否是管理员
            is_admin = (self._admin_id is not None and sender_id == self._admin_id)

            # 群组/频道中，检查是否提及机器人或回复机器人消息
            is_mentioned = False
            is_reply_to_bot = False
            if event.is_group or event.is_channel:
                 # 检查文本提及
                 if self.username and f'@{self.username}' in event.raw_text:
                      is_mentioned = True
                 # 检查实体提及 (@机器人)
                 elif event.message.mentioned:
                      # 检查被提及的是否是自己
                      if event.message.entities:
                           for entity in event.message.entities:
                               if isinstance(entity, MessageEntityMentionName):
                                    if entity.user_id == my_id:
                                         is_mentioned = True
                                         break
                 # 检查是否回复了机器人的消息
                 if event.message.is_reply:
                      reply_msg = await event.message.get_reply_message()
                      if reply_msg and reply_msg.sender_id == my_id:
                           is_reply_to_bot = True


            # 处理条件：私聊；或在群组/频道中被提及；或在群组/频道中回复了机器人
            should_process = event.is_private or is_mentioned or is_reply_to_bot

            if not should_process:
                 # 在群组中且未被提及也未回复机器人，忽略
                 self._logger.debug(f"Ignoring message in group/channel {event.chat_id} from {sender_id} (not mentioned/reply).")
                 return

            # 私人模式权限检查 (只对非管理员进行)
            if self._cfg.private_mode and not is_admin:
                 # 获取聊天的 share_id (对于私聊就是对方 user_id)
                 try:
                     chat_share_id = get_share_id(event.chat_id)
                 except Exception: # 处理无效 chat_id
                     chat_share_id = None # 或其他标记

                 is_sender_whitelisted = sender_id in self._cfg.private_whitelist
                 is_chat_whitelisted = chat_share_id is not None and chat_share_id in self._cfg.private_whitelist

                 if not is_sender_whitelisted and not is_chat_whitelisted:
                     self._logger.info(f"Blocked access for user {sender_id} in chat {event.chat_id} ({chat_share_id}) due to private mode.")
                     # 只在私聊中回复拒绝信息，避免在群组中刷屏
                     if event.is_private:
                         await event.reply('抱歉，由于隐私设置，您无法使用本机器人。')
                     return

            # 根据是否是管理员分发消息
            handler_task = None
            if is_admin:
                # 如果是管理员发的，优先尝试管理员命令处理器
                handler_task = self._admin_msg_handler(event)
            else:
                # 普通用户使用普通消息处理器
                handler_task = self._normal_msg_handler(event)

            # 执行处理器并捕获异常
            if handler_task:
                 try:
                      await handler_task
                 except whoosh.index.LockError:
                      await event.reply('⏳ 索引当前正在被其他操作锁定，请稍后再试。')
                 except EntityNotFoundError as e:
                      await event.reply(f'❌ 未找到指定的对话或用户: {e.entity}')
                 except telethon.errors.rpcerrorlist.UserIsBlockedError:
                       self._logger.warning(f"User {sender_id} has blocked the bot.")
                       # 无法回复，只能记录
                 except telethon.errors.rpcerrorlist.ChatWriteForbiddenError:
                       self._logger.warning(f"Bot does not have permission to send messages in chat {event.chat_id}.")
                 except Exception as e:
                      # 捕获通用错误
                      err_type = type(e).__name__
                      self._logger.error(f"Error handling message from {sender_id}: {err_type}: {e}", exc_info=True)
                      try:
                           # 尝试向用户发送错误信息
                           await event.reply(f'处理您的请求时发生错误: {err_type}。\n请联系管理员检查日志。')
                      except Exception as reply_e:
                           self._logger.error(f"Failed to reply error message to {sender_id}: {reply_e}")
                      # 可以选择将详细错误信息发送给管理员
                      if self._admin_id and event.chat_id != self._admin_id: # 避免重复发送
                           try:
                               await self.bot.send_message(
                                    self._admin_id,
                                    f"处理用户 {sender_id} (在聊天 {event.chat_id} 中) 的消息时发生错误:\n"
                                    f"<pre>{html.escape(format_exc())}</pre>",
                                    parse_mode='html'
                               )
                           except Exception as admin_notify_e:
                                self._logger.error(f"Failed to notify admin about error: {admin_notify_e}")


    async def _get_selected_chat_from_reply(self, event: events.NewMessage.Event) -> Optional[Tuple[int, str]]:
        """检查消息是否回复了“选择对话”的消息，并从 Redis 获取 chat_id"""
        msg = event.message
        if not msg.is_reply:
            return None

        # 获取被回复的消息 ID
        reply_to_msg_id = msg.reply_to_msg_id
        if not reply_to_msg_id:
             return None # Should not happen if is_reply is true, but check anyway

        # 查询 Redis
        redis_prefix = f'{self.id}:'
        select_key = f'{redis_prefix}select_chat:{event.chat_id}:{reply_to_msg_id}'
        redis_result = self._redis.get(select_key)

        if redis_result:
            try:
                chat_id = int(redis_result) # chat_id 存的是 share_id
                # 尝试获取 chat_name 用于后续提示
                try:
                     # 使用 chat_id (share_id) 获取名称
                     chat_name = await self.backend.translate_chat_id(chat_id)
                except EntityNotFoundError:
                     chat_name = f"未知对话 ({chat_id})"
                self._logger.info(f"Message from {event.sender_id} is a reply to chat selection message for chat {chat_id}")
                return chat_id, chat_name
            except ValueError:
                 self._logger.warning(f"Invalid chat_id found in Redis key {select_key}: {redis_result}")
                 # 删除无效的 key
                 self._redis.delete(select_key)
                 return None
            except Exception as e:
                 self._logger.error(f"Error processing selected chat context for key {select_key}: {e}")
                 return None
        else:
            # 不是回复选择对话的消息，或者 Redis 记录已过期
            return None


    async def _register_commands(self):
        # 检查管理员 ID 是否有效
        admin_input_peer = None
        if not self._admin_id:
             self._logger.warning("Admin ID is not valid. Skipping registration of admin-specific commands.")
        else:
             try:
                 # 使用已解析的整数 ID 获取 input entity
                 admin_input_peer = await self.bot.get_input_entity(self._admin_id)
             except (ValueError, TypeError) as e: # 处理 ID 无效或类型错误
                 self._logger.error(
                     f'Failed to get input entity for admin ID {self._admin_id}. '
                     f'Ensure the bot has interacted with the admin. Admin commands might not register correctly. Error: {e}')
                 admin_input_peer = None
             except Exception as e:
                  self._logger.error(f"Unexpected error getting admin input entity {self._admin_id}: {e}", exc_info=True)
                  admin_input_peer = None

        # 定义命令
        admin_commands = [
            BotCommand(command="download_chat", description='[选项] [对话...] 下载并索引历史消息'),
            BotCommand(command="monitor_chat", description='对话... 将对话加入监听列表'),
            BotCommand(command="clear", description='[对话...|all] 清除索引'),
            BotCommand(command="stat", description='查询后端索引状态'),
            BotCommand(command="find_chat_id", description='关键词 根据名称查找对话 ID'),
            BotCommand(command="refresh_chat_names", description='刷新对话名称缓存'),
        ]
        common_commands = [
            BotCommand(command="search", description='关键词 搜索消息 (直接发送也可)'),
            BotCommand(command="chats", description='[关键词] 列出/选择已索引对话'),
            BotCommand(command="random", description='随机返回一条已索引消息'),
            BotCommand(command="help", description='显示帮助信息'),
        ]

        # 为管理员设置合并后的命令
        if admin_input_peer:
            try:
                 await self.bot(
                     SetBotCommandsRequest(
                         scope=BotCommandScopePeer(admin_input_peer),
                         lang_code='', # 空 lang_code 表示所有语言
                         commands=admin_commands + common_commands # 管理员看到所有命令
                     )
                 )
                 self._logger.info(f"Successfully set commands for admin {self._admin_id}.")
            except Exception as e:
                 self._logger.error(f"Failed to set commands for admin {self._admin_id}: {e}", exc_info=True)

        # 为其他用户设置通用命令
        try:
            await self.bot(
                SetBotCommandsRequest(
                    scope=BotCommandScopeDefault(), # 默认范围应用于所有非特定用户
                    lang_code='',
                    commands=common_commands # 普通用户只看到通用命令
                )
            )
            self._logger.info("Successfully set default commands for other users.")
        except Exception as e:
             self._logger.error(f"Failed to set default commands: {e}", exc_info=True)

    # --- _render_response_text 已包含 filename 处理 ---
    async def _render_response_text(self, result: SearchResult, used_time: float) -> str:
        """渲染搜索结果为 HTML 格式的文本"""
        if result.total_results == 0:
             return "未找到相关消息。"

        string_builder = [f'找到 {result.total_results} 条结果，用时 {used_time:.3f} 秒:\n\n']
        for i, hit in enumerate(result.hits, 1):
            # hit.msg 是 IndexMsg 对象
            msg: IndexMsg = hit.msg
            try:
                 chat_title = await self.backend.translate_chat_id(msg.chat_id)
            except EntityNotFoundError:
                 chat_title = f"未知对话 ({msg.chat_id})"
            except Exception as e:
                 chat_title = f"对话 {msg.chat_id} (获取名称出错)"
                 self._logger.warning(f"Error translating chat_id {msg.chat_id} for result display: {e}")


            # 1. 消息头: 对话标题, 发送者, 时间
            header_parts = [f"<b>{i}. {html.escape(chat_title)}</b>"] # 添加序号
            if msg.sender:
                 header_parts.append(f"(<u>{html.escape(msg.sender)}</u>)")
            # 使用更简洁的日期格式
            header_parts.append(f'[{msg.post_time.strftime("%y-%m-%d %H:%M")}]')
            string_builder.append(' '.join(header_parts) + '\n')

            # 2. 文件名 (如果存在)
            if msg.filename:
                 string_builder.append(f"📎 文件: <b>{html.escape(msg.filename)}</b>\n")

            # 3. 消息链接和内容/标题
            # hit.highlighted 是对 msg.content (消息文本/标题) 的高亮结果
            link_text = hit.highlighted.strip() if hit.highlighted else ""
            # 如果高亮为空，尝试使用原始内容或文件名
            if not link_text:
                if msg.content: # 优先使用原始内容
                    link_text = html.escape(brief_content(msg.content, 50))
                elif msg.filename: # 其次使用文件名
                    link_text = f"跳转到文件: {html.escape(brief_content(msg.filename, 50))}"
                else: # 兜底
                    link_text = "跳转到消息"

            # 确保 URL 存在且有效
            if msg.url:
                string_builder.append(f'<a href="{html.escape(msg.url)}">{link_text}</a>\n\n')
            else:
                string_builder.append(f"{link_text} (无链接)\n\n")


        # 限制总长度
        final_text = ''.join(string_builder)
        max_len = 4096 # Telegram 消息长度限制
        if len(final_text) > max_len:
             # 尝试从最后一个换行符截断
             last_newline = final_text.rfind('\n\n', 0, max_len - 50) # 留出截断提示空间
             cutoff_msg = "\n\n...(结果过多，仅显示部分)"
             if last_newline != -1:
                  final_text = final_text[:last_newline] + cutoff_msg
             else:
                  # 如果无法按换行截断，直接硬截断
                  final_text = final_text[:max_len - len(cutoff_msg)] + cutoff_msg

        return final_text
    # --- 结束修改 ---

    # --- _render_respond_buttons 已更新 ---
    def _render_respond_buttons(self, result: SearchResult, cur_page_num: int) -> Optional[List[List[Button]]]:
        """创建搜索结果的翻页按钮"""
        if result.total_results == 0:
             return None # 没有结果不需要按钮

        # 计算总页数
        try:
             # 确保 page_len > 0
             page_len = self._cfg.page_len if self._cfg.page_len > 0 else 10
             total_pages = (result.total_results + page_len - 1) // page_len
        except ZeroDivisionError:
             total_pages = 1 # 防止除零错误

        if total_pages <= 1:
             return None # 只有一页或没有结果不需要按钮

        buttons = []
        row = []

        # 上一页按钮
        if cur_page_num > 1:
            row.append(Button.inline('⬅️ 上一页', f'search_page={cur_page_num - 1}'))
        else:
            # 可以添加一个占位符或者禁用按钮，或者不显示
            # row.append(Button.inline(' ', 'noop')) # 占位符
            pass # 这里选择不显示

        # 页码显示
        row.append(Button.inline(f'{cur_page_num} / {total_pages}', 'noop')) # noop 表示按钮不可点

        # 下一页按钮
        # 检查 is_last_page 是否可靠，并结合页码判断
        if not result.is_last_page and cur_page_num < total_pages:
             row.append(Button.inline('下一页 ➡️', f'search_page={cur_page_num + 1}'))
        else:
             # row.append(Button.inline(' ', 'noop')) # 占位符
             pass

        if row: # 只有当有按钮时才添加这一行
            buttons.append(row)

        return buttons if buttons else None # 如果没有任何按钮，返回 None
    # --- 结束修改 ---
