# -*- coding: utf-8 -*-
from pathlib import Path
from datetime import datetime
import random
from typing import Optional, Union, List, Set

from whoosh import index, writing
from whoosh.fields import Schema, TEXT, ID, DATETIME, NUMERIC # 索引字段类型
# 移除 OrGroup 导入, 默认使用 AndGroup (之前代码已移除)
from whoosh.qparser import QueryParser, MultifieldPlugin
from whoosh.writing import IndexWriter
from whoosh.query import Term, Or, And, Not, Every # 查询对象
import whoosh.highlight as highlight # 高亮模块
from jieba.analyse.analyzer import ChineseAnalyzer # 中文分词器
import html # 用于 HTML 转义

# 日志记录器设置
try:
    from .common import get_logger, brief_content
    logger = get_logger('indexer')
except (ImportError, AttributeError, ModuleNotFoundError):
    import logging
    logger = logging.getLogger('indexer')
    if not logger.hasHandlers():
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logger.info("Indexer logger initialized with basicConfig.")
    # 如果 common.py 不可用，定义一个简单的 brief_content
    def brief_content(text: Optional[str], max_len: int = 100) -> str:
        if text is None: return ""
        text = text.strip().replace('\n', ' ')
        return text[:max_len] + ('...' if len(text) > max_len else '')


class IndexMsg:
    """代表一条待索引或已索引消息的数据结构"""
    # 定义索引的 Schema (结构) - 包含文件相关字段
    schema = Schema(
        content=TEXT(stored=True, analyzer=ChineseAnalyzer()), # 消息内容，使用中文分词
        url=ID(stored=True, unique=True), # 消息链接，作为唯一标识符
        chat_id=TEXT(stored=True), # 对话 ID (使用 TEXT 存储，便于 Whoosh 处理)
        post_time=DATETIME(stored=True, sortable=True), # 发送时间，可排序
        sender=TEXT(stored=True), # 发送者名称
        filename=TEXT(stored=True, analyzer=ChineseAnalyzer()), # 文件名，使用中文分词
        has_file=NUMERIC(stored=True, numtype=int) # 是否包含文件 (0: no, 1: yes)
    )

    def __init__(self, content: str, url: str, chat_id: Union[int, str],
                post_time: datetime, sender: str, filename: Optional[str] = None):
        """初始化 IndexMsg 对象"""
        self.content = content
        self.url = url
        try:
            # 内部存储为 int，但 Schema 中是 TEXT
            self.chat_id = int(chat_id)
        except (ValueError, TypeError):
            logger.warning(f"Invalid chat_id '{chat_id}' passed to IndexMsg, using 0.")
            self.chat_id = 0
        # 确保 post_time 是 datetime 对象
        if not isinstance(post_time, datetime):
            logger.warning(f"Invalid post_time type '{type(post_time)}' passed to IndexMsg for url {url}, using current time.")
            self.post_time = datetime.now()
        else:
            self.post_time = post_time
        self.sender = sender
        self.filename = filename
        # 根据 filename 是否存在设置 has_file 标志
        self.has_file = 1 if filename else 0

    def as_dict(self) -> dict:
        """将 IndexMsg 对象转换为适合 Whoosh 存储的字典"""
        return {
            'content': self.content or "", # 确保非 None
            'url': self.url or "", # 确保非 None
            'chat_id': str(self.chat_id), # Schema 需要 TEXT
            'post_time': self.post_time,
            'sender': self.sender or "", # 确保非 None
            'filename': self.filename, # filename 可以是 None
            'has_file': self.has_file
        }

    def __str__(self) -> str:
        """返回对象的字符串表示形式"""
        fields = self.as_dict()
        fields['post_time'] = fields['post_time'].isoformat() if isinstance(fields['post_time'], datetime) else fields['post_time']
        return f'IndexMsg(' + ', '.join(f'{k}={repr(v)}' for k, v in fields.items()) + ')'


class SearchHit:
    """代表一个搜索结果条目，包含原始消息和高亮后的文本片段"""
    def __init__(self, msg: IndexMsg, highlighted: str):
        self.msg = msg
        # highlighted 包含 Whoosh 生成的带 <b> 标签的 HTML 片段
        self.highlighted = highlighted

    def __str__(self) -> str:
        return f'SearchHit(highlighted={repr(self.highlighted)}, msg={self.msg})'


class SearchResult:
    """代表一次搜索操作的结果集合"""
    def __init__(self, hits: List[SearchHit], is_last_page: bool, total_results: int, current_page: int = 1): # 添加 current_page
        self.hits = hits # 当前页的搜索结果列表
        self.is_last_page = is_last_page # 是否为最后一页
        self.total_results = total_results # 匹配的总结果数
        self.current_page = current_page # 当前页码


class Indexer:
    """封装 Whoosh 索引操作的核心类"""

    def __init__(self, index_dir: Path, from_scratch: bool = False):
        """
        初始化 Indexer。

        :param index_dir: 索引文件存储目录。
        :param from_scratch: 是否清空并重新创建索引。
        """
        index_name = 'index' # 索引名称
        self.index_dir = index_dir # 保存索引目录路径
        # 确保索引目录存在
        if not Path(index_dir).exists():
            try:
                Path(index_dir).mkdir(parents=True, exist_ok=True)
                logger.info(f"Created index directory: {index_dir}")
            except OSError as e:
                logger.critical(f"Failed to create index directory {index_dir}: {e}")
                raise

        # 定义内部清空索引的函数
        def _clear():
            import shutil
            if index_dir.exists():
                logger.warning(f"Clearing index directory: {index_dir}")
                try:
                    # 尝试获取写锁以确保没有其他进程在写入
                    lock = writing.Lock(index_dir, "WRITE")
                    if lock.acquire(blocking=False): # 非阻塞获取
                        try:
                            shutil.rmtree(index_dir)
                            index_dir.mkdir(parents=True, exist_ok=True)
                            logger.info(f"Index directory cleared: {index_dir}")
                        finally:
                            lock.release() # 释放锁
                    else:
                         logger.error(f"Could not acquire write lock to clear index directory {index_dir}. Another process might be holding it.")
                         # 如果无法获取锁，则不尝试删除，避免冲突
                         raise writing.LockError(f"Cannot clear index, directory {index_dir} is locked.")
                except OSError as e:
                    logger.error(f"Failed to clear index directory {index_dir}: {e}")
                except Exception as e: # 捕获 LockError 等其他错误
                     logger.error(f"Error during index clear operation: {e}", exc_info=True)
                     raise # 重新抛出，让上层知道清理失败
            # 尝试创建新索引
            try:
                self.ix = index.create_in(index_dir, IndexMsg.schema, index_name)
                logger.info(f"New index created in {index_dir} after clear attempt.")
            except Exception as e:
                logger.critical(f"Failed to create new index after clear attempt: {e}")
                raise

        # 如果指定了 from_scratch，则清空索引
        if from_scratch:
            _clear()

        # 打开或创建索引
        try:
            if index.exists_in(index_dir, index_name):
                logger.info(f"Opening existing index in {index_dir}")
                self.ix = index.open_dir(index_dir, index_name)
            else:
                logger.info(f"Creating new index in {index_dir}")
                self.ix = index.create_in(index_dir, IndexMsg.schema, index_name)
        except index.EmptyIndexError:
            logger.warning(f"Index in {index_dir} was empty or corrupted, creating new index.")
            self.ix = index.create_in(index_dir, IndexMsg.schema, index_name)
        except writing.LockError:
            logger.critical(f"Index '{index_dir}' is locked. Another process might be using it. Please check and retry.")
            raise
        except Exception as e:
            logger.critical(f"Failed to open or create index in {index_dir}: {e}", exc_info=True)
            raise

        # 检查磁盘上的 Schema 是否与代码中的定义兼容
        if not self.ix.is_empty():
            try:
                expected_fields = sorted(IndexMsg.schema.names())
                actual_fields = sorted(self.ix.schema.names())
                if expected_fields != actual_fields:
                    expected_set = set(expected_fields); actual_set = set(actual_fields)
                    missing_in_actual = expected_set - actual_set; extra_in_actual = actual_set - expected_set
                    error_msg = f"Incompatible schema in index '{index_dir}'\n"
                    if missing_in_actual: error_msg += f"\tMissing fields on disk: {sorted(list(missing_in_actual))}\n"
                    if extra_in_actual: error_msg += f"\tUnexpected fields on disk: {sorted(list(extra_in_actual))}\n"
                    error_msg += f"\tExpected: {expected_fields}\n"; error_msg += f"\tOn disk:  {actual_fields}\n"
                    error_msg += "Please clear the index (use -c or delete directory)."
                    raise ValueError(error_msg)
            except Exception as schema_e:
                 logger.error(f"Error checking index schema compatibility: {schema_e}")
                 # 根据需要决定是否要因为 schema 检查失败而中止
                 raise ValueError("Schema check failed.") # schema 不兼容是严重问题，需要停止

        self._clear = _clear # 将内部清理函数赋给实例变量，供外部调用

        # --- QueryParser 配置 ---
        # 初始化查询解析器，默认搜索 'content' 和 'filename' 字段
        self.query_parser = QueryParser('content', IndexMsg.schema)
        self.query_parser.add_plugin(MultifieldPlugin(["content", "filename"]))

        # 配置高亮器 (Highlighter)
        self.highlighter = highlight.Highlighter(
            fragmenter=highlight.ContextFragmenter(maxchars=250, surround=80), # 片段最大字符数, 关键词前后字符数
            formatter=highlight.HtmlFormatter(tagname="b", between=" ... ") # 高亮标签, 多片段连接符
        )

    def retrieve_random_document(self) -> IndexMsg:
        """从索引中随机检索一条消息"""
        if self.ix.is_empty(): raise IndexError("Index is empty")
        searcher = None
        try:
            searcher = self.ix.searcher()
            reader = searcher.reader()
            doc_nums = list(reader.all_doc_ids()) # 获取所有文档编号
            if not doc_nums: raise IndexError("Index contains no documents")

            random_doc_num = random.choice(doc_nums) # 随机选择一个文档编号
            msg_dict = searcher.stored_fields(random_doc_num) # 获取存储的字段
            if not msg_dict: raise IndexError(f"No stored fields for doc {random_doc_num}")

            # 填充可能的缺失字段，确保能构造 IndexMsg 对象
            msg_dict.setdefault('filename', None)
            msg_dict.setdefault('content', '')
            msg_dict.setdefault('url', '')
            msg_dict.setdefault('chat_id', 0)
            msg_dict.setdefault('post_time', datetime.now())
            msg_dict.setdefault('sender', '')

            try:
                post_time = msg_dict['post_time']
                if not isinstance(post_time, datetime):
                    logger.warning(f"Retrieved non-datetime post_time ({type(post_time)}) for random doc {random_doc_num}, using current time.")
                    post_time = datetime.now()
                # 从字典重建 IndexMsg 对象
                return IndexMsg(
                    content=msg_dict['content'], url=msg_dict['url'],
                    chat_id=msg_dict['chat_id'], post_time=post_time,
                    sender=msg_dict['sender'], filename=msg_dict['filename']
                )
            except Exception as e:
                logger.error(f"Error reconstructing IndexMsg from random doc {msg_dict}: {e}", exc_info=True)
                raise ValueError(f"Failed to reconstruct IndexMsg: {e}")
        except writing.LockError:
            logger.error("Index locked, cannot retrieve random document.")
            raise IndexError("Index is locked.") # 或者抛出 LockError
        finally:
             if searcher: searcher.close()


    def add_document(self, message: IndexMsg, writer: Optional[IndexWriter] = None):
        """
        向索引中添加单个文档。

        :param message: 要添加的 IndexMsg 对象。
        :param writer: 可选的外部 IndexWriter，用于批量添加。如果为 None，则内部创建并提交。
        """
        commit_locally = False # 标记是否需要在函数内部提交 writer
        temp_writer = None # 临时 writer 变量
        try:
            if writer is None:
                # 如果没有提供外部 writer，则尝试获取内部 writer
                try:
                    temp_writer = self.ix.writer()
                    commit_locally = True
                    writer = temp_writer # 使用临时 writer
                except writing.LockError:
                    logger.error("Failed to get index writer (LockError). Document not added.")
                    raise # 重新抛出锁错误
                except Exception as e:
                    logger.error(f"Failed to get index writer: {e}", exc_info=True)
                    raise # 重新抛出其他错误

            # --- 文档数据准备 ---
            doc_data = message.as_dict() # 将 IndexMsg 转换为字典
            if not doc_data.get('url'):
                logger.warning(f"Skipping document with empty URL. Content: {brief_content(doc_data.get('content'))}")
                if commit_locally and writer: writer.cancel() # 取消写入操作
                return
            # 确保字段类型符合 Schema 要求
            # filename 可以为 None，但 Whoosh 添加时最好是空字符串
            doc_data['filename'] = doc_data['filename'] if doc_data['filename'] is not None else ""
            if not isinstance(doc_data['post_time'], datetime):
                logger.warning(f"Correcting invalid post_time type {type(doc_data['post_time'])} for URL {doc_data['url']} before indexing.")
                doc_data['post_time'] = datetime.now()
            doc_data['has_file'] = int(doc_data.get('has_file', 0))

            # --- 添加文档 ---
            writer.add_document(**doc_data)

            # --- 提交或取消 ---
            if commit_locally:
                writer.commit() # 提交本地创建的 writer
                writer = None # 标记已关闭
        except Exception as e:
            logger.error(f"Error adding document (URL: {message.url}): {e}", exc_info=True)
            # 如果添加过程中出错，且是本地 writer，则取消写入
            if commit_locally and writer:
                try: writer.cancel()
                except Exception as cancel_e: logger.error(f"Error cancelling writer after add error: {cancel_e}")
            raise # 重新抛出异常，让调用者知道添加失败
        finally:
             # 如果使用了临时 writer 且未被提交或取消 (例如在获取 writer 后立即出错)
             if temp_writer and not temp_writer.is_closed:
                 try: temp_writer.cancel() # 确保关闭
                 except Exception: pass


    def search(self, q_str: str, in_chats: Optional[List[int]], page_len: int, page_num: int = 1, file_filter: str = "all") -> SearchResult:
        """
        执行搜索查询。
        """
        try:
            q = self.query_parser.parse(q_str)
            logger.debug(f"Parsed query: {q}")
        except Exception as e:
            logger.error(f"Failed to parse query '{q_str}': {e}")
            return SearchResult([], True, 0, page_num) # 返回空结果

        searcher = None
        try:
            searcher = self.ix.searcher()
            # --- 构建过滤器 ---
            chat_filter = None
            if in_chats:
                valid_chat_ids = [str(cid) for cid in in_chats if isinstance(cid, int) or (isinstance(cid, str) and cid.lstrip('-').isdigit())]
                if valid_chat_ids:
                    chat_filter = Or([Term('chat_id', cid) for cid in valid_chat_ids])

            type_filter = None
            if file_filter == "text_only":
                type_filter = Term("has_file", 0)
            elif file_filter == "file_only":
                type_filter = Term("has_file", 1)

            final_filter = None
            filters_to_and = [f for f in [chat_filter, type_filter] if f is not None]
            if len(filters_to_and) > 1:
                final_filter = And(filters_to_and)
            elif len(filters_to_and) == 1:
                final_filter = filters_to_and[0]
            # --- 过滤器构建结束 ---

            logger.debug(f"Executing search with query='{q}' and filter='{final_filter}' for page {page_num}")
            result_page = searcher.search_page(q, page_num, page_len, filter=final_filter,
                                               sortedby='post_time', reverse=True,
                                               terms=True) # terms=True 用于高亮
            logger.debug(f"Search found {result_page.total} results. Page {page_num} has {len(result_page)} hits.")

            hits = []
            for hit in result_page:
                try:
                    stored_fields = hit.fields()
                    if not stored_fields:
                        logger.warning(f"Hit {hit.docnum} had no stored fields.")
                        continue

                    post_time = stored_fields.get('post_time')
                    if not isinstance(post_time, datetime):
                        logger.warning(f"Retrieved non-datetime post_time ({type(post_time)}) for hit {hit.docnum}, using current time.")
                        post_time = datetime.now()

                    msg = IndexMsg(
                        content=stored_fields.get('content', ''),
                        url=stored_fields.get('url', ''),
                        chat_id=stored_fields.get('chat_id', '0'), # 获取的是 str
                        post_time=post_time,
                        sender=stored_fields.get('sender', ''),
                        filename=stored_fields.get('filename') # 可能为 None
                    )

                    highlighted_content = ""
                    # 优先高亮 content 字段
                    if msg.content:
                        try:
                            highlighted_content = self.highlighter.highlight_hit(hit, 'content') or ""
                        except Exception as high_e:
                            logger.error(f"Error highlighting hit {hit.docnum} content: {high_e}")
                            highlighted_content = html.escape(brief_content(msg.content, 150))
                    # 如果 content 为空但有 filename，尝试高亮 filename（虽然通常意义不大，但保持一致性）
                    elif msg.filename:
                        try:
                            # 注意：MultiFieldPlugin 默认OR连接，高亮可能只匹配一个字段
                            # 如果用户搜索词只匹配文件名，这里尝试高亮 filename
                            temp_highlight = self.highlighter.highlight_hit(hit, 'filename') or ""
                            if temp_highlight:
                                 highlighted_content = temp_highlight
                        except Exception as high_e:
                            logger.error(f"Error highlighting hit {hit.docnum} filename: {high_e}")
                            # 高亮失败，不设置高亮

                    hits.append(SearchHit(msg, highlighted_content))
                except Exception as e:
                    logger.error(f"Error processing hit {hit.docnum} (URL: {stored_fields.get('url', 'N/A')}): {e}", exc_info=True)

            # 确保 total_results 是整数
            total_results_int = result_page.total if result_page.total is not None else 0
            is_last = result_page.is_last_page() if result_page.total is not None else True
            return SearchResult(hits, is_last, total_results_int, page_num)

        except writing.LockError:
            logger.error("Index is locked, cannot perform search.")
            return SearchResult([], True, 0, page_num)
        except Exception as e:
            logger.error(f"Search execution failed for query '{q_str}': {e}", exc_info=True)
            return SearchResult([], True, 0, page_num)
        finally:
             if searcher: searcher.close()


    def list_indexed_chats(self) -> Set[int]:
        """列出索引中存在的所有 chat_id"""
        if self.ix.is_empty(): return set()
        chat_ids = set()
        reader = None
        try:
            reader = self.ix.reader()
            # 使用 reader 获取 'chat_id' 字段的词典 (lexicon)
            for chat_id_bytes in reader.lexicon('chat_id'):
                try:
                    # 解码并转换为整数
                    chat_ids.add(int(chat_id_bytes.decode('utf-8')))
                except ValueError:
                    logger.warning(f"Could not convert chat_id '{chat_id_bytes}' from lexicon to int.")
        except KeyError:
             logger.warning("Field 'chat_id' not found in index lexicon.")
             return set()
        except writing.LockError: logger.error("Index locked, cannot list chats."); return set()
        except Exception as e: logger.error(f"Error listing indexed chats: {e}", exc_info=True); return set()
        finally:
             if reader: reader.close()
        return chat_ids


    def count_by_query(self, query: Optional[Term] = None) -> int:
        """根据 Whoosh Query 对象计算文档数量"""
        if self.ix.is_empty(): return 0

        searcher = None
        try:
            searcher = self.ix.searcher()
            if query is None:
                # 如果没有提供查询，返回总文档数
                return searcher.doc_count_all()
            else:
                # 使用提供的查询对象计数
                return searcher.doc_count(query=query)
        except writing.LockError:
            logger.error("Index locked, cannot count docs.")
            return 0
        except Exception as e:
            logger.error(f"Error counting docs for query {query}: {e}", exc_info=True)
            return 0
        finally:
             if searcher: searcher.close()


    def delete(self, url: str):
        """根据 URL 删除索引中的文档"""
        if not url: return
        writer = None
        try:
            writer = self.ix.writer()
            deleted_count = writer.delete_by_term('url', url) # url 是 unique 字段
            writer.commit() # 提交删除
            writer = None # 标记已关闭
            if deleted_count > 0:
                 logger.debug(f"Deleted {deleted_count} doc(s) with URL '{url}'")
        except writing.LockError: logger.error(f"Index locked, cannot delete doc by url '{url}'")
        except Exception as e:
             logger.error(f"Error deleting doc by url '{url}': {e}", exc_info=True)
             if writer: # 如果出错，尝试取消
                 try: writer.cancel()
                 except Exception: pass
        finally:
             # 确保 writer 关闭
             if writer and not writer.is_closed:
                 try: writer.cancel()
                 except Exception: pass


    def get_document_fields(self, url: str) -> Optional[dict]:
        """根据 URL 获取文档存储的所有字段"""
        if self.ix.is_empty() or not url: return None
        searcher = None
        try:
            searcher = self.ix.searcher()
            return searcher.document(url=url)
        except KeyError: return None # 文档不存在
        except writing.LockError: logger.error(f"Index locked, cannot get doc fields for url '{url}'"); return None
        except Exception as e: logger.error(f"Error getting doc fields for url '{url}': {e}", exc_info=True); return None
        finally:
             if searcher: searcher.close()


    def replace_document(self, url: str, new_fields: dict):
        """
        替换索引中具有相同 URL 的文档。
        """
        if not url: raise ValueError("Cannot replace document with empty URL.")
        required = ['content', 'url', 'chat_id', 'post_time', 'sender'] # 确保基本字段存在
        missing = [k for k in required if k not in new_fields]
        if missing: raise ValueError(f"Missing required field(s) {missing} for replace url '{url}'")

        # 准备符合 Schema 的数据
        post_time = new_fields.get('post_time', datetime.now())
        if not isinstance(post_time, datetime):
            logger.warning(f"Correcting invalid post_time type {type(post_time)} for replace URL {url}.")
            post_time = datetime.now()

        filename = new_fields.get('filename') # filename 可以是 None
        doc_data = {
            'content': new_fields.get('content', ''),
            'url': url,
            'chat_id': str(new_fields.get('chat_id', '0')),
            'post_time': post_time,
            'sender': new_fields.get('sender', ''),
            'filename': filename if filename is not None else "", # 存储空字符串而非None
            'has_file': 1 if filename else 0
        }

        writer = None
        try:
            writer = self.ix.writer()
            writer.update_document(**doc_data) # 使用 update_document 替换
            writer.commit()
            writer = None # 标记已关闭
        except writing.LockError:
            logger.error(f"Index locked, cannot replace document url '{url}'")
            raise # 重新抛出锁错误
        except Exception as e:
            logger.error(f"Error replacing document url '{url}': {e}", exc_info=True)
            if writer: # 出错时取消
                 try: writer.cancel()
                 except Exception: pass
            raise e # 重新抛出
        finally:
             # 确保 writer 关闭
             if writer and not writer.is_closed:
                 try: writer.cancel()
                 except Exception: pass


    def clear(self):
        """清空整个索引"""
        try:
            self._clear() # 调用内部的清理函数
        except writing.LockError:
            logger.error("Index locked, cannot clear.")
            raise # 重新抛出锁错误
        except Exception as e:
            logger.error(f"Error during index clear operation: {e}")
            raise


    def is_empty(self, chat_id: Optional[Union[int, str]] = None) -> bool:
        """检查索引是否为空，或者特定 chat_id 是否没有文档。"""
        try:
            if self.ix.is_empty(): return True
        except writing.LockError:
            logger.error("Index locked, cannot check emptiness."); return True

        if chat_id is not None:
            searcher = None
            try:
                searcher = self.ix.searcher()
                q = Term("chat_id", str(chat_id)) # Chat ID 在 schema 中是 TEXT
                return searcher.doc_count(query=q) == 0
            except writing.LockError: logger.error(f"Index locked, cannot check emptiness for chat {chat_id}."); return True
            except Exception as e: logger.error(f"Error checking emptiness for chat {chat_id}: {e}"); return True
            finally:
                 if searcher: searcher.close()
        else:
            # 索引非空，且未指定 chat_id，则返回 False
            return False
