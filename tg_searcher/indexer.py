# -*- coding: utf-8 -*-
from pathlib import Path
from datetime import datetime
import random
from typing import Optional, Union, List, Set

from whoosh import index, writing
from whoosh.fields import Schema, TEXT, ID, DATETIME, NUMERIC # 索引字段类型
# 移除 OrGroup 导入, 默认使用 AndGroup
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
    # 定义索引的 Schema (结构)
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
            logger.warning(f"Invalid post_time type '{type(post_time)}' passed to IndexMsg, using current time.")
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
    def __init__(self, hits: List[SearchHit], is_last_page: bool, total_results: int):
        self.hits = hits # 当前页的搜索结果列表
        self.is_last_page = is_last_page # 是否为最后一页
        self.total_results = total_results # 匹配的总结果数


class Indexer:
    """封装 Whoosh 索引操作的核心类"""

    def __init__(self, index_dir: Path, from_scratch: bool = False):
        """
        初始化 Indexer。

        :param index_dir: 索引文件存储目录。
        :param from_scratch: 是否清空并重新创建索引。
        """
        index_name = 'index' # 索引名称
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
                    shutil.rmtree(index_dir)
                    index_dir.mkdir(parents=True, exist_ok=True)
                    logger.info(f"Index directory cleared: {index_dir}")
                except OSError as e:
                    logger.error(f"Failed to clear index directory {index_dir}: {e}")
            # 尝试创建新索引
            try:
                self.ix = index.create_in(index_dir, IndexMsg.schema, index_name)
                logger.info(f"New index created after clear attempt.")
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
             # 处理索引为空或损坏的情况
            logger.warning(f"Index in {index_dir} was empty or corrupted, creating new index.")
            self.ix = index.create_in(index_dir, IndexMsg.schema, index_name)
        except writing.LockError:
            # 处理索引被锁定的情况 (可能其他进程正在写入)
            logger.critical(f"Index '{index_dir}' is locked. Another process might be using it. Please check and retry.")
            raise
        except Exception as e:
            logger.critical(f"Failed to open or create index in {index_dir}: {e}", exc_info=True)
            raise

        # 检查磁盘上的 Schema 是否与代码中的定义兼容
        if not self.ix.is_empty():
            expected_fields = sorted(IndexMsg.schema.names())
            actual_fields = sorted(self.ix.schema.names())
            if expected_fields != actual_fields:
                 # 构造详细的错误信息
                expected_set = set(expected_fields); actual_set = set(actual_fields)
                missing_in_actual = expected_set - actual_set; extra_in_actual = actual_set - expected_set
                error_msg = f"Incompatible schema in index '{index_dir}'\n"
                if missing_in_actual: error_msg += f"\tMissing fields on disk: {sorted(list(missing_in_actual))}\n"
                if extra_in_actual: error_msg += f"\tUnexpected fields on disk: {sorted(list(extra_in_actual))}\n"
                error_msg += f"\tExpected: {expected_fields}\n"; error_msg += f"\tOn disk:  {actual_fields}\n"
                error_msg += "Please clear the index (use -c or delete directory)."
                raise ValueError(error_msg)

        self._clear = _clear # 将内部清理函数赋给实例变量，供外部调用

        # --- QueryParser 配置 (恢复默认 AND 逻辑) ---
        # 初始化查询解析器，默认搜索 'content' 字段
        # 移除了 group=OrGroup 参数，使其默认使用 AndGroup (要求所有词都匹配)
        self.query_parser = QueryParser('content', IndexMsg.schema)
        # --- 结束修改点 ---

        # 添加插件，使得搜索同时作用于 'content' 和 'filename' 字段
        self.query_parser.add_plugin(MultifieldPlugin(["content", "filename"]))

        # 配置高亮器 (Highlighter)
        self.highlighter = highlight.Highlighter(
            # 使用 ContextFragmenter 获取关键词上下文
            fragmenter=highlight.ContextFragmenter(maxchars=250, surround=80), # 片段最大字符数, 关键词前后字符数
            # 使用 HtmlFormatter 将关键词用 <b> 标签包裹
            formatter=highlight.HtmlFormatter(tagname="b", between=" ... ") # 高亮标签, 多片段连接符
        )

    def retrieve_random_document(self) -> IndexMsg:
        """从索引中随机检索一条消息"""
        if self.ix.is_empty(): raise IndexError("Index is empty")
        with self.ix.searcher() as searcher:
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
                logger.error(f"Error reconstructing IndexMsg from {msg_dict}: {e}", exc_info=True)
                raise ValueError(f"Failed to reconstruct IndexMsg: {e}")

    def add_document(self, message: IndexMsg, writer: Optional[IndexWriter] = None):
        """
        向索引中添加单个文档。

        :param message: 要添加的 IndexMsg 对象。
        :param writer: 可选的外部 IndexWriter，用于批量添加。如果为 None，则内部创建并提交。
        """
        commit_locally = False # 标记是否需要在函数内部提交 writer
        if writer is None:
            # 如果没有提供外部 writer，则尝试获取内部 writer
            try:
                writer = self.ix.writer()
                commit_locally = True
            except writing.LockError:
                logger.error("Failed to get index writer (LockError). Document not added.")
                raise # 重新抛出锁错误
            except Exception as e:
                logger.error(f"Failed to get index writer: {e}", exc_info=True)
                raise # 重新抛出其他错误

        try:
            doc_data = message.as_dict() # 将 IndexMsg 转换为字典
            # URL 是必须的，否则跳过
            if not doc_data.get('url'):
                logger.warning(f"Skipping document with empty URL. Content: {doc_data.get('content', '')[:50]}")
                if commit_locally and writer: writer.cancel() # 取消写入操作
                return

            # 确保字段类型符合 Schema 要求
            doc_data['filename'] = doc_data['filename'] if doc_data['filename'] is not None else ""
            if not isinstance(doc_data['post_time'], datetime):
                logger.warning(f"Correcting invalid post_time type {type(doc_data['post_time'])} for URL {doc_data['url']} before indexing.")
                doc_data['post_time'] = datetime.now()
            doc_data['has_file'] = int(doc_data.get('has_file', 0))

            # 添加文档
            writer.add_document(**doc_data)
            # 如果是本地创建的 writer，则提交
            if commit_locally:
                writer.commit()
        except Exception as e:
            # 如果添加过程中出错，且是本地 writer，则取消写入
            if commit_locally and writer:
                try: writer.cancel()
                except Exception as cancel_e: logger.error(f"Error cancelling writer: {cancel_e}")
            logger.error(f"Error adding document (URL: {message.url}): {e}", exc_info=True)
            # 可以选择是否重新抛出异常，取决于是否允许批量添加中部分失败

    def search(self, q_str: str, in_chats: Optional[List[int]], page_len: int, page_num: int = 1, file_filter: str = "all") -> SearchResult:
        """
        执行搜索查询。

        :param q_str: 用户输入的搜索字符串。
        :param in_chats: 可选，限定在指定的 chat_id 列表中搜索。
        :param page_len: 每页结果数量。
        :param page_num: 要获取的页码 (从 1 开始)。
        :param file_filter: 文件类型过滤 ('all', 'text_only', 'file_only')。
        :return: SearchResult 对象。
        """
        try:
            # 解析用户查询字符串
            q = self.query_parser.parse(q_str)
            logger.debug(f"Parsed query: {q}")
        except Exception as e:
            logger.error(f"Failed to parse query '{q_str}': {e}")
            return SearchResult([], True, 0) # 返回空结果

        try:
             # 获取 Searcher 对象
            with self.ix.searcher() as searcher:
                 # 构建 chat_id 过滤器
                base_filter = None
                if in_chats:
                        valid_chat_ids = [str(cid) for cid in in_chats if isinstance(cid, int) or (isinstance(cid, str) and cid.lstrip('-').isdigit())]
                        if valid_chat_ids: base_filter = Or([Term('chat_id', cid) for cid in valid_chat_ids])

                 # 构建文件类型过滤器
                type_filter = None
                if file_filter == "text_only": type_filter = Term("has_file", 0)
                elif file_filter == "file_only": type_filter = Term("has_file", 1)

                 # 合并过滤器
                final_filter = None
                if base_filter and type_filter: final_filter = And([base_filter, type_filter])
                elif base_filter: final_filter = base_filter
                elif type_filter: final_filter = type_filter

                logger.debug(f"Executing search with query='{q}' and filter='{final_filter}'")
                 # 执行分页搜索
                result_page = searcher.search_page(q, page_num, page_len, filter=final_filter,
                                                    sortedby='post_time', reverse=True, # 按时间倒序
                                                    terms=True) # terms=True 用于高亮
                logger.debug(f"Search found {result_page.total} results. Page {page_num} has {len(result_page)} hits.")

                hits = [] # 存储处理后的结果
                for hit in result_page:
                    try:
                        stored_fields = hit.fields() # 获取存储的字段
                        if not stored_fields:
                                logger.warning(f"Hit {hit.docnum} had no stored fields.")
                                continue

                         # 确保时间字段是 datetime 对象
                        post_time = stored_fields.get('post_time')
                        if not isinstance(post_time, datetime):
                                logger.warning(f"Retrieved non-datetime post_time ({type(post_time)}) for hit {hit.docnum}, using current time.")
                                post_time = datetime.now()

                         # 重建 IndexMsg 对象
                        msg = IndexMsg(
                            content=stored_fields.get('content', ''),
                            url=stored_fields.get('url', ''),
                            chat_id=stored_fields.get('chat_id', '0'),
                            post_time=post_time,
                            sender=stored_fields.get('sender', ''),
                            filename=stored_fields.get('filename')
                        )

                         # 获取高亮文本片段
                        highlighted_content = ""
                        if msg.content: # 仅当有内容时才尝试高亮
                            try:
                                highlighted_content = self.highlighter.highlight_hit(hit, 'content') or ""
                            except Exception as high_e:
                                logger.error(f"Error highlighting hit {hit.docnum} content: {high_e}")
                         # 如果高亮失败但有内容，使用简短原文作为后备
                        if not highlighted_content and msg.content:
                                highlighted_content = html.escape(brief_content(msg.content, 150)) # 增加后备长度

                         # 创建 SearchHit 对象
                        hits.append(SearchHit(msg, highlighted_content))
                    except Exception as e:
                        logger.error(f"Error processing hit {hit.docnum} (URL: {stored_fields.get('url')}): {e}", exc_info=True)

                 # 计算是否为最后一页
                is_last = (page_num * page_len) >= result_page.total
                return SearchResult(hits, is_last, result_page.total)

        except writing.LockError:
            logger.error("Index is locked, cannot perform search.")
            return SearchResult([], True, 0)
        except Exception as e:
            logger.error(f"Search execution failed for query '{q_str}': {e}", exc_info=True)
            return SearchResult([], True, 0)


    def list_indexed_chats(self) -> Set[int]:
        """列出索引中存在的所有 chat_id"""
        if self.ix.is_empty(): return set()
        chat_ids = set()
        try:
             # 使用 reader 获取 'chat_id' 字段的词典 (lexicon)
            with self.ix.reader() as r:
                for chat_id_bytes in r.lexicon('chat_id'):
                    try:
                         # 解码并转换为整数
                        chat_ids.add(int(chat_id_bytes.decode('utf-8')))
                    except ValueError:
                        logger.warning(f"Could not convert chat_id '{chat_id_bytes}' from lexicon to int.")
        except KeyError:
             # 如果 'chat_id' 字段不存在
            logger.warning("Field 'chat_id' not found in index lexicon.")
            return set()
        except writing.LockError: logger.error("Index locked, cannot list chats."); return set()
        except Exception as e: logger.error(f"Error listing indexed chats: {e}", exc_info=True); return set()
        return chat_ids


    def count_by_query(self, **kw) -> int:
        """根据简单的字段=值查询计算文档数量 (主要用于内部检查或简单统计)"""
        if self.ix.is_empty(): return 0
        # 如果没有查询条件，返回总文档数
        if not kw: return self.ix.doc_count()
        try:
             # 假设只有一个查询条件
            field, value = list(kw.items())[0]
             # 根据字段类型构建 Term 查询
            if field == 'has_file':
                query = Term(field, int(value)) # NUMERIC
            else:
                query = Term(field, str(value)) # TEXT or ID
        except (IndexError, ValueError, TypeError):
            logger.warning(f"Invalid arguments for count_by_query: {kw}. Returning total count.")
            return self.ix.doc_count()

        try:
             # 使用 searcher.doc_count 获取匹配数量
            with self.ix.searcher() as s: return s.doc_count(query=query)
        except writing.LockError: logger.error("Index locked, cannot count docs."); return 0
        except Exception as e: logger.error(f"Error counting docs for {kw}: {e}", exc_info=True); return 0


    def delete(self, url: str):
        """根据 URL 删除索引中的文档"""
        if not url: return
        try:
            # 使用 writer 删除指定 term 的文档
            with self.ix.writer() as writer:
                deleted_count = writer.delete_by_term('url', url) # url 是 unique 字段
                logger.debug(f"Deleted {deleted_count} doc(s) with URL '{url}'")
        except writing.LockError: logger.error(f"Index locked, cannot delete doc by url '{url}'")
        except Exception as e: logger.error(f"Error deleting doc by url '{url}': {e}", exc_info=True)


    def get_document_fields(self, url: str) -> Optional[dict]:
        """根据 URL 获取文档存储的所有字段"""
        if self.ix.is_empty() or not url: return None
        try:
            # 使用 searcher.document() 直接获取
            with self.ix.searcher() as searcher:
                return searcher.document(url=url)
        except KeyError:
             # 如果文档不存在，Whoosh 会抛出 KeyError
            return None
        except writing.LockError: logger.error(f"Index locked, cannot get doc fields for url '{url}'"); return None
        except Exception as e: logger.error(f"Error getting doc fields for url '{url}': {e}", exc_info=True); return None


    def replace_document(self, url: str, new_fields: dict):
        """
        替换索引中具有相同 URL 的文档。

        :param url: 要替换文档的 URL。
        :param new_fields: 包含新文档所有字段的字典。
        """
        if not url: raise ValueError("Cannot replace document with empty URL.")
        # 检查必需字段是否存在
        required = ['content', 'url', 'chat_id', 'post_time', 'sender']
        missing = [k for k in required if k not in new_fields]
        if missing: raise ValueError(f"Missing required field(s) {missing} for replace url '{url}'")

        # 确保字段类型正确
        post_time = new_fields.get('post_time', datetime.now())
        if not isinstance(post_time, datetime):
            logger.warning(f"Correcting invalid post_time type {type(post_time)} for replace URL {url}.")
            post_time = datetime.now()

        # 准备传递给 Whoosh 的字典
        doc_data = {
            'content': new_fields.get('content', ''), 'url': url,
            'chat_id': str(new_fields.get('chat_id', '0')),
            'post_time': post_time,
            'sender': new_fields.get('sender', ''),
            'filename': new_fields.get('filename'), # Allow None
            'has_file': 1 if new_fields.get('filename') else 0 # 重新计算 has_file
        }
        try:
            # 使用 writer.update_document() 进行替换
            with self.ix.writer() as writer: writer.update_document(**doc_data)
        except writing.LockError:
            logger.error(f"Index locked, cannot replace document url '{url}'")
            raise # 重新抛出锁错误
        except Exception as e:
            logger.error(f"Error replacing document url '{url}': {e}", exc_info=True); raise e


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

    def is_empty(self, chat_id=None) -> bool:
        """
        检查索引是否为空，或者特定 chat_id 是否没有文档。

        :param chat_id: 可选，要检查的特定 chat_id。
        :return: 如果索引 (或特定 chat_id) 为空则返回 True，否则返回 False。
        """
         # 先检查整体是否为空 (最快)
        try:
            if self.ix.is_empty(): return True
        except writing.LockError:
            logger.error("Index locked, cannot check emptiness."); return True # 无法检查时保守返回 True

        # 如果指定了 chat_id，则检查该 chat_id 的文档数量
        if chat_id is not None:
            try:
                with self.ix.searcher() as searcher:
                    q = Term("chat_id", str(chat_id))
                    return searcher.doc_count(query=q) == 0
            except writing.LockError: logger.error(f"Index locked, cannot check emptiness for chat {chat_id}."); return True
            except Exception as e: logger.error(f"Error checking emptiness for chat {chat_id}: {e}"); return True # 出错时保守返回 True
        else:
            # 如果没有指定 chat_id，且索引整体不为空，则返回 False
            return False
