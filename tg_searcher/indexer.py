# -*- coding: utf-8 -*-
from pathlib import Path
from datetime import datetime
import random
from typing import Optional, Union, List, Set

from whoosh import index
from whoosh.fields import Schema, TEXT, ID, DATETIME, NUMERIC # 确保 TEXT, ID, DATETIME 被导入
from whoosh.qparser import QueryParser, MultifieldPlugin, OrGroup # 导入 MultifieldPlugin, OrGroup
from whoosh.writing import IndexWriter
from whoosh.query import Term, Or
import whoosh.highlight as highlight
from jieba.analyse.analyzer import ChineseAnalyzer

# 尝试获取日志记录器，如果 common.py 不可用或 get_logger 未定义，则使用标准 logging
try:
    from .common import get_logger
    logger = get_logger('indexer')
except (ImportError, AttributeError):
    import logging
    logger = logging.getLogger('indexer')
    # 配置基本的日志记录，以便在无法从 common 获取时仍能看到信息
    if not logger.hasHandlers():
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class IndexMsg:
    # --- Schema 已包含 filename ---
    schema = Schema(
        content=TEXT(stored=True, analyzer=ChineseAnalyzer()),
        url=ID(stored=True, unique=True), # 确保 url 是 unique=True
        chat_id=TEXT(stored=True),
        post_time=DATETIME(stored=True, sortable=True),
        sender=TEXT(stored=True),
        filename=TEXT(stored=True, analyzer=ChineseAnalyzer()) # 添加 filename 字段
    )
    # --- 结束修改 ---

    # --- __init__ 已包含 filename ---
    def __init__(self, content: str, url: str, chat_id: Union[int, str],
                 post_time: datetime, sender: str, filename: Optional[str] = None): # 添加 filename 参数
        self.content = content # 消息文本/标题
        self.url = url
        try:
            self.chat_id = int(chat_id)
        except (ValueError, TypeError):
             logger.warning(f"Invalid chat_id '{chat_id}' passed to IndexMsg, using 0.")
             self.chat_id = 0
        self.post_time = post_time
        self.sender = sender
        self.filename = filename # 文件名
    # --- 结束修改 ---

    # --- as_dict 已包含 filename ---
    def as_dict(self):
        # 返回适合 Whoosh 存储的字典，确保 chat_id 是字符串
        return {
            'content': self.content or "", # 确保非 None
            'url': self.url or "", # 确保非 None
            'chat_id': str(self.chat_id),
            'post_time': self.post_time,
            'sender': self.sender or "", # 确保非 None
            'filename': self.filename # filename 可以是 None
        }
    # --- 结束修改 ---

    def __str__(self):
        # 稍微调整 __str__ 以包含 filename (可选)
        fields = self.as_dict()
        # filename 可能为 None， repr(None) 是 'None'，没问题
        return f'IndexMsg(' + ', '.join(f'{k}={repr(v)}' for k, v in fields.items()) + ')'


class SearchHit:
    def __init__(self, msg: IndexMsg, highlighted: str):
        self.msg = msg
        # highlighted 仍然是基于 'content' 字段的高亮
        self.highlighted = highlighted

    def __str__(self):
        return f'SearchHit(highlighted={repr(self.highlighted)}, msg={self.msg})'


class SearchResult:
    def __init__(self, hits: List[SearchHit], is_last_page: bool, total_results: int):
        self.hits = hits
        self.is_last_page = is_last_page
        self.total_results = total_results


class Indexer:
    # Whoosh 的封装

    def __init__(self, index_dir: Path, from_scratch: bool = False):
        index_name = 'index'
        if not Path(index_dir).exists():
            Path(index_dir).mkdir()

        def _clear():
            import shutil
            logger.warning(f"Clearing index directory: {index_dir}")
            shutil.rmtree(index_dir)
            index_dir.mkdir()
            self.ix = index.create_in(index_dir, IndexMsg.schema, index_name)
            logger.info(f"Index directory cleared and new index created.")

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
        except index.EmptyIndexError: # 处理可能的空索引错误
             logger.warning(f"Index in {index_dir} was empty or corrupted, creating new index.")
             self.ix = index.create_in(index_dir, IndexMsg.schema, index_name)
        except Exception as e:
             logger.critical(f"Failed to open or create index in {index_dir}: {e}", exc_info=True)
             raise # 重新抛出严重错误


        # 检查 Schema 是否兼容
        if not self.ix.is_empty():
            # 仅在索引非空时检查 schema 兼容性
            expected_fields = sorted(IndexMsg.schema.names())
            actual_fields = sorted(self.ix.schema.names())
            if expected_fields != actual_fields:
                 # 使用更友好的异常，避免程序直接退出
                 raise ValueError(
                    f"Incompatible schema in your index '{index_dir}'\n"
                    f"\tExpected fields: {expected_fields}\n"
                    f"\tFields on disk:  {actual_fields}\n"
                    f"Please clear the index (use -c argument or delete the directory) or use a different index directory."
                 )

        self._clear = _clear

        # --- QueryParser 初始化已包含 filename ---
        # 使用 OrGroup 让字段间的默认关系是 OR
        # 配置 MultifieldPlugin 使默认搜索查询 content 和 filename 字段
        self.query_parser = QueryParser('content', IndexMsg.schema, group=OrGroup)
        self.query_parser.add_plugin(MultifieldPlugin(["content", "filename"]))
        # --- 结束修改 ---

        # 高亮仍然基于 content 字段
        # 使用 ContextFragmenter 并增加 maxchars 和 surround
        self.highlighter = highlight.Highlighter(
            fragmenter=highlight.ContextFragmenter(maxchars=300, surround=100), # 增加高亮范围
            formatter=highlight.HtmlFormatter(tagname="u") # 使用下划线高亮
        )

    def retrieve_random_document(self) -> IndexMsg:
        with self.ix.searcher() as searcher:
            # 确保索引不是空的
            if searcher.doc_count() == 0:
                raise IndexError("Cannot retrieve random document from an empty index")

            # --- BUG FIX: 使用 reader().all_doc_ids() 替代 searcher.all_doc_ids() ---
            reader = searcher.reader()
            doc_nums = list(reader.all_doc_ids())
            if not doc_nums: # 再次检查以防万一
                raise IndexError("Cannot retrieve random document from an empty index (no doc IDs found)")
            # --- 结束 BUG FIX ---

            random_doc_num = random.choice(doc_nums)
            # 获取随机文档的字段
            msg_dict = searcher.stored_fields(random_doc_num)
            if not msg_dict: # 如果获取不到字段
                 raise IndexError(f"Could not retrieve stored fields for random doc number {random_doc_num}")

            # 修复可能因为 schema 变更导致的缺少字段问题（给默认值）
            # filename 可以是 None
            msg_dict.setdefault('filename', None)
            # 确保 IndexMsg 需要的其他字段存在
            msg_dict.setdefault('content', '')
            msg_dict.setdefault('url', '') # URL 不应该为空，但以防万一
            msg_dict.setdefault('chat_id', 0) # 提供默认值
            msg_dict.setdefault('post_time', datetime.now()) # 提供默认值
            msg_dict.setdefault('sender', '')

            try:
                # 使用构造函数处理类型转换
                return IndexMsg(
                    content=msg_dict['content'],
                    url=msg_dict['url'],
                    chat_id=msg_dict['chat_id'],
                    post_time=msg_dict['post_time'],
                    sender=msg_dict['sender'],
                    filename=msg_dict['filename']
                )
            except Exception as e:
                 # 如果仍然缺少字段或类型错误
                 logger.error(f"Failed to reconstruct IndexMsg from stored fields {msg_dict}: {e}", exc_info=True)
                 raise ValueError(f"Failed to reconstruct IndexMsg from stored fields: {e}")

    def add_document(self, message: IndexMsg, writer: Optional[IndexWriter] = None):
        commit_locally = False
        if writer is None:
            writer = self.ix.writer()
            commit_locally = True

        try:
            # 获取适合 Whoosh 的字典，确保字段值非 None (除了 filename)
            doc_data = message.as_dict()
            # filename 可以是 None，Whoosh TEXT 字段能处理
            # 其他字段在 as_dict 中已确保非 None

            # 检查 URL 是否为空，如果为空则跳过或记录错误
            if not doc_data.get('url'):
                 logger.warning(f"Skipping document with empty URL. Content: {doc_data.get('content', '')[:50]}")
                 if commit_locally: writer.cancel() # 如果是独立写入，取消
                 return # 不添加没有 URL 的文档

            writer.add_document(**doc_data)
            if commit_locally:
                writer.commit()
        except Exception as e:
            if commit_locally:
                writer.cancel() # 出错时取消写入
            logger.error(f"Error adding document (URL: {message.url}): {e}", exc_info=True)
            raise e # 重新抛出异常

    # --- search 方法已修复 AttributeError ---
    def search(self, q_str: str, in_chats: Optional[List[int]], page_len: int, page_num: int = 1) -> SearchResult:
        # 解析查询字符串
        try:
            q = self.query_parser.parse(q_str)
            logger.debug(f"Parsed query: {q}")
        except Exception as e:
             logger.error(f"Failed to parse query '{q_str}': {e}")
             return SearchResult([], True, 0) # 返回空结果

        with self.ix.searcher() as searcher:
            q_filter = None
            if in_chats:
                 # 确保 chat_id 列表非空且元素有效
                 valid_chat_ids = [str(chat_id) for chat_id in in_chats if isinstance(chat_id, int) or (isinstance(chat_id, str) and chat_id.isdigit())]
                 if valid_chat_ids:
                      q_filter = Or([Term('chat_id', chat_id_str) for chat_id_str in valid_chat_ids])
                      logger.debug(f"Applying chat filter: {q_filter}")

            try:
                # 使用 allow_fragments=True 可以在结果少于页长时仍获取片段
                result_page = searcher.search_page(q, page_num, page_len, filter=q_filter,
                                                   sortedby='post_time', reverse=True, mask=q_filter,
                                                   terms=True) # terms=True 用于高亮
                logger.debug(f"Search executed. Page {page_num} has {len(result_page)} results. Total found: {result_page.total}")
            except Exception as e:
                logger.error(f"Search execution failed for query '{q}' and filter '{q_filter}': {e}", exc_info=True)
                return SearchResult([], True, 0) # 返回空结果

            hits = []
            for hit in result_page: # hit 是 Hit 对象
                try:
                    # 从 Hit 对象获取存储的字段字典
                    stored_fields = hit.fields()
                    if not stored_fields: # 如果没有存储字段，跳过
                         logger.warning(f"Hit {hit.docnum} has no stored fields, skipping.")
                         continue

                    # 为 IndexMsg 准备参数，处理可能的 None 值和类型
                    msg_content = stored_fields.get('content', '')
                    msg_url = stored_fields.get('url', '')
                    msg_chat_id_str = stored_fields.get('chat_id', '0')
                    try: msg_chat_id = int(msg_chat_id_str)
                    except (ValueError, TypeError): msg_chat_id = 0
                    msg_post_time = stored_fields.get('post_time', datetime.now())
                    msg_sender = stored_fields.get('sender', '')
                    msg_filename = stored_fields.get('filename', None) # filename 可以是 None

                    # 创建 IndexMsg
                    msg = IndexMsg(
                        content=msg_content,
                        url=msg_url,
                        chat_id=msg_chat_id,
                        post_time=msg_post_time,
                        sender=msg_sender,
                        filename=msg_filename
                    )

                    # 使用 Hit 对象进行高亮
                    highlighted_content = self.highlighter.highlight_hit(hit, 'content', top=1) # 只取第一个高亮片段
                    if not highlighted_content and msg_content: # 如果没有高亮但有内容，显示部分内容
                         highlighted_content = msg_content[:150] + ('...' if len(msg_content) > 150 else '')

                    hits.append(SearchHit(msg, highlighted_content or "")) # 确保高亮不是 None

                except Exception as e:
                    logger.error(f"Failed to process hit {hit.docnum} with fields {stored_fields}: {e}", exc_info=True)
                    # 跳过这条有问题的结果
                    pass

            # is_last_page() 可能不完全准确，用数量判断更可靠
            is_last = (page_num * page_len) >= result_page.total
            return SearchResult(hits, is_last, result_page.total)
    # --- 结束修复 search ---


    def list_indexed_chats(self) -> Set[int]:
        if self.ix.is_empty():
             return set()
        with self.ix.reader() as r:
            try:
                # 使用 lexicon 获取所有唯一的 chat_id 词项 (bytes)
                return {int(chat_id_bytes.decode('utf-8')) for chat_id_bytes in r.lexicon('chat_id')}
            except KeyError:
                logger.warning("'chat_id' field not found in index lexicon.")
                return set()
            except ValueError as e:
                 logger.error(f"Error converting chat_id from lexicon to int: {e}")
                 return set() # 返回空，避免因一个错误 ID 导致整体失败
            except Exception as e:
                 logger.error(f"Error listing indexed chats from lexicon: {e}", exc_info=True)
                 return set()


    # --- count_by_query 方法已添加 ---
    def count_by_query(self, **kw):
        """根据提供的字段=值查询文档数量"""
        if self.ix.is_empty():
            return 0
        if not kw:
            return self.ix.doc_count()

        # 构建查询条件 (目前只支持一个条件)
        try:
             field, value = list(kw.items())[0]
             query = Term(field, str(value)) # 确保值为字符串
        except IndexError: # 如果 kw 为空或格式错误
             logger.warning("count_by_query called with invalid arguments.")
             return self.ix.doc_count() # 返回总数

        try:
             with self.ix.searcher() as s:
                 return s.doc_count(query=query)
        except Exception as e:
             logger.error(f"Error counting documents for query {kw}: {e}", exc_info=True)
             return 0
    # --- 结束添加 ---

    def delete(self, url: str):
        if not url: # 避免删除空 URL
             logger.warning("Attempted to delete document with empty URL.")
             return
        with self.ix.writer() as writer:
            try:
                writer.delete_by_term('url', url)
            except Exception as e:
                 logger.error(f"Error deleting document by url '{url}': {e}", exc_info=True)
                 # 不取消，可能其他删除操作需要继续

    def get_document_fields(self, url: str) -> Optional[dict]:
        """获取指定 URL 的文档的所有存储字段"""
        if self.ix.is_empty() or not url:
            return None
        with self.ix.searcher() as searcher:
            try:
                 doc = searcher.document(url=url)
                 return doc
            except KeyError:
                 logger.debug(f"Document with url '{url}' not found.")
                 return None
            except Exception as e:
                 logger.error(f"Error getting document fields for url '{url}': {e}", exc_info=True)
                 return None


    # --- replace_document 方法已添加 ---
    def replace_document(self, url: str, new_fields: dict):
        """用新的字段完全替换指定 URL 的文档"""
        if not url:
             raise ValueError("Cannot replace document with empty URL.")
        # 确保必要的字段存在
        required_keys = ['content', 'url', 'chat_id', 'post_time', 'sender'] # filename 是可选的
        for key in required_keys:
            if key not in new_fields:
                 raise ValueError(f"Missing required field '{key}' when replacing document for url '{url}'")

        # 准备 Whoosh 接受的字典
        doc_data = {
            'content': new_fields.get('content', ''),
            'url': url, # 确保 URL 正确
            'chat_id': str(new_fields.get('chat_id', 0)), # 确保是字符串
            'post_time': new_fields.get('post_time', datetime.now()),
            'sender': new_fields.get('sender', ''),
            'filename': new_fields.get('filename', None) # filename 可以是 None
        }

        with self.ix.writer() as writer:
            try:
                # update_document 会先删除匹配 unique 字段 (url) 的文档，然后添加新文档
                writer.update_document(**doc_data)
            except Exception as e:
                 logger.error(f"Error replacing document with url '{url}': {e}", exc_info=True)
                 writer.cancel() # 替换失败时取消
                 raise e # 重新抛出
    # --- 结束添加 ---

    def clear(self):
        # 调用内部的 _clear 函数
        self._clear()

    def is_empty(self, chat_id=None) -> bool:
         """检查索引或特定聊天的索引是否为空"""
         if self.ix.is_empty():
              return True
         if chat_id is not None:
              try:
                   with self.ix.searcher() as searcher:
                        q = Term("chat_id", str(chat_id))
                        results = searcher.search(q, limit=0) # limit=0 只获取数量
                        return results.estimated_length() == 0
              except Exception as e:
                   logger.error(f"Error checking if chat {chat_id} is empty: {e}", exc_info=True)
                   return True # 出错时假设为空
         else:
              # 如果索引非空，则整体不为空
              return False
