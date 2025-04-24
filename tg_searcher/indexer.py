# -*- coding: utf-8 -*-
from pathlib import Path
from datetime import datetime
import random
from typing import Optional, Union, List, Set

from whoosh import index, writing
from whoosh.fields import Schema, TEXT, ID, DATETIME, NUMERIC # 确保导入 NUMERIC
from whoosh.qparser import QueryParser, MultifieldPlugin, OrGroup # 导入 MultifieldPlugin, OrGroup
from whoosh.writing import IndexWriter
from whoosh.query import Term, Or, And, Not, Every # 导入 And, Not, Every
import whoosh.highlight as highlight
from jieba.analyse.analyzer import ChineseAnalyzer

# 尝试获取日志记录器，如果 common.py 不可用或 get_logger 未定义，则使用标准 logging
try:
    from .common import get_logger
    logger = get_logger('indexer')
except (ImportError, AttributeError, ModuleNotFoundError): # 添加 ModuleNotFoundError
    import logging
    logger = logging.getLogger('indexer')
    # 配置基本的日志记录，以便在无法从 common 获取时仍能看到信息
    if not logger.hasHandlers():
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logger.info("Indexer logger initialized with basicConfig.")


class IndexMsg:
    # --- Schema 已包含 has_file ---
    schema = Schema(
        content=TEXT(stored=True, analyzer=ChineseAnalyzer()),
        url=ID(stored=True, unique=True), # 确保 url 是 unique=True
        chat_id=TEXT(stored=True), # 使用 TEXT 以便列出所有值
        post_time=DATETIME(stored=True, sortable=True),
        sender=TEXT(stored=True),
        filename=TEXT(stored=True, analyzer=ChineseAnalyzer()),
        has_file=NUMERIC(stored=True, numtype=int) # 0 for no, 1 for yes
    )

    def __init__(self, content: str, url: str, chat_id: Union[int, str],
                 post_time: datetime, sender: str, filename: Optional[str] = None):
        self.content = content
        self.url = url
        try:
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
        # 根据 filename 设置 has_file
        self.has_file = 1 if filename else 0

    def as_dict(self):
        # 返回适合 Whoosh 存储的字典
        return {
            'content': self.content or "", # 确保非 None
            'url': self.url or "", # 确保非 None
            'chat_id': str(self.chat_id),
            'post_time': self.post_time,
            'sender': self.sender or "", # 确保非 None
            'filename': self.filename, # filename 可以是 None
            'has_file': self.has_file
        }

    def __str__(self):
        fields = self.as_dict()
        return f'IndexMsg(' + ', '.join(f'{k}={repr(v)}' for k, v in fields.items()) + ')'


class SearchHit:
    def __init__(self, msg: IndexMsg, highlighted: str):
        self.msg = msg
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
            try:
                Path(index_dir).mkdir(parents=True, exist_ok=True) # 确保父目录也存在
                logger.info(f"Created index directory: {index_dir}")
            except OSError as e:
                 logger.critical(f"Failed to create index directory {index_dir}: {e}")
                 raise

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
                     # 即使失败也尝试创建索引
            try:
                self.ix = index.create_in(index_dir, IndexMsg.schema, index_name)
                logger.info(f"New index created after clear attempt.")
            except Exception as e:
                 logger.critical(f"Failed to create new index after clear attempt: {e}")
                 raise

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
        # 捕获可能的写锁错误
        except writing.LockError:
            logger.critical(f"Index '{index_dir}' is locked. Another process might be using it. Please check and retry.")
            raise
        except Exception as e:
             logger.critical(f"Failed to open or create index in {index_dir}: {e}", exc_info=True)
             raise


        # 检查 Schema 是否兼容
        if not self.ix.is_empty():
            expected_fields = sorted(IndexMsg.schema.names())
            actual_fields = sorted(self.ix.schema.names())
            if expected_fields != actual_fields:
                 raise ValueError(
                    f"Incompatible schema in index '{index_dir}'\n"
                    f"\tExpected: {expected_fields}\n"
                    f"\tOn disk:  {actual_fields}\n"
                    f"Please clear the index (use -c or delete directory)."
                 )

        self._clear = _clear

        # QueryParser 配置
        self.query_parser = QueryParser('content', IndexMsg.schema, group=OrGroup)
        self.query_parser.add_plugin(MultifieldPlugin(["content", "filename"]))

        # Highlighter 配置 (使用加粗)
        self.highlighter = highlight.Highlighter(
            fragmenter=highlight.ContextFragmenter(maxchars=150, surround=50),
            formatter=highlight.HtmlFormatter(tagname="b")
        )

    def retrieve_random_document(self) -> IndexMsg:
        if self.ix.is_empty(): raise IndexError("Index is empty")
        with self.ix.searcher() as searcher:
            reader = searcher.reader()
            doc_nums = list(reader.all_doc_ids())
            if not doc_nums: raise IndexError("Index contains no documents")

            random_doc_num = random.choice(doc_nums)
            msg_dict = searcher.stored_fields(random_doc_num)
            if not msg_dict: raise IndexError(f"No stored fields for doc {random_doc_num}")

            # 填充默认值以匹配 IndexMsg 构造函数
            msg_dict.setdefault('filename', None)
            msg_dict.setdefault('content', '')
            msg_dict.setdefault('url', '')
            msg_dict.setdefault('chat_id', 0)
            msg_dict.setdefault('post_time', datetime.now())
            msg_dict.setdefault('sender', '')
            # has_file 字段主要用于过滤，如果未存储则不需要处理

            try:
                return IndexMsg(
                    content=msg_dict['content'], url=msg_dict['url'],
                    chat_id=msg_dict['chat_id'], post_time=msg_dict['post_time'],
                    sender=msg_dict['sender'], filename=msg_dict['filename']
                )
            except Exception as e:
                 logger.error(f"Error reconstructing IndexMsg from {msg_dict}: {e}", exc_info=True)
                 raise ValueError(f"Failed to reconstruct IndexMsg: {e}")

    def add_document(self, message: IndexMsg, writer: Optional[IndexWriter] = None):
        commit_locally = False
        if writer is None:
            try:
                writer = self.ix.writer()
                commit_locally = True
            except writing.LockError:
                 logger.error("Failed to get index writer (LockError). Document not added.")
                 # 可以选择重试或直接返回/抛出异常
                 raise # 重新抛出锁错误，让调用者知道
            except Exception as e:
                 logger.error(f"Failed to get index writer: {e}", exc_info=True)
                 raise # 重新抛出其他错误

        try:
            doc_data = message.as_dict()
            if not doc_data.get('url'):
                 logger.warning(f"Skipping document with empty URL. Content: {doc_data.get('content', '')[:50]}")
                 if commit_locally: writer.cancel()
                 return

            # filename 为 None 时 Whoosh TEXT 能处理，不用转换为空字符串
            doc_data['filename'] = doc_data['filename'] if doc_data['filename'] is not None else ""

            writer.add_document(**doc_data)
            if commit_locally:
                writer.commit()
        except Exception as e:
            if commit_locally:
                try: writer.cancel()
                except Exception as cancel_e: logger.error(f"Error cancelling writer: {cancel_e}")
            logger.error(f"Error adding document (URL: {message.url}): {e}", exc_info=True)
            # 不重新抛出，避免中断批量写入，但外部应检查返回值或日志

    def search(self, q_str: str, in_chats: Optional[List[int]], page_len: int, page_num: int = 1, file_filter: str = "all") -> SearchResult:
        try:
            q = self.query_parser.parse(q_str)
            logger.debug(f"Parsed query: {q}")
        except Exception as e:
             logger.error(f"Failed to parse query '{q_str}': {e}")
             return SearchResult([], True, 0)

        try:
             with self.ix.searcher() as searcher:
                 # 聊天过滤器
                 base_filter = None
                 if in_chats:
                      valid_chat_ids = [str(cid) for cid in in_chats if isinstance(cid, int) or (isinstance(cid, str) and cid.isdigit())]
                      if valid_chat_ids: base_filter = Or([Term('chat_id', cid) for cid in valid_chat_ids])

                 # 文件类型过滤器
                 type_filter = None
                 if file_filter == "text_only": type_filter = Term("has_file", 0)
                 elif file_filter == "file_only": type_filter = Term("has_file", 1)

                 # 合并过滤器
                 final_filter = None
                 if base_filter and type_filter: final_filter = And([base_filter, type_filter])
                 elif base_filter: final_filter = base_filter
                 elif type_filter: final_filter = type_filter
                 # 如果都没有，final_filter 为 None，不过滤

                 logger.debug(f"Executing search with query '{q}' and filter '{final_filter}'")
                 result_page = searcher.search_page(q, page_num, page_len, filter=final_filter,
                                                    sortedby='post_time', reverse=True, mask=final_filter,
                                                    terms=True) # terms=True 用于高亮
                 logger.debug(f"Search found {result_page.total} results. Page {page_num} has {len(result_page)} hits.")

                 hits = []
                 for hit in result_page:
                     try:
                         stored_fields = hit.fields()
                         if not stored_fields: continue

                         # 使用 IndexMsg 构造函数处理类型和默认值
                         msg = IndexMsg(
                             content=stored_fields.get('content', ''),
                             url=stored_fields.get('url', ''),
                             chat_id=stored_fields.get('chat_id', 0),
                             post_time=stored_fields.get('post_time', datetime.now()),
                             sender=stored_fields.get('sender', ''),
                             filename=stored_fields.get('filename', None)
                         )

                         # 高亮 content
                         highlighted_content = self.highlighter.highlight_hit(hit, 'content', top=1) or ""
                         # 如果无高亮但有内容，取简短原文
                         if not highlighted_content and msg.content:
                              highlighted_content = html.escape(brief_content(msg.content)) # 转义以防纯文本中有 HTML

                         hits.append(SearchHit(msg, highlighted_content))
                     except Exception as e:
                         logger.error(f"Error processing hit {hit.docnum}: {e}", exc_info=True)

                 is_last = (page_num * page_len) >= result_page.total
                 return SearchResult(hits, is_last, result_page.total)
        except Exception as e:
            logger.error(f"Search execution failed for query '{q_str}': {e}", exc_info=True)
            return SearchResult([], True, 0)


    def list_indexed_chats(self) -> Set[int]:
        if self.ix.is_empty(): return set()
        with self.ix.reader() as r:
            try:
                # lexicon 返回 bytes
                return {int(chat_id_bytes.decode('utf-8')) for chat_id_bytes in r.lexicon('chat_id')}
            except KeyError: return set() # 'chat_id' 字段不存在
            except ValueError as e: logger.error(f"Error converting chat_id from lexicon: {e}"); return set()
            except Exception as e: logger.error(f"Error listing indexed chats: {e}", exc_info=True); return set()


    def count_by_query(self, **kw):
        if self.ix.is_empty(): return 0
        if not kw: return self.ix.doc_count()
        try:
             field, value = list(kw.items())[0]
             query = Term(field, str(value))
        except IndexError: return self.ix.doc_count() # 无效参数则返回总数
        try:
             with self.ix.searcher() as s: return s.doc_count(query=query)
        except Exception as e: logger.error(f"Error counting docs for {kw}: {e}", exc_info=True); return 0


    def delete(self, url: str):
        if not url: return
        try:
            with self.ix.writer() as writer:
                writer.delete_by_term('url', url)
        except Exception as e: logger.error(f"Error deleting doc by url '{url}': {e}", exc_info=True)


    def get_document_fields(self, url: str) -> Optional[dict]:
        if self.ix.is_empty() or not url: return None
        try:
            with self.ix.searcher() as searcher: return searcher.document(url=url)
        except KeyError: return None # 文档不存在
        except Exception as e: logger.error(f"Error getting doc fields for url '{url}': {e}", exc_info=True); return None


    def replace_document(self, url: str, new_fields: dict):
        if not url: raise ValueError("Cannot replace document with empty URL.")
        required = ['content', 'url', 'chat_id', 'post_time', 'sender']
        if any(k not in new_fields for k in required): raise ValueError(f"Missing required field for replace url '{url}'")
        # 准备 Whoosh 接受的字典
        doc_data = {
            'content': new_fields.get('content', ''), 'url': url,
            'chat_id': str(new_fields.get('chat_id', 0)), 'post_time': new_fields.get('post_time', datetime.now()),
            'sender': new_fields.get('sender', ''),
            'filename': new_fields.get('filename', None),
            # 确保 has_file 字段也更新
            'has_file': 1 if new_fields.get('filename') else 0
        }
        try:
            with self.ix.writer() as writer: writer.update_document(**doc_data)
        except Exception as e:
            logger.error(f"Error replacing document url '{url}': {e}", exc_info=True); raise e


    def clear(self):
        self._clear() # 调用内部函数

    def is_empty(self, chat_id=None) -> bool:
         if self.ix.is_empty(): return True
         if chat_id is not None:
              try:
                   with self.ix.searcher() as searcher:
                        q = Term("chat_id", str(chat_id))
                        return searcher.doc_count(query=q) == 0 # 使用 doc_count 更直接
              except Exception as e: logger.error(f"Error checking emptiness for chat {chat_id}: {e}"); return True
         else: return False # 索引非空且未指定 chat_id
