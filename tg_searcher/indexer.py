# -*- coding: utf-8 -*-
from pathlib import Path
from datetime import datetime
import random
from typing import Optional, Union, List, Set

from whoosh import index
# 确保导入所有需要的类型
from whoosh.fields import Schema, TEXT, ID, DATETIME, NUMERIC
from whoosh.qparser import QueryParser, MultifieldPlugin, OrGroup
from whoosh.writing import IndexWriter
from whoosh.query import Term, Or, And, Not # 导入 And, Not
import whoosh.highlight as highlight
from jieba.analyse.analyzer import ChineseAnalyzer

# 尝试获取日志记录器
try:
    from .common import get_logger
    logger = get_logger('indexer')
except (ImportError, AttributeError):
    import logging
    logger = logging.getLogger('indexer')
    if not logger.hasHandlers():
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class IndexMsg:
    # --- Schema: 添加了 filename 和 has_file ---
    schema = Schema(
        content=TEXT(stored=True, analyzer=ChineseAnalyzer()),
        url=ID(stored=True, unique=True),
        chat_id=TEXT(stored=True),
        post_time=DATETIME(stored=True, sortable=True),
        sender=TEXT(stored=True),
        filename=TEXT(stored=True, analyzer=ChineseAnalyzer()),
        has_file=NUMERIC(stored=True, numtype=int) # 0 for no, 1 for yes
    )

    def __init__(self, content: str, url: str, chat_id: Union[int, str],
                 post_time: datetime, sender: str, filename: Optional[str] = None):
        self.content = content
        self.url = url
        try: self.chat_id = int(chat_id)
        except (ValueError, TypeError): self.chat_id = 0
        self.post_time = post_time
        self.sender = sender
        self.filename = filename
        # --- 根据 filename 设置 has_file ---
        self.has_file = 1 if filename else 0

    def as_dict(self):
        # 返回适合 Whoosh 存储的字典
        return {
            'content': self.content or "",
            'url': self.url or "",
            'chat_id': str(self.chat_id),
            'post_time': self.post_time,
            'sender': self.sender or "",
            'filename': self.filename, # 可以是 None
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
        if not Path(index_dir).exists(): Path(index_dir).mkdir()

        def _clear():
            import shutil; logger.warning(f"Clearing index directory: {index_dir}")
            shutil.rmtree(index_dir); index_dir.mkdir()
            self.ix = index.create_in(index_dir, IndexMsg.schema, index_name)
            logger.info(f"Index directory cleared and new index created.")

        if from_scratch: _clear()

        try:
            if index.exists_in(index_dir, index_name):
                logger.info(f"Opening existing index in {index_dir}")
                self.ix = index.open_dir(index_dir, index_name)
            else:
                logger.info(f"Creating new index in {index_dir}")
                self.ix = index.create_in(index_dir, IndexMsg.schema, index_name)
        except index.EmptyIndexError:
             logger.warning(f"Index in {index_dir} empty/corrupted, creating new.")
             self.ix = index.create_in(index_dir, IndexMsg.schema, index_name)
        except Exception as e:
             logger.critical(f"Failed to open/create index in {index_dir}: {e}", exc_info=True); raise

        # 检查 Schema 兼容性
        if not self.ix.is_empty():
            expected = sorted(IndexMsg.schema.names()); actual = sorted(self.ix.schema.names())
            if expected != actual:
                 raise ValueError(f"Incompatible schema: Expected {expected}, On disk {actual}. Clear index (-c).")

        self._clear = _clear
        # --- QueryParser: 搜索 content 和 filename ---
        self.query_parser = QueryParser('content', IndexMsg.schema, group=OrGroup)
        self.query_parser.add_plugin(MultifieldPlugin(["content", "filename"]))
        # --- Highlighter: 使用 <b> 高亮 ---
        self.highlighter = highlight.Highlighter(
            fragmenter=highlight.ContextFragmenter(maxchars=150, surround=50), # 调整摘要长度
            formatter=highlight.HtmlFormatter(tagname="b") # 使用加粗高亮
        )

    def retrieve_random_document(self) -> IndexMsg:
        with self.ix.searcher() as searcher:
            if searcher.doc_count() == 0: raise IndexError("Index is empty")
            reader = searcher.reader(); doc_nums = list(reader.all_doc_ids())
            if not doc_nums: raise IndexError("Index has no doc IDs")
            random_doc_num = random.choice(doc_nums)
            msg_dict = searcher.stored_fields(random_doc_num)
            if not msg_dict: raise IndexError(f"Could not get fields for doc {random_doc_num}")
            # 确保字段存在
            msg_dict.setdefault('filename', None); msg_dict.setdefault('content', '')
            msg_dict.setdefault('url', ''); msg_dict.setdefault('chat_id', 0)
            msg_dict.setdefault('post_time', datetime.now()); msg_dict.setdefault('sender', '')
            try: return IndexMsg(**msg_dict) # 使用 ** 传递字典创建对象
            except Exception as e: logger.error(f"Failed to reconstruct IndexMsg: {e}", exc_info=True); raise

    def add_document(self, message: IndexMsg, writer: Optional[IndexWriter] = None):
        commit_locally = writer is None
        if commit_locally: writer = self.ix.writer()
        try:
            doc_data = message.as_dict() # 包含 has_file
            if not doc_data.get('url'): logger.warning(f"Skipping document with empty URL."); return
            # filename 和其他 TEXT/ID 字段为 None 时会被 Whoosh 处理
            writer.add_document(**doc_data)
            if commit_locally: writer.commit()
        except Exception as e:
            if commit_locally: writer.cancel()
            logger.error(f"Error adding document (URL: {message.url}): {e}", exc_info=True)
            # 可以选择是否重新抛出 raise e

    # --- search 方法: 接受 file_filter 参数 ---
    def search(self, q_str: str, in_chats: Optional[List[int]], page_len: int, page_num: int = 1, file_filter: str = "all") -> SearchResult:
        try: q = self.query_parser.parse(q_str); logger.debug(f"Parsed query: {q}")
        except Exception as e: logger.error(f"Query parse error '{q_str}': {e}"); return SearchResult([], True, 0)

        with self.ix.searcher() as searcher:
            # 聊天过滤器
            base_filter = None
            if in_chats:
                valid_cids = [str(cid) for cid in in_chats if isinstance(cid, int) or (isinstance(cid, str) and cid.isdigit())]
                if valid_cids: base_filter = Or([Term('chat_id', cid) for cid in valid_cids]); logger.debug(f"Chat filter: {base_filter}")
            # 文件类型过滤器
            type_filter = None
            if file_filter == "text_only": type_filter = Term("has_file", 0); logger.debug("File filter: text_only")
            elif file_filter == "file_only": type_filter = Term("has_file", 1); logger.debug("File filter: file_only")
            # 合并过滤器
            final_filter = base_filter
            if type_filter: final_filter = And([final_filter, type_filter]) if final_filter else type_filter

            try:
                result_page = searcher.search_page(q, page_num, page_len, filter=final_filter,
                                                   sortedby='post_time', reverse=True, mask=final_filter, terms=True)
                logger.debug(f"Search: Page {page_num}, Hits {len(result_page)}, Total {result_page.total}, Filter {final_filter}")
            except Exception as e: logger.error(f"Search execution error: {e}", exc_info=True); return SearchResult([], True, 0)

            # 处理结果 (已修复 AttributeError)
            hits = []
            for hit in result_page:
                try:
                    stored = hit.fields()
                    if not stored: logger.warning(f"Hit {hit.docnum} has no stored fields."); continue
                    msg = IndexMsg( # 使用字段创建 IndexMsg
                        content=stored.get('content', ''), url=stored.get('url', ''), chat_id=stored.get('chat_id', 0),
                        post_time=stored.get('post_time', datetime.now()), sender=stored.get('sender', ''),
                        filename=stored.get('filename', None) )
                    hl_content = self.highlighter.highlight_hit(hit, 'content', top=1) or "" # 高亮摘要
                    if not hl_content and msg.content: hl_content = msg.content[:150] + ('...' if len(msg.content) > 150 else '')
                    hits.append(SearchHit(msg, hl_content))
                except Exception as e: logger.error(f"Failed to process hit {hit.docnum}: {e}", exc_info=True)

            is_last = (page_num * page_len) >= result_page.total
            return SearchResult(hits, is_last, result_page.total)

    def list_indexed_chats(self) -> Set[int]:
        if self.ix.is_empty(): return set()
        with self.ix.reader() as r:
            try: return {int(cid_bytes.decode('utf-8')) for cid_bytes in r.lexicon('chat_id')}
            except KeyError: logger.warning("'chat_id' field not found in lexicon."); return set()
            except ValueError as e: logger.error(f"Error converting chat_id from lexicon: {e}"); return set()
            except Exception as e: logger.error(f"Error listing indexed chats: {e}", exc_info=True); return set()

    def count_by_query(self, **kw):
        if self.ix.is_empty(): return 0
        if not kw: return self.ix.doc_count()
        try: field, value = list(kw.items())[0]; query = Term(field, str(value))
        except IndexError: logger.warning("count_by_query invalid args."); return self.ix.doc_count()
        try:
            with self.ix.searcher() as s: return s.doc_count(query=query)
        except Exception as e: logger.error(f"Error counting docs for {kw}: {e}", exc_info=True); return 0

    def delete(self, url: str):
        if not url: logger.warning("Attempted delete with empty URL."); return
        with self.ix.writer() as writer:
            try: writer.delete_by_term('url', url)
            except Exception as e: logger.error(f"Error deleting doc url '{url}': {e}", exc_info=True)

    def get_document_fields(self, url: str) -> Optional[dict]:
        if self.ix.is_empty() or not url: return None
        with self.ix.searcher() as searcher:
            try: return searcher.document(url=url)
            except KeyError: logger.debug(f"Doc url '{url}' not found."); return None
            except Exception as e: logger.error(f"Error getting doc fields for url '{url}': {e}", exc_info=True); return None

    def replace_document(self, url: str, new_fields: dict):
        if not url: raise ValueError("Cannot replace document with empty URL.")
        required = ['content', 'url', 'chat_id', 'post_time', 'sender']
        if any(k not in new_fields for k in required): raise ValueError(f"Missing required field for replacing {url}")
        doc_data = { # 准备数据
            'content': new_fields.get('content', ''), 'url': url, 'chat_id': str(new_fields.get('chat_id', 0)),
            'post_time': new_fields.get('post_time', datetime.now()), 'sender': new_fields.get('sender', ''),
            'filename': new_fields.get('filename', None), 'has_file': 1 if new_fields.get('filename') else 0 } # 更新 has_file
        with self.ix.writer() as writer:
            try: writer.update_document(**doc_data)
            except Exception as e: logger.error(f"Error replacing doc url '{url}': {e}", exc_info=True); writer.cancel(); raise e

    def clear(self): self._clear()

    def is_empty(self, chat_id=None) -> bool:
         if self.ix.is_empty(): return True
         if chat_id is not None:
              try:
                  with self.ix.searcher() as s: q = Term("chat_id", str(chat_id)); return s.search(q, limit=0).estimated_length() == 0
              except Exception as e: logger.error(f"Error checking emptiness for chat {chat_id}: {e}", exc_info=True); return True
         else: return False # Index is not empty overall
