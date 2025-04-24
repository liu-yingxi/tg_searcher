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
        self.chat_id = int(chat_id)
        self.post_time = post_time
        self.sender = sender
        self.filename = filename # 文件名
    # --- 结束修改 ---

    # --- as_dict 已包含 filename ---
    def as_dict(self):
        return {
            'content': self.content,
            'url': self.url,
            'chat_id': str(self.chat_id),
            'post_time': self.post_time,
            'sender': self.sender,
            'filename': self.filename # 添加 filename
        }
    # --- 结束修改 ---

    def __str__(self):
        # 稍微调整 __str__ 以包含 filename (可选)
        fields = self.as_dict()
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
            shutil.rmtree(index_dir)
            index_dir.mkdir()
            self.ix = index.create_in(index_dir, IndexMsg.schema, index_name)

        if from_scratch:
            _clear()

        # 打开或创建索引
        try:
            self.ix = index.open_dir(index_dir, index_name) \
                if index.exists_in(index_dir, index_name) \
                else index.create_in(index_dir, IndexMsg.schema, index_name)
        except index.EmptyIndexError: # 处理可能的空索引错误
             self.ix = index.create_in(index_dir, IndexMsg.schema, index_name)


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
        self.highlighter = highlight.Highlighter(fragmenter=highlight.ContextFragmenter(maxchars=200, surround=50))

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
            # 修复可能因为 schema 变更导致的缺少字段问题（给默认值）
            msg_dict.setdefault('filename', None)
            # 确保所有必要的字段都存在，否则提供默认值或抛出错误
            # （根据 IndexMsg.__init__ 的需要来补充）
            msg_dict.setdefault('content', '')
            msg_dict.setdefault('url', '') # URL 通常不应该为空
            msg_dict.setdefault('chat_id', 0) # 需要一个有效的 chat_id
            msg_dict.setdefault('post_time', datetime.now()) # 可能需要一个默认时间
            msg_dict.setdefault('sender', '')

            try:
                return IndexMsg(**msg_dict)
            except TypeError as e:
                 # 如果仍然缺少字段，记录错误并抛出
                 raise ValueError(f"Failed to reconstruct IndexMsg from stored fields {msg_dict}: {e}")

    def add_document(self, message: IndexMsg, writer: Optional[IndexWriter] = None):
        commit_locally = False
        if writer is None:
            writer = self.ix.writer()
            commit_locally = True

        try:
            # 确保传递给 add_document 的是字典
            doc_data = message.as_dict()
            # 处理 None 值，Whoosh 不接受 None
            for key, value in doc_data.items():
                if value is None:
                    # 根据字段类型设置合适的默认值，例如空字符串
                    if isinstance(IndexMsg.schema[key], (TEXT, ID)):
                         doc_data[key] = ""
                    # 可以为其他类型添加处理，但这里主要是 filename 和 content
            writer.add_document(**doc_data)
            if commit_locally:
                writer.commit()
        except Exception as e:
            if commit_locally:
                writer.cancel() # 出错时取消写入
            raise e # 重新抛出异常

    def search(self, q_str: str, in_chats: Optional[List[int]], page_len: int, page_num: int = 1) -> SearchResult:
        # 解析查询字符串
        try:
            q = self.query_parser.parse(q_str)
        except Exception as e:
             # 处理查询解析错误，例如语法错误
             # 可以返回一个空结果或者特定的错误信息
             # raise ValueError(f"Invalid search query: {e}") # 抛出异常让上层处理
             return SearchResult([], True, 0) # 或者返回空结果

        with self.ix.searcher() as searcher:
            q_filter = None
            if in_chats:
                 # 确保 chat_id 列表非空
                 valid_chat_ids = [str(chat_id) for chat_id in in_chats if chat_id]
                 if valid_chat_ids:
                      q_filter = Or([Term('chat_id', chat_id_str) for chat_id_str in valid_chat_ids])

            try:
                result_page = searcher.search_page(q, page_num, page_len, filter=q_filter,
                                                   sortedby='post_time', reverse=True, mask=q_filter) # 使用 mask 优化过滤
            except Exception as e:
                # 处理搜索执行错误
                # raise RuntimeError(f"Search execution failed: {e}")
                return SearchResult([], True, 0) # 返回空结果

            hits = []
            for hit_doc_fields in result_page: # hit_doc 是字段字典
                # 修复字段缺失问题
                hit_doc_fields.setdefault('filename', None)
                # 确保其他必要字段存在，为 IndexMsg 提供默认值
                hit_doc_fields.setdefault('content', '')
                hit_doc_fields.setdefault('url', hit_doc_fields.get('url', '')) # URL 应该总是存在
                hit_doc_fields.setdefault('chat_id', 0)
                hit_doc_fields.setdefault('post_time', datetime.now())
                hit_doc_fields.setdefault('sender', '')

                try:
                    msg = IndexMsg(**hit_doc_fields)
                    # 高亮 content 字段
                    highlighted_content = self.highlighter.highlight_hit(hit_doc_fields, 'content', top=3)
                    hits.append(SearchHit(msg, highlighted_content))
                except Exception as e:
                    # 如果从存储字段创建 IndexMsg 失败，记录日志并跳过此条目
                    # logger.error(f"Failed to create IndexMsg from hit {hit_doc_fields}: {e}")
                    pass # 或者记录日志

            return SearchResult(hits, result_page.is_last_page(), result_page.total)

    def list_indexed_chats(self) -> Set[int]:
        if self.ix.is_empty():
             return set()
        # 使用 Reader 上下文管理器确保关闭
        with self.ix.reader() as r:
            # 使用 set comprehension 避免重复
            try:
                # 使用 terms=True 获取字段的所有唯一词项
                return {int(chat_id_bytes.decode('utf-8')) for chat_id_bytes in r.lexicon('chat_id')}
            except KeyError: # 如果 'chat_id' 字段不存在或为空
                return set()
            except Exception as e: # 捕获其他潜在错误
                 # logger.error(f"Error listing indexed chats: {e}")
                 return set()

    # --- count_by_query 方法已添加 ---
    def count_by_query(self, **kw):
        """根据提供的字段=值查询文档数量"""
        if self.ix.is_empty():
            return 0
        if not kw:
            return self.ix.doc_count() # 如果没有指定查询，返回总数

        # 构建查询条件 (目前只支持一个条件)
        field, value = list(kw.items())[0]
        query = Term(field, str(value)) # 确保值为字符串

        try:
             with self.ix.searcher() as s:
                 # 使用 doc_count(query=...)
                 return s.doc_count(query=query)
        except Exception as e:
             # logger.error(f"Error counting documents for query {kw}: {e}")
             return 0 # 出错时返回 0
    # --- 结束添加 ---

    def delete(self, url: str):
        # 使用 Term 对象删除更精确
        with self.ix.writer() as writer:
            writer.delete_by_term('url', url)

    def get_document_fields(self, url: str) -> Optional[dict]:
        """获取指定 URL 的文档的所有存储字段"""
        if self.ix.is_empty():
            return None
        with self.ix.searcher() as searcher:
            try:
                 # 使用 document() 方法通过 unique 字段获取
                 doc = searcher.document(url=url)
                 return doc # 返回 dict 或 None
            except KeyError: # 如果 URL 不存在
                 return None
            except Exception as e:
                 # logger.error(f"Error getting document fields for url {url}: {e}")
                 return None


    # --- replace_document 方法已添加 ---
    def replace_document(self, url: str, new_fields: dict):
        """用新的字段完全替换指定 URL 的文档"""
        # 确保必要的字段存在
        required_keys = ['content', 'url', 'chat_id', 'post_time', 'sender'] # filename 是可选的
        for key in required_keys:
            if key not in new_fields:
                # 可以抛出错误，或者设置默认值
                 raise ValueError(f"Missing required field '{key}' when replacing document for url '{url}'")

        # 处理 None 值
        doc_data = new_fields.copy()
        for key, value in doc_data.items():
             if value is None:
                  if isinstance(IndexMsg.schema[key], (TEXT, ID)):
                       doc_data[key] = ""
                  # 可以为其他类型添加处理

        with self.ix.writer() as writer:
            # update_document 会先删除匹配 unique 字段 (url) 的文档，然后添加新文档
            writer.update_document(**doc_data)
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
                        # 使用 limit=0 只获取数量，可能比 limit=1 更快
                        results = searcher.search(q, limit=0)
                        # 使用 results.estimated_length() 获取匹配数量
                        return results.estimated_length() == 0
              except Exception as e:
                   # logger.error(f"Error checking if chat {chat_id} is empty: {e}")
                   return True # 出错时假设为空
         else:
              # 如果索引非空，则整体不为空
              return False
