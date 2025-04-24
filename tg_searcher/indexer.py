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
    # --- 修改 Schema ---
    schema = Schema(
        content=TEXT(stored=True, analyzer=ChineseAnalyzer()),
        url=ID(stored=True, unique=True), # 确保 url 是 unique=True
        chat_id=TEXT(stored=True),
        post_time=DATETIME(stored=True, sortable=True),
        sender=TEXT(stored=True),
        filename=TEXT(stored=True, analyzer=ChineseAnalyzer()) # 添加 filename 字段
    )
    # --- 结束修改 ---

    # --- 修改 __init__ ---
    def __init__(self, content: str, url: str, chat_id: Union[int, str],
                 post_time: datetime, sender: str, filename: Optional[str] = None): # 添加 filename 参数
        self.content = content # 消息文本/标题
        self.url = url
        self.chat_id = int(chat_id)
        self.post_time = post_time
        self.sender = sender
        self.filename = filename # 文件名
    # --- 结束修改 ---

    # --- 修改 as_dict ---
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
                 raise ValueError(
                    f"Incompatible schema in your index '{index_dir}'\n"
                    f"\tExpected fields: {expected_fields}\n"
                    f"\tFields on disk:  {actual_fields}\n"
                    f"Please clear the index (use -c argument) or use a different index directory."
                 )

        self._clear = _clear

        # --- 修改 QueryParser 初始化 ---
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
            # 获取所有文档编号
            doc_nums = list(searcher.all_doc_ids())
            random_doc_num = random.choice(doc_nums)
            # 获取随机文档的字段
            msg_dict = searcher.stored_fields(random_doc_num)
            # 修复可能因为 schema 变更导致的缺少字段问题（给默认值）
            msg_dict.setdefault('filename', None)
            return IndexMsg(**msg_dict)

    def add_document(self, message: IndexMsg, writer: Optional[IndexWriter] = None):
        commit_locally = False
        if writer is None:
            writer = self.ix.writer()
            commit_locally = True

        try:
            writer.add_document(**message.as_dict())
            if commit_locally:
                writer.commit()
        except Exception as e:
            if commit_locally:
                writer.cancel() # 出错时取消写入
            raise e # 重新抛出异常

    def search(self, q_str: str, in_chats: Optional[List[int]], page_len: int, page_num: int = 1) -> SearchResult:
        q = self.query_parser.parse(q_str)
        with self.ix.searcher() as searcher:
            q_filter = in_chats and Or([Term('chat_id', str(chat_id)) for chat_id in in_chats])
            result_page = searcher.search_page(q, page_num, page_len, filter=q_filter,
                                               sortedby='post_time', reverse=True)

            hits = []
            for hit_doc in result_page:
                # 修复字段缺失问题
                hit_doc.setdefault('filename', None)
                # 高亮 content 字段
                highlighted_content = self.highlighter.highlight_hit(hit_doc, 'content', top=3)
                hits.append(SearchHit(IndexMsg(**hit_doc), highlighted_content))

            return SearchResult(hits, result_page.is_last_page(), result_page.total)

    def list_indexed_chats(self) -> Set[int]:
        if self.ix.is_empty():
             return set()
        with self.ix.reader() as r:
            # 使用 set comprehension 避免重复
            try:
                return {int(chat_id) for chat_id in r.field_terms('chat_id')}
            except KeyError: # 如果 'chat_id' 字段不存在或为空
                return set()


    def count_by_query(self, **kw):
        if self.ix.is_empty():
            return 0
        with self.ix.searcher() as s:
            # 使用 s.doc_count_by() 可能更高效
            q = Term(list(kw.keys())[0], list(kw.values())[0]) # 假设只有一个查询条件
            return s.doc_count(query=q)

    def delete(self, url: str):
        # 使用 Term 对象删除更精确
        with self.ix.writer() as writer:
            writer.delete_by_term('url', url)

    # --- 修改 update 方法为 replace_document ---
    # def update(self, content: str, url: str): # 原来的方法，只更新 content
    #     # 这个方法不够健壮，因为它只更新了 content，如果其他字段也需要更新怎么办？
    #     # 而且它依赖于 Whoosh 的 update_document 只更新指定字段，但实际行为是替换整个文档
    #     # 因此我们改用更明确的 replace_document
    #     pass

    def get_document_fields(self, url: str) -> Optional[dict]:
        """获取指定 URL 的文档的所有存储字段"""
        if self.ix.is_empty():
            return None
        with self.ix.searcher() as searcher:
            # 使用 document() 方法通过 unique 字段获取
            doc = searcher.document(url=url)
            return doc # 返回 dict 或 None

    def replace_document(self, url: str, new_fields: dict):
        """用新的字段完全替换指定 URL 的文档"""
        # 确保必要的字段存在
        required_keys = ['content', 'url', 'chat_id', 'post_time', 'sender', 'filename']
        for key in required_keys:
            if key not in new_fields:
                # 可以抛出错误，或者设置默认值
                 raise ValueError(f"Missing required field '{key}' when replacing document for url '{url}'")
                # new_fields.setdefault(key, None) # 或者设置默认值，但可能不是预期行为

        with self.ix.writer() as writer:
            # update_document 会先删除匹配 unique 字段 (url) 的文档，然后添加新文档
            writer.update_document(**new_fields)
    # --- 结束修改 ---

    def clear(self):
        # 调用内部的 _clear 函数
        self._clear()

    def is_empty(self, chat_id=None) -> bool:
         """检查索引或特定聊天的索引是否为空"""
         if self.ix.is_empty():
              return True
         if chat_id is not None:
              with self.ix.searcher() as searcher:
                   q = Term("chat_id", str(chat_id))
                   results = searcher.search(q, limit=1)
                   return len(results) == 0
         else:
              # 如果索引非空，则整体不为空
              return False
