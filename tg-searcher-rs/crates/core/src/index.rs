//! Tantivy-backed text index with Chinese segmentation, exact/boolean/phrase
//! search, a configurable fuzzy fallback, chat/file filters and pagination.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use rand::Rng;
use regex::RegexBuilder;
use tantivy::collector::{Count, DocSetCollector, TopDocs};
use tantivy::query::{
    AllQuery, BooleanQuery, EmptyQuery, FuzzyTermQuery, Occur, PhraseQuery, Query, RegexQuery,
    TermQuery,
};
use tantivy::schema::{
    Field, IndexRecordOption, NumericOptions, Schema, TantivyDocument, TextFieldIndexing,
    TextOptions, Value, STORED, STRING,
};
use tantivy::snippet::SnippetGenerator;
use tantivy::{Index, IndexReader, Order, Term};
use tracing::warn;

use crate::config::{FuzzyConfig, FuzzyMode};
use crate::fuzzy;
use crate::model::{FileFilter, IndexedMessage, SearchHit, SearchRequest, SearchResult};
use crate::query::{parse_query, QueryNode};
use crate::text::{brief_content, escape_html};
use crate::tokenizer::{analyzers, bigrams};
use crate::{CoreError, Result};

const URL: &str = "url";
const CHAT_ID: &str = "chat_id";
const CONTENT: &str = "content";
const FILENAME: &str = "filename";
const POST_TIME: &str = "post_time";
const SENDER: &str = "sender";
const HAS_FILE: &str = "has_file";
const CONTENT_GRAMS: &str = "content_grams";
const FILENAME_GRAMS: &str = "filename_grams";

const SNIPPET_CHARS: usize = 250;
const WRITER_MEMORY: usize = 50_000_000;

#[derive(Clone, Copy)]
struct Fields {
    url: Field,
    chat_id: Field,
    content: Field,
    filename: Field,
    post_time: Field,
    sender: Field,
    has_file: Field,
    content_grams: Field,
    filename_grams: Field,
}

/// A single tantivy index directory.
pub struct TextIndex {
    index: Index,
    reader: IndexReader,
    fields: Fields,
    fuzzy: FuzzyConfig,
    path: PathBuf,
}

impl TextIndex {
    /// Open (or create) an index. `from_scratch` deletes an existing directory
    /// first, matching the original `-c/--clear` flag.
    pub fn open(dir: impl AsRef<Path>, fuzzy: FuzzyConfig, from_scratch: bool) -> Result<Self> {
        let dir = dir.as_ref();
        if from_scratch && dir.exists() {
            warn!(path = %dir.display(), "clearing existing text index");
            fs::remove_dir_all(dir)?;
        }
        fs::create_dir_all(dir)?;

        let schema = build_schema(&fuzzy);
        let index = if dir.join("meta.json").exists() {
            Index::open_in_dir(dir).map_err(|e| {
                CoreError::Index(format!(
                    "cannot open index at {}: {e}; use --clear if it is corrupted",
                    dir.display()
                ))
            })?
        } else {
            Index::create_in_dir(dir, schema.clone())
                .map_err(|e| CoreError::Index(format!("cannot create index: {e}")))?
        };

        // The tokenizer manager is shared by the index; registering after open
        // makes custom analyzers available for both writes and searches.
        for (name, analyzer) in analyzers(fuzzy.ngram_size) {
            index.tokenizers().register(name, analyzer);
        }

        let fields = resolve_fields(&index.schema())?;
        let reader = index
            .reader()
            .map_err(|e| CoreError::Index(format!("cannot open index reader: {e}")))?;

        Ok(Self {
            index,
            reader,
            fields,
            fuzzy,
            path: dir.to_path_buf(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn fuzzy_config(&self) -> &FuzzyConfig {
        &self.fuzzy
    }

    pub fn set_fuzzy_config(&mut self, fuzzy: FuzzyConfig) {
        self.fuzzy = fuzzy;
    }

    fn writer(&self) -> Result<tantivy::IndexWriter> {
        self.index.writer(WRITER_MEMORY).map_err(|e| {
            CoreError::Index(format!(
                "cannot acquire index writer (is another process using {}?): {e}",
                self.path.display()
            ))
        })
    }

    fn reload(&self) -> Result<()> {
        self.reader
            .reload()
            .map_err(|e| CoreError::Index(format!("cannot reload index: {e}")))
    }

    /// Insert or replace messages (unique by URL).
    pub fn upsert_messages(&mut self, messages: &[IndexedMessage]) -> Result<usize> {
        if messages.is_empty() {
            return Ok(0);
        }
        let mut writer = self.writer()?;
        let mut count = 0usize;
        for message in messages {
            if message.url.is_empty() {
                continue;
            }
            if message.content.is_empty() && message.filename.is_none() {
                continue;
            }
            writer.delete_term(Term::from_field_text(self.fields.url, &message.url));
            let document = self.document_for(message);
            writer
                .add_document(document)
                .map_err(|e| CoreError::Index(format!("add_document failed: {e}")))?;
            count += 1;
        }
        writer
            .commit()
            .map_err(|e| CoreError::Index(format!("commit failed: {e}")))?;
        self.reload()?;
        Ok(count)
    }

    pub fn add_message(&mut self, message: &IndexedMessage) -> Result<usize> {
        self.upsert_messages(std::slice::from_ref(message))
    }

    fn document_for(&self, message: &IndexedMessage) -> TantivyDocument {
        let mut document = TantivyDocument::new();
        document.add_text(self.fields.url, &message.url);
        document.add_i64(self.fields.chat_id, message.chat_id);
        document.add_text(self.fields.content, &message.content);
        document.add_text(
            self.fields.filename,
            message.filename.as_deref().unwrap_or_default(),
        );
        document.add_i64(self.fields.post_time, message.post_time);
        document.add_text(self.fields.sender, &message.sender);
        document.add_u64(self.fields.has_file, u64::from(message.filename.is_some()));
        document.add_text(self.fields.content_grams, &message.content);
        document.add_text(
            self.fields.filename_grams,
            message.filename.as_deref().unwrap_or_default(),
        );
        document
    }

    /// Delete all messages from a chat.
    pub fn delete_chat(&mut self, chat_id: i64) -> Result<usize> {
        let mut writer = self.writer()?;
        writer.delete_term(Term::from_field_i64(self.fields.chat_id, chat_id));
        writer
            .commit()
            .map_err(|e| CoreError::Index(format!("commit failed: {e}")))?;
        self.reload()?;
        Ok(1)
    }

    pub fn delete_urls(&mut self, urls: &[String]) -> Result<usize> {
        if urls.is_empty() {
            return Ok(0);
        }
        let mut writer = self.writer()?;
        for url in urls {
            writer.delete_term(Term::from_field_text(self.fields.url, url));
        }
        writer
            .commit()
            .map_err(|e| CoreError::Index(format!("commit failed: {e}")))?;
        self.reload()?;
        Ok(urls.len())
    }

    pub fn delete_all(&mut self) -> Result<()> {
        let mut writer = self.writer()?;
        writer
            .delete_all_documents()
            .map_err(|e| CoreError::Index(format!("delete_all_documents failed: {e}")))?;
        writer
            .commit()
            .map_err(|e| CoreError::Index(format!("commit failed: {e}")))?;
        self.reload()?;
        Ok(())
    }

    pub fn total_documents(&self) -> usize {
        self.reader.searcher().num_docs() as usize
    }

    pub fn is_empty(&self) -> bool {
        self.total_documents() == 0
    }

    pub fn chat_count(&self, chat_id: i64) -> Result<usize> {
        let searcher = self.reader.searcher();
        let query = TermQuery::new(
            Term::from_field_i64(self.fields.chat_id, chat_id),
            IndexRecordOption::Basic,
        );
        let count = searcher
            .search(&query, &Count)
            .map_err(|e| CoreError::Index(format!("chat count failed: {e}")))?;
        Ok(count)
    }

    /// Fetch a single indexed message by its unique URL.
    pub fn get_message(&self, url: &str) -> Result<Option<IndexedMessage>> {
        if url.is_empty() {
            return Ok(None);
        }
        let searcher = self.reader.searcher();
        let query = TermQuery::new(
            Term::from_field_text(self.fields.url, url),
            IndexRecordOption::Basic,
        );
        let hits: Vec<(tantivy::Score, tantivy::DocAddress)> = searcher
            .search(&query, &TopDocs::with_limit(1).order_by_score())
            .map_err(|e| CoreError::Index(format!("url lookup failed: {e}")))?;
        let Some((_, address)) = hits.into_iter().next() else {
            return Ok(None);
        };
        let document: TantivyDocument = searcher
            .doc(address)
            .map_err(|e| CoreError::Index(format!("read document failed: {e}")))?;
        Ok(Some(self.message_from_document(&document)))
    }

    /// All chat ids present in the index.
    pub fn chat_ids(&self) -> Result<BTreeSet<i64>> {
        let searcher = self.reader.searcher();
        let addresses = searcher
            .search(&AllQuery, &DocSetCollector)
            .map_err(|e| CoreError::Index(format!("list chats failed: {e}")))?;
        let mut chats = BTreeSet::new();
        for address in addresses {
            let document: TantivyDocument = searcher
                .doc(address)
                .map_err(|e| CoreError::Index(format!("read document failed: {e}")))?;
            if let Some(value) = document.get_first(self.fields.chat_id) {
                if let Some(id) = value.as_i64() {
                    chats.insert(id);
                }
            }
        }
        Ok(chats)
    }

    pub fn random_message(&self) -> Result<Option<IndexedMessage>> {
        let searcher = self.reader.searcher();
        let addresses = searcher
            .search(&AllQuery, &DocSetCollector)
            .map_err(|e| CoreError::Index(format!("random message failed: {e}")))?;
        if addresses.is_empty() {
            return Ok(None);
        }
        let index = rand::thread_rng().gen_range(0..addresses.len());
        let address = addresses
            .into_iter()
            .nth(index)
            .ok_or_else(|| CoreError::Index("random message address missing".into()))?;
        let document: TantivyDocument = searcher
            .doc(address)
            .map_err(|e| CoreError::Index(format!("read document failed: {e}")))?;
        Ok(Some(self.message_from_document(&document)))
    }

    /// Newest indexed message of a chat.
    pub fn newest_message(&self, chat_id: i64) -> Result<Option<IndexedMessage>> {
        let mut request = SearchRequest::new("*");
        request.chats = Some(vec![chat_id]);
        request.page_len = 1;
        let result = self.search(&request)?;
        Ok(result.hits.into_iter().next().map(|hit| hit.message))
    }

    /// Run a search request.
    pub fn search(&self, request: &SearchRequest) -> Result<SearchResult> {
        let page = request.page.max(1);
        let page_len = request.page_len.max(1);
        let ast = parse_query(&request.query)
            .map_err(|e| CoreError::Invalid(format!("查询语法错误: {e}")))?;

        let exact = self.search_exact(&ast, request, page, page_len)?;

        if request.exact_only || !self.fuzzy.enabled || self.fuzzy.mode == FuzzyMode::Off {
            return Ok(exact);
        }
        if ast.is_all() {
            return Ok(exact);
        }
        let fuzzy_threshold = request
            .fuzzy_threshold
            .unwrap_or_else(|| self.fuzzy.effective_threshold())
            .clamp(0.0, 1.0);

        let should_fuzzy = match self.fuzzy.mode {
            FuzzyMode::Fallback => exact.total < self.fuzzy.fallback_below_results.max(1),
            FuzzyMode::Always => true,
            FuzzyMode::Off => false,
        };
        if !should_fuzzy {
            return Ok(exact);
        }

        let fallback_text = ast.fallback_text();
        if fallback_text.chars().count() < self.fuzzy.min_query_chars {
            return Ok(exact);
        }

        let fuzzy_hits = self.search_fuzzy(&fallback_text, request, fuzzy_threshold)?;
        if fuzzy_hits.is_empty() {
            return Ok(exact);
        }

        if self.fuzzy.mode == FuzzyMode::Always && exact.total > 0 {
            let mut merged: Vec<SearchHit> = Vec::new();
            let mut seen = BTreeSet::new();
            for hit in exact.hits.iter().cloned().chain(fuzzy_hits.iter().cloned()) {
                if seen.insert(hit.message.url.clone()) {
                    merged.push(hit);
                }
            }
            merged.sort_by_key(|hit| std::cmp::Reverse(hit.message.post_time));
            let total = merged.len();
            let start = (page - 1) * page_len;
            let hits = merged.into_iter().skip(start).take(page_len).collect();
            return Ok(SearchResult {
                hits,
                total,
                page,
                page_len,
                is_last_page: page * page_len >= total,
                fuzzy_used: true,
                fuzzy_threshold: Some(fuzzy_threshold),
            });
        }

        let total = fuzzy_hits.len();
        let start = (page - 1) * page_len;
        let hits = fuzzy_hits.into_iter().skip(start).take(page_len).collect();
        Ok(SearchResult {
            hits,
            total,
            page,
            page_len,
            is_last_page: page * page_len >= total,
            fuzzy_used: true,
            fuzzy_threshold: Some(fuzzy_threshold),
        })
    }

    fn search_exact(
        &self,
        ast: &QueryNode,
        request: &SearchRequest,
        page: usize,
        page_len: usize,
    ) -> Result<SearchResult> {
        let searcher = self.reader.searcher();
        let user_query = self.build_query(ast)?;
        let filtered = self.apply_filters(user_query, request)?;
        let offset = (page - 1) * page_len;
        let top_docs = TopDocs::with_limit(page_len)
            .and_offset(offset)
            .order_by_fast_field::<i64>(POST_TIME, Order::Desc);

        let (addresses, total) = searcher
            .search(&filtered, &(top_docs, Count))
            .map_err(|e| CoreError::Index(format!("search failed: {e}")))?;

        let mut snippet_generator =
            SnippetGenerator::create(&searcher, &*filtered, self.fields.content)
                .map_err(|e| CoreError::Index(format!("cannot create snippet generator: {e}")))?;
        snippet_generator.set_max_num_chars(SNIPPET_CHARS);

        let mut hits = Vec::with_capacity(addresses.len());
        for (_, address) in addresses {
            let document: TantivyDocument = searcher
                .doc(address)
                .map_err(|e| CoreError::Index(format!("read document failed: {e}")))?;
            let message = self.message_from_document(&document);
            let highlighted = if !message.content.is_empty() {
                let snippet = snippet_generator.snippet_from_doc(&document).to_html();
                if snippet.trim().is_empty() {
                    escape_html(&brief_content(&message.content, 150))
                } else {
                    snippet
                }
            } else if message.filename.is_some() {
                fallback_filename_snippet(&message)
            } else {
                String::new()
            };
            hits.push(SearchHit {
                message,
                highlighted,
                score: 0.0,
            });
        }

        Ok(SearchResult {
            hits,
            total,
            page,
            page_len,
            is_last_page: page * page_len >= total,
            fuzzy_used: false,
            fuzzy_threshold: None,
        })
    }

    fn search_fuzzy(
        &self,
        query_text: &str,
        request: &SearchRequest,
        threshold: f32,
    ) -> Result<Vec<SearchHit>> {
        let searcher = self.reader.searcher();
        let candidate_query = self.build_fuzzy_candidates(query_text)?;
        let Some(candidate_query) = candidate_query else {
            return Ok(Vec::new());
        };
        let filtered = self.apply_filters(Box::new(candidate_query), request)?;
        let limit = self.fuzzy.candidate_limit.max(1);
        let top_docs = TopDocs::with_limit(limit).order_by_score();
        let candidates = searcher
            .search(&filtered, &top_docs)
            .map_err(|e| CoreError::Index(format!("fuzzy search failed: {e}")))?;

        let mut scored: Vec<(f32, i64, IndexedMessage)> = Vec::new();
        for (score, address) in candidates {
            let document: TantivyDocument = searcher
                .doc(address)
                .map_err(|e| CoreError::Index(format!("read document failed: {e}")))?;
            let message = self.message_from_document(&document);
            let similarity =
                fuzzy::similarity(query_text, &message.content, message.filename.as_deref());
            if similarity + f32::EPSILON < threshold {
                continue;
            }
            let _ = score;
            scored.push((similarity, message.post_time, message));
        }
        scored.sort_by(|a, b| {
            b.0.partial_cmp(&a.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| b.1.cmp(&a.1))
                .then_with(|| a.2.url.cmp(&b.2.url))
        });
        scored.dedup_by(|a, b| a.2.url == b.2.url);

        Ok(scored
            .into_iter()
            .map(|(score, _, message)| {
                let highlighted = fuzzy_highlight(
                    &message.content,
                    query_text,
                    message.filename.as_deref(),
                    150,
                );
                SearchHit {
                    message,
                    highlighted,
                    score,
                }
            })
            .collect())
    }

    fn message_from_document(&self, document: &TantivyDocument) -> IndexedMessage {
        let content = text_field(document, self.fields.content);
        let filename = text_field(document, self.fields.filename);
        let sender = text_field(document, self.fields.sender);
        let url = text_field(document, self.fields.url);
        let chat_id = document
            .get_first(self.fields.chat_id)
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let post_time = document
            .get_first(self.fields.post_time)
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        IndexedMessage {
            url,
            chat_id,
            content,
            post_time,
            sender,
            filename: if filename.is_empty() {
                None
            } else {
                Some(filename)
            },
        }
    }

    fn build_query(&self, node: &QueryNode) -> Result<Box<dyn Query>> {
        Ok(match node {
            QueryNode::All => Box::new(AllQuery),
            QueryNode::Term(term) => self.term_query(term),
            QueryNode::Phrase(phrase) => self.phrase_query(phrase),
            QueryNode::Wildcard(pattern) => self.wildcard_query(pattern),
            QueryNode::Not(inner) => {
                let inner = self.build_query(inner)?;
                Box::new(BooleanQuery::new(vec![
                    (Occur::Must, Box::new(AllQuery) as Box<dyn Query>),
                    (Occur::MustNot, inner),
                ]))
            }
            QueryNode::And(children) => {
                let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();
                let mut only_negative = true;
                for child in children {
                    match child {
                        QueryNode::Not(inner) => {
                            clauses.push((Occur::MustNot, self.build_query(inner)?));
                        }
                        _ => {
                            only_negative = false;
                            clauses.push((Occur::Must, self.build_query(child)?));
                        }
                    }
                }
                if only_negative {
                    clauses.push((Occur::Must, Box::new(AllQuery)));
                }
                if clauses.is_empty() {
                    Box::new(AllQuery)
                } else if clauses.len() == 1 && clauses[0].0 == Occur::Must {
                    clauses.pop().unwrap().1
                } else {
                    Box::new(BooleanQuery::new(clauses))
                }
            }
            QueryNode::Or(children) => {
                let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();
                for child in children {
                    let query = self.build_query(child)?;
                    clauses.push((Occur::Should, query));
                }
                if clauses.is_empty() {
                    Box::new(AllQuery)
                } else {
                    Box::new(BooleanQuery::new(clauses))
                }
            }
        })
    }

    fn term_query(&self, term: &str) -> Box<dyn Query> {
        let tokens = fuzzy::query_tokens(term);
        if tokens.is_empty() {
            return Box::new(EmptyQuery);
        }
        let mut per_token: Vec<(Occur, Box<dyn Query>)> = Vec::new();
        for token in tokens {
            let mut fields: Vec<(Occur, Box<dyn Query>)> = Vec::new();
            for field in [self.fields.content, self.fields.filename] {
                fields.push((
                    Occur::Should,
                    Box::new(TermQuery::new(
                        Term::from_field_text(field, &token),
                        IndexRecordOption::WithFreqs,
                    )),
                ));
            }
            per_token.push((Occur::Must, Box::new(BooleanQuery::new(fields))));
        }
        if per_token.len() == 1 {
            per_token.pop().unwrap().1
        } else {
            Box::new(BooleanQuery::new(per_token))
        }
    }

    fn phrase_query(&self, phrase: &str) -> Box<dyn Query> {
        let tokens = fuzzy::query_tokens(phrase);
        if tokens.is_empty() {
            return Box::new(EmptyQuery);
        }
        if tokens.len() == 1 {
            return self.term_query(phrase);
        }
        let mut alternatives: Vec<(Occur, Box<dyn Query>)> = Vec::new();
        for field in [self.fields.content, self.fields.filename] {
            let terms: Vec<Term> = tokens
                .iter()
                .map(|token| Term::from_field_text(field, token))
                .collect();
            alternatives.push((Occur::Should, Box::new(PhraseQuery::new(terms))));
        }
        Box::new(BooleanQuery::new(alternatives))
    }

    fn wildcard_query(&self, pattern: &str) -> Box<dyn Query> {
        let regex = wildcard_to_regex(pattern);
        let mut alternatives: Vec<(Occur, Box<dyn Query>)> = Vec::new();
        for field in [self.fields.content, self.fields.filename] {
            match RegexQuery::from_pattern(&regex, field) {
                Ok(query) => alternatives.push((Occur::Should, Box::new(query))),
                Err(e) => {
                    warn!(pattern, "invalid wildcard query: {e}");
                }
            }
        }
        if alternatives.is_empty() {
            Box::new(EmptyQuery)
        } else {
            Box::new(BooleanQuery::new(alternatives))
        }
    }

    fn build_fuzzy_candidates(&self, query_text: &str) -> Result<Option<Box<dyn Query>>> {
        let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();
        for gram in bigrams(query_text).into_iter().take(64) {
            clauses.push((
                Occur::Should,
                Box::new(TermQuery::new(
                    Term::from_field_text(self.fields.content_grams, &gram),
                    IndexRecordOption::WithFreqs,
                )),
            ));
            clauses.push((
                Occur::Should,
                Box::new(TermQuery::new(
                    Term::from_field_text(self.fields.filename_grams, &gram),
                    IndexRecordOption::WithFreqs,
                )),
            ));
        }
        if self.fuzzy.max_edit_distance > 0 {
            for token in fuzzy::query_tokens(query_text) {
                if token.chars().count() < 4 {
                    continue;
                }
                clauses.push((
                    Occur::Should,
                    Box::new(FuzzyTermQuery::new(
                        Term::from_field_text(self.fields.content, &token),
                        self.fuzzy.max_edit_distance,
                        true,
                    )),
                ));
            }
        }
        if clauses.is_empty() {
            return Ok(None);
        }
        Ok(Some(Box::new(BooleanQuery::new(clauses))))
    }

    fn apply_filters(
        &self,
        user_query: Box<dyn Query>,
        request: &SearchRequest,
    ) -> Result<Box<dyn Query>> {
        let mut clauses: Vec<(Occur, Box<dyn Query>)> = vec![(Occur::Must, user_query)];
        if let Some(chats) = &request.chats {
            let chats: Vec<i64> = chats.iter().copied().filter(|id| *id != 0).collect();
            if !chats.is_empty() {
                let chat_clauses: Vec<(Occur, Box<dyn Query>)> = chats
                    .iter()
                    .map(|id| {
                        (
                            Occur::Should,
                            Box::new(TermQuery::new(
                                Term::from_field_i64(self.fields.chat_id, *id),
                                IndexRecordOption::Basic,
                            )) as Box<dyn Query>,
                        )
                    })
                    .collect();
                clauses.push((Occur::Must, Box::new(BooleanQuery::new(chat_clauses))));
            }
        }
        match request.file_filter {
            FileFilter::All => {}
            FileFilter::TextOnly => clauses.push((
                Occur::Must,
                Box::new(TermQuery::new(
                    Term::from_field_u64(self.fields.has_file, 0),
                    IndexRecordOption::Basic,
                )),
            )),
            FileFilter::FileOnly => clauses.push((
                Occur::Must,
                Box::new(TermQuery::new(
                    Term::from_field_u64(self.fields.has_file, 1),
                    IndexRecordOption::Basic,
                )),
            )),
        }
        if clauses.len() == 1 {
            Ok(clauses.pop().unwrap().1)
        } else {
            Ok(Box::new(BooleanQuery::new(clauses)))
        }
    }
}

fn text_field(document: &TantivyDocument, field: Field) -> String {
    document
        .get_first(field)
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_string()
}

fn build_schema(_fuzzy: &FuzzyConfig) -> Schema {
    let mut builder = Schema::builder();
    let jieba = TextOptions::default()
        .set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer("jieba")
                .set_index_option(IndexRecordOption::WithFreqsAndPositions),
        )
        .set_stored();
    let grams = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer("ngram")
            .set_index_option(IndexRecordOption::WithFreqs),
    );

    builder.add_text_field(URL, STRING | STORED);
    builder.add_i64_field(
        CHAT_ID,
        NumericOptions::default()
            .set_indexed()
            .set_stored()
            .set_fast(),
    );
    builder.add_text_field(CONTENT, jieba.clone());
    builder.add_text_field(FILENAME, jieba);
    builder.add_i64_field(
        POST_TIME,
        NumericOptions::default()
            .set_indexed()
            .set_stored()
            .set_fast(),
    );
    builder.add_text_field(SENDER, TextOptions::default().set_stored());
    builder.add_u64_field(
        HAS_FILE,
        NumericOptions::default()
            .set_indexed()
            .set_stored()
            .set_fast(),
    );
    builder.add_text_field(CONTENT_GRAMS, grams.clone());
    builder.add_text_field(FILENAME_GRAMS, grams);
    builder.build()
}

fn resolve_fields(schema: &Schema) -> Result<Fields> {
    let field = |name: &str| {
        schema.get_field(name).map_err(|_| {
            CoreError::Index(format!(
                "index schema is missing field `{name}`; it was created by an older version. \
                 Run with --clear (or delete the index directory) to rebuild."
            ))
        })
    };
    Ok(Fields {
        url: field(URL)?,
        chat_id: field(CHAT_ID)?,
        content: field(CONTENT)?,
        filename: field(FILENAME)?,
        post_time: field(POST_TIME)?,
        sender: field(SENDER)?,
        has_file: field(HAS_FILE)?,
        content_grams: field(CONTENT_GRAMS)?,
        filename_grams: field(FILENAME_GRAMS)?,
    })
}

/// Convert a wildcard term to an anchored regular expression.
fn wildcard_to_regex(pattern: &str) -> String {
    let mut out = String::from("^");
    for ch in pattern.chars() {
        match ch {
            '*' => out.push_str(".*"),
            '?' => out.push('.'),
            c => out.push_str(&regex::escape(&c.to_string())),
        }
    }
    out.push('$');
    out
}

fn fallback_filename_snippet(message: &IndexedMessage) -> String {
    let filename = message.filename.clone().unwrap_or_default();
    format!("📎 {}", escape_html(&brief_content(&filename, 80)))
}

/// Build a highlighted snippet for fuzzy results (exact-term snippets do not
/// apply because the matched text may differ from the query).
fn fuzzy_highlight(content: &str, query: &str, filename: Option<&str>, max_chars: usize) -> String {
    let mut tokens: Vec<String> = fuzzy::segment(query);
    tokens.sort_by_key(|t| std::cmp::Reverse(t.chars().count()));
    tokens.truncate(16);
    tokens.retain(|t| !t.is_empty());

    if content.is_empty() {
        if let Some(filename) = filename {
            return escape_html(&brief_content(filename, max_chars));
        }
        return String::new();
    }

    let regex: Option<regex::Regex> = if tokens.is_empty() {
        None
    } else {
        let alternatives = tokens
            .iter()
            .map(|t| regex::escape(t))
            .collect::<Vec<_>>()
            .join("|");
        RegexBuilder::new(&format!("({alternatives})"))
            .case_insensitive(true)
            .build()
            .ok()
    };
    let first_match = regex
        .as_ref()
        .and_then(|re| re.find(content))
        .map(|m| content[..m.start()].chars().count())
        .unwrap_or(0);
    let chars: Vec<char> = content.chars().collect();
    let start = first_match.saturating_sub(60);
    let end = (start + max_chars).min(chars.len());
    let mut window: String = chars[start..end].iter().collect();
    if start > 0 {
        window.insert(0, '…');
    }
    if end < chars.len() {
        window.push('…');
    }
    let escaped = escape_html(&window);
    if !tokens.is_empty() {
        // Rebuild the regex against escaped text; tokens are word-like so this
        // is safe, and prevents matching inside the inserted tags.
        let alternatives = tokens
            .iter()
            .map(|t| regex::escape(&escape_html(t)))
            .collect::<Vec<_>>()
            .join("|");
        if let Ok(pattern) = RegexBuilder::new(&format!("({alternatives})"))
            .case_insensitive(true)
            .build()
        {
            return pattern.replace_all(&escaped, "<b>$1</b>").into_owned();
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::FuzzyConfig;
    use tempfile::TempDir;

    fn temp_index() -> (TempDir, TextIndex) {
        // %TEMP% can be locked down on some Windows hosts; keep test data in
        // the crate's target directory instead.
        let base = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp");
        std::fs::create_dir_all(&base).unwrap();
        let temp = tempfile::Builder::new()
            .prefix("tantivy-test-")
            .tempdir_in(&base)
            .unwrap();
        let index =
            TextIndex::open(temp.path().join("index"), FuzzyConfig::default(), true).unwrap();
        (temp, index)
    }

    fn message(
        url: &str,
        chat: i64,
        content: &str,
        time: i64,
        file: Option<&str>,
    ) -> IndexedMessage {
        IndexedMessage {
            url: url.to_string(),
            chat_id: chat,
            content: content.to_string(),
            post_time: time,
            sender: "tester".to_string(),
            filename: file.map(ToString::to_string),
        }
    }

    fn seed(index: &mut TextIndex) {
        let messages = vec![
            message("https://t.me/c/1/1", 1, "机器学习入门教程", 100, None),
            message("https://t.me/c/1/2", 1, "深度学习与神经网络", 200, None),
            message(
                "https://t.me/c/2/3",
                2,
                "Rust programming language",
                300,
                None,
            ),
            message(
                "https://t.me/c/2/4",
                2,
                "季度报告附件",
                400,
                Some("2024年季度报告.pdf"),
            ),
            message("https://t.me/c/3/5", 3, "今天天气很好", 500, None),
        ];
        assert_eq!(index.upsert_messages(&messages).unwrap(), 5);
    }

    #[test]
    fn add_search_and_read_fields() {
        let (_temp, mut index) = temp_index();
        seed(&mut index);
        assert_eq!(index.total_documents(), 5);
        let result = index.search(&SearchRequest::new("机器学习")).unwrap();
        assert_eq!(result.total, 1);
        assert_eq!(result.hits[0].message.url, "https://t.me/c/1/1");
        assert!(result.hits[0].highlighted.contains("<b>"));
    }

    #[test]
    fn boolean_and_phrase_queries() {
        let (_temp, mut index) = temp_index();
        seed(&mut index);
        let r = index
            .search(&SearchRequest::new("机器学习 OR 深度学习"))
            .unwrap();
        assert_eq!(r.total, 2);
        let r = index
            .search(&SearchRequest::new("\"机器学习入门\""))
            .unwrap();
        assert_eq!(r.total, 1);
        let r = index.search(&SearchRequest::new("NOT 天气")).unwrap();
        assert_eq!(r.total, 4);
    }

    #[test]
    fn filter_by_chat_and_file() {
        let (_temp, mut index) = temp_index();
        seed(&mut index);
        let mut request = SearchRequest::new("报告");
        request.chats = Some(vec![2]);
        assert_eq!(index.search(&request).unwrap().total, 1);
        let mut request = SearchRequest::new("*");
        request.file_filter = FileFilter::FileOnly;
        assert_eq!(index.search(&request).unwrap().total, 1);
        let mut request = SearchRequest::new("*");
        request.file_filter = FileFilter::TextOnly;
        assert_eq!(index.search(&request).unwrap().total, 4);
    }

    #[test]
    fn sort_and_paginate() {
        let (_temp, mut index) = temp_index();
        seed(&mut index);
        let mut request = SearchRequest::new("*");
        request.page_len = 2;
        let page1 = index.search(&request).unwrap();
        assert_eq!(page1.hits.len(), 2);
        assert_eq!(page1.hits[0].message.post_time, 500);
        assert_eq!(page1.total, 5);
        assert!(!page1.is_last_page);
        request.page = 3;
        let page3 = index.search(&request).unwrap();
        assert_eq!(page3.hits.len(), 1);
        assert!(page3.is_last_page);
    }

    #[test]
    fn fuzzy_fallback_finds_typos_and_threshold_applies() {
        let (_temp, mut index) = temp_index();
        seed(&mut index);
        // Exact search has no hit for the typo; fuzzy fallback should find it.
        let r = index.search(&SearchRequest::new("机器学刁")).unwrap();
        assert!(r.total >= 1);
        assert!(r.fuzzy_used);

        let mut strict = SearchRequest::new("机器学刁");
        strict.fuzzy_threshold = Some(0.99);
        let strict_result = index.search(&strict).unwrap();
        let lenient = SearchRequest::new("机器学刁");
        let lenient_result = index.search(&lenient).unwrap();
        assert!(
            lenient_result.total >= strict_result.total,
            "lower threshold should not reduce results"
        );
    }

    #[test]
    fn exact_only_disables_fuzzy() {
        let (_temp, mut index) = temp_index();
        seed(&mut index);
        let mut request = SearchRequest::new("机器学刁");
        request.exact_only = true;
        let r = index.search(&request).unwrap();
        assert_eq!(r.total, 0);
        assert!(!r.fuzzy_used);
    }

    #[test]
    fn update_and_delete() {
        let (_temp, mut index) = temp_index();
        seed(&mut index);
        let mut updated = message("https://t.me/c/1/1", 1, "更新后的内容", 100, None);
        updated.sender = "editor".into();
        index.add_message(&updated).unwrap();
        assert_eq!(index.total_documents(), 5);
        let r = index.search(&SearchRequest::new("更新后的内容")).unwrap();
        assert_eq!(r.total, 1);
        index
            .delete_urls(&["https://t.me/c/1/2".to_string()])
            .unwrap();
        assert_eq!(index.total_documents(), 4);
        index.delete_chat(2).unwrap();
        assert_eq!(index.total_documents(), 2);
    }

    #[test]
    fn chat_ids_and_random() {
        let (_temp, mut index) = temp_index();
        seed(&mut index);
        let chats = index.chat_ids().unwrap();
        assert_eq!(chats.iter().copied().collect::<Vec<_>>(), vec![1, 2, 3]);
        assert_eq!(index.chat_count(1).unwrap(), 2);
        assert!(index.random_message().unwrap().is_some());
        assert_eq!(index.newest_message(1).unwrap().unwrap().post_time, 200);
    }

    #[test]
    fn wildcard_query() {
        let (_temp, mut index) = temp_index();
        seed(&mut index);
        let r = index.search(&SearchRequest::new("机器*")).unwrap();
        assert_eq!(r.total, 1);
        let r = index.search(&SearchRequest::new("prog?amming")).unwrap();
        assert_eq!(r.total, 1);
    }

    #[test]
    fn clear_and_empty() {
        let (_temp, mut index) = temp_index();
        seed(&mut index);
        index.delete_all().unwrap();
        assert!(index.is_empty());
        assert!(index.random_message().unwrap().is_none());
        let r = index.search(&SearchRequest::new("*")).unwrap();
        assert_eq!(r.total, 0);
    }
}
