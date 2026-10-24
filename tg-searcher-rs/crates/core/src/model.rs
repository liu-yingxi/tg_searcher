//! Shared data structures used by the indexer, the bot frontends and tests.

use std::path::PathBuf;

use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

pub type ChatId = i64;
pub type UnixTime = i64;

/// A single indexed Telegram message.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexedMessage {
    pub url: String,
    pub chat_id: ChatId,
    pub content: String,
    pub post_time: UnixTime,
    pub sender: String,
    pub filename: Option<String>,
}

impl IndexedMessage {
    pub fn has_file(&self) -> bool {
        self.filename.is_some()
    }

    pub fn post_datetime(&self) -> Option<DateTime<Utc>> {
        Utc.timestamp_opt(self.post_time, 0).single()
    }
}

/// Result of one text search.
#[derive(Clone, Debug, Default)]
pub struct SearchResult {
    pub hits: Vec<SearchHit>,
    pub total: usize,
    pub page: usize,
    pub page_len: usize,
    pub is_last_page: bool,
    /// True when the result was produced by the fuzzy fallback.
    pub fuzzy_used: bool,
    /// The fuzzy similarity threshold that was applied, if any.
    pub fuzzy_threshold: Option<f32>,
}

impl SearchResult {
    pub fn total_pages(&self) -> usize {
        if self.page_len == 0 || self.total == 0 {
            1
        } else {
            self.total.div_ceil(self.page_len)
        }
    }
}

#[derive(Clone, Debug)]
pub struct SearchHit {
    pub message: IndexedMessage,
    /// HTML snippet with `<b>` around matched terms. Already HTML-escaped.
    pub highlighted: String,
    /// Similarity for fuzzy hits, BM25-like score otherwise.
    pub score: f32,
}

/// Which messages to include based on their attachment state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileFilter {
    #[default]
    All,
    TextOnly,
    FileOnly,
}

impl FileFilter {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::TextOnly => "text_only",
            Self::FileOnly => "file_only",
        }
    }

    pub fn label_zh(self) -> &'static str {
        match self {
            Self::All => "全部",
            Self::TextOnly => "纯文本",
            Self::FileOnly => "仅文件",
        }
    }

    pub fn parse(value: &str) -> Self {
        match value {
            "text_only" => Self::TextOnly,
            "file_only" => Self::FileOnly,
            _ => Self::All,
        }
    }
}

/// Parameters for a text search.
#[derive(Clone, Debug)]
pub struct SearchRequest {
    pub query: String,
    pub chats: Option<Vec<ChatId>>,
    pub page: usize,
    pub page_len: usize,
    pub file_filter: FileFilter,
    /// Per-query override of the configured fuzzy threshold.
    pub fuzzy_threshold: Option<f32>,
    /// Disable the fuzzy fallback for this query.
    pub exact_only: bool,
}

impl SearchRequest {
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            chats: None,
            page: 1,
            page_len: 10,
            file_filter: FileFilter::All,
            fuzzy_threshold: None,
            exact_only: false,
        }
    }
}

/// Public view of an indexed image (descriptors are deliberately omitted).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ImageMessage {
    pub url: String,
    pub chat_id: ChatId,
    pub post_time: UnixTime,
    pub sender: String,
    pub content: String,
    pub filename: Option<String>,
    pub width: u32,
    pub height: u32,
    pub thumbnail: Option<PathBuf>,
}

/// Internal image index record. `descriptors` is the raw ORB descriptor matrix
/// (32 bytes per row); it is not exposed through the public API.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ImageRecord {
    pub message: ImageMessage,
    pub thumbnail_file: Option<String>,
    pub phash: u64,
    pub keypoints: u32,
    pub descriptor_rows: u32,
    pub descriptors: Vec<u8>,
}

impl ImageRecord {
    pub fn view(&self) -> ImageMessage {
        self.message.clone()
    }
}

#[derive(Clone, Debug)]
pub struct ImageMatch {
    pub message: ImageMessage,
    pub similarity: f32,
    pub good_matches: usize,
    pub phash_distance: u32,
    pub thumbnail: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImageSearchMode {
    /// Return the text/caption of visually matching messages.
    ImageToText,
    /// Return the matching messages together with their thumbnails.
    ImageToImage,
}

impl ImageSearchMode {
    pub fn command(self) -> &'static str {
        match self {
            Self::ImageToText => "/img2text",
            Self::ImageToImage => "/img2img",
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ImageSearchResult {
    pub matches: Vec<ImageMatch>,
    pub total_candidates: usize,
    pub min_similarity: f32,
    pub scanned: usize,
}

/// A chat known to the backend.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChatInfo {
    pub id: ChatId,
    pub title: String,
}

#[derive(Clone, Debug, Default)]
pub struct DownloadReport {
    pub chat_id: Option<ChatId>,
    pub processed: usize,
    pub indexed: usize,
    pub images_indexed: usize,
    pub elapsed_ms: u128,
}

#[derive(Clone, Debug, Default)]
pub struct MonitorReport {
    pub added: Vec<ChatId>,
    pub failed: Vec<(ChatId, String)>,
}

#[derive(Clone, Debug)]
pub struct DownloadProgress {
    pub chat_id: ChatId,
    pub current_message_id: i64,
    pub downloaded: usize,
    pub processed: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_filter_roundtrip() {
        for f in [FileFilter::All, FileFilter::TextOnly, FileFilter::FileOnly] {
            assert_eq!(FileFilter::parse(f.as_str()), f);
        }
    }

    #[test]
    fn total_pages_never_zero() {
        let r = SearchResult {
            total: 0,
            page_len: 10,
            ..Default::default()
        };
        assert_eq!(r.total_pages(), 1);
        let r = SearchResult {
            total: 21,
            page_len: 10,
            ..Default::default()
        };
        assert_eq!(r.total_pages(), 3);
    }
}
