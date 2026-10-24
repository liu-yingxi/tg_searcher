//! Core, Telegram-independent functionality: configuration, Chinese-aware text
//! indexing with configurable fuzzy matching, and OpenCV image search.

pub mod config;
pub mod fuzzy;
pub mod image;
pub mod index;
pub mod model;
pub mod query;
pub mod text;
pub mod tokenizer;

pub use config::{Config, ConfigError, FuzzyConfig, FuzzyMode, ImageConfig, ProxyConfig};
pub use image::{image_caption, image_message, ImageDescriptor, ImageStore};
pub use index::TextIndex;
pub use model::{
    ChatId, ChatInfo, DownloadProgress, DownloadReport, FileFilter, ImageMatch, ImageMessage,
    ImageRecord, ImageSearchMode, ImageSearchResult, IndexedMessage, MonitorReport, SearchHit,
    SearchRequest, SearchResult,
};
pub use query::{parse_query, QueryError, QueryNode};
pub use text::{brief_content, escape_html, normalize_content, share_id_from_peer_id};

/// Errors surfaced by the core layer.
#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    #[error("index error: {0}")]
    Index(String),
    #[error("image index error: {0}")]
    Image(String),
    #[error("invalid argument: {0}")]
    Invalid(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    OpenCv(#[from] opencv::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, CoreError>;
