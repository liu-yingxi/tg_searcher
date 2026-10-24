//! Boundary between the frontend logic and the Telegram indexing backend.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;

use tgsearcher_core::{
    ChatId, DownloadProgress, DownloadReport, ImageSearchMode, ImageSearchResult, IndexedMessage,
    MonitorReport, SearchRequest, SearchResult,
};

/// Progress callback invoked while downloading history.
pub type ProgressSink = Arc<dyn Fn(DownloadProgress) + Send + Sync>;

#[async_trait]
pub trait SearchBackend: Send + Sync {
    /// Backend instance id (the `backends[].id` from the config).
    fn id(&self) -> &str;

    async fn search(&self, request: SearchRequest) -> anyhow::Result<SearchResult>;
    async fn random_message(&self) -> anyhow::Result<Option<IndexedMessage>>;

    /// Human-readable HTML status (index size, monitored chats, newest message).
    async fn status(&self, length_limit: usize) -> String;

    async fn monitored_chats(&self) -> Vec<ChatId>;
    async fn excluded_chats(&self) -> Vec<ChatId>;
    async fn chat_title(&self, chat_id: ChatId) -> Option<String>;

    /// Resolve a numeric id, username or t.me link to a share id.
    async fn resolve_chat_id(&self, input: &str) -> anyhow::Result<ChatId>;
    async fn find_chat_ids(&self, query: &str) -> Vec<ChatId>;

    async fn download_history(
        &self,
        chat: &str,
        min_id: i64,
        max_id: i64,
        progress: ProgressSink,
    ) -> anyhow::Result<DownloadReport>;

    async fn add_monitoring(&self, chat_ids: &[ChatId]) -> anyhow::Result<MonitorReport>;
    /// `None` clears everything.
    async fn clear(&self, chat_ids: Option<Vec<ChatId>>) -> anyhow::Result<usize>;
    async fn refresh_chat_names(&self) -> anyhow::Result<()>;

    async fn search_image(
        &self,
        image: &Path,
        mode: ImageSearchMode,
        chats: Option<&[ChatId]>,
    ) -> anyhow::Result<ImageSearchResult>;

    /// Permanently exclude a chat (used for the frontend bot's own account).
    async fn exclude_chat(&self, chat_id: ChatId);
}
