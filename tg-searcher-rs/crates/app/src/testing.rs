//! In-memory Telegram + backend doubles used by regression and system tests.
//!
//! They are compiled into the normal library so integration tests (and anyone
//! experimenting locally) can drive the full frontend without network access.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use tgsearcher_core::{
    escape_html, image_message, ChatId, DownloadProgress, DownloadReport, FuzzyConfig, ImageConfig,
    ImageMatch, ImageMessage, ImageSearchMode, ImageSearchResult, ImageStore, IndexedMessage,
    MonitorReport, SearchRequest, SearchResult, TextIndex,
};

use crate::backend::{ProgressSink, SearchBackend};
use crate::cache::MemoryCache;
use crate::frontend::{Frontend, FrontendSettings};
use crate::messenger::{
    ButtonDef, CallbackEvent, IncomingMessage, IncomingUpdate, InlineKeyboard, Messenger,
    ReplyContext, SentMessage,
};
use crate::render::truncate_telegram;

// ----------------------------------------------------------------------
// Fake Messenger
// ----------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub enum RecordedCall {
    Text {
        message_id: i64,
        chat_id: ChatId,
        text: String,
        markup: Option<InlineKeyboard>,
        reply_to: Option<i64>,
    },
    Edit {
        chat_id: ChatId,
        message_id: i64,
        text: String,
        markup: Option<InlineKeyboard>,
    },
    Callback {
        callback_id: String,
        text: Option<String>,
        alert: bool,
    },
    Photo {
        chat_id: ChatId,
        path: PathBuf,
        caption: String,
    },
}

#[derive(Default)]
pub struct FakeMessenger {
    calls: Mutex<Vec<RecordedCall>>,
    next_message_id: AtomicI64,
}

impl FakeMessenger {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::new(Vec::new()),
            next_message_id: AtomicI64::new(5000),
        })
    }

    pub fn calls(&self) -> Vec<RecordedCall> {
        self.calls.lock().expect("messenger mutex poisoned").clone()
    }

    pub fn clear(&self) {
        self.calls.lock().expect("messenger mutex poisoned").clear();
    }

    pub fn texts(&self) -> Vec<String> {
        self.calls()
            .into_iter()
            .filter_map(|call| match call {
                RecordedCall::Text { text, .. } | RecordedCall::Edit { text, .. } => Some(text),
                _ => None,
            })
            .collect()
    }

    pub fn last_text(&self) -> Option<String> {
        self.calls().into_iter().rev().find_map(|call| match call {
            RecordedCall::Text { text, .. } | RecordedCall::Edit { text, .. } => Some(text),
            _ => None,
        })
    }

    pub fn photos(&self) -> Vec<RecordedCall> {
        self.calls()
            .into_iter()
            .filter(|call| matches!(call, RecordedCall::Photo { .. }))
            .collect()
    }

    pub fn last_sent_markup(&self) -> Option<InlineKeyboard> {
        self.calls().into_iter().rev().find_map(|call| match call {
            RecordedCall::Text { markup, .. } => markup,
            _ => None,
        })
    }

    pub fn sent_text_count(&self) -> usize {
        self.calls()
            .iter()
            .filter(|call| matches!(call, RecordedCall::Text { .. }))
            .count()
    }
}

#[async_trait]
impl Messenger for FakeMessenger {
    async fn send_text(
        &self,
        chat_id: ChatId,
        text: &str,
        markup: Option<InlineKeyboard>,
        reply_to: Option<i64>,
    ) -> Result<SentMessage> {
        let message_id = self.next_message_id.fetch_add(1, Ordering::SeqCst);
        self.calls
            .lock()
            .expect("messenger mutex poisoned")
            .push(RecordedCall::Text {
                message_id,
                chat_id,
                text: text.to_string(),
                markup,
                reply_to,
            });
        Ok(SentMessage {
            chat_id,
            message_id,
        })
    }

    async fn edit_text(
        &self,
        chat_id: ChatId,
        message_id: i64,
        text: &str,
        markup: Option<InlineKeyboard>,
    ) -> Result<()> {
        self.calls
            .lock()
            .expect("messenger mutex poisoned")
            .push(RecordedCall::Edit {
                chat_id,
                message_id,
                text: text.to_string(),
                markup,
            });
        Ok(())
    }

    async fn answer_callback(
        &self,
        callback_id: &str,
        text: Option<&str>,
        alert: bool,
    ) -> Result<()> {
        self.calls
            .lock()
            .expect("messenger mutex poisoned")
            .push(RecordedCall::Callback {
                callback_id: callback_id.to_string(),
                text: text.map(ToString::to_string),
                alert,
            });
        Ok(())
    }

    async fn send_photo(
        &self,
        chat_id: ChatId,
        path: &Path,
        caption: &str,
        _markup: Option<InlineKeyboard>,
    ) -> Result<SentMessage> {
        let message_id = self.next_message_id.fetch_add(1, Ordering::SeqCst);
        self.calls
            .lock()
            .expect("messenger mutex poisoned")
            .push(RecordedCall::Photo {
                chat_id,
                path: path.to_path_buf(),
                caption: caption.to_string(),
            });
        Ok(SentMessage {
            chat_id,
            message_id,
        })
    }
}

// ----------------------------------------------------------------------
// Fake backend
// ----------------------------------------------------------------------

struct FakeState {
    index: TextIndex,
    images: ImageStore,
    chats: HashMap<ChatId, String>,
    monitored: BTreeSet<ChatId>,
    excluded: BTreeSet<ChatId>,
}

pub struct FakeBackend {
    id: String,
    state: Mutex<FakeState>,
}

impl FakeBackend {
    pub fn new(base: impl AsRef<Path>) -> Result<Arc<Self>> {
        let base = base.as_ref();
        let index = TextIndex::open(base.join("index"), FuzzyConfig::default(), true)
            .context("create fake text index")?;
        let images = ImageStore::open(base.join("images"), ImageConfig::default())
            .context("create fake image index")?;
        Ok(Arc::new(Self {
            id: "fake-backend".into(),
            state: Mutex::new(FakeState {
                index,
                images,
                chats: HashMap::new(),
                monitored: BTreeSet::new(),
                excluded: BTreeSet::new(),
            }),
        }))
    }

    pub fn add_chat(&self, id: ChatId, title: impl Into<String>) {
        self.state
            .lock()
            .expect("backend mutex poisoned")
            .chats
            .insert(id, title.into());
    }

    pub fn add_message(&self, message: IndexedMessage) -> Result<()> {
        let mut state = self.state.lock().expect("backend mutex poisoned");
        state.index.add_message(&message)?;
        state.monitored.insert(message.chat_id);
        Ok(())
    }

    pub fn add_image(&self, path: &Path, message: ImageMessage) -> Result<()> {
        let mut state = self.state.lock().expect("backend mutex poisoned");
        state.images.add_image(path, message.clone())?;
        state.monitored.insert(message.chat_id);
        Ok(())
    }

    pub fn monitored(&self) -> Vec<ChatId> {
        self.state
            .lock()
            .expect("backend mutex poisoned")
            .monitored
            .iter()
            .copied()
            .collect()
    }

    pub fn chat_ids(&self) -> Vec<ChatId> {
        self.state
            .lock()
            .expect("backend mutex poisoned")
            .chats
            .keys()
            .copied()
            .collect()
    }

    pub fn image_count(&self) -> usize {
        self.state
            .lock()
            .expect("backend mutex poisoned")
            .images
            .len()
    }

    fn chat_title_sync(state: &FakeState, chat_id: ChatId) -> Option<String> {
        state.chats.get(&chat_id).cloned()
    }
}

#[async_trait]
impl SearchBackend for FakeBackend {
    fn id(&self) -> &str {
        &self.id
    }

    async fn search(&self, request: SearchRequest) -> Result<SearchResult> {
        let state = self.state.lock().expect("backend mutex poisoned");
        Ok(state.index.search(&request)?)
    }

    async fn random_message(&self) -> Result<Option<IndexedMessage>> {
        let state = self.state.lock().expect("backend mutex poisoned");
        Ok(state.index.random_message()?)
    }

    async fn status(&self, length_limit: usize) -> String {
        let state = self.state.lock().expect("backend mutex poisoned");
        let total = state.index.total_documents();
        let mut html = format!(
            "后端 \"{}\" 总消息: <b>{total}</b>\n\n",
            escape_html(&self.id)
        );
        if !state.excluded.is_empty() {
            html.push_str(&format!("{} 个对话被禁止索引:\n", state.excluded.len()));
            for id in &state.excluded {
                let title =
                    Self::chat_title_sync(&state, *id).unwrap_or_else(|| format!("对话 {id}"));
                html.push_str(&format!(
                    "- <a href=\"https://t.me/c/{id}/1\">{}</a> (`{id}`)\n",
                    escape_html(&title)
                ));
            }
            html.push('\n');
        }
        let monitored: Vec<ChatId> = state
            .monitored
            .iter()
            .copied()
            .filter(|id| !state.excluded.contains(id))
            .collect();
        html.push_str(&format!(
            "总计 {} 个对话被加入了索引 (且未被排除):\n",
            monitored.len()
        ));
        for id in &monitored {
            let title =
                Self::chat_title_sync(&state, *id).unwrap_or_else(|| format!("未知对话 ({id})"));
            let count = state.index.chat_count(*id).unwrap_or(0);
            html.push_str(&format!(
                "- <a href=\"https://t.me/c/{id}/1\">{}</a> (`{id}`) 共 {count} 条消息\n",
                escape_html(&title)
            ));
        }
        if !state.images.is_empty() {
            html.push_str(&format!(
                "\n图片索引: <b>{}</b> 张 (阈值 {:.0}%)",
                state.images.len(),
                state.images.config().effective_min_similarity() * 100.0
            ));
        }
        truncate_telegram(&html, length_limit.max(200))
    }

    async fn monitored_chats(&self) -> Vec<ChatId> {
        self.monitored()
    }

    async fn excluded_chats(&self) -> Vec<ChatId> {
        self.state
            .lock()
            .expect("backend mutex poisoned")
            .excluded
            .iter()
            .copied()
            .collect()
    }

    async fn chat_title(&self, chat_id: ChatId) -> Option<String> {
        let state = self.state.lock().expect("backend mutex poisoned");
        Self::chat_title_sync(&state, chat_id)
    }

    async fn resolve_chat_id(&self, input: &str) -> Result<ChatId> {
        let input = input.trim().trim_start_matches('@');
        if let Ok(id) = input.parse::<i64>() {
            return Ok(id);
        }
        let state = self.state.lock().expect("backend mutex poisoned");
        let lowered = input.to_lowercase();
        if let Some((id, _)) = state
            .chats
            .iter()
            .find(|(_, title)| title.to_lowercase() == lowered)
        {
            return Ok(*id);
        }
        if let Some((id, _)) = state
            .chats
            .iter()
            .find(|(_, title)| title.to_lowercase().contains(&lowered))
        {
            return Ok(*id);
        }
        bail!("找不到对话: {input}")
    }

    async fn find_chat_ids(&self, query: &str) -> Vec<ChatId> {
        let state = self.state.lock().expect("backend mutex poisoned");
        let query = query.to_lowercase();
        let mut ids: Vec<ChatId> = state
            .chats
            .iter()
            .filter(|(id, title)| {
                title.to_lowercase().contains(&query) || id.to_string().contains(&query)
            })
            .map(|(id, _)| *id)
            .collect();
        ids.sort_unstable();
        ids
    }

    async fn download_history(
        &self,
        chat: &str,
        min_id: i64,
        max_id: i64,
        progress: ProgressSink,
    ) -> Result<DownloadReport> {
        let started = std::time::Instant::now();
        let chat_id = self.resolve_chat_id(chat).await?;
        let title = {
            let state = self.state.lock().expect("backend mutex poisoned");
            Self::chat_title_sync(&state, chat_id).unwrap_or_else(|| format!("对话 {chat_id}"))
        };
        let mut state = self.state.lock().expect("backend mutex poisoned");
        let base_id = 1000 + chat_id.unsigned_abs() as i64 % 1000;
        let mut indexed = 0usize;
        let mut processed = 0usize;
        for offset in 0..3i64 {
            let message_id = base_id + offset;
            if message_id < min_id || (max_id != 0 && message_id > max_id) {
                continue;
            }
            processed += 1;
            let message = IndexedMessage {
                url: format!("https://t.me/c/{chat_id}/{message_id}"),
                chat_id,
                content: format!("{title} 的第 {} 条下载测试消息 关键词{chat_id}", offset + 1),
                post_time: 1_700_100_000 + offset,
                sender: "downloader".into(),
                filename: if offset == 2 {
                    Some(format!("{title}-报告.pdf"))
                } else {
                    None
                },
            };
            state.index.add_message(&message)?;
            state.monitored.insert(chat_id);
            indexed += 1;
            progress(DownloadProgress {
                chat_id,
                current_message_id: message_id,
                downloaded: indexed,
                processed,
            });
        }
        Ok(DownloadReport {
            chat_id: Some(chat_id),
            processed,
            indexed,
            images_indexed: 0,
            elapsed_ms: started.elapsed().as_millis(),
        })
    }

    async fn add_monitoring(&self, chat_ids: &[ChatId]) -> Result<MonitorReport> {
        let mut state = self.state.lock().expect("backend mutex poisoned");
        let mut report = MonitorReport::default();
        for chat_id in chat_ids {
            if state.excluded.contains(chat_id) {
                report.failed.push((*chat_id, "对话已被排除".into()));
            } else if state.monitored.contains(chat_id) {
                report.failed.push((*chat_id, "已在监控中".into()));
            } else {
                state.monitored.insert(*chat_id);
                report.added.push(*chat_id);
            }
        }
        Ok(report)
    }

    async fn clear(&self, chat_ids: Option<Vec<ChatId>>) -> Result<usize> {
        let mut state = self.state.lock().expect("backend mutex poisoned");
        match chat_ids {
            Some(ids) => {
                let mut removed = 0usize;
                for chat_id in ids {
                    removed += state.index.chat_count(chat_id).unwrap_or(0);
                    state.index.delete_chat(chat_id)?;
                    state.images.remove_chat(chat_id)?;
                    state.monitored.remove(&chat_id);
                }
                Ok(removed)
            }
            None => {
                let removed = state.index.total_documents();
                state.index.delete_all()?;
                state.images.clear()?;
                state.monitored.clear();
                Ok(removed)
            }
        }
    }

    async fn refresh_chat_names(&self) -> Result<()> {
        Ok(())
    }

    async fn search_image(
        &self,
        image: &Path,
        _mode: ImageSearchMode,
        chats: Option<&[ChatId]>,
    ) -> Result<ImageSearchResult> {
        let state = self.state.lock().expect("backend mutex poisoned");
        Ok(state.images.search_path(image, chats)?)
    }

    async fn exclude_chat(&self, chat_id: ChatId) {
        let mut state = self.state.lock().expect("backend mutex poisoned");
        state.excluded.insert(chat_id);
        state.monitored.remove(&chat_id);
    }
}

// ----------------------------------------------------------------------
// Test harness
// ----------------------------------------------------------------------

pub struct TestHarness {
    pub backend: Arc<FakeBackend>,
    pub messenger: Arc<FakeMessenger>,
    pub frontend: Arc<Frontend>,
    pub cache: Arc<MemoryCache>,
    pub temp: tempfile::TempDir,
}

impl TestHarness {
    pub async fn new() -> Result<Self> {
        let base = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp");
        std::fs::create_dir_all(&base)?;
        let temp = tempfile::Builder::new()
            .prefix("app-test-")
            .tempdir_in(&base)?;
        Self::with_dir(temp).await
    }

    pub async fn with_dir(temp: tempfile::TempDir) -> Result<Self> {
        Self::with_dir_and_settings(
            temp,
            FrontendSettings {
                id: "test-frontend".into(),
                bot_username: "test_bot".into(),
                bot_user_id: 999,
                admin_id: 42,
                page_len: 3,
                private_mode: false,
                private_whitelist: HashSet::new(),
                no_redis: false,
                send_photo_limit: 2,
            },
        )
        .await
    }

    pub async fn with_settings(settings: FrontendSettings) -> Result<Self> {
        let base = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp");
        std::fs::create_dir_all(&base)?;
        let temp = tempfile::Builder::new()
            .prefix("app-test-")
            .tempdir_in(&base)?;
        Self::with_dir_and_settings(temp, settings).await
    }

    pub async fn with_dir_and_settings(
        temp: tempfile::TempDir,
        settings: FrontendSettings,
    ) -> Result<Self> {
        let backend = FakeBackend::new(temp.path())?;
        let messenger = FakeMessenger::new();
        let cache = Arc::new(MemoryCache::new());
        let frontend = Arc::new(Frontend::new(
            settings,
            backend.clone(),
            messenger.clone(),
            cache.clone(),
        ));
        Ok(Self {
            backend,
            messenger,
            frontend,
            cache,
            temp,
        })
    }

    pub fn path(&self) -> &Path {
        self.temp.path()
    }

    /// Feed a plain text message from `sender` in `chat`.
    pub async fn user_message(&self, chat_id: ChatId, sender_id: ChatId, text: &str) {
        self.feed_message(IncomingMessage {
            chat_id,
            message_id: self.next_user_message_id(),
            sender_id,
            text: text.to_string(),
            is_private: true,
            mentioned_bot: false,
            reply: None,
            image_path: None,
        })
        .await;
    }

    pub async fn user_message_with_image(
        &self,
        chat_id: ChatId,
        sender_id: ChatId,
        caption: &str,
        image: PathBuf,
    ) {
        self.feed_message(IncomingMessage {
            chat_id,
            message_id: self.next_user_message_id(),
            sender_id,
            text: caption.to_string(),
            is_private: true,
            mentioned_bot: false,
            reply: None,
            image_path: Some(image),
        })
        .await;
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn reply_message_with_image(
        &self,
        chat_id: ChatId,
        sender_id: ChatId,
        text: &str,
        reply_message_id: i64,
        reply_sender_id: ChatId,
        reply_text: &str,
        image: PathBuf,
    ) {
        self.feed_message(IncomingMessage {
            chat_id,
            message_id: self.next_user_message_id(),
            sender_id,
            text: text.to_string(),
            is_private: true,
            mentioned_bot: false,
            reply: Some(ReplyContext {
                message_id: reply_message_id,
                sender_id: reply_sender_id,
                text: reply_text.to_string(),
                image_path: Some(image),
            }),
            image_path: None,
        })
        .await;
    }

    pub async fn feed_message(&self, message: IncomingMessage) {
        self.frontend
            .handle_update(IncomingUpdate::Message(message))
            .await;
    }

    pub async fn callback(&self, chat_id: ChatId, message_id: i64, sender_id: ChatId, data: &str) {
        self.frontend
            .handle_update(IncomingUpdate::Callback(CallbackEvent {
                callback_id: format!("cb-{message_id}-{data}"),
                chat_id,
                message_id,
                sender_id,
                data: data.to_string(),
            }))
            .await;
    }

    fn next_user_message_id(&self) -> i64 {
        static NEXT: AtomicI64 = AtomicI64::new(1);
        NEXT.fetch_add(1, Ordering::SeqCst)
    }

    /// Last message id sent by the bot (for replies/callbacks).
    pub fn last_sent_message_id(&self) -> Option<i64> {
        self.messenger
            .calls()
            .into_iter()
            .rev()
            .find_map(|call| match call {
                RecordedCall::Text { message_id, .. } => Some(message_id),
                _ => None,
            })
    }
}

/// Create the standard fixture: two chats and a handful of messages.
pub async fn seed_harness() -> Result<TestHarness> {
    let harness = TestHarness::new().await?;
    harness.backend.add_chat(1001, "中文测试群");
    harness.backend.add_chat(1002, "Rust Group");
    harness.backend.add_chat(1003, "文件群");
    for (index, (chat_id, content, file)) in [
        (1001, "机器学习入门教程", None),
        (1001, "深度学习与神经网络", None),
        (1001, "今天天气很好", None),
        (1002, "Rust async programming", None),
        (1002, "Machine learning with Rust", Some("ml.pdf")),
        (1003, "季度报告附件", Some("2024年季度报告.pdf")),
        (1003, "会议记录", None),
    ]
    .into_iter()
    .enumerate()
    {
        harness.backend.add_message(IndexedMessage {
            url: format!("https://t.me/c/{chat_id}/{}", index + 1),
            chat_id,
            content: content.to_string(),
            post_time: 1_700_000_000 + index as i64 * 10,
            sender: "fixture".into(),
            filename: file.map(ToString::to_string),
        })?;
    }
    Ok(harness)
}

/// Generate a useful `ImageMessage` for a fixture file.
pub fn fixture_image_message(
    url: &str,
    chat_id: ChatId,
    content: &str,
    filename: Option<&str>,
) -> ImageMessage {
    image_message(
        url,
        chat_id,
        1_700_000_050,
        "fixture",
        content,
        filename.map(ToString::to_string),
        922,
        1170,
    )
}

/// Match helper for tests that only need the count.
pub fn total_image_matches(result: &ImageSearchResult) -> usize {
    result.matches.len()
}

/// Access the custom button constructor from tests.
pub fn button(text: &str, data: &str) -> ButtonDef {
    ButtonDef::new(text, data)
}

/// Helper to build an image match for render tests.
pub fn match_for_test(message: ImageMessage) -> ImageMatch {
    ImageMatch {
        message,
        similarity: 0.9,
        good_matches: 20,
        phash_distance: 1,
        thumbnail: None,
    }
}

/// Ensure a reload of a `TextIndex` does not silently fail. Used by tests.
pub fn ensure_exists(path: &Path) -> Result<()> {
    if path.exists() {
        Ok(())
    } else {
        Err(anyhow!("expected path to exist: {}", path.display()))
    }
}
