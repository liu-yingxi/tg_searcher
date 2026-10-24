//! Transport-neutral types exchanged between the Telegram adapters and the
//! frontend logic.

use std::path::{Path, PathBuf};

use async_trait::async_trait;

use tgsearcher_core::ChatId;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ButtonDef {
    pub text: String,
    pub data: String,
}

impl ButtonDef {
    pub fn new(text: impl Into<String>, data: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            data: data.into(),
        }
    }
}

/// A Telegram inline keyboard: rows of buttons.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InlineKeyboard {
    pub rows: Vec<Vec<ButtonDef>>,
}

impl InlineKeyboard {
    pub fn new(rows: Vec<Vec<ButtonDef>>) -> Self {
        Self { rows }
    }

    pub fn is_empty(&self) -> bool {
        self.rows.iter().all(|row| row.is_empty())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SentMessage {
    pub chat_id: ChatId,
    pub message_id: i64,
}

#[derive(Clone, Debug, Default)]
pub struct ReplyContext {
    pub message_id: i64,
    pub sender_id: ChatId,
    pub text: String,
    /// If the replied-to message contains an image, the adapter downloads it.
    pub image_path: Option<PathBuf>,
}

/// A user message, normalized from the Telegram update.
#[derive(Clone, Debug, Default)]
pub struct IncomingMessage {
    pub chat_id: ChatId,
    pub message_id: i64,
    pub sender_id: ChatId,
    pub text: String,
    pub is_private: bool,
    pub mentioned_bot: bool,
    pub reply: Option<ReplyContext>,
    /// If this message itself carries a downloadable image, the adapter
    /// downloads it to a local file first.
    pub image_path: Option<PathBuf>,
}

#[derive(Clone, Debug, Default)]
pub struct CallbackEvent {
    pub callback_id: String,
    pub chat_id: ChatId,
    pub message_id: i64,
    pub sender_id: ChatId,
    /// Raw callback payload, e.g. `search_page=2`.
    pub data: String,
}

#[derive(Clone, Debug)]
pub enum IncomingUpdate {
    Message(IncomingMessage),
    Callback(CallbackEvent),
}

/// The subset of Telegram messaging the frontend needs.
#[async_trait]
pub trait Messenger: Send + Sync {
    async fn send_text(
        &self,
        chat_id: ChatId,
        text: &str,
        markup: Option<InlineKeyboard>,
        reply_to: Option<i64>,
    ) -> anyhow::Result<SentMessage>;

    async fn edit_text(
        &self,
        chat_id: ChatId,
        message_id: i64,
        text: &str,
        markup: Option<InlineKeyboard>,
    ) -> anyhow::Result<()>;

    async fn answer_callback(
        &self,
        callback_id: &str,
        text: Option<&str>,
        alert: bool,
    ) -> anyhow::Result<()>;

    async fn send_photo(
        &self,
        chat_id: ChatId,
        path: &Path,
        caption: &str,
        markup: Option<InlineKeyboard>,
    ) -> anyhow::Result<SentMessage>;
}
