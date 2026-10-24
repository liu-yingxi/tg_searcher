//! Teloxide bot frontend adapter: long polling, update conversion, file
//! downloads, inline keyboards and bot command registration.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use teloxide::net::Download;
use teloxide::prelude::*;
use teloxide::sugar::request::{RequestLinkPreviewExt, RequestReplyExt};
use teloxide::types::{
    BotCommand, BotCommandScope, CallbackQuery, CallbackQueryId, ChatId as TgChatId, Document,
    FileId, InlineKeyboardButton, InlineKeyboardMarkup, InputFile, MaybeInaccessibleMessage,
    Message, MessageEntityKind, MessageId as TgMessageId, ParseMode, PhotoSize, Recipient, Update,
    UpdateKind,
};
use tracing::{debug, error, info, warn};

use tgsearcher_app::cache::build_cache;
use tgsearcher_app::messenger::{
    ButtonDef, CallbackEvent, IncomingMessage, IncomingUpdate, InlineKeyboard, Messenger,
    ReplyContext, SentMessage,
};
use tgsearcher_app::{Frontend, FrontendSettings, SearchBackend};
use tgsearcher_core::{escape_html, ChatId};

const MAX_BOT_API_FILE: u64 = 20 * 1024 * 1024;

#[derive(Clone)]
pub struct BotFrontendConfig {
    pub id: String,
    pub bot_token: String,
    pub admin_id: ChatId,
    pub page_len: usize,
    pub private_mode: bool,
    pub private_whitelist: Vec<ChatId>,
    pub redis: Option<String>,
    pub no_redis: bool,
    pub proxy: Option<String>,
    pub send_photo_limit: usize,
    pub tmp_dir: PathBuf,
}

impl std::fmt::Debug for BotFrontendConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BotFrontendConfig")
            .field("id", &self.id)
            .field("bot_token", &"<redacted>")
            .field("admin_id", &self.admin_id)
            .field("page_len", &self.page_len)
            .field("private_mode", &self.private_mode)
            .field("redis", &self.redis)
            .field("no_redis", &self.no_redis)
            .field("proxy", &self.proxy)
            .field("send_photo_limit", &self.send_photo_limit)
            .field("tmp_dir", &self.tmp_dir)
            .finish()
    }
}

pub struct BotFrontend {
    bot: Bot,
    frontend: Arc<Frontend>,
    messenger: Arc<TeloxideMessenger>,
    config: BotFrontendConfig,
    username: String,
    bot_user_id: ChatId,
}

impl BotFrontend {
    pub async fn start(config: BotFrontendConfig, backend: Arc<dyn SearchBackend>) -> Result<Self> {
        std::fs::create_dir_all(&config.tmp_dir)
            .with_context(|| format!("create tmp dir {}", config.tmp_dir.display()))?;

        let client = build_http_client(config.proxy.as_deref())?;
        let bot = Bot::with_client(config.bot_token.clone(), client);
        let me = bot.get_me().await.context("Telegram bot getMe")?;
        let username = me.username().to_string();
        let bot_user_id = me.user.id.0 as ChatId;
        info!(username = %username, id = bot_user_id, "bot frontend logged in");

        // Never index the bot account itself.
        backend.exclude_chat(bot_user_id).await;

        let (cache, cache_fallback) = build_cache(config.no_redis, config.redis.as_deref()).await;
        if cache_fallback {
            warn!(
                "frontend {} is using the volatile in-memory cache",
                config.id
            );
        }
        let messenger = Arc::new(TeloxideMessenger { bot: bot.clone() });
        let settings = FrontendSettings {
            id: config.id.clone(),
            bot_username: username.clone(),
            bot_user_id,
            admin_id: config.admin_id,
            page_len: config.page_len,
            private_mode: config.private_mode,
            private_whitelist: config.private_whitelist.iter().copied().collect(),
            no_redis: config.no_redis || cache_fallback,
            send_photo_limit: config.send_photo_limit,
        };
        let frontend = Arc::new(Frontend::new(
            settings,
            Arc::clone(&backend),
            Arc::clone(&messenger) as Arc<dyn Messenger>,
            cache,
        ));

        let frontend_handle = BotFrontend {
            bot: bot.clone(),
            frontend,
            messenger,
            config,
            username,
            bot_user_id,
        };

        frontend_handle.register_commands().await;
        frontend_handle.notify_admin_startup(backend.as_ref()).await;

        Ok(frontend_handle)
    }

    pub fn frontend(&self) -> &Arc<Frontend> {
        &self.frontend
    }

    pub async fn run(&self) -> Result<()> {
        info!(frontend = %self.config.id, "bot frontend is listening");
        let mut offset = 0i32;
        loop {
            let updates = tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("Ctrl+C received; stopping bot frontend");
                    break;
                }
                result = self.bot.get_updates().offset(offset).timeout(30) => {
                    match result {
                        Ok(updates) => updates,
                        Err(e) => {
                            warn!("getUpdates failed: {e}");
                            tokio::time::sleep(Duration::from_secs(3)).await;
                            continue;
                        }
                    }
                }
            };
            for update in updates {
                offset = update.id.0 as i32 + 1;
                self.handle_update(update).await;
            }
        }
        Ok(())
    }

    async fn handle_update(&self, update: Update) {
        match update.kind {
            UpdateKind::Message(message) => {
                if let Err(e) = self.handle_message(message).await {
                    error!(?e, "cannot convert incoming message");
                }
            }
            UpdateKind::CallbackQuery(query) => {
                if let Err(e) = self.handle_callback(query).await {
                    error!(?e, "cannot handle callback query");
                }
            }
            _ => {}
        }
    }

    async fn handle_message(&self, message: Message) -> Result<()> {
        let chat_id = message.chat.id.0;
        let message_id = message.id.0 as i64;
        let sender_id = message
            .from
            .as_ref()
            .map(|user| user.id.0 as ChatId)
            .unwrap_or(0);
        let text = message
            .text()
            .or_else(|| message.caption())
            .unwrap_or_default()
            .to_string();
        let mentioned = mentions_bot(&message, &self.username, self.bot_user_id);

        debug!(
            chat_id,
            message_id,
            sender_id,
            has_text = !text.is_empty(),
            mentioned,
            "converted Telegram message"
        );

        let mut temporary = Vec::new();
        let image_path = self.download_image(&message).await;
        if let Some(path) = &image_path {
            temporary.push(path.clone());
        }

        let reply = if let Some(replied) = message.reply_to_message() {
            let reply_image = self.download_image(replied).await;
            if let Some(path) = &reply_image {
                temporary.push(path.clone());
            }
            Some(ReplyContext {
                message_id: replied.id.0 as i64,
                sender_id: replied
                    .from
                    .as_ref()
                    .map(|user| user.id.0 as ChatId)
                    .unwrap_or(0),
                text: replied
                    .text()
                    .or_else(|| replied.caption())
                    .unwrap_or_default()
                    .to_string(),
                image_path: reply_image,
            })
        } else {
            None
        };

        self.frontend
            .handle_update(IncomingUpdate::Message(IncomingMessage {
                chat_id,
                message_id,
                sender_id,
                text,
                is_private: message.chat.is_private(),
                mentioned_bot: mentioned,
                reply,
                image_path,
            }))
            .await;

        for path in temporary {
            let _ = tokio::fs::remove_file(path).await;
        }
        Ok(())
    }

    async fn handle_callback(&self, query: CallbackQuery) -> Result<()> {
        let Some(message) = &query.message else {
            let _ = self
                .bot
                .answer_callback_query(query.id.clone())
                .text("无法定位原消息，请重新发起搜索。")
                .show_alert(true)
                .await;
            return Ok(());
        };
        let chat_id = message.chat().id.0;
        let message_id = message.id().0 as i64;
        let data = query.data.clone().unwrap_or_default();
        let sender_id = query.from.id.0 as ChatId;
        self.frontend
            .handle_update(IncomingUpdate::Callback(CallbackEvent {
                callback_id: query.id.0.clone(),
                chat_id,
                message_id,
                sender_id,
                data,
            }))
            .await;
        Ok(())
    }

    async fn download_image(&self, message: &Message) -> Option<PathBuf> {
        let (file_id, size) = image_file(message)?;
        if size > MAX_BOT_API_FILE {
            debug!(size, "file too large for Bot API download; skipping image");
            return None;
        }
        let file = match self.bot.get_file(file_id).await {
            Ok(file) => file,
            Err(e) => {
                warn!("getFile failed: {e}");
                return None;
            }
        };
        let path = self
            .config
            .tmp_dir
            .join(format!("query-{}-{}.bin", message.id.0, now_nanos()));
        let mut destination = match tokio::fs::File::create(&path).await {
            Ok(file) => file,
            Err(e) => {
                warn!(path = %path.display(), "cannot create temp file: {e}");
                return None;
            }
        };
        if let Err(e) = self.bot.download_file(&file.path, &mut destination).await {
            warn!("download_file failed: {e}");
            let _ = tokio::fs::remove_file(&path).await;
            return None;
        }
        Some(path)
    }

    async fn register_commands(&self) {
        let user_commands = vec![
            BotCommand::new("s", "搜索消息（支持关键词和布尔语法）"),
            BotCommand::new("search", "搜索消息（同 /s）"),
            BotCommand::new("ss", "搜索消息（同 /s）"),
            BotCommand::new("chats", "列出/筛选已索引对话"),
            BotCommand::new("random", "随机返回一条已索引消息"),
            BotCommand::new("img2text", "回复图片，按图搜索相关文字"),
            BotCommand::new("img2img", "回复图片，按图搜索相似图片"),
            BotCommand::new("help", "显示帮助"),
        ];
        if let Err(e) = self.bot.set_my_commands(user_commands.clone()).await {
            warn!("cannot register default bot commands: {e}");
        }
        if self.config.admin_id != 0 {
            let mut admin_commands = user_commands;
            admin_commands.extend([
                BotCommand::new("download_chat", "下载并索引指定对话的历史消息"),
                BotCommand::new("monitor_chat", "将对话加入实时监控"),
                BotCommand::new("clear", "清除索引数据"),
                BotCommand::new("stat", "查看后端索引状态"),
                BotCommand::new("find_chat_id", "按名称查找对话 ID"),
                BotCommand::new("refresh_chat_names", "刷新对话名称缓存"),
                BotCommand::new("usage", "查看机器人使用统计"),
            ]);
            let scope = BotCommandScope::Chat {
                chat_id: Recipient::Id(TgChatId(self.config.admin_id)),
            };
            if let Err(e) = self.bot.set_my_commands(admin_commands).scope(scope).await {
                warn!("cannot register admin bot commands: {e}");
            }
        }
    }

    async fn notify_admin_startup(&self, backend: &dyn SearchBackend) {
        if self.config.admin_id == 0 {
            warn!("admin id is unresolved; startup notification skipped");
            return;
        }
        let status = backend.status(3700).await;
        let text = format!(
            "✅ Bot frontend 启动成功 ({})\n\n{status}",
            escape_html(&self.config.id)
        );
        if let Err(e) = self
            .messenger
            .send_text(self.config.admin_id, &text, None, None)
            .await
        {
            warn!("cannot send startup notification to admin: {e}");
        }
    }
}

struct TeloxideMessenger {
    bot: Bot,
}

#[async_trait]
impl Messenger for TeloxideMessenger {
    async fn send_text(
        &self,
        chat_id: ChatId,
        text: &str,
        markup: Option<InlineKeyboard>,
        reply_to: Option<i64>,
    ) -> Result<SentMessage> {
        let mut request = self
            .bot
            .send_message(TgChatId(chat_id), text)
            .parse_mode(ParseMode::Html)
            .disable_link_preview(true);
        if let Some(markup) = markup {
            if !markup.is_empty() {
                request = request.reply_markup(to_markup(markup));
            }
        }
        if let Some(reply_to) = reply_to {
            request = request.reply_to(TgMessageId(reply_to as i32));
        }
        let message = request.await?;
        Ok(SentMessage {
            chat_id,
            message_id: message.id.0 as i64,
        })
    }

    async fn edit_text(
        &self,
        chat_id: ChatId,
        message_id: i64,
        text: &str,
        markup: Option<InlineKeyboard>,
    ) -> Result<()> {
        let markup = markup.map(to_markup).unwrap_or_else(empty_markup);
        self.bot
            .edit_message_text(TgChatId(chat_id), TgMessageId(message_id as i32), text)
            .parse_mode(ParseMode::Html)
            .reply_markup(markup)
            .await?;
        Ok(())
    }

    async fn answer_callback(
        &self,
        callback_id: &str,
        text: Option<&str>,
        alert: bool,
    ) -> Result<()> {
        let request = self
            .bot
            .answer_callback_query(CallbackQueryId(callback_id.to_string()));
        let request = match text {
            Some(text) => request.text(text),
            None => request,
        };
        request.show_alert(alert).await?;
        Ok(())
    }

    async fn send_photo(
        &self,
        chat_id: ChatId,
        path: &Path,
        caption: &str,
        markup: Option<InlineKeyboard>,
    ) -> Result<SentMessage> {
        let mut request = self
            .bot
            .send_photo(TgChatId(chat_id), InputFile::file(path))
            .caption(caption)
            .parse_mode(ParseMode::Html);
        if let Some(markup) = markup {
            if !markup.is_empty() {
                request = request.reply_markup(to_markup(markup));
            }
        }
        let message = request.await?;
        Ok(SentMessage {
            chat_id,
            message_id: message.id.0 as i64,
        })
    }
}

fn build_http_client(proxy: Option<&str>) -> Result<reqwest::Client> {
    let mut builder = teloxide::net::default_reqwest_settings();
    if let Some(proxy) = proxy {
        match reqwest::Proxy::all(proxy) {
            Ok(proxy) => {
                builder = builder.proxy(proxy);
                info!("bot frontend using proxy");
            }
            Err(e) => warn!("invalid proxy `{proxy}` for bot frontend: {e}"),
        }
    }
    Ok(builder.build()?)
}

/// Verify a bot token against the real Bot API without starting the bot.
/// Returns `(username, bot_user_id)`. Used by `tg-searcher --bot-smoke`.
pub async fn bot_smoke_test(token: &str, proxy: Option<&str>) -> Result<(String, ChatId)> {
    let client = build_http_client(proxy)?;
    let bot = Bot::with_client(token.to_string(), client);
    let me = bot.get_me().await.context("Telegram bot getMe")?;
    Ok((me.username().to_string(), me.user.id.0 as ChatId))
}

fn to_markup(keyboard: InlineKeyboard) -> InlineKeyboardMarkup {
    let rows: Vec<Vec<InlineKeyboardButton>> = keyboard
        .rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|button| InlineKeyboardButton::callback(button.text, button.data))
                .collect()
        })
        .collect();
    InlineKeyboardMarkup::new(rows)
}

fn empty_markup() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(Vec::<Vec<InlineKeyboardButton>>::new())
}

fn mentions_bot(message: &Message, username: &str, bot_user_id: ChatId) -> bool {
    let mention = format!("@{username}");
    let entities = message
        .parse_entities()
        .or_else(|| message.parse_caption_entities())
        .unwrap_or_default();
    entities.iter().any(|entity| match entity.kind() {
        MessageEntityKind::Mention => entity.text().eq_ignore_ascii_case(&mention),
        MessageEntityKind::TextMention { user } => user.id.0 as ChatId == bot_user_id,
        _ => false,
    })
}

fn image_file(message: &Message) -> Option<(FileId, u64)> {
    if let Some(photos) = message.photo() {
        let largest = photos.iter().max_by_key(|photo| photo.file.size);
        return largest.map(|photo| (photo.file.id.clone(), photo.file.size as u64));
    }
    if let Some(document) = message.document() {
        if document_is_image(document) {
            return Some((document.file.id.clone(), document.file.size as u64));
        }
    }
    None
}

fn document_is_image(document: &Document) -> bool {
    document
        .mime_type
        .as_ref()
        .map(|mime| mime.to_string().starts_with("image/"))
        .unwrap_or(false)
}

fn now_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0)
}

#[allow(dead_code)]
fn _assert_types(_: &MaybeInaccessibleMessage, _: &[PhotoSize], _: &ButtonDef) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markup_conversion() {
        let keyboard = InlineKeyboard::new(vec![vec![ButtonDef::new("A", "a=1")]]);
        let markup = to_markup(keyboard);
        assert_eq!(markup.inline_keyboard.len(), 1);
        assert_eq!(markup.inline_keyboard[0][0].text, "A");
    }

    #[test]
    fn token_is_redacted_in_debug() {
        let cfg = BotFrontendConfig {
            id: "x".into(),
            bot_token: "123:secret".into(),
            admin_id: 1,
            page_len: 10,
            private_mode: false,
            private_whitelist: Vec::new(),
            redis: None,
            no_redis: true,
            proxy: None,
            send_photo_limit: 3,
            tmp_dir: PathBuf::from("/tmp"),
        };
        let debug = format!("{cfg:?}");
        assert!(!debug.contains("secret"));
        assert!(debug.contains("<redacted>"));
    }
}
