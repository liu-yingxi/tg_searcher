//! Transport-neutral frontend: command dispatch, search pagination contexts,
//! admin operations and image search commands.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use tokio::sync::mpsc;
use tracing::{debug, error, warn};

use tgsearcher_core::{escape_html, ChatId, FileFilter, ImageSearchMode, SearchRequest};

use crate::backend::{ProgressSink, SearchBackend};
use crate::cache::Cache;
use crate::commands::{
    extract_selected_chat_id, parse_command, parse_download_args, parse_image_args,
    parse_search_args, split_args, CommandKind, ParsedCommand,
};
use crate::messenger::{CallbackEvent, IncomingMessage, IncomingUpdate, Messenger};
use crate::render::{
    help_text, image_caption, render_chats, render_image_result, render_random_message,
    render_search_result, search_buttons, truncate_telegram, TELEGRAM_TEXT_LIMIT,
};

const SEARCH_CONTEXT_TTL: Duration = Duration::from_secs(3600);
const SELECT_CHAT_TTL: Duration = Duration::from_secs(3600);
const CLEAR_CONFIRM_TTL: Duration = Duration::from_secs(60);
const TOTAL_USERS_KEY: &str = "tgsearcher_shared:total_users";
const ACTIVE_USERS_KEY: &str = "tgsearcher_shared:active_users_15m";
const ACTIVE_USER_TTL: Duration = Duration::from_secs(900);

#[derive(Clone, Debug)]
pub struct FrontendSettings {
    pub id: String,
    pub bot_username: String,
    pub bot_user_id: ChatId,
    /// 0 when the administrator could not be resolved.
    pub admin_id: ChatId,
    pub page_len: usize,
    pub private_mode: bool,
    pub private_whitelist: HashSet<ChatId>,
    pub no_redis: bool,
    pub send_photo_limit: usize,
}

impl Default for FrontendSettings {
    fn default() -> Self {
        Self {
            id: "frontend".into(),
            bot_username: String::new(),
            bot_user_id: 0,
            admin_id: 0,
            page_len: 10,
            private_mode: false,
            private_whitelist: HashSet::new(),
            no_redis: false,
            send_photo_limit: 3,
        }
    }
}

#[derive(Clone, Debug)]
struct SearchContext {
    query: String,
    chats: Option<Vec<ChatId>>,
    filter: FileFilter,
    page: usize,
}

pub struct Frontend {
    pub settings: FrontendSettings,
    backend: Arc<dyn SearchBackend>,
    messenger: Arc<dyn Messenger>,
    cache: Arc<dyn Cache>,
}

impl Frontend {
    pub fn new(
        settings: FrontendSettings,
        backend: Arc<dyn SearchBackend>,
        messenger: Arc<dyn Messenger>,
        cache: Arc<dyn Cache>,
    ) -> Self {
        let mut settings = settings;
        settings.page_len = settings.page_len.max(1);
        settings.send_photo_limit = settings.send_photo_limit.max(1);
        Self {
            settings,
            backend,
            messenger,
            cache,
        }
    }

    pub fn backend(&self) -> &Arc<dyn SearchBackend> {
        &self.backend
    }

    fn prefix(&self) -> String {
        format!("{}:", self.settings.id)
    }

    fn is_admin(&self, user_id: ChatId) -> bool {
        self.settings.admin_id != 0 && user_id == self.settings.admin_id
    }

    /// Entry point used by the Telegram adapter.
    pub async fn handle_update(&self, update: IncomingUpdate) {
        match update {
            IncomingUpdate::Message(message) => {
                if let Err(e) = self.handle_message(&message).await {
                    error!(?e, "message handling failed");
                    let _ = self
                        .messenger
                        .send_text(
                            message.chat_id,
                            &format!("🆘 处理请求时发生内部错误: {}", escape_html(&e.to_string())),
                            None,
                            Some(message.message_id),
                        )
                        .await;
                }
            }
            IncomingUpdate::Callback(callback) => {
                if let Err(e) = self.handle_callback(&callback).await {
                    error!(?e, "callback handling failed");
                    let _ = self
                        .messenger
                        .answer_callback(
                            &callback.callback_id,
                            Some("处理请求时发生内部错误。"),
                            true,
                        )
                        .await;
                }
            }
        }
    }

    async fn handle_message(&self, message: &IncomingMessage) -> Result<()> {
        if message.sender_id == self.settings.bot_user_id {
            return Ok(());
        }
        self.track_user(message.sender_id).await;

        if self.settings.private_mode
            && !self.is_admin(message.sender_id)
            && !self.settings.private_whitelist.contains(&message.sender_id)
        {
            debug!(user = message.sender_id, "ignored in private mode");
            return Ok(());
        }

        if let Some(command) = parse_command(&message.text) {
            return self.dispatch_command(message, command).await;
        }

        if message.text.trim().is_empty() {
            if message.image_path.is_some() {
                self.reply(
                    message,
                    "收到图片。请发送带说明文字的图片（例如说明为 <code>/img2text</code>），\
                     或回复该图片后发送 <code>/img2text</code> / <code>/img2img</code>。",
                )
                .await?;
            }
            return Ok(());
        }

        let mentioned = message.mentioned_bot;
        if message.is_private || mentioned {
            let mut query = message.text.trim().to_string();
            if mentioned && !self.settings.bot_username.is_empty() {
                let mention = format!("@{}", self.settings.bot_username.to_ascii_lowercase());
                if query.to_ascii_lowercase().starts_with(&mention) {
                    query = query[mention.len()..].trim().to_string();
                }
            }
            if !query.is_empty() {
                return self.handle_search(message, &query).await;
            }
        }
        Ok(())
    }

    async fn dispatch_command(
        &self,
        message: &IncomingMessage,
        command: ParsedCommand,
    ) -> Result<()> {
        debug!(kind = %command.kind, "dispatching command");
        if command.kind.is_admin_only() && !self.is_admin(message.sender_id) {
            self.reply(message, "⛔ 此命令仅管理员可用。").await?;
            return Ok(());
        }
        match command.kind {
            CommandKind::Search => self.handle_search(message, &command.args).await,
            CommandKind::Chats => self.handle_chats(message, &command.args).await,
            CommandKind::Random => self.handle_random(message).await,
            CommandKind::Help => {
                self.reply(message, &help_text(self.is_admin(message.sender_id)))
                    .await
            }
            CommandKind::ImageToText => {
                self.handle_image(message, &command.args, ImageSearchMode::ImageToText)
                    .await
            }
            CommandKind::ImageToImage => {
                self.handle_image(message, &command.args, ImageSearchMode::ImageToImage)
                    .await
            }
            CommandKind::DownloadChat => self.handle_download(message, &command.args).await,
            CommandKind::MonitorChat => self.handle_monitor(message, &command.args).await,
            CommandKind::Clear => self.handle_clear(message, &command.args).await,
            CommandKind::Stat => self.handle_stat(message).await,
            CommandKind::FindChatId => self.handle_find_chat_id(message, &command.args).await,
            CommandKind::RefreshNames => self.handle_refresh_names(message).await,
            CommandKind::Usage => self.handle_usage(message).await,
            CommandKind::Unknown(name) => {
                self.reply(
                    message,
                    &format!(
                        "未知命令 <code>/{}</code>，请使用 /help 查看可用命令。",
                        escape_html(&name)
                    ),
                )
                .await
            }
        }
    }

    async fn reply(&self, message: &IncomingMessage, text: &str) -> Result<()> {
        self.messenger
            .send_text(message.chat_id, text, None, Some(message.message_id))
            .await?;
        Ok(())
    }

    async fn send(&self, chat_id: ChatId, text: &str) -> Result<crate::messenger::SentMessage> {
        self.messenger.send_text(chat_id, text, None, None).await
    }

    async fn edit(
        &self,
        chat_id: ChatId,
        message_id: i64,
        text: &str,
        markup: Option<crate::messenger::InlineKeyboard>,
    ) -> Result<()> {
        self.messenger
            .edit_text(chat_id, message_id, text, markup)
            .await?;
        Ok(())
    }

    async fn titles_for(&self, chat_ids: &[ChatId]) -> HashMap<ChatId, String> {
        let mut unique = HashSet::new();
        let mut titles = HashMap::new();
        for chat_id in chat_ids {
            if !unique.insert(*chat_id) {
                continue;
            }
            let title = self
                .backend
                .chat_title(*chat_id)
                .await
                .unwrap_or_else(|| format!("未知对话 ({chat_id})"));
            titles.insert(*chat_id, title);
        }
        titles
    }

    // ------------------------------------------------------------------
    // Search
    // ------------------------------------------------------------------

    async fn handle_search(&self, message: &IncomingMessage, args: &str) -> Result<()> {
        let options = match parse_search_args(args) {
            Ok(options) => options,
            Err(e) => {
                self.reply(message, &format!("❌ {e}")).await?;
                return Ok(());
            }
        };
        if options.query.trim().is_empty() {
            self.reply(
                message,
                "请输入要搜索的关键词，例如 <code>/s 机器学习</code>。",
            )
            .await?;
            return Ok(());
        }
        let selected = self.selected_chat(message).await;
        let chats = selected.map(|id| vec![id]);

        let mut request = SearchRequest::new(options.query.clone());
        request.page = 1;
        request.page_len = self.settings.page_len;
        request.chats = chats.clone();
        request.file_filter = FileFilter::All;
        request.fuzzy_threshold = options.threshold;
        request.exact_only = options.exact_only;

        let started = Instant::now();
        let result = match self.backend.search(request).await {
            Ok(result) => result,
            Err(e) => {
                self.reply(
                    message,
                    &format!("❌ 搜索失败: {}", escape_html(&e.to_string())),
                )
                .await?;
                return Ok(());
            }
        };
        let elapsed = started.elapsed();
        let chat_ids: Vec<ChatId> = result.hits.iter().map(|hit| hit.message.chat_id).collect();
        let titles = self.titles_for(&chat_ids).await;
        let text = render_search_result(&result, &titles, elapsed);
        let buttons = search_buttons(&result, FileFilter::All);
        let sent = self
            .messenger
            .send_text(message.chat_id, &text, buttons, Some(message.message_id))
            .await?;
        if result.total > 0 {
            self.save_context(
                sent.chat_id,
                sent.message_id,
                &options.query,
                chats.as_deref(),
                FileFilter::All,
                1,
            )
            .await;
        }
        Ok(())
    }

    async fn save_context(
        &self,
        chat_id: ChatId,
        message_id: i64,
        query: &str,
        chats: Option<&[ChatId]>,
        filter: FileFilter,
        page: usize,
    ) {
        let prefix = self.prefix();
        let query_key = format!("{prefix}query_text:{chat_id}:{message_id}");
        let chats_key = format!("{prefix}query_chats:{chat_id}:{message_id}");
        let filter_key = format!("{prefix}query_filter:{chat_id}:{message_id}");
        let page_key = format!("{prefix}query_page:{chat_id}:{message_id}");
        self.cache_set(&query_key, query).await;
        match chats {
            Some(chats) if !chats.is_empty() => {
                let joined = chats
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(",");
                self.cache_set(&chats_key, &joined).await;
            }
            _ => self.cache_delete(&[&chats_key]).await,
        }
        self.cache_set(&filter_key, filter.as_str()).await;
        self.cache_set(&page_key, &page.to_string()).await;
    }

    async fn load_context(&self, chat_id: ChatId, message_id: i64) -> Option<SearchContext> {
        let prefix = self.prefix();
        let query_key = format!("{prefix}query_text:{chat_id}:{message_id}");
        let chats_key = format!("{prefix}query_chats:{chat_id}:{message_id}");
        let filter_key = format!("{prefix}query_filter:{chat_id}:{message_id}");
        let page_key = format!("{prefix}query_page:{chat_id}:{message_id}");

        let query = self.cache_get(&query_key).await?;
        let chats = self.cache_get(&chats_key).await.and_then(|value| {
            let ids: Vec<ChatId> = value
                .split(',')
                .filter_map(|part| part.trim().parse().ok())
                .collect();
            if ids.is_empty() {
                None
            } else {
                Some(ids)
            }
        });
        let filter = self
            .cache_get(&filter_key)
            .await
            .map(|value| FileFilter::parse(&value))
            .unwrap_or_default();
        let page = self
            .cache_get(&page_key)
            .await
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(1)
            .max(1);
        Some(SearchContext {
            query,
            chats,
            filter,
            page,
        })
    }

    async fn expire_context_keys(&self, chat_id: ChatId, message_id: i64) {
        let prefix = self.prefix();
        let keys = [
            format!("{prefix}query_text:{chat_id}:{message_id}"),
            format!("{prefix}query_chats:{chat_id}:{message_id}"),
            format!("{prefix}query_filter:{chat_id}:{message_id}"),
            format!("{prefix}query_page:{chat_id}:{message_id}"),
        ];
        let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        self.cache_delete(&refs).await;
    }

    async fn selected_chat(&self, message: &IncomingMessage) -> Option<ChatId> {
        let reply = message.reply.as_ref()?;
        if reply.sender_id != self.settings.bot_user_id && self.settings.bot_user_id != 0 {
            return None;
        }
        if !reply.text.contains("☑️ 已选择:") {
            return None;
        }
        let prefix = self.prefix();
        let key = format!(
            "{prefix}select_chat:{}:{}",
            message.chat_id, reply.message_id
        );
        if let Some(value) = self.cache_get(&key).await {
            if let Ok(id) = value.parse::<ChatId>() {
                return Some(id);
            }
        }
        extract_selected_chat_id(&reply.text)
    }

    // ------------------------------------------------------------------
    // Callbacks
    // ------------------------------------------------------------------

    async fn handle_callback(&self, callback: &CallbackEvent) -> Result<()> {
        self.track_user(callback.sender_id).await;
        let Some((action, value)) = callback.data.split_once('=') else {
            self.answer_text(callback, "无效的回调操作。", true).await?;
            return Ok(());
        };
        match action {
            "search_page" => self.handle_page_callback(callback, value).await,
            "search_filter" => self.handle_filter_callback(callback, value).await,
            "select_chat" => self.handle_select_callback(callback, value).await,
            "noop" => {
                self.answer(callback, None, false).await?;
                Ok(())
            }
            other => {
                self.answer_text(callback, &format!("未知的操作类型: {other}"), true)
                    .await?;
                Ok(())
            }
        }
    }

    async fn answer(
        &self,
        callback: &CallbackEvent,
        text: Option<&str>,
        alert: bool,
    ) -> Result<()> {
        self.messenger
            .answer_callback(&callback.callback_id, text, alert)
            .await?;
        Ok(())
    }

    async fn answer_text(&self, callback: &CallbackEvent, text: &str, alert: bool) -> Result<()> {
        self.answer(callback, Some(text), alert).await
    }

    async fn context_or_expire(&self, callback: &CallbackEvent) -> Result<Option<SearchContext>> {
        let context = self
            .load_context(callback.chat_id, callback.message_id)
            .await;
        if context.is_none() {
            let _ = self
                .edit(
                    callback.chat_id,
                    callback.message_id,
                    "这次搜索的信息已过期，请重新发起搜索。",
                    None,
                )
                .await;
            self.expire_context_keys(callback.chat_id, callback.message_id)
                .await;
            self.answer_text(callback, "搜索已过期。", true).await?;
        }
        Ok(context)
    }

    async fn handle_page_callback(&self, callback: &CallbackEvent, value: &str) -> Result<()> {
        let page: usize = match value.parse() {
            Ok(page) if page > 0 => page,
            _ => {
                self.answer_text(callback, "无效的页码。", true).await?;
                return Ok(());
            }
        };
        let Some(mut context) = self.context_or_expire(callback).await? else {
            return Ok(());
        };
        context.page = page;
        self.run_callback_search(callback, context, page).await
    }

    async fn handle_filter_callback(&self, callback: &CallbackEvent, value: &str) -> Result<()> {
        let filter = match value {
            "text_only" => FileFilter::TextOnly,
            "file_only" => FileFilter::FileOnly,
            _ => FileFilter::All,
        };
        let Some(mut context) = self.context_or_expire(callback).await? else {
            return Ok(());
        };
        context.filter = filter;
        context.page = 1;
        self.run_callback_search(callback, context, 1).await
    }

    async fn run_callback_search(
        &self,
        callback: &CallbackEvent,
        context: SearchContext,
        page: usize,
    ) -> Result<()> {
        let mut request = SearchRequest::new(context.query.clone());
        request.page = page;
        request.page_len = self.settings.page_len;
        request.chats = context.chats.clone();
        request.file_filter = context.filter;
        request.fuzzy_threshold = None;

        let started = Instant::now();
        let result = match self.backend.search(request).await {
            Ok(result) => result,
            Err(e) => {
                self.answer_text(callback, &format!("后端搜索失败: {e}"), true)
                    .await?;
                return Ok(());
            }
        };
        let elapsed = started.elapsed();
        let chat_ids: Vec<ChatId> = result.hits.iter().map(|hit| hit.message.chat_id).collect();
        let titles = self.titles_for(&chat_ids).await;
        let text = if result.total == 0 && context.filter != FileFilter::All {
            format!(
                "在 <b>{}</b> 筛选条件下，未找到与 “<code>{}</code>” 相关的消息。",
                context.filter.label_zh(),
                escape_html(&context.query)
            )
        } else {
            render_search_result(&result, &titles, elapsed)
        };
        let buttons = search_buttons(&result, context.filter);
        if let Err(e) = self
            .edit(callback.chat_id, callback.message_id, &text, buttons)
            .await
        {
            warn!("cannot edit search result: {e}");
            self.answer_text(callback, "无法更新结果，原消息可能已被删除。", true)
                .await?;
            return Ok(());
        }
        self.save_context(
            callback.chat_id,
            callback.message_id,
            &context.query,
            context.chats.as_deref(),
            context.filter,
            page,
        )
        .await;
        self.answer(callback, None, false).await?;
        Ok(())
    }

    async fn handle_select_callback(&self, callback: &CallbackEvent, value: &str) -> Result<()> {
        let chat_id: ChatId = match value.parse() {
            Ok(id) => id,
            Err(_) => {
                self.answer_text(callback, "无效的对话 ID。", true).await?;
                return Ok(());
            }
        };
        let title = self
            .backend
            .chat_title(chat_id)
            .await
            .unwrap_or_else(|| format!("未知对话 ({chat_id})"));
        let text = format!(
            "☑️ 已选择: <b>{}</b> (<code>{chat_id}</code>)\n\n请回复此消息以在此对话中搜索或执行管理操作。",
            escape_html(&title)
        );
        let _ = self
            .edit(callback.chat_id, callback.message_id, &text, None)
            .await;
        let prefix = self.prefix();
        let key = format!(
            "{prefix}select_chat:{}:{}",
            callback.chat_id, callback.message_id
        );
        self.cache_set_with_ttl(&key, &chat_id.to_string(), SELECT_CHAT_TTL)
            .await;
        self.answer(callback, None, false).await?;
        Ok(())
    }

    // ------------------------------------------------------------------
    // Simple commands
    // ------------------------------------------------------------------

    async fn handle_chats(&self, message: &IncomingMessage, args: &str) -> Result<()> {
        let monitored = self.backend.monitored_chats().await;
        let excluded: HashSet<ChatId> = self.backend.excluded_chats().await.into_iter().collect();
        let ids: Vec<ChatId> = monitored
            .into_iter()
            .filter(|id| !excluded.contains(id))
            .take(200)
            .collect();
        if ids.is_empty() {
            self.reply(message, "目前没有正在监控或已索引的对话。")
                .await?;
            return Ok(());
        }
        let filter_query = args.trim().to_string();
        let mut chats = Vec::new();
        for chat_id in ids {
            let title = self
                .backend
                .chat_title(chat_id)
                .await
                .unwrap_or_else(|| format!("对话 {chat_id} (获取名称出错)"));
            if filter_query.is_empty()
                || title.to_lowercase().contains(&filter_query.to_lowercase())
                || chat_id.to_string().contains(&filter_query)
            {
                chats.push((chat_id, title));
            }
        }
        if chats.is_empty() {
            self.reply(
                message,
                &format!(
                    "找不到名称或 ID 中包含 “{}” 的已索引对话。",
                    escape_html(&filter_query)
                ),
            )
            .await?;
            return Ok(());
        }
        let rendered = render_chats(&chats, &filter_query, false);
        self.messenger
            .send_text(
                message.chat_id,
                &rendered.text,
                rendered.keyboard,
                Some(message.message_id),
            )
            .await?;
        Ok(())
    }

    async fn handle_random(&self, message: &IncomingMessage) -> Result<()> {
        match self.backend.random_message().await {
            Ok(Some(indexed)) => {
                let title = self
                    .backend
                    .chat_title(indexed.chat_id)
                    .await
                    .unwrap_or_else(|| format!("未知对话 ({})", indexed.chat_id));
                let text = render_random_message(&indexed, &title, "");
                self.reply(message, &text).await?;
            }
            Ok(None) => {
                self.reply(message, "索引库中没有任何消息可供随机选择。")
                    .await?;
            }
            Err(e) => {
                self.reply(
                    message,
                    &format!("🆘 获取随机消息时发生错误: {}", escape_html(&e.to_string())),
                )
                .await?;
            }
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Admin commands
    // ------------------------------------------------------------------

    async fn handle_download(&self, message: &IncomingMessage, args: &str) -> Result<()> {
        let parsed = match parse_download_args(args) {
            Ok(parsed) => parsed,
            Err(e) => {
                self.reply(
                    message,
                    &format!(
                        "❌ 参数错误: {e}\n\n用法: <code>/download_chat [--min ID] [--max ID] [对话...]</code>"
                    ),
                )
                .await?;
                return Ok(());
            }
        };
        let mut chats = parsed.chats;
        if chats.is_empty() {
            if let Some(id) = self.selected_chat(message).await {
                chats.push(id.to_string());
            }
        }
        if chats.is_empty() {
            self.reply(
                message,
                "请指定至少一个对话的 ID、用户名、链接，或回复一个已选择的对话消息。",
            )
            .await?;
            return Ok(());
        }

        let status = self
            .send(
                message.chat_id,
                &format!("⏳ 正在准备下载 {} 个对话...", chats.len()),
            )
            .await?;
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
        let messenger = Arc::clone(&self.messenger);
        let status_chat = status.chat_id;
        let status_message = status.message_id;
        let ticker = tokio::spawn(async move {
            let mut latest: Option<tgsearcher_core::DownloadProgress> = None;
            let mut last_edit = Instant::now();
            loop {
                tokio::select! {
                    item = progress_rx.recv() => {
                        match item {
                            Some(progress) => latest = Some(progress),
                            None => break,
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(400)) => {
                        if let Some(progress) = &latest {
                            if last_edit.elapsed() >= Duration::from_secs(3) {
                                let text = format!(
                                    "⏳ 正在下载对话 {}: 已处理 {} 条消息 (当前 ID: {})...",
                                    progress.chat_id, progress.processed, progress.current_message_id
                                );
                                let _ = messenger
                                    .edit_text(status_chat, status_message, &text, None)
                                    .await;
                                last_edit = Instant::now();
                            }
                        }
                    }
                }
            }
        });

        let mut ok_count = 0usize;
        let mut fail_count = 0usize;
        let mut lines = Vec::new();
        for chat_input in &chats {
            let tx = progress_tx.clone();
            let sink: ProgressSink = Arc::new(move |progress| {
                let _ = tx.send(progress);
            });
            let started = Instant::now();
            match self
                .backend
                .download_history(chat_input, parsed.min_id, parsed.max_id, sink)
                .await
            {
                Ok(report) => {
                    ok_count += 1;
                    let id = report
                        .chat_id
                        .map(|id| id.to_string())
                        .unwrap_or_else(|| chat_input.clone());
                    let title = match report.chat_id {
                        Some(id) => self
                            .backend
                            .chat_title(id)
                            .await
                            .unwrap_or_else(|| id.to_string()),
                        None => chat_input.clone(),
                    };
                    lines.push(format!(
                        "✅ 成功下载 \"{}\" ({id}): 处理 {} 条，索引 {} 条，图片 {} 张 (耗时 {:.2} 秒)",
                        escape_html(&title),
                        report.processed,
                        report.indexed,
                        report.images_indexed,
                        report.elapsed_ms as f64 / 1000.0
                    ));
                }
                Err(e) => {
                    fail_count += 1;
                    lines.push(format!(
                        "❌ 下载 {} 失败: {}",
                        escape_html(chat_input),
                        escape_html(&e.to_string())
                    ));
                }
            }
            let _ = started;
        }
        drop(progress_tx);
        let _ = ticker.await;

        let report = format!(
            "下载任务完成 ({} 成功, {} 失败):\n\n{}",
            ok_count,
            fail_count,
            lines.join("\n")
        );
        let report = truncate_telegram(&report, TELEGRAM_TEXT_LIMIT);
        if self
            .edit(status.chat_id, status.message_id, &report, None)
            .await
            .is_err()
        {
            let _ = self.send(message.chat_id, &report).await;
        }
        Ok(())
    }

    async fn handle_monitor(&self, message: &IncomingMessage, args: &str) -> Result<()> {
        let mut inputs = split_args(args);
        if inputs.is_empty() {
            if let Some(id) = self.selected_chat(message).await {
                inputs.push(id.to_string());
            }
        }
        if inputs.is_empty() {
            self.reply(
                message,
                "请指定至少一个要监控的对话的 ID、用户名、链接，或回复一个已选择的对话消息。",
            )
            .await?;
            return Ok(());
        }

        let mut ids = Vec::new();
        let mut errors = Vec::new();
        for input in &inputs {
            match self.backend.resolve_chat_id(input).await {
                Ok(id) => ids.push(id),
                Err(e) => errors.push(format!(
                    "❌ {}: {}",
                    escape_html(input),
                    escape_html(&e.to_string())
                )),
            }
        }
        if ids.is_empty() {
            self.reply(message, &format!("无法添加监控:\n\n{}", errors.join("\n")))
                .await?;
            return Ok(());
        }

        let report = self.backend.add_monitoring(&ids).await?;
        let mut lines = Vec::new();
        for id in &report.added {
            let title = self
                .backend
                .chat_title(*id)
                .await
                .unwrap_or_else(|| format!("未知名称 ({id})"));
            lines.push(format!("✅ 已添加监控: \"{}\" ({id})", escape_html(&title)));
        }
        for (id, reason) in &report.failed {
            let title = self
                .backend
                .chat_title(*id)
                .await
                .unwrap_or_else(|| format!("未知名称 ({id})"));
            lines.push(format!(
                "⚠️ {} ({id}): {}",
                escape_html(&title),
                escape_html(reason)
            ));
        }
        lines.extend(errors);
        let text = truncate_telegram(
            &format!("监控请求处理完成:\n\n{}", lines.join("\n")),
            TELEGRAM_TEXT_LIMIT,
        );
        self.reply(message, &text).await?;
        Ok(())
    }

    async fn handle_clear(&self, message: &IncomingMessage, args: &str) -> Result<()> {
        let mut tokens = split_args(args);
        let clear_all = tokens.iter().any(|token| token.eq_ignore_ascii_case("all"));
        tokens.retain(|token| !token.eq_ignore_ascii_case("all"));

        if clear_all {
            let confirm_key = format!(
                "{}confirm_clear_all:{}:{}",
                self.prefix(),
                message.chat_id,
                message.sender_id
            );
            let pending = self.cache_get(&confirm_key).await.as_deref() == Some("pending");
            if pending {
                self.cache_delete(&[&confirm_key]).await;
                let status = self.send(message.chat_id, "⏳ 正在清除所有索引...").await?;
                match self.backend.clear(None).await {
                    Ok(removed) => {
                        let _ = self
                            .edit(
                                status.chat_id,
                                status.message_id,
                                &format!("✅ 已清除所有索引数据 (删除 {removed} 条记录)，并停止监控所有对话。"),
                                None,
                            )
                            .await;
                    }
                    Err(e) => {
                        let _ = self
                            .edit(
                                status.chat_id,
                                status.message_id,
                                &format!(
                                    "❌ 清除所有索引时发生错误: {}",
                                    escape_html(&e.to_string())
                                ),
                                None,
                            )
                            .await;
                    }
                }
            } else {
                self.cache_set_with_ttl(&confirm_key, "pending", CLEAR_CONFIRM_TTL)
                    .await;
                self.reply(
                    message,
                    "⚠️ <b>警告!</b> 您确定要清除 <b>所有</b> 对话的索引数据吗？此操作不可恢复。\n\n\
                     <b>请在 60 秒内再次发送 <code>/clear all</code> 进行确认。</b>",
                )
                .await?;
            }
            return Ok(());
        }

        if tokens.is_empty() {
            if let Some(id) = self.selected_chat(message).await {
                tokens.push(id.to_string());
            }
        }
        if tokens.is_empty() {
            self.reply(
                message,
                "请指定要清除的对话 ID/用户名/链接，或回复一个已选择的对话消息，或使用 <code>all</code> 清除全部。",
            )
            .await?;
            return Ok(());
        }

        let mut ids = Vec::new();
        let mut lines = Vec::new();
        let mut seen = HashSet::new();
        for input in &tokens {
            if !seen.insert(input.clone()) {
                continue;
            }
            match self.backend.resolve_chat_id(input).await {
                Ok(id) => {
                    let title = self
                        .backend
                        .chat_title(id)
                        .await
                        .unwrap_or_else(|| format!("未知名称 ({id})"));
                    lines.push(format!("• \"{}\" ({id})", escape_html(&title)));
                    ids.push(id);
                }
                Err(e) => lines.push(format!(
                    "❌ 找不到对话 {}: {}",
                    escape_html(input),
                    escape_html(&e.to_string())
                )),
            }
        }
        if ids.is_empty() {
            self.reply(
                message,
                &format!("没有找到有效的对话进行清除。\n\n{}", lines.join("\n")),
            )
            .await?;
            return Ok(());
        }
        match self.backend.clear(Some(ids.clone())).await {
            Ok(removed) => {
                self.reply(
                    message,
                    &format!(
                        "✅ 已清除指定的 {} 个对话的索引数据 (删除 {removed} 条记录):\n\n{}",
                        ids.len(),
                        lines.join("\n")
                    ),
                )
                .await?;
            }
            Err(e) => {
                self.reply(
                    message,
                    &format!("❌ 清除索引时发生错误: {}", escape_html(&e.to_string())),
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn handle_stat(&self, message: &IncomingMessage) -> Result<()> {
        let status = self.send(message.chat_id, "⏳ 正在获取后端状态...").await?;
        let text = self.backend.status(3800).await;
        let text = truncate_telegram(&text, TELEGRAM_TEXT_LIMIT);
        let _ = self
            .edit(status.chat_id, status.message_id, &text, None)
            .await;
        Ok(())
    }

    async fn handle_find_chat_id(&self, message: &IncomingMessage, args: &str) -> Result<()> {
        let query = args.trim();
        if query.is_empty() {
            self.reply(message, "请输入要查找的对话关键词 (名称或用户名)。")
                .await?;
            return Ok(());
        }
        let status = self
            .send(
                message.chat_id,
                &format!("⏳ 正在查找包含 “{}” 的对话...", escape_html(query)),
            )
            .await?;
        let ids = self.backend.find_chat_ids(query).await;
        if ids.is_empty() {
            let _ = self
                .edit(
                    status.chat_id,
                    status.message_id,
                    &format!("找不到名称或用户名中包含 “{}” 的对话。", escape_html(query)),
                    None,
                )
                .await;
            return Ok(());
        }
        let mut lines = vec![format!("找到 {} 个匹配对话:", ids.len())];
        for id in &ids {
            let title = self
                .backend
                .chat_title(*id)
                .await
                .unwrap_or_else(|| "(获取名称出错)".to_string());
            lines.push(format!("- {} (<code>{id}</code>)", escape_html(&title)));
        }
        let text = truncate_telegram(&lines.join("\n"), TELEGRAM_TEXT_LIMIT);
        let _ = self
            .edit(status.chat_id, status.message_id, &text, None)
            .await;
        Ok(())
    }

    async fn handle_refresh_names(&self, message: &IncomingMessage) -> Result<()> {
        let status = self
            .send(message.chat_id, "⏳ 正在请求后端刷新对话名称缓存...")
            .await?;
        match self.backend.refresh_chat_names().await {
            Ok(()) => {
                let _ = self
                    .edit(
                        status.chat_id,
                        status.message_id,
                        "✅ 后端对话名称缓存已刷新。",
                        None,
                    )
                    .await;
            }
            Err(e) => {
                let _ = self
                    .edit(
                        status.chat_id,
                        status.message_id,
                        &format!("🆘 刷新对话名称缓存时出错: {}", escape_html(&e.to_string())),
                        None,
                    )
                    .await;
            }
        }
        Ok(())
    }

    async fn handle_usage(&self, message: &IncomingMessage) -> Result<()> {
        let total = match self.cache.set_card(TOTAL_USERS_KEY).await {
            Ok(value) => value,
            Err(e) => {
                self.reply(
                    message,
                    &format!("❌ 获取统计时缓存出错: {}", escape_html(&e.to_string())),
                )
                .await?;
                return Ok(());
            }
        };
        let active = self.cache.set_card(ACTIVE_USERS_KEY).await.unwrap_or(0);
        let mut text = format!(
            "📊 <b>机器人使用统计 ({})</b>\n\n- <b>总互动用户数:</b> {total}\n- <b>最近 15 分钟活跃用户数:</b> {active}",
            escape_html(&self.settings.id)
        );
        if self.cache.is_volatile() || self.settings.no_redis {
            text.push_str("\n\n<i>注意: 当前使用内存缓存，统计数据在重启后会丢失。</i>");
        }
        self.reply(message, &text).await?;
        Ok(())
    }

    // ------------------------------------------------------------------
    // Image search
    // ------------------------------------------------------------------

    async fn handle_image(
        &self,
        message: &IncomingMessage,
        args: &str,
        mode: ImageSearchMode,
    ) -> Result<()> {
        let image = message.image_path.clone().or_else(|| {
            message
                .reply
                .as_ref()
                .and_then(|reply| reply.image_path.clone())
        });
        let Some(image) = image else {
            self.reply(
                message,
                &format!(
                    "请发送图片并在说明中带上 <code>{}</code>，或回复一张图片后发送该命令。",
                    mode.command()
                ),
            )
            .await?;
            return Ok(());
        };

        let parsed = match parse_image_args(args) {
            Ok(parsed) => parsed,
            Err(e) => {
                self.reply(message, &format!("❌ {e}")).await?;
                return Ok(());
            }
        };
        let mut chats = parsed.chats;
        if chats.is_empty() {
            if let Some(id) = self.selected_chat(message).await {
                chats.push(id);
            }
        }
        let target = if chats.is_empty() {
            None
        } else {
            Some(chats.as_slice())
        };

        let started = Instant::now();
        let result = match self.backend.search_image(&image, mode, target).await {
            Ok(result) => result,
            Err(e) => {
                self.reply(
                    message,
                    &format!("❌ 图片搜索失败: {}", escape_html(&e.to_string())),
                )
                .await?;
                return Ok(());
            }
        };
        let elapsed = started.elapsed();
        let chat_ids: Vec<ChatId> = result.matches.iter().map(|m| m.message.chat_id).collect();
        let titles = self.titles_for(&chat_ids).await;
        let text = render_image_result(&result, &titles, mode, elapsed);
        self.reply(message, &text).await?;

        if mode == ImageSearchMode::ImageToImage {
            for item in result.matches.iter().take(self.settings.send_photo_limit) {
                let Some(thumbnail) = &item.thumbnail else {
                    continue;
                };
                let title = titles
                    .get(&item.message.chat_id)
                    .cloned()
                    .unwrap_or_else(|| format!("未知对话 ({})", item.message.chat_id));
                let caption = image_caption(item, &title);
                if let Err(e) = self
                    .messenger
                    .send_photo(message.chat_id, thumbnail, &caption, None)
                    .await
                {
                    warn!("cannot send matched photo: {e}");
                }
            }
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Cache helpers (failures never abort a user request)
    // ------------------------------------------------------------------

    async fn cache_get(&self, key: &str) -> Option<String> {
        match self.cache.get(key).await {
            Ok(value) => value,
            Err(e) => {
                warn!("cache get failed: {e}");
                None
            }
        }
    }

    async fn cache_set(&self, key: &str, value: &str) {
        self.cache_set_with_ttl(key, value, SEARCH_CONTEXT_TTL)
            .await;
    }

    async fn cache_set_with_ttl(&self, key: &str, value: &str, ttl: Duration) {
        if let Err(e) = self.cache.set(key, value, Some(ttl)).await {
            warn!("cache set failed: {e}");
        }
    }

    async fn cache_delete(&self, keys: &[&str]) {
        if keys.is_empty() {
            return;
        }
        if let Err(e) = self.cache.delete(keys).await {
            warn!("cache delete failed: {e}");
        }
    }

    async fn track_user(&self, user_id: ChatId) {
        if self.settings.no_redis
            || user_id == 0
            || user_id == self.settings.bot_user_id
            || user_id == self.settings.admin_id
        {
            return;
        }
        let user = user_id.to_string();
        if let Err(e) = self.cache.set_add(TOTAL_USERS_KEY, &[&user]).await {
            warn!("usage tracking failed: {e}");
            return;
        }
        if let Err(e) = self.cache.set_add(ACTIVE_USERS_KEY, &[&user]).await {
            warn!("usage tracking failed: {e}");
            return;
        }
        if let Err(e) = self.cache.expire(ACTIVE_USERS_KEY, ACTIVE_USER_TTL).await {
            warn!("usage tracking expire failed: {e}");
        }
    }
}

/// Build a `SearchBackend`-independent error for adapters.
pub fn unsupported(message: &str) -> anyhow::Error {
    anyhow!(message.to_string())
}

/// Handler helper used by adapters that need to map an error into a reply.
pub fn describe_error(error: &anyhow::Error) -> String {
    format!("🆘 {}", escape_html(&error.to_string()))
}

/// Convenience for callers that need the exact command aliases.
pub fn command_help() -> &'static str {
    "s|search|ss, chats, random, help, img2text|i2t, img2img|i2i"
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messenger::InlineKeyboard;

    #[test]
    fn settings_defaults_are_sane() {
        let settings = FrontendSettings::default();
        assert_eq!(settings.page_len, 10);
        assert_eq!(settings.admin_id, 0);
        assert!(!settings.private_mode);
    }

    #[test]
    fn command_kind_flags() {
        assert!(crate::commands::CommandKind::Stat.is_admin_only());
        assert!(!crate::commands::CommandKind::Help.is_admin_only());
    }

    #[test]
    fn keyboard_is_sendable() {
        let keyboard =
            InlineKeyboard::new(vec![vec![crate::messenger::ButtonDef::new("x", "noop")]]);
        assert!(!keyboard.is_empty());
    }

    #[test]
    fn errors_are_escaped() {
        let error = anyhow!("<bad> & wrong");
        let text = describe_error(&error);
        assert!(text.contains("&lt;bad&gt; &amp; wrong"));
    }
}
