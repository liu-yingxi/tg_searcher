//! Grammers-backed userbot backend: history download, live indexing of new,
//! edited and deleted messages, media (OpenCV image) indexing and chat
//! resolution.

use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use grammers_client::media::Media;
use grammers_client::update::Update;
use grammers_session::types::PeerRef;
use grammers_session::updates::UpdatesLike;
use grammers_session::Session;
use tokio::sync::mpsc::Receiver;
use tracing::{debug, error, info, warn};

use tgsearcher_app::backend::{ProgressSink, SearchBackend};
use tgsearcher_app::render::truncate_telegram;
use tgsearcher_core::config::{BackendEntry, CommonConfig, FuzzyConfig, ImageConfig};
use tgsearcher_core::image::{image_message, ImageStore};
use tgsearcher_core::text::{escape_html, normalize_content, share_id_from_peer_id, IdOrName};
use tgsearcher_core::{
    ChatId, DownloadProgress, DownloadReport, ImageSearchMode, ImageSearchResult, IndexedMessage,
    MonitorReport, SearchRequest, SearchResult, TextIndex,
};

use crate::session::{peer_id_candidates, UserSession};

struct State {
    monitored: BTreeSet<ChatId>,
    excluded: BTreeSet<ChatId>,
    newest: HashMap<ChatId, IndexedMessage>,
    titles: HashMap<ChatId, String>,
    peers: HashMap<ChatId, PeerRef>,
}

pub struct UserbotBackend {
    id: String,
    session: Arc<UserSession>,
    index: Arc<Mutex<TextIndex>>,
    images: Arc<Mutex<ImageStore>>,
    state: Mutex<State>,
    cfg: BackendEntry,
    image_cfg: ImageConfig,
    tmp_dir: PathBuf,
}

impl UserbotBackend {
    pub async fn new(
        common: &CommonConfig,
        entry: BackendEntry,
        session: Arc<UserSession>,
        from_scratch: bool,
        fuzzy: FuzzyConfig,
        image_cfg: ImageConfig,
    ) -> Result<Arc<Self>> {
        let paths = common.paths();
        paths.create()?;
        let index = TextIndex::open(paths.index_dir(&entry.id), fuzzy, from_scratch)
            .with_context(|| format!("open text index for backend {}", entry.id))?;
        let images = ImageStore::open(paths.image_dir(&entry.id), image_cfg.clone())
            .with_context(|| format!("open image index for backend {}", entry.id))?;

        let mut monitored: BTreeSet<ChatId> = index.chat_ids()?.into_iter().collect();
        let mut newest = HashMap::new();
        for chat_id in &monitored {
            if let Some(message) = index.newest_message(*chat_id)? {
                newest.insert(*chat_id, message);
            }
        }

        let mut titles = HashMap::new();
        let mut peers = HashMap::new();
        match session.dialogs().await {
            Ok(dialogs) => {
                for dialog in dialogs {
                    if !dialog.title.is_empty() {
                        titles.insert(dialog.chat_id, dialog.title);
                    }
                    peers.insert(dialog.chat_id, dialog.peer);
                }
            }
            Err(e) => warn!("cannot load dialogs for title cache: {e}"),
        }

        // Resolve configured exclusions.
        let mut excluded = BTreeSet::new();
        for item in &entry.config.exclude_chats {
            match resolve_exclusion(session.as_ref(), item).await {
                Ok(chat_id) => {
                    excluded.insert(chat_id);
                    info!("excluded chat {chat_id}");
                }
                Err(e) => warn!("cannot resolve exclude_chats entry {item:?}: {e}"),
            }
        }

        // Excluded chats are removed from monitoring.
        for chat_id in excluded.iter() {
            if monitored.remove(chat_id) {
                newest.remove(chat_id);
                info!("chat {chat_id} is excluded and will not be monitored");
            }
        }

        let backend = Arc::new(Self {
            id: entry.id.clone(),
            session: Arc::clone(&session),
            index: Arc::new(Mutex::new(index)),
            images: Arc::new(Mutex::new(images)),
            state: Mutex::new(State {
                monitored,
                excluded,
                newest,
                titles,
                peers,
            }),
            cfg: entry,
            image_cfg,
            tmp_dir: paths.tmp,
        });

        backend.validate_monitored().await;
        Ok(backend)
    }

    pub fn session(&self) -> &Arc<UserSession> {
        &self.session
    }

    pub fn share_id_of_self(&self) -> ChatId {
        self.session.me.id
    }

    /// Start consuming updates for this backend alone.
    pub fn spawn_updates(self: &Arc<Self>, updates: Receiver<UpdatesLike>) {
        let client = self.session.client.clone();
        spawn_update_dispatch(client, updates, vec![Arc::clone(self)]);
    }

    async fn validate_monitored(&self) {
        let monitored: Vec<ChatId> = {
            let state = self.state.lock().expect("state mutex poisoned");
            state.monitored.iter().copied().collect()
        };
        let mut remove = Vec::new();
        for chat_id in monitored {
            let has_title = {
                let state = self.state.lock().expect("state mutex poisoned");
                state.titles.contains_key(&chat_id)
            };
            if has_title {
                continue;
            }
            match self.resolve_peer_input(&chat_id.to_string()).await {
                Ok((_, _, Some(title))) => {
                    let mut state = self.state.lock().expect("state mutex poisoned");
                    state.titles.insert(chat_id, title);
                }
                Ok(_) => {}
                Err(e) => {
                    warn!("monitored chat {chat_id} is not accessible: {e}");
                    remove.push(chat_id);
                }
            }
        }
        if !remove.is_empty() {
            let mut state = self.state.lock().expect("state mutex poisoned");
            for chat_id in &remove {
                state.monitored.remove(chat_id);
                state.newest.remove(chat_id);
            }
            info!(count = remove.len(), "removed inaccessible monitored chats");
        }
    }

    /// Resolve numeric ids, @usernames and t.me links to a share id + PeerRef.
    pub async fn resolve_peer_input(
        &self,
        input: &str,
    ) -> Result<(ChatId, PeerRef, Option<String>)> {
        let input = input.trim();
        if input.is_empty() {
            bail!("对话不能为空");
        }
        let numeric = parse_tme_id(input).or_else(|| input.parse::<i64>().ok());
        if let Some(raw) = numeric {
            let share = share_id_from_peer_id(raw);
            let cached = {
                let state = self.state.lock().expect("state mutex poisoned");
                state
                    .peers
                    .get(&share)
                    .copied()
                    .map(|peer| (peer, state.titles.get(&share).cloned()))
            };
            if let Some((peer, title)) = cached {
                return Ok((share, peer, title));
            }
            let peer = if let Some(peer) = self.session.peer_ref_for_chat_id(share).await {
                peer
            } else {
                let mut found = None;
                for candidate in peer_id_candidates(share) {
                    if let Ok(peer) = self
                        .session
                        .client
                        .resolve_peer(candidate.to_ambient_ref())
                        .await
                    {
                        let _ = peer;
                        if let Ok(Some(peer_ref)) = self.session.session.peer_ref(candidate).await {
                            found = Some(peer_ref);
                            break;
                        }
                    }
                }
                found.ok_or_else(|| {
                    anyhow::anyhow!("找不到对话 {input}，请确认后端账号已加入该对话或传入 @用户名")
                })?
            };
            let title = {
                let state = self.state.lock().expect("state mutex poisoned");
                state.titles.get(&share).cloned()
            };
            let title = match title {
                Some(title) => Some(title),
                None => self.session.fetch_title(peer).await,
            };
            let mut state = self.state.lock().expect("state mutex poisoned");
            state.peers.insert(share, peer);
            if let Some(title) = &title {
                state.titles.insert(share, title.clone());
            }
            return Ok((share, peer, title));
        }

        // Username or t.me/<username>.
        let username = input
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .trim_start_matches("t.me/")
            .trim_start_matches('@');
        let username = username.split(['/', '?', '#']).next().unwrap_or(username);
        let (share, peer, title) = self.session.resolve_username(username).await?;
        let mut state = self.state.lock().expect("state mutex poisoned");
        state.peers.insert(share, peer);
        state.titles.insert(share, title.clone());
        Ok((share, peer, Some(title)))
    }

    fn should_monitor(&self, chat_id: ChatId) -> bool {
        let state = self.state.lock().expect("state mutex poisoned");
        if state.excluded.contains(&chat_id) {
            return false;
        }
        self.cfg.config.monitor_all || state.monitored.contains(&chat_id)
    }

    fn sender_name(message: &grammers_client::message::Message) -> String {
        message
            .sender()
            .and_then(|peer| peer.name())
            .unwrap_or_default()
            .to_string()
    }

    fn media_filename(media: &Media) -> Option<String> {
        match media {
            Media::Document(document) => document.name().map(ToString::to_string),
            _ => None,
        }
    }

    fn media_is_image(media: &Media) -> bool {
        match media {
            Media::Photo(_) => true,
            Media::Document(document) => document
                .mime_type()
                .map(|mime| mime.starts_with("image/"))
                .unwrap_or(false),
            _ => false,
        }
    }

    fn media_size(media: &Media) -> Option<usize> {
        match media {
            Media::Photo(photo) => photo.size(),
            Media::Document(document) => document.size(),
            _ => None,
        }
    }

    async fn handle_update(&self, update: Update) -> Result<()> {
        match update {
            Update::NewMessage(message) => {
                let Some(bare) = message.peer_id().bare_id() else {
                    return Ok(());
                };
                let chat_id = share_id_from_peer_id(bare);
                if !self.should_monitor(chat_id) {
                    return Ok(());
                }
                if !message.text().trim().is_empty() || message.media().is_some() {
                    if let Some(indexed) = self.index_message(&message, true, true).await? {
                        debug!(url = %indexed.url, "indexed live message");
                    }
                }
                Ok(())
            }
            Update::MessageEdited(message) => self.handle_edited(&message).await,
            Update::MessageDeleted(deletion) => self.handle_deleted(&deletion).await,
            _ => Ok(()),
        }
    }

    async fn handle_edited(&self, message: &grammers_client::message::Message) -> Result<()> {
        let Some(bare) = message.peer_id().bare_id() else {
            return Ok(());
        };
        let chat_id = share_id_from_peer_id(bare);
        if !self.should_monitor(chat_id) {
            return Ok(());
        }
        let url = format!("https://t.me/c/{chat_id}/{}", message.id());
        let content = normalize_content(message.text());
        let old = {
            let index = self.index();
            index.get_message(&url)?
        };
        let indexed = match old {
            Some(mut existing) => {
                if existing.content == content {
                    return Ok(());
                }
                existing.content = content;
                existing
            }
            None => {
                if content.is_empty() {
                    return Ok(());
                }
                IndexedMessage {
                    url: url.clone(),
                    chat_id,
                    content,
                    post_time: message.date().as_second(),
                    sender: Self::sender_name(message),
                    filename: message.media().as_ref().and_then(Self::media_filename),
                }
            }
        };
        {
            let mut index = self.index();
            index.add_message(&indexed)?;
        }
        {
            let mut images = self.images();
            images.update_text(&url, &indexed.content, indexed.filename.as_deref())?;
        }
        let mut state = self.state.lock().expect("state mutex poisoned");
        if state
            .newest
            .get(&chat_id)
            .is_some_and(|newest| newest.url == url)
        {
            state.newest.insert(chat_id, indexed);
        }
        Ok(())
    }

    async fn handle_deleted(
        &self,
        deletion: &grammers_client::update::MessageDeletion,
    ) -> Result<()> {
        let ids: Vec<i32> = deletion.messages().to_vec();
        if ids.is_empty() {
            return Ok(());
        }
        let chats: Vec<ChatId> = match deletion.channel_id() {
            Some(channel_id) => vec![channel_id],
            None => {
                let state = self.state.lock().expect("state mutex poisoned");
                state.monitored.iter().copied().collect()
            }
        };
        for chat_id in chats {
            let mut urls = Vec::new();
            for id in &ids {
                let url = format!("https://t.me/c/{chat_id}/{id}");
                let exists = {
                    let index = self.index();
                    index.get_message(&url)?.is_some()
                };
                if exists {
                    urls.push(url);
                }
            }
            if urls.is_empty() {
                continue;
            }
            {
                let mut index = self.index();
                index.delete_urls(&urls)?;
            }
            {
                let mut images = self.images();
                for url in &urls {
                    images.remove_url(url)?;
                }
            }
            let mut state = self.state.lock().expect("state mutex poisoned");
            if state
                .newest
                .get(&chat_id)
                .is_some_and(|newest| urls.contains(&newest.url))
            {
                state.newest.remove(&chat_id);
                if let Some(message) = self.index().newest_message(chat_id)? {
                    state.newest.insert(chat_id, message);
                }
            }
        }
        Ok(())
    }

    /// Convert and index one Telegram message. Returns the indexed message when
    /// it had any text, filename or image.
    async fn index_message(
        &self,
        message: &grammers_client::message::Message,
        allow_images: bool,
        update_newest: bool,
    ) -> Result<Option<IndexedMessage>> {
        let Some(bare) = message.peer_id().bare_id() else {
            return Ok(None);
        };
        let chat_id = share_id_from_peer_id(bare);
        let media = message.media();
        let filename = media.as_ref().and_then(Self::media_filename);
        let content = normalize_content(message.text());
        let is_image = media.as_ref().map(Self::media_is_image).unwrap_or(false);
        if content.is_empty() && filename.is_none() && !is_image {
            return Ok(None);
        }
        let url = format!("https://t.me/c/{chat_id}/{}", message.id());
        let indexed = IndexedMessage {
            url: url.clone(),
            chat_id,
            content: content.clone(),
            post_time: message.date().as_second(),
            sender: Self::sender_name(message),
            filename: filename.clone(),
        };
        {
            let mut index = self.index();
            index.add_message(&indexed)?;
        }
        if update_newest {
            let mut state = self.state.lock().expect("state mutex poisoned");
            let newer = state
                .newest
                .get(&chat_id)
                .map(|existing| indexed.post_time >= existing.post_time)
                .unwrap_or(true);
            if newer {
                state.newest.insert(chat_id, indexed.clone());
            }
        }

        if let Some(media) = media {
            if allow_images && self.image_cfg.enabled && is_image {
                let size_ok = Self::media_size(&media)
                    .map(|size| size as u64 <= self.image_cfg.max_download_bytes)
                    .unwrap_or(true);
                if !size_ok {
                    debug!(url, "skipping oversized image media");
                } else {
                    let temp = self.tmp_dir.join(format!(
                        "img-{chat_id}-{}-{}.bin",
                        message.id(),
                        now_nanos()
                    ));
                    match message.download_media(&temp).await {
                        Ok(true) => {
                            let image_meta = image_message(
                                &url,
                                chat_id,
                                indexed.post_time,
                                &indexed.sender,
                                &content,
                                filename.clone(),
                                0,
                                0,
                            );
                            let result = {
                                let mut images = self.images();
                                images.add_image(&temp, image_meta)
                            };
                            if let Err(e) = result {
                                warn!(url, "cannot index image: {e}");
                            }
                        }
                        Ok(false) => warn!(url, "Telegram returned no downloadable media"),
                        Err(e) => warn!(url, "cannot download media: {e}"),
                    }
                    let _ = tokio::fs::remove_file(&temp).await;
                }
            }
        }
        Ok(Some(indexed))
    }

    fn index(&self) -> std::sync::MutexGuard<'_, TextIndex> {
        self.index
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn images(&self) -> std::sync::MutexGuard<'_, ImageStore> {
        self.images
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    async fn status_inner(&self, length_limit: usize) -> String {
        let total = {
            let index = self.index();
            index.total_documents()
        };
        let (excluded, monitored, newest, image_count) = {
            let state = self.state.lock().expect("state mutex poisoned");
            let excluded: Vec<ChatId> = state.excluded.iter().copied().collect();
            let monitored: Vec<ChatId> = state
                .monitored
                .iter()
                .copied()
                .filter(|id| !state.excluded.contains(id))
                .collect();
            let newest = state.newest.clone();
            drop(state);
            let images = self.images();
            (excluded, monitored, newest, images.len())
        };

        let mut out = format!(
            "后端 \"{}\" (会话: \"{}\") 总消息: <b>{total}</b>\n\n",
            escape_html(&self.id),
            escape_html(&self.session.name)
        );
        if !excluded.is_empty() {
            out.push_str(&format!("{} 个对话被禁止索引:\n", excluded.len()));
            for chat_id in excluded.iter().take(50) {
                let title = self
                    .chat_title(*chat_id)
                    .await
                    .unwrap_or_else(|| format!("对话 {chat_id}"));
                out.push_str(&format!(
                    "- <a href=\"https://t.me/c/{chat_id}/1\">{}</a> (`{chat_id}`)\n",
                    escape_html(&title)
                ));
            }
            out.push('\n');
        }
        out.push_str(&format!(
            "总计 {} 个对话被加入了索引 (且未被排除):\n",
            monitored.len()
        ));
        for chat_id in monitored.iter().take(100) {
            let title = self
                .chat_title(*chat_id)
                .await
                .unwrap_or_else(|| format!("未知对话 ({chat_id})"));
            let count = {
                let index = self.index();
                index.chat_count(*chat_id).unwrap_or(0)
            };
            out.push_str(&format!(
                "- <a href=\"https://t.me/c/{chat_id}/1\">{}</a> (`{chat_id}`) 共 {count} 条消息\n",
                escape_html(&title)
            ));
            if let Some(message) = newest.get(chat_id) {
                let time = Utc
                    .timestamp_opt(message.post_time, 0)
                    .single()
                    .map(|time| time.format("%y-%m-%d %H:%M").to_string())
                    .unwrap_or_else(|| "[未知时间]".into());
                let display = if message.content.is_empty() {
                    "(空消息)".to_string()
                } else {
                    message.content.chars().take(50).collect::<String>()
                };
                out.push_str(&format!(
                    "  最新: <a href=\"{}\">{}</a> (@{time})\n",
                    escape_html(&message.url),
                    escape_html(&display)
                ));
            }
        }
        if image_count > 0 {
            out.push_str(&format!(
                "\n图片索引: <b>{image_count}</b> 张 (相似度阈值 {:.0}%)",
                self.image_cfg.effective_min_similarity() * 100.0
            ));
        }
        truncate_telegram(&out, length_limit.max(200))
    }
}

fn parse_tme_id(input: &str) -> Option<i64> {
    let trimmed = input
        .trim()
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_start_matches("t.me/");
    let rest = trimmed.strip_prefix("c/")?;
    let id = rest.split('/').next()?;
    id.parse().ok()
}

async fn resolve_exclusion(session: &UserSession, item: &IdOrName) -> Result<ChatId> {
    if let Some(id) = item.as_id() {
        return Ok(share_id_from_peer_id(id));
    }
    let (share, _, _) = session.resolve_username(&item.as_string()).await?;
    Ok(share)
}

fn now_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0)
}

/// Fan out one session's update stream to all backends that share it.
pub fn spawn_update_dispatch(
    client: grammers_client::Client,
    updates: Receiver<UpdatesLike>,
    backends: Vec<Arc<UserbotBackend>>,
) {
    tokio::spawn(async move {
        match client
            .stream_updates(
                updates,
                grammers_mtsender::UpdatesConfiguration { catch_up: true },
            )
            .await
        {
            Ok(mut stream) => loop {
                match stream.next().await {
                    Ok(update) => {
                        for backend in &backends {
                            if let Err(e) = backend.handle_update(update.clone()).await {
                                error!(
                                    backend = %backend.id,
                                    ?e,
                                    "error handling Telegram update"
                                );
                            }
                        }
                    }
                    Err(e) => {
                        warn!("update stream error: {e}; retrying");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            },
            Err(e) => error!("cannot start update stream: {e}"),
        }
    });
}

#[async_trait]
impl SearchBackend for UserbotBackend {
    fn id(&self) -> &str {
        &self.id
    }

    async fn search(&self, request: SearchRequest) -> Result<SearchResult> {
        let index = Arc::clone(&self.index);
        let result = tokio::task::spawn_blocking(move || {
            let index = index
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            index.search(&request)
        })
        .await
        .context("search task panicked")??;
        Ok(result)
    }

    async fn random_message(&self) -> Result<Option<IndexedMessage>> {
        let index = Arc::clone(&self.index);
        let result = tokio::task::spawn_blocking(move || {
            let index = index
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            index.random_message()
        })
        .await
        .context("random task panicked")??;
        Ok(result)
    }

    async fn status(&self, length_limit: usize) -> String {
        self.status_inner(length_limit).await
    }

    async fn monitored_chats(&self) -> Vec<ChatId> {
        self.state
            .lock()
            .expect("state mutex poisoned")
            .monitored
            .iter()
            .copied()
            .collect()
    }

    async fn excluded_chats(&self) -> Vec<ChatId> {
        self.state
            .lock()
            .expect("state mutex poisoned")
            .excluded
            .iter()
            .copied()
            .collect()
    }

    async fn chat_title(&self, chat_id: ChatId) -> Option<String> {
        let cached = {
            let state = self.state.lock().expect("state mutex poisoned");
            state.titles.get(&chat_id).cloned()
        };
        if cached.is_some() {
            return cached;
        }
        match tokio::time::timeout(
            Duration::from_secs(8),
            self.resolve_peer_input(&chat_id.to_string()),
        )
        .await
        {
            Ok(Ok((_, _, title))) => title,
            _ => None,
        }
    }

    async fn resolve_chat_id(&self, input: &str) -> Result<ChatId> {
        Ok(self.resolve_peer_input(input).await?.0)
    }

    async fn find_chat_ids(&self, query: &str) -> Vec<ChatId> {
        let query = query.to_lowercase();
        let state = self.state.lock().expect("state mutex poisoned");
        let mut ids: Vec<ChatId> = state
            .titles
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
        let started = Instant::now();
        let (chat_id, peer_ref, title) = self.resolve_peer_input(chat).await?;
        if self
            .state
            .lock()
            .expect("state mutex poisoned")
            .excluded
            .contains(&chat_id)
        {
            bail!("对话 {chat_id} 已被排除，无法下载");
        }
        let was_new = {
            let mut state = self.state.lock().expect("state mutex poisoned");
            state.monitored.insert(chat_id)
        };
        if let Some(title) = title {
            self.state
                .lock()
                .expect("state mutex poisoned")
                .titles
                .insert(chat_id, title);
        }
        info!(chat_id, min_id, max_id, "starting history download");

        let mut iterator = self.session.client.iter_messages(peer_ref);
        if max_id > 0 {
            let offset = max_id.saturating_add(1).min(i32::MAX as i64) as i32;
            iterator = iterator.offset_id(offset);
        }

        let mut processed = 0usize;
        let mut indexed = 0usize;
        let mut images_indexed = 0usize;
        let mut batch: Vec<IndexedMessage> = Vec::new();
        let mut newest: Option<IndexedMessage> = None;
        let mut images_in_chat = {
            let images = self.images();
            images.count_chat(chat_id)
        };

        let result: Result<()> = async {
            while let Some(telegram_message) = iterator.next().await? {
                let id = telegram_message.id();
                if id <= min_id as i32 {
                    break;
                }
                if max_id > 0 && id > max_id as i32 {
                    continue;
                }
                processed += 1;
                let media = telegram_message.media();
                let filename = media.as_ref().and_then(Self::media_filename);
                let content = normalize_content(telegram_message.text());
                let is_image = media.as_ref().map(Self::media_is_image).unwrap_or(false);
                if content.is_empty() && filename.is_none() && !is_image {
                    continue;
                }
                let url = format!("https://t.me/c/{chat_id}/{id}");
                let indexed_message = IndexedMessage {
                    url: url.clone(),
                    chat_id,
                    content: content.clone(),
                    post_time: telegram_message.date().as_second(),
                    sender: Self::sender_name(&telegram_message),
                    filename: filename.clone(),
                };
                if newest
                    .as_ref()
                    .map(|current| indexed_message.post_time >= current.post_time)
                    .unwrap_or(true)
                {
                    newest = Some(indexed_message.clone());
                }
                batch.push(indexed_message.clone());
                indexed += 1;

                if self.image_cfg.index_history
                    && self.image_cfg.enabled
                    && is_image
                    && (self.image_cfg.max_images_per_chat == 0
                        || images_in_chat < self.image_cfg.max_images_per_chat)
                {
                    if let Some(media) = media.as_ref() {
                        let size_ok = Self::media_size(media)
                            .map(|size| size as u64 <= self.image_cfg.max_download_bytes)
                            .unwrap_or(true);
                        if size_ok {
                            let temp = self
                                .tmp_dir
                                .join(format!("img-{chat_id}-{id}-{}.bin", now_nanos()));
                            match telegram_message.download_media(&temp).await {
                                Ok(true) => {
                                    let image_meta = image_message(
                                        &url,
                                        chat_id,
                                        indexed_message.post_time,
                                        &indexed_message.sender,
                                        &content,
                                        filename.clone(),
                                        0,
                                        0,
                                    );
                                    let added = {
                                        let mut images = self.images();
                                        images.add_image_deferred(&temp, image_meta)
                                    };
                                    match added {
                                        Ok(_) => {
                                            images_indexed += 1;
                                            images_in_chat += 1;
                                        }
                                        Err(e) => warn!(url, "cannot index image: {e}"),
                                    }
                                }
                                Ok(false) => {}
                                Err(e) => warn!(url, "cannot download image: {e}"),
                            }
                            let _ = tokio::fs::remove_file(&temp).await;
                        }
                    }
                }

                if batch.len() >= 200 {
                    let mut index = self.index();
                    index.upsert_messages(&batch)?;
                    batch.clear();
                    debug!(chat_id, indexed, "flushed index batch");
                }
                if processed.is_multiple_of(100) {
                    progress(DownloadProgress {
                        chat_id,
                        current_message_id: id as i64,
                        downloaded: indexed,
                        processed,
                    });
                }
            }
            if !batch.is_empty() {
                let mut index = self.index();
                index.upsert_messages(&batch)?;
            }
            {
                let mut images = self.images();
                images.flush()?;
            }
            Ok(())
        }
        .await;

        if let Err(e) = result {
            if was_new {
                self.state
                    .lock()
                    .expect("state mutex poisoned")
                    .monitored
                    .remove(&chat_id);
            }
            return Err(e);
        }

        if let Some(newest) = newest {
            let mut state = self.state.lock().expect("state mutex poisoned");
            let should_update = state
                .newest
                .get(&chat_id)
                .map(|existing| newest.post_time >= existing.post_time)
                .unwrap_or(true);
            if should_update {
                state.newest.insert(chat_id, newest);
            }
        }
        progress(DownloadProgress {
            chat_id,
            current_message_id: 0,
            downloaded: indexed,
            processed,
        });
        info!(
            chat_id,
            processed,
            indexed,
            images_indexed,
            elapsed_ms = started.elapsed().as_millis(),
            "history download complete"
        );
        Ok(DownloadReport {
            chat_id: Some(chat_id),
            processed,
            indexed,
            images_indexed,
            elapsed_ms: started.elapsed().as_millis(),
        })
    }
    async fn add_monitoring(&self, chat_ids: &[ChatId]) -> Result<MonitorReport> {
        let mut state = self.state.lock().expect("state mutex poisoned");
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
        match chat_ids {
            Some(ids) => {
                let mut removed = 0usize;
                for chat_id in ids {
                    removed += {
                        let index = self.index();
                        index.chat_count(chat_id).unwrap_or(0)
                    };
                    {
                        let mut index = self.index();
                        index.delete_chat(chat_id)?;
                    }
                    {
                        let mut images = self.images();
                        images.remove_chat(chat_id)?;
                    }
                    let mut state = self.state.lock().expect("state mutex poisoned");
                    state.monitored.remove(&chat_id);
                    state.newest.remove(&chat_id);
                }
                Ok(removed)
            }
            None => {
                let removed = {
                    let index = self.index();
                    index.total_documents()
                };
                {
                    let mut index = self.index();
                    index.delete_all()?;
                }
                {
                    let mut images = self.images();
                    images.clear()?;
                }
                let mut state = self.state.lock().expect("state mutex poisoned");
                state.monitored.clear();
                state.newest.clear();
                Ok(removed)
            }
        }
    }

    async fn refresh_chat_names(&self) -> Result<()> {
        let dialogs = self.session.dialogs().await?;
        let mut state = self.state.lock().expect("state mutex poisoned");
        for dialog in dialogs {
            if !dialog.title.is_empty() {
                state.titles.insert(dialog.chat_id, dialog.title);
            }
            state.peers.insert(dialog.chat_id, dialog.peer);
        }
        Ok(())
    }

    async fn search_image(
        &self,
        image: &std::path::Path,
        _mode: ImageSearchMode,
        chats: Option<&[ChatId]>,
    ) -> Result<ImageSearchResult> {
        let images = Arc::clone(&self.images);
        let path = image.to_path_buf();
        let chats = chats.map(ToOwned::to_owned);
        let result = tokio::task::spawn_blocking(move || {
            let images = images
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            images.search_path(&path, chats.as_deref())
        })
        .await
        .context("image search task panicked")??;
        Ok(result)
    }

    async fn exclude_chat(&self, chat_id: ChatId) {
        let mut state = self.state.lock().expect("state mutex poisoned");
        state.excluded.insert(chat_id);
        state.monitored.remove(&chat_id);
        state.newest.remove(&chat_id);
        info!("chat {chat_id} added to the exclusion list");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_tme_links() {
        assert_eq!(
            parse_tme_id("https://t.me/c/1234567890/42"),
            Some(1234567890)
        );
        assert_eq!(
            parse_tme_id("t.me/c/-1001234567890/7"),
            Some(-1001234567890)
        );
        assert_eq!(parse_tme_id("https://t.me/somechannel"), None);
        assert_eq!(parse_tme_id("12345"), None);
    }

    #[test]
    fn peer_candidates_cover_all_kinds() {
        let candidates = peer_id_candidates(123456);
        assert_eq!(candidates.len(), 3);
        assert!(candidates
            .iter()
            .any(|peer| peer.kind() == grammers_session::types::PeerKind::Channel));
        assert!(candidates
            .iter()
            .any(|peer| peer.kind() == grammers_session::types::PeerKind::Chat));
        assert!(candidates
            .iter()
            .any(|peer| peer.kind() == grammers_session::types::PeerKind::User));
    }

    #[test]
    fn marked_ids_become_share_ids() {
        assert_eq!(share_id_from_peer_id(-1001234567890), 1234567890);
        assert_eq!(share_id_from_peer_id(1234567890), 1234567890);
    }
}
