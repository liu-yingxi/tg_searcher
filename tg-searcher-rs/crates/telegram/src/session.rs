//! Grammers (MTProto) user session handling: persistent SQLite session,
//! first-run interactive login and a dialog cache warm-up.

use std::io::Write;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use grammers_client::{Client, SignInError};
use grammers_mtsender::{ConnectionParams, SenderPool};
use grammers_session::storages::SqliteSession;
use grammers_session::types::PeerRef;
use grammers_session::updates::UpdatesLike;
use grammers_session::Session;
use tokio::sync::Mutex;
use tracing::{info, warn};

use tgsearcher_core::config::{CommonConfig, ProxyConfig, ProxyScheme, SessionConfig};
use tgsearcher_core::text::share_id_from_peer_id;

#[derive(Clone, Debug)]
pub struct DialogInfo {
    pub chat_id: i64,
    pub title: String,
    pub peer: PeerRef,
}

#[derive(Clone, Debug)]
pub struct SelfInfo {
    pub id: i64,
    pub username: Option<String>,
    pub name: String,
}

pub struct UserSession {
    pub name: String,
    pub client: Client,
    pub session: Arc<SqliteSession>,
    pub me: SelfInfo,
    updates: Mutex<Option<tokio::sync::mpsc::Receiver<UpdatesLike>>>,
    _runner: tokio::task::JoinHandle<()>,
}

impl UserSession {
    pub async fn connect(
        common: &CommonConfig,
        session_cfg: &SessionConfig,
        proxy: Option<&ProxyConfig>,
    ) -> Result<Arc<Self>> {
        let paths = common.paths();
        paths.create()?;
        let session_file = paths.session_file(&session_cfg.name);
        let session = Arc::new(
            SqliteSession::open(&session_file)
                .await
                .with_context(|| format!("open session {}", session_file.display()))?,
        );

        let proxy_url = proxy.and_then(|proxy| match proxy.scheme {
            ProxyScheme::Socks5 => {
                let mut url = String::from("socks5://");
                if let (Some(user), Some(password)) = (&proxy.username, &proxy.password) {
                    url.push_str(&urlencoding(user));
                    url.push(':');
                    url.push_str(&urlencoding(password));
                    url.push('@');
                }
                url.push_str(&proxy.host);
                url.push(':');
                url.push_str(&proxy.port.to_string());
                Some(url)
            }
            ProxyScheme::Http => {
                warn!(
                    "grammers supports SOCKS5 proxies only; ignoring HTTP proxy {}:{} for the backend",
                    proxy.host, proxy.port
                );
                None
            }
        });

        let connection_params = ConnectionParams {
            proxy_url,
            ..Default::default()
        };
        let SenderPool {
            runner,
            handle,
            updates,
        } = SenderPool::with_configuration(Arc::clone(&session), common.api_id, connection_params);
        let runner = tokio::spawn(runner.run());
        let client = Client::new(handle);

        let authorized = client
            .is_authorized()
            .await
            .context("check Telegram authorization")?;
        if !authorized {
            info!(
                session = %session_cfg.name,
                "session is not authorized; starting interactive login"
            );
            login(&client, &session_cfg.phone, &common.api_hash).await?;
        }

        let me = client.get_me().await.context("getMe after login")?;
        let id = me.id().bare_id().unwrap_or_default();
        let username = me.username().map(ToString::to_string);
        let name = me.full_name().trim().to_string();
        let me = SelfInfo {
            id,
            username,
            name: if name.is_empty() {
                format!("user {id}")
            } else {
                name
            },
        };
        info!(session = %session_cfg.name, id = me.id, "user session ready");

        Ok(Arc::new(Self {
            name: session_cfg.name.clone(),
            client,
            session,
            me,
            updates: Mutex::new(Some(updates)),
            _runner: runner,
        }))
    }

    /// Hand the raw grammers update receiver to the backend update loop.
    pub async fn take_updates(&self) -> Option<tokio::sync::mpsc::Receiver<UpdatesLike>> {
        self.updates.lock().await.take()
    }

    /// Iterate all dialogs and return share ids, titles and full peer refs.
    pub async fn dialogs(&self) -> Result<Vec<DialogInfo>> {
        let mut out = Vec::new();
        let mut iterator = self.client.iter_dialogs();
        while let Some(dialog) = iterator.next().await? {
            let peer = dialog.peer();
            let Some(bare) = peer.id().bare_id() else {
                continue;
            };
            let chat_id = share_id_from_peer_id(bare);
            if chat_id <= 0 {
                continue;
            }
            let title = peer.name().unwrap_or_default().to_string();
            let peer_ref = dialog.peer_ref();
            out.push(DialogInfo {
                chat_id,
                title,
                peer: peer_ref,
            });
        }
        info!(count = out.len(), "loaded Telegram dialogs");
        Ok(out)
    }

    /// Resolve a username (`@name` or plain name) to a peer ref and share id.
    pub async fn resolve_username(&self, username: &str) -> Result<(i64, PeerRef, String)> {
        let name = username.trim().trim_start_matches('@');
        let peer = self
            .client
            .resolve_username(name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("找不到用户名 @{name}"))?;
        let peer_ref = peer
            .to_ref()
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .ok_or_else(|| anyhow::anyhow!("无法获取 @{name} 的访问凭证"))?;
        let title = peer.name().unwrap_or(name).to_string();
        let bare = peer_ref.id.bare_id().unwrap_or_default();
        Ok((share_id_from_peer_id(bare), peer_ref, title))
    }

    /// Try known peer kinds for a bare chat id. Returns a full `PeerRef` when
    /// the session has an access hash for it.
    pub async fn peer_ref_for_chat_id(&self, chat_id: i64) -> Option<PeerRef> {
        let candidates = peer_id_candidates(chat_id);
        for peer_id in candidates {
            if let Ok(Some(peer_ref)) = self.session.peer_ref(peer_id).await {
                return Some(peer_ref);
            }
            let ambient = peer_id.to_ambient_ref();
            if let Ok(Some(peer_ref)) = self.session.peer_ref(ambient.id).await {
                return Some(peer_ref);
            }
        }
        None
    }

    /// Resolve a peer id to a title via the network (dialog cache first).
    pub async fn fetch_title(&self, peer_ref: PeerRef) -> Option<String> {
        match self.client.resolve_peer(peer_ref).await {
            Ok(peer) => peer.name().map(ToString::to_string),
            Err(e) => {
                warn!(?e, "cannot resolve peer title");
                None
            }
        }
    }
}

/// Best-effort candidates for a share id. Users, small groups and channels use
/// separate namespaces, so all three are tried.
pub fn peer_id_candidates(chat_id: i64) -> Vec<grammers_session::types::PeerId> {
    let bare = share_id_from_peer_id(chat_id).unsigned_abs() as i64;
    let mut out = Vec::new();
    if let Some(peer) = grammers_session::types::PeerId::channel(bare) {
        out.push(peer);
    }
    if let Some(peer) = grammers_session::types::PeerId::chat(bare) {
        out.push(peer);
    }
    if let Some(peer) = grammers_session::types::PeerId::user(bare) {
        out.push(peer);
    }
    out
}

async fn login(client: &Client, phone: &str, api_hash: &str) -> Result<()> {
    let token = client
        .request_login_code(phone, api_hash)
        .await
        .context("request login code")?;
    let code = prompt("请输入 Telegram 登录验证码: ")?;
    match client.sign_in(&token, code.trim()).await {
        Ok(_) => {
            info!("login succeeded");
            Ok(())
        }
        Err(SignInError::PasswordRequired(password_token)) => {
            let hint = password_token.hint().unwrap_or("none");
            println!("该账号启用了两步验证 (提示: {hint})");
            let password = prompt("请输入两步验证密码: ")?;
            client
                .check_password(password_token, password.trim())
                .await
                .context("check two-factor password")?;
            info!("two-factor login succeeded");
            Ok(())
        }
        Err(SignInError::InvalidCode) => bail!("验证码无效，请删除 session 文件后重试"),
        Err(e) => bail!("登录失败: {e}"),
    }
}

fn prompt(label: &str) -> Result<String> {
    print!("{label}");
    std::io::stdout().flush()?;
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;
    Ok(line)
}

fn urlencoding(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}
