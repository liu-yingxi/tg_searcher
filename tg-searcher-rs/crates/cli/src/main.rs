//! Entry point: load the YAML configuration, connect the userbot sessions and
//! start the bot frontends.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::Parser;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use tgsearcher_app::SearchBackend;
use tgsearcher_core::config::{Config, IdOrName};
use tgsearcher_core::text::share_id_from_peer_id;
use tgsearcher_telegram::{
    bot_smoke_test, spawn_update_dispatch, BotFrontend, BotFrontendConfig, UserSession,
    UserbotBackend,
};

#[derive(Debug, Parser)]
#[command(
    name = "tg-searcher",
    version,
    about = "Telegram message search bot with Chinese fuzzy search and OpenCV image search"
)]
struct Args {
    /// Clear existing text/image indexes before starting.
    #[arg(short = 'c', long)]
    clear: bool,

    /// Path to the YAML configuration file.
    #[arg(short = 'f', long, default_value = "searcher.yaml")]
    config: PathBuf,

    /// Enable debug logging.
    #[arg(long)]
    debug: bool,

    /// Validate the configuration and print a summary, then exit.
    #[arg(long)]
    check: bool,

    /// Verify the first frontend's bot token against the Telegram Bot API and exit.
    #[arg(long)]
    bot_smoke: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    init_logging(args.debug);

    let mut config = Config::load(&args.config)
        .with_context(|| format!("load config {}", args.config.display()))?;
    config.search.fuzzy.apply_env_override();
    config.search.fuzzy.validate()?;
    config.common.paths().create()?;

    info!(
        config = %args.config.display(),
        name = %config.common.name,
        fuzzy_threshold = config.search.fuzzy.threshold,
        image_search = config.image.enabled,
        "configuration loaded"
    );

    if args.check {
        print_summary(&config);
        return Ok(());
    }

    if args.bot_smoke {
        return run_bot_smoke(&config).await;
    }

    run(&config, args.clear).await
}

async fn run(config: &Config, clear: bool) -> Result<()> {
    let proxy = match &config.common.proxy {
        Some(proxy) => Some(
            tgsearcher_core::config::parse_proxy(proxy)
                .map_err(|e| anyhow::anyhow!(e.to_string()))?,
        ),
        None => None,
    };

    // 1. Log in every configured user session.
    let mut sessions: HashMap<String, Arc<UserSession>> = HashMap::new();
    for session_cfg in &config.sessions {
        let session = UserSession::connect(&config.common, session_cfg, proxy.as_ref())
            .await
            .with_context(|| format!("connect session `{}`", session_cfg.name))?;
        sessions.insert(session_cfg.name.clone(), session);
    }

    // 2. Build backends and group them by the session whose updates they need.
    let mut backends: HashMap<String, Arc<UserbotBackend>> = HashMap::new();
    let mut backends_by_session: HashMap<String, Vec<Arc<UserbotBackend>>> = HashMap::new();
    for entry in &config.backends {
        let session = sessions
            .get(&entry.use_session)
            .ok_or_else(|| anyhow::anyhow!("backend `{}` has no session", entry.id))?
            .clone();
        let backend = UserbotBackend::new(
            &config.common,
            entry.clone(),
            session,
            clear,
            config.search.fuzzy.clone(),
            config.image.clone(),
        )
        .await
        .with_context(|| format!("start backend `{}`", entry.id))?;
        backends_by_session
            .entry(entry.use_session.clone())
            .or_default()
            .push(Arc::clone(&backend));
        backends.insert(entry.id.clone(), backend);
    }

    // 3. Start one update dispatcher per session (all backends sharing it get
    //    every update).
    for (session_name, backends) in backends_by_session {
        let Some(session) = sessions.get(&session_name) else {
            continue;
        };
        let Some(updates) = session.take_updates().await else {
            warn!(session = %session_name, "update receiver already taken");
            continue;
        };
        spawn_update_dispatch(session.client.clone(), updates, backends);
        info!(session = %session_name, "Telegram update dispatcher started");
    }

    // 4. Start bot frontends.
    let mut frontends = Vec::new();
    for entry in &config.frontends {
        let backend = backends
            .get(&entry.use_backend)
            .ok_or_else(|| anyhow::anyhow!("frontend `{}` has no backend", entry.id))?
            .clone();
        let backend_dyn: Arc<dyn SearchBackend> = backend.clone();
        let admin_id = resolve_admin(&entry.config.admin_id, backend_dyn.as_ref()).await;

        let frontend_cfg = BotFrontendConfig {
            id: entry.id.clone(),
            bot_token: entry.config.bot_token.clone(),
            admin_id,
            page_len: entry.config.page_len,
            private_mode: entry.config.private_mode,
            private_whitelist: entry.config.private_whitelist.clone(),
            redis: entry.config.redis.clone(),
            no_redis: entry.config.no_redis,
            proxy: config.common.proxy.clone(),
            send_photo_limit: config.image.send_photo_limit,
            tmp_dir: config.common.paths().tmp.join(&entry.id),
        };
        let frontend = BotFrontend::start(frontend_cfg, backend_dyn)
            .await
            .with_context(|| format!("start frontend `{}`", entry.id))?;
        frontends.push(frontend);
    }
    if frontends.is_empty() {
        bail!("no frontend started");
    }

    info!(count = frontends.len(), "all frontends started");

    let mut tasks = tokio::task::JoinSet::new();
    for frontend in frontends {
        tasks.spawn(async move { frontend.run().await });
    }
    let mut last_error: Option<anyhow::Error> = None;
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                error!(?e, "frontend stopped with an error");
                last_error = Some(e);
            }
            Err(e) => {
                error!(?e, "frontend task panicked");
                last_error = Some(anyhow::anyhow!("frontend task panicked: {e}"));
            }
        }
    }
    match last_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

async fn resolve_admin(admin: &IdOrName, backend: &dyn SearchBackend) -> i64 {
    if let Some(id) = admin.as_id() {
        return share_id_from_peer_id(id);
    }
    let input = admin.as_string();
    match backend.resolve_chat_id(&input).await {
        Ok(id) => id,
        Err(e) => {
            warn!("cannot resolve admin `{input}`: {e}; admin commands disabled");
            0
        }
    }
}

async fn run_bot_smoke(config: &Config) -> Result<()> {
    let Some(frontend) = config.frontends.first() else {
        bail!("no frontend configured");
    };
    match bot_smoke_test(&frontend.config.bot_token, config.common.proxy.as_deref()).await {
        Ok((username, id)) => {
            println!("bot token OK: @{username} (id {id})");
            Ok(())
        }
        Err(e) => Err(e).context("bot token smoke test failed"),
    }
}

fn print_summary(config: &Config) {
    println!("configuration OK");
    println!("  instance: {}", config.common.name);
    println!("  runtime:  {}", config.common.runtime_dir.display());
    println!("  sessions: {}", config.sessions.len());
    println!(
        "  backends: {}",
        config
            .backends
            .iter()
            .map(|backend| backend.id.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!(
        "  frontends: {}",
        config
            .frontends
            .iter()
            .map(|frontend| frontend.id.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!(
        "  fuzzy:    mode={:?} threshold={:.2}",
        config.search.fuzzy.mode, config.search.fuzzy.threshold
    );
    println!(
        "  image:    enabled={} min_similarity={:.2}",
        config.image.enabled,
        config.image.effective_min_similarity()
    );
}

fn init_logging(debug: bool) {
    let default = if debug { "debug" } else { "info" };
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("{default},grammers_session=warn")));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}
