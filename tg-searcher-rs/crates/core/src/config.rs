//! YAML configuration handling.
//!
//! The file format is intentionally compatible with the original Python
//! project, with two additions:
//!
//! * `${ENV_VAR}` placeholders are expanded when the file is loaded, so that
//!   bot tokens and API hashes never have to live in the repository.
//! * a `search.fuzzy` section exposing the fuzzy-search threshold and related
//!   tuning knobs independently of the search implementation.

use std::collections::HashSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use regex::Regex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::text::is_numeric_id;
pub use crate::text::IdOrName;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("cannot read config file {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid YAML: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("environment variable `{0}` used in the config is not set")]
    MissingEnv(String),
    #[error("invalid configuration: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub common: CommonConfig,
    #[serde(default)]
    pub search: SearchConfig,
    #[serde(default)]
    pub image: ImageConfig,
    pub sessions: Vec<SessionConfig>,
    pub backends: Vec<BackendEntry>,
    pub frontends: Vec<FrontendEntry>,
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let text = fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let expanded = interpolate_env(&text)?;
        let cfg: Config = serde_yaml::from_str(&expanded)?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn from_yaml_str(text: &str) -> Result<Self, ConfigError> {
        let expanded = interpolate_env(text)?;
        let cfg: Config = serde_yaml::from_str(&expanded)?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.common.name.trim().is_empty() {
            return Err(ConfigError::Invalid("common.name must not be empty".into()));
        }
        if self.common.api_hash.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "common.api_hash must not be empty".into(),
            ));
        }
        if self.common.api_id <= 0 {
            return Err(ConfigError::Invalid(
                "common.api_id must be positive".into(),
            ));
        }
        if let Some(proxy) = &self.common.proxy {
            parse_proxy(proxy)?;
        }
        self.search.fuzzy.validate()?;
        self.image.validate()?;

        let mut sessions = HashSet::new();
        for session in &self.sessions {
            if !sessions.insert(session.name.as_str()) {
                return Err(ConfigError::Invalid(format!(
                    "duplicated session name `{}`",
                    session.name
                )));
            }
            if session.phone.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "session `{}` has an empty phone number",
                    session.name
                )));
            }
        }

        let mut backends = HashSet::new();
        for backend in &self.backends {
            if !backends.insert(backend.id.as_str()) {
                return Err(ConfigError::Invalid(format!(
                    "duplicated backend id `{}`",
                    backend.id
                )));
            }
            if !sessions.contains(backend.use_session.as_str()) {
                return Err(ConfigError::Invalid(format!(
                    "backend `{}` refers to unknown session `{}`",
                    backend.id, backend.use_session
                )));
            }
        }

        let mut frontends = HashSet::new();
        for frontend in &self.frontends {
            if !frontends.insert(frontend.id.as_str()) {
                return Err(ConfigError::Invalid(format!(
                    "duplicated frontend id `{}`",
                    frontend.id
                )));
            }
            if frontend.kind != "bot" {
                return Err(ConfigError::Invalid(format!(
                    "frontend `{}` has unsupported type `{}` (only `bot` is supported)",
                    frontend.id, frontend.kind
                )));
            }
            if !backends.contains(frontend.use_backend.as_str()) {
                return Err(ConfigError::Invalid(format!(
                    "frontend `{}` refers to unknown backend `{}`",
                    frontend.id, frontend.use_backend
                )));
            }
            if frontend.config.bot_token.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "frontend `{}` has an empty bot_token (did you set the referenced environment variable?)",
                    frontend.id
                )));
            }
            if frontend.config.page_len == 0 {
                return Err(ConfigError::Invalid(format!(
                    "frontend `{}` has page_len = 0",
                    frontend.id
                )));
            }
            if frontend.config.admin_id.as_string().trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "frontend `{}` has an empty admin_id",
                    frontend.id
                )));
            }
        }
        if self.frontends.is_empty() {
            return Err(ConfigError::Invalid(
                "at least one frontend must be configured".into(),
            ));
        }
        Ok(())
    }

    pub fn backend(&self, id: &str) -> Option<&BackendEntry> {
        self.backends.iter().find(|b| b.id == id)
    }

    pub fn session(&self, name: &str) -> Option<&SessionConfig> {
        self.sessions.iter().find(|s| s.name == name)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CommonConfig {
    pub name: String,
    pub runtime_dir: PathBuf,
    #[serde(default)]
    pub proxy: Option<String>,
    pub api_id: i32,
    pub api_hash: String,
}

impl CommonConfig {
    pub fn paths(&self) -> RuntimePaths {
        let base = self.runtime_dir.join(&self.name);
        RuntimePaths {
            base: base.clone(),
            sessions: base.join("session"),
            indexes: base.join("index"),
            images: base.join("images"),
            tmp: base.join("tmp"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimePaths {
    pub base: PathBuf,
    pub sessions: PathBuf,
    pub indexes: PathBuf,
    pub images: PathBuf,
    pub tmp: PathBuf,
}

impl RuntimePaths {
    pub fn create(&self) -> std::io::Result<()> {
        for dir in [
            &self.base,
            &self.sessions,
            &self.indexes,
            &self.images,
            &self.tmp,
        ] {
            fs::create_dir_all(dir)?;
        }
        Ok(())
    }

    pub fn session_file(&self, session_name: &str) -> PathBuf {
        self.sessions.join(format!("{session_name}.session"))
    }

    pub fn index_dir(&self, backend_id: &str) -> PathBuf {
        self.indexes.join(backend_id)
    }

    pub fn image_dir(&self, backend_id: &str) -> PathBuf {
        self.images.join(backend_id)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SessionConfig {
    pub name: String,
    pub phone: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BackendEntry {
    pub id: String,
    pub use_session: String,
    #[serde(default)]
    pub config: BackendOptions,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct BackendOptions {
    /// Index every chat that the backend account is a member of.
    #[serde(default)]
    pub monitor_all: bool,
    /// Chat ids or usernames that must never be indexed.
    #[serde(default)]
    pub exclude_chats: Vec<IdOrName>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FrontendEntry {
    #[serde(rename = "type", default = "default_frontend_kind")]
    pub kind: String,
    pub id: String,
    pub use_backend: String,
    #[serde(default)]
    pub config: FrontendOptions,
}

fn default_frontend_kind() -> String {
    "bot".to_string()
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FrontendOptions {
    /// Numeric user id or username of the administrator.
    pub admin_id: IdOrName,
    /// Bot token. Use `${TG_BOT_TOKEN}` so the secret stays out of the repo.
    pub bot_token: String,
    #[serde(default = "default_page_len")]
    pub page_len: usize,
    /// Redis address (`host:port`). Empty or absent disables Redis.
    #[serde(default)]
    pub redis: Option<String>,
    /// Use the in-memory cache even if Redis is configured.
    #[serde(default, deserialize_with = "de_bool_or_truthy_string")]
    pub no_redis: bool,
    #[serde(default)]
    pub private_mode: bool,
    #[serde(default)]
    pub private_whitelist: Vec<i64>,
}

fn default_page_len() -> usize {
    10
}

impl Default for FrontendOptions {
    fn default() -> Self {
        Self {
            admin_id: IdOrName::Name(String::new()),
            bot_token: String::new(),
            page_len: default_page_len(),
            redis: None,
            no_redis: false,
            private_mode: false,
            private_whitelist: Vec::new(),
        }
    }
}

/// Accept `true`/`false`, but also the accidental `no_redis: host:port`
/// spelling that the original documentation used (any non-empty value means
/// "on").
fn de_bool_or_truthy_string<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Raw {
        Bool(bool),
        Str(String),
    }
    Ok(match Raw::deserialize(deserializer)? {
        Raw::Bool(b) => b,
        Raw::Str(s) => !s.is_empty(),
    })
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct SearchConfig {
    #[serde(default)]
    pub fuzzy: FuzzyConfig,
}

/// Fuzzy-search tuning. The threshold is deliberately a standalone knob: it is
/// read from the config, can be overridden per query and can be changed without
/// touching the search implementation.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FuzzyConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub mode: FuzzyMode,
    /// Minimum similarity (0..=1) a candidate must reach to be returned.
    #[serde(default = "default_fuzzy_threshold")]
    pub threshold: f32,
    /// Queries shorter than this (in Unicode scalar values) never trigger the
    /// fuzzy fallback.
    #[serde(default = "default_min_query_chars")]
    pub min_query_chars: usize,
    /// Maximum Levenshtein distance for `FuzzyTermQuery` candidates.
    #[serde(default = "default_edit_distance")]
    pub max_edit_distance: u8,
    /// Character n-gram size used by the auxiliary index field.
    #[serde(default = "default_ngram_size")]
    pub ngram_size: usize,
    /// Upper bound on candidates fetched from the auxiliary index.
    #[serde(default = "default_candidate_limit")]
    pub candidate_limit: usize,
    /// If exact search returns fewer than this many hits, try fuzzy search.
    #[serde(default = "default_fallback_below")]
    pub fallback_below_results: usize,
}

impl Default for FuzzyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            mode: FuzzyMode::Fallback,
            threshold: default_fuzzy_threshold(),
            min_query_chars: default_min_query_chars(),
            max_edit_distance: default_edit_distance(),
            ngram_size: default_ngram_size(),
            candidate_limit: default_candidate_limit(),
            fallback_below_results: default_fallback_below(),
        }
    }
}

impl FuzzyConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if !(0.0..=1.0).contains(&self.threshold) || self.threshold.is_nan() {
            return Err(ConfigError::Invalid(format!(
                "search.fuzzy.threshold must be within [0, 1], got {}",
                self.threshold
            )));
        }
        if self.max_edit_distance > 3 {
            return Err(ConfigError::Invalid(
                "search.fuzzy.max_edit_distance must be <= 3".into(),
            ));
        }
        if !(1..=4).contains(&self.ngram_size) {
            return Err(ConfigError::Invalid(
                "search.fuzzy.ngram_size must be within [1, 4]".into(),
            ));
        }
        if self.candidate_limit == 0 {
            return Err(ConfigError::Invalid(
                "search.fuzzy.candidate_limit must be positive".into(),
            ));
        }
        Ok(())
    }

    /// Configured threshold, clamped defensively.
    pub fn effective_threshold(&self) -> f32 {
        self.threshold.clamp(0.0, 1.0)
    }

    /// Environment override: `TG_SEARCH_FUZZY_THRESHOLD=0.4`.
    pub fn apply_env_override(&mut self) {
        if let Ok(value) = env::var("TG_SEARCH_FUZZY_THRESHOLD") {
            if let Ok(parsed) = value.trim().parse::<f32>() {
                self.threshold = parsed.clamp(0.0, 1.0);
            }
        }
    }
}

fn default_true() -> bool {
    true
}
fn default_fuzzy_threshold() -> f32 {
    0.5
}
fn default_min_query_chars() -> usize {
    2
}
fn default_edit_distance() -> u8 {
    2
}
fn default_ngram_size() -> usize {
    2
}
fn default_candidate_limit() -> usize {
    500
}
fn default_fallback_below() -> usize {
    1
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FuzzyMode {
    /// Only search fuzzily when exact search does not return enough hits.
    #[default]
    Fallback,
    /// Always merge exact and fuzzy hits.
    Always,
    /// Never use the fuzzy fallback.
    Off,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ImageConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// ORB keypoint limit per image.
    #[serde(default = "default_max_features")]
    pub max_features: i32,
    /// Lowe's ratio test cutoff for descriptor matching.
    #[serde(default = "default_match_ratio")]
    pub match_ratio: f32,
    /// Minimum number of good descriptor matches.
    #[serde(default = "default_min_good_matches")]
    pub min_good_matches: usize,
    /// Minimum visual similarity (0..=1) for a result.
    #[serde(default = "default_min_similarity")]
    pub min_similarity: f32,
    #[serde(default = "default_thumb_side")]
    pub thumbnail_max_side: i32,
    #[serde(default = "default_jpeg_quality")]
    pub jpeg_quality: i32,
    /// Do not download media larger than this (bytes).
    #[serde(default = "default_max_download_bytes")]
    pub max_download_bytes: u64,
    /// Index images found while downloading chat history.
    #[serde(default = "default_true")]
    pub index_history: bool,
    /// Stop indexing images after this many per chat (0 = unlimited).
    #[serde(default)]
    pub max_images_per_chat: usize,
    /// Number of pHash candidates passed to the slower ORB matcher.
    #[serde(default = "default_phash_candidates")]
    pub phash_candidate_limit: usize,
    /// Maximum number of results returned by image search.
    #[serde(default = "default_image_results")]
    pub result_limit: usize,
    /// Maximum number of photos sent by `/img2img`.
    #[serde(default = "default_send_photo_limit")]
    pub send_photo_limit: usize,
}

impl Default for ImageConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_features: default_max_features(),
            match_ratio: default_match_ratio(),
            min_good_matches: default_min_good_matches(),
            min_similarity: default_min_similarity(),
            thumbnail_max_side: default_thumb_side(),
            jpeg_quality: default_jpeg_quality(),
            max_download_bytes: default_max_download_bytes(),
            index_history: true,
            max_images_per_chat: 0,
            phash_candidate_limit: default_phash_candidates(),
            result_limit: default_image_results(),
            send_photo_limit: default_send_photo_limit(),
        }
    }
}

impl ImageConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.max_features < 50 {
            return Err(ConfigError::Invalid(
                "image.max_features must be >= 50".into(),
            ));
        }
        if !(0.0..1.0).contains(&self.match_ratio) {
            return Err(ConfigError::Invalid(
                "image.match_ratio must be within (0, 1)".into(),
            ));
        }
        if !(0.0..=1.0).contains(&self.min_similarity) {
            return Err(ConfigError::Invalid(
                "image.min_similarity must be within [0, 1]".into(),
            ));
        }
        if self.thumbnail_max_side < 64 {
            return Err(ConfigError::Invalid(
                "image.thumbnail_max_side must be >= 64".into(),
            ));
        }
        if !(1..=100).contains(&self.jpeg_quality) {
            return Err(ConfigError::Invalid(
                "image.jpeg_quality must be within [1, 100]".into(),
            ));
        }
        if self.result_limit == 0 || self.phash_candidate_limit == 0 {
            return Err(ConfigError::Invalid(
                "image.result_limit and image.phash_candidate_limit must be positive".into(),
            ));
        }
        Ok(())
    }

    pub fn effective_min_similarity(&self) -> f32 {
        self.min_similarity.clamp(0.0, 1.0)
    }
}

fn default_max_features() -> i32 {
    500
}
fn default_match_ratio() -> f32 {
    0.75
}
fn default_min_good_matches() -> usize {
    8
}
fn default_min_similarity() -> f32 {
    0.12
}
fn default_thumb_side() -> i32 {
    320
}
fn default_jpeg_quality() -> i32 {
    80
}
fn default_max_download_bytes() -> u64 {
    20 * 1024 * 1024
}
fn default_phash_candidates() -> usize {
    512
}
fn default_image_results() -> usize {
    10
}
fn default_send_photo_limit() -> usize {
    3
}

/// Parse `socks5://user:pass@host:1080` or `http://host:8080`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyConfig {
    pub scheme: ProxyScheme,
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyScheme {
    Socks5,
    Http,
}

pub fn parse_proxy(value: &str) -> Result<ProxyConfig, ConfigError> {
    let url = url::Url::parse(value)
        .map_err(|e| ConfigError::Invalid(format!("invalid proxy `{value}`: {e}")))?;
    let scheme = match url.scheme() {
        "socks5" | "socks5h" => ProxyScheme::Socks5,
        "http" | "https" => ProxyScheme::Http,
        other => {
            return Err(ConfigError::Invalid(format!(
                "unsupported proxy scheme `{other}` (use socks5:// or http://)"
            )))
        }
    };
    let host = url
        .host_str()
        .ok_or_else(|| ConfigError::Invalid(format!("proxy `{value}` has no host")))?
        .to_string();
    let port = url.port().unwrap_or(match scheme {
        ProxyScheme::Socks5 => 1080,
        ProxyScheme::Http => 8080,
    });
    let username = if url.username().is_empty() {
        None
    } else {
        Some(url.username().to_string())
    };
    let password = url.password().map(ToString::to_string);
    Ok(ProxyConfig {
        scheme,
        host,
        port,
        username,
        password,
    })
}

/// Expand `${VAR}` placeholders outside YAML comments. Missing variables are
/// an error so a deployment cannot silently start with an empty bot token.
pub fn interpolate_env(text: &str) -> Result<String, ConfigError> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}").expect("valid regex"));
    let mut out = String::with_capacity(text.len());
    for line in text.split_inclusive('\n') {
        let (body, newline) = match line.strip_suffix('\n') {
            Some(body) => (body, "\n"),
            None => (line, ""),
        };
        let comment_at = yaml_comment_index(body);
        let (code, comment) = body.split_at(comment_at);
        let mut last = 0;
        for caps in re.captures_iter(code) {
            let m = caps.get(0).unwrap();
            out.push_str(&code[last..m.start()]);
            let name = &caps[1];
            match env::var(name) {
                Ok(value) => out.push_str(&value),
                Err(_) => return Err(ConfigError::MissingEnv(name.to_string())),
            }
            last = m.end();
        }
        out.push_str(&code[last..]);
        out.push_str(comment);
        out.push_str(newline);
    }
    Ok(out)
}

/// Byte offset of a YAML comment marker, respecting quoted strings. A `#`
/// only starts a comment at the beginning of a line or after whitespace.
fn yaml_comment_index(line: &str) -> usize {
    let mut quote: Option<char> = None;
    let mut escaped = false;
    let mut previous: Option<char> = None;
    for (index, ch) in line.char_indices() {
        if escaped {
            escaped = false;
            previous = Some(ch);
            continue;
        }
        match quote {
            Some('"') => match ch {
                '\\' => escaped = true,
                '"' => quote = None,
                _ => {}
            },
            Some('\'') => {
                if ch == '\'' {
                    quote = None;
                }
            }
            _ => match ch {
                '"' | '\'' => quote = Some(ch),
                '#' if previous.is_none() || previous.is_some_and(char::is_whitespace) => {
                    return index;
                }
                _ => {}
            },
        }
        previous = Some(ch);
    }
    line.len()
}

/// Convenience for the administrator id: usernames are resolved at runtime by
/// the backend userbot.
pub fn admin_is_numeric(id: &IdOrName) -> bool {
    match id {
        IdOrName::Id(_) => true,
        IdOrName::Name(s) => is_numeric_id(s),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_yaml(token: &str) -> String {
        format!(
            r#"
common:
  name: test
  runtime_dir: /tmp/tgsearcher-test
  api_id: 12345
  api_hash: deadbeef
sessions:
  - name: alice
    phone: '+15551234567'
backends:
  - id: pub
    use_session: alice
frontends:
  - type: bot
    id: public
    use_backend: pub
    config:
      admin_id: 42
      bot_token: "{token}"
"#
        )
    }

    #[test]
    fn parses_minimal_config() {
        let cfg = Config::from_yaml_str(&minimal_yaml("123:abc")).unwrap();
        assert_eq!(cfg.common.name, "test");
        assert_eq!(cfg.frontends[0].config.page_len, 10);
        assert_eq!(cfg.search.fuzzy.threshold, 0.5);
        assert!(cfg.image.enabled);
    }

    #[test]
    fn env_interpolation_and_missing_var() {
        // A unique variable name avoids interference from parallel tests.
        unsafe { std::env::set_var("TGS_TEST_TOKEN_XYZ", "999:xyz") };
        let cfg = Config::from_yaml_str(&minimal_yaml("${TGS_TEST_TOKEN_XYZ}")).unwrap();
        assert_eq!(cfg.frontends[0].config.bot_token, "999:xyz");
        let err = Config::from_yaml_str(&minimal_yaml("${TGS_TEST_MISSING_XYZ}")).unwrap_err();
        assert!(matches!(err, ConfigError::MissingEnv(_)));
    }

    #[test]
    fn env_placeholders_in_comments_are_ignored() {
        let yaml = format!(
            "# never expand ${{TGS_COMMENT_MISSING_XYZ}} in comments\n{}",
            minimal_yaml("123:abc")
        );
        let cfg = Config::from_yaml_str(&yaml).unwrap();
        assert_eq!(cfg.frontends[0].config.bot_token, "123:abc");
    }

    #[test]
    fn rejects_bad_threshold() {
        let mut cfg = Config::from_yaml_str(&minimal_yaml("1:a")).unwrap();
        cfg.search.fuzzy.threshold = 1.5;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_duplicate_backends() {
        let mut cfg = Config::from_yaml_str(&minimal_yaml("1:a")).unwrap();
        cfg.backends.push(cfg.backends[0].clone());
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn parses_proxy() {
        let p = parse_proxy("socks5://user:pass@localhost:1080").unwrap();
        assert_eq!(p.scheme, ProxyScheme::Socks5);
        assert_eq!(p.host, "localhost");
        assert_eq!(p.port, 1080);
        assert_eq!(p.username.as_deref(), Some("user"));
        let p = parse_proxy("http://127.0.0.1").unwrap();
        assert_eq!(p.scheme, ProxyScheme::Http);
        assert_eq!(p.port, 8080);
        assert!(parse_proxy("ftp://x").is_err());
    }

    #[test]
    fn accepts_truthy_no_redis_string() {
        let yaml = minimal_yaml("1:a").replace(
            "      bot_token:",
            "      no_redis: localhost:6379\n      bot_token:",
        );
        let cfg = Config::from_yaml_str(&yaml).unwrap();
        assert!(cfg.frontends[0].config.no_redis);
    }

    #[test]
    fn paths_are_derived_from_runtime_dir() {
        let cfg = Config::from_yaml_str(&minimal_yaml("1:a")).unwrap();
        let paths = cfg.common.paths();
        assert!(paths.sessions.ends_with("session"));
        assert_eq!(paths.index_dir("pub"), paths.indexes.join("pub"));
        assert_eq!(
            paths.session_file("alice"),
            paths.sessions.join("alice.session")
        );
    }
}
