//! Text helpers shared by the indexer and the Telegram frontend.

/// Escape text for Telegram's HTML parse mode.
pub fn escape_html(content: &str) -> String {
    let mut out = String::with_capacity(content.len());
    for ch in content.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            _ => out.push(ch),
        }
    }
    out
}

/// Normalize a message before it is stored in the index. Newlines are folded
/// into spaces so that snippets remain single-line, matching the original
/// Python behaviour while keeping the stored text plain (not HTML-escaped).
pub fn normalize_content(content: &str) -> String {
    content
        .replace("\r\n", "\n")
        .replace('\r', "\n")
        .replace('\n', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Escape and flatten content (kept for API compatibility with the Python
/// implementation; new code should call [`normalize_content`] plus
/// [`escape_html`] at render time).
pub fn escape_content(content: &str) -> String {
    escape_html(content).replace('\n', " ")
}

/// Shorten content for display. Uses character counts so CJK text is handled
/// correctly. Matches the original semantics: `prefix…suffix` when too long.
pub fn brief_content(content: &str, trim_len: usize) -> String {
    let chars: Vec<char> = content.chars().collect();
    if chars.len() < trim_len {
        return content.to_string();
    }
    let keep_head = trim_len.saturating_sub(4);
    let mut out = String::new();
    out.extend(chars.iter().take(keep_head));
    out.push('…');
    out.extend(chars.iter().skip(chars.len().saturating_sub(2)));
    out
}

/// Drop the first whitespace-delimited word (used for `@bot query`).
pub fn remove_first_word(text: &str) -> String {
    match text.find(char::is_whitespace) {
        None => String::new(),
        Some(idx) => text[idx..].trim_start().to_string(),
    }
}

/// Convert a peer/marked id into the share id used by `t.me/c/<id>/<msg>`.
///
/// * channels and supergroups are marked with the `-100` prefix in some APIs;
/// * basic groups and users are used as-is (absolute value).
pub fn share_id_from_peer_id(peer_id: i64) -> i64 {
    let abs = peer_id.unsigned_abs();
    if peer_id < 0 {
        let s = abs.to_string();
        if let Some(rest) = s.strip_prefix("100") {
            if rest.len() >= 6 {
                if let Ok(id) = rest.parse::<i64>() {
                    return id;
                }
            }
        }
        return abs as i64;
    }
    abs as i64
}

/// Inverse helper: build a marked channel id from a bare share id.
pub fn marked_channel_id(share_id: i64) -> i64 {
    -(1_000_000_000_000_i64 + share_id.unsigned_abs() as i64)
}

/// True if the string looks like a numeric Telegram id (possibly negative).
pub fn is_numeric_id(value: &str) -> bool {
    let v = value.trim();
    if v.is_empty() {
        return false;
    }
    let digits = v.strip_prefix('-').unwrap_or(v);
    !digits.is_empty() && digits.chars().all(|c| c.is_ascii_digit())
}

/// Parse a configuration value that may be a numeric id or a username.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdOrName {
    Id(i64),
    Name(String),
}

impl IdOrName {
    pub fn as_string(&self) -> String {
        match self {
            Self::Id(id) => id.to_string(),
            Self::Name(name) => name.clone(),
        }
    }

    pub fn as_id(&self) -> Option<i64> {
        match self {
            Self::Id(id) => Some(*id),
            Self::Name(name) => {
                if is_numeric_id(name) {
                    name.trim().parse().ok()
                } else {
                    None
                }
            }
        }
    }
}

impl std::fmt::Display for IdOrName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_string())
    }
}

impl<'de> serde::Deserialize<'de> for IdOrName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Int(i64),
            Str(String),
        }
        match Raw::deserialize(deserializer)? {
            Raw::Int(v) => Ok(IdOrName::Id(v)),
            Raw::Str(v) => Ok(IdOrName::Name(v)),
        }
    }
}

impl serde::Serialize for IdOrName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Id(v) => serializer.serialize_i64(*v),
            Self::Name(v) => serializer.serialize_str(v),
        }
    }
}

/// Format a chat title the way Telethon's `format_entity_name` does.
pub fn format_entity_name(first: Option<&str>, last: Option<&str>, title: Option<&str>) -> String {
    if let Some(title) = title {
        return title.to_string();
    }
    let joined = [first.unwrap_or(""), last.unwrap_or("")]
        .iter()
        .filter(|s| !s.is_empty())
        .copied()
        .collect::<Vec<_>>()
        .join(" ");
    joined.trim().to_string()
}

/// Split a user supplied `/command` line into (command, args).
///
/// The command is lower-cased and has any `@botname` suffix removed.
pub fn split_command(text: &str) -> Option<(String, &str)> {
    let trimmed = text.trim_start();
    let without_slash = trimmed.strip_prefix('/')?;
    let mut parts = without_slash.splitn(2, char::is_whitespace);
    let mut command = parts.next()?.to_ascii_lowercase();
    if command.is_empty() {
        return None;
    }
    if let Some(at) = command.find('@') {
        command.truncate(at);
    }
    if command.is_empty() {
        return None;
    }
    let args = parts.next().unwrap_or("").trim();
    Some((command, args))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn html_escaping() {
        assert_eq!(
            escape_html("<b>a & b</b> \"q\""),
            "&lt;b&gt;a &amp; b&lt;/b&gt; &quot;q&quot;"
        );
    }

    #[test]
    fn content_normalization_folds_newlines() {
        assert_eq!(normalize_content("a\r\nb\nc"), "a b c");
        assert_eq!(normalize_content("  a   b  "), "a b");
    }

    #[test]
    fn brief_content_cjk_safe() {
        assert_eq!(brief_content("短内容", 20), "短内容");
        let out = brief_content("这是一个很长的中文消息内容用于测试", 10);
        assert_eq!(out.chars().count(), 9);
        assert!(out.contains('…'));
        assert!(out.ends_with("测试"));
    }

    #[test]
    fn remove_word() {
        assert_eq!(remove_first_word("@bot hello world"), "hello world");
        assert_eq!(remove_first_word("word"), "");
    }

    #[test]
    fn share_ids() {
        assert_eq!(share_id_from_peer_id(-1001234567890), 1234567890);
        assert_eq!(share_id_from_peer_id(1234567890), 1234567890);
        assert_eq!(share_id_from_peer_id(-987654), 987654);
        assert_eq!(share_id_from_peer_id(-100123), 100123);
        assert!(is_numeric_id("-1001234"));
        assert!(!is_numeric_id("abc"));
    }

    #[test]
    fn command_split() {
        let (cmd, args) = split_command("/Search foo bar").unwrap();
        assert_eq!(cmd, "search");
        assert_eq!(split_command("/s@MyBot hi").unwrap().0, "s");
        assert_eq!(args, "foo bar");
        assert!(split_command("not a command").is_none());
    }

    #[test]
    fn entity_name() {
        assert_eq!(format_entity_name(Some("A"), Some("B"), None), "A B");
        assert_eq!(format_entity_name(None, None, Some("Chat")), "Chat");
        assert_eq!(format_entity_name(Some("A"), Some(""), None), "A");
    }
}
