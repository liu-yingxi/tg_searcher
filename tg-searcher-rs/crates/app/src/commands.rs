//! Command-line style parsing for Telegram slash commands.

use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandKind {
    Search,
    Chats,
    Random,
    Help,
    ImageToText,
    ImageToImage,
    DownloadChat,
    MonitorChat,
    Clear,
    Stat,
    FindChatId,
    RefreshNames,
    Usage,
    Unknown(String),
}

impl CommandKind {
    pub fn is_admin_only(&self) -> bool {
        matches!(
            self,
            CommandKind::DownloadChat
                | CommandKind::MonitorChat
                | CommandKind::Clear
                | CommandKind::Stat
                | CommandKind::FindChatId
                | CommandKind::RefreshNames
                | CommandKind::Usage
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedCommand {
    pub kind: CommandKind,
    pub args: String,
}

/// Parse a leading `/command` and its arguments. Returns `None` for ordinary
/// text. A `@botname` suffix is always stripped (Telegram appends it in
/// groups).
pub fn parse_command(text: &str) -> Option<ParsedCommand> {
    let trimmed = text.trim_start();
    let without_slash = trimmed.strip_prefix('/')?;
    let (head, args) = match without_slash.split_once(char::is_whitespace) {
        Some((head, args)) => (head, args.trim()),
        None => (without_slash, ""),
    };
    let head = head.split('@').next().unwrap_or(head).to_ascii_lowercase();
    if head.is_empty() {
        return None;
    }
    let kind = match head.as_str() {
        "s" | "search" | "ss" => CommandKind::Search,
        "chats" | "chat" => CommandKind::Chats,
        "random" | "rand" => CommandKind::Random,
        "help" | "start" => CommandKind::Help,
        "img2text" | "i2t" | "image2text" => CommandKind::ImageToText,
        "img2img" | "i2i" | "image2image" => CommandKind::ImageToImage,
        "download_chat" | "download" => CommandKind::DownloadChat,
        "monitor_chat" | "monitor" => CommandKind::MonitorChat,
        "clear" => CommandKind::Clear,
        "stat" | "status" => CommandKind::Stat,
        "find_chat_id" | "findchat" => CommandKind::FindChatId,
        "refresh_chat_names" | "refresh_names" => CommandKind::RefreshNames,
        "usage" | "stats" => CommandKind::Usage,
        other => CommandKind::Unknown(other.to_string()),
    };
    Some(ParsedCommand {
        kind,
        args: args.to_string(),
    })
}

/// Shell-like splitting used by the admin commands, supporting single and
/// double quotes.
pub fn split_args(input: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    let mut escaped = false;
    for ch in input.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }
        match ch {
            '\\' => escaped = true,
            '"' | '\'' => {
                if quote == Some(ch) {
                    quote = None;
                } else if quote.is_none() {
                    quote = Some(ch);
                } else {
                    current.push(ch);
                }
            }
            c if c.is_whitespace() && quote.is_none() => {
                if !current.is_empty() {
                    args.push(std::mem::take(&mut current));
                }
            }
            c => current.push(c),
        }
    }
    if !current.is_empty() {
        args.push(current);
    }
    args
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SearchArgs {
    /// Remaining text, keeping its original spacing.
    pub query: String,
    pub threshold: Option<f32>,
    pub exact_only: bool,
}

/// Parse `/s`-style options. Options are only recognized at the beginning, so
/// a query may itself contain `--` later.
pub fn parse_search_args(args: &str) -> Result<SearchArgs, String> {
    let mut rest = args.trim();
    let mut result = SearchArgs::default();
    loop {
        if rest.starts_with("--exact") {
            result.exact_only = true;
            rest = rest.trim_start_matches("--exact").trim_start();
            continue;
        }
        if rest.starts_with("--fuzzy") {
            result.exact_only = false;
            rest = rest.trim_start_matches("--fuzzy").trim_start();
            continue;
        }
        if let Some(after) = rest.strip_prefix("--threshold=") {
            let (value, remainder) = match after.split_once(char::is_whitespace) {
                Some((value, remainder)) => (value, remainder.trim_start()),
                None => (after, ""),
            };
            result.threshold = Some(parse_threshold(value)?);
            rest = remainder;
            continue;
        }
        if let Some(after) = rest.strip_prefix("--threshold") {
            let after = after.trim_start();
            let (value, remainder) = match after.split_once(char::is_whitespace) {
                Some((value, remainder)) => (value, remainder.trim_start()),
                None => (after, ""),
            };
            if value.is_empty() {
                return Err("--threshold 需要 0 到 1 之间的数值".into());
            }
            result.threshold = Some(parse_threshold(value)?);
            rest = remainder;
            continue;
        }
        break;
    }
    result.query = rest.trim().to_string();
    Ok(result)
}

fn parse_threshold(value: &str) -> Result<f32, String> {
    let parsed: f32 = value
        .parse()
        .map_err(|_| format!("无效的阈值 `{value}`，应为 0 到 1 之间的数值"))?;
    if !(0.0..=1.0).contains(&parsed) || parsed.is_nan() {
        return Err(format!("阈值 `{value}` 超出范围，应在 0 到 1 之间"));
    }
    Ok(parsed)
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DownloadArgs {
    pub min_id: i64,
    pub max_id: i64,
    pub chats: Vec<String>,
}

pub fn parse_download_args(args: &str) -> Result<DownloadArgs, String> {
    let mut result = DownloadArgs::default();
    let mut tokens = split_args(args).into_iter().peekable();
    while let Some(token) = tokens.next() {
        if let Some(value) = token.strip_prefix("--min=") {
            result.min_id = parse_id(value, "--min")?;
        } else if token == "--min" {
            let value = tokens.next().ok_or("--min 缺少参数")?;
            result.min_id = parse_id(&value, "--min")?;
        } else if let Some(value) = token.strip_prefix("--max=") {
            result.max_id = parse_id(value, "--max")?;
        } else if token == "--max" {
            let value = tokens.next().ok_or("--max 缺少参数")?;
            result.max_id = parse_id(&value, "--max")?;
        } else if token.starts_with("--") {
            return Err(format!("未知选项 `{token}`"));
        } else {
            result.chats.push(token);
        }
    }
    if result.min_id < 0 || result.max_id < 0 {
        return Err("消息 ID 不能为负数".into());
    }
    if result.max_id != 0 && result.max_id <= result.min_id {
        return Err("--max 必须大于 --min（或为 0 表示无上限）".into());
    }
    Ok(result)
}

fn parse_id(value: &str, option: &str) -> Result<i64, String> {
    value
        .parse()
        .map_err(|_| format!("{option} 的值 `{value}` 不是有效的整数"))
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ImageArgs {
    pub chats: Vec<i64>,
}

/// Parse `/img2text` / `/img2img` options: `--chat <id>` and/or positional
/// numeric chat ids. `--threshold` is accepted for symmetry with `/s` but the
/// image threshold lives in the config.
pub fn parse_image_args(args: &str) -> Result<ImageArgs, String> {
    let mut result = ImageArgs::default();
    let mut tokens = split_args(args).into_iter().peekable();
    while let Some(token) = tokens.next() {
        let value = if let Some(value) = token.strip_prefix("--chat=") {
            Some(value.to_string())
        } else if token == "--chat" {
            Some(tokens.next().ok_or("--chat 缺少参数")?)
        } else if token.starts_with("--") {
            return Err(format!("未知选项 `{token}`"));
        } else {
            Some(token)
        };
        if let Some(value) = value {
            let id: i64 = value
                .parse()
                .map_err(|_| format!("无效的对话 ID `{value}`"))?;
            result.chats.push(id);
        }
    }
    Ok(result)
}

/// Extract the chat id from a "☑️ 已选择: ... (`123`)" message.
pub fn extract_selected_chat_id(text: &str) -> Option<i64> {
    let marker = "(`";
    let start = text.rfind(marker)? + marker.len();
    let end = text[start..].find("`)")? + start;
    text[start..end].trim().parse().ok()
}

impl fmt::Display for CommandKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommandKind::Search => write!(f, "search"),
            CommandKind::Chats => write!(f, "chats"),
            CommandKind::Random => write!(f, "random"),
            CommandKind::Help => write!(f, "help"),
            CommandKind::ImageToText => write!(f, "img2text"),
            CommandKind::ImageToImage => write!(f, "img2img"),
            CommandKind::DownloadChat => write!(f, "download_chat"),
            CommandKind::MonitorChat => write!(f, "monitor_chat"),
            CommandKind::Clear => write!(f, "clear"),
            CommandKind::Stat => write!(f, "stat"),
            CommandKind::FindChatId => write!(f, "find_chat_id"),
            CommandKind::RefreshNames => write!(f, "refresh_chat_names"),
            CommandKind::Usage => write!(f, "usage"),
            CommandKind::Unknown(name) => write!(f, "unknown:{name}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_aliases_and_bot_suffix() {
        assert_eq!(parse_command("/s foo").unwrap().kind, CommandKind::Search);
        assert_eq!(
            parse_command("/search@MyBot foo").unwrap().kind,
            CommandKind::Search
        );
        assert_eq!(
            parse_command("/i2t").unwrap().kind,
            CommandKind::ImageToText
        );
        assert_eq!(
            parse_command("/img2img").unwrap().kind,
            CommandKind::ImageToImage
        );
        assert_eq!(parse_command("/clear all").unwrap().args, "all");
        assert!(parse_command("hello").is_none());
        assert_eq!(
            parse_command("/nope").unwrap().kind,
            CommandKind::Unknown("nope".into())
        );
    }

    #[test]
    fn search_options() {
        let args = parse_search_args("--exact 机器学习").unwrap();
        assert!(args.exact_only);
        assert_eq!(args.query, "机器学习");
        let args = parse_search_args("--threshold=0.35 foo bar").unwrap();
        assert_eq!(args.threshold, Some(0.35));
        assert_eq!(args.query, "foo bar");
        let args = parse_search_args("--threshold 0.2 foo").unwrap();
        assert_eq!(args.threshold, Some(0.2));
        assert!(parse_search_args("--threshold=2 foo")
            .unwrap_err()
            .contains("超出范围"));
        let args = parse_search_args("foo --exact").unwrap();
        assert!(!args.exact_only);
        assert_eq!(args.query, "foo --exact");
    }

    #[test]
    fn shell_splitting() {
        assert_eq!(
            split_args("--min 10 \"hello world\" chat"),
            vec!["--min", "10", "hello world", "chat"]
        );
        assert_eq!(split_args("a 'b c'"), vec!["a", "b c"]);
    }

    #[test]
    fn download_options() {
        let args = parse_download_args("--min=10 --max 100 chat1 @chat2").unwrap();
        assert_eq!(args.min_id, 10);
        assert_eq!(args.max_id, 100);
        assert_eq!(args.chats, vec!["chat1", "@chat2"]);
        assert!(parse_download_args("--min 10 --max 5").is_err());
        assert!(parse_download_args("--min -1").is_err());
        assert!(parse_download_args("--wat").is_err());
    }

    #[test]
    fn image_options() {
        assert_eq!(parse_image_args("--chat 123").unwrap().chats, vec![123]);
        assert_eq!(parse_image_args("123").unwrap().chats, vec![123]);
        assert!(parse_image_args("abc").is_err());
    }

    #[test]
    fn selected_chat_id() {
        let text = "☑️ 已选择: **群组** (`-1001234567890`)\n\n请回复";
        assert_eq!(extract_selected_chat_id(text), Some(-1001234567890));
        assert_eq!(extract_selected_chat_id("nothing"), None);
    }

    #[test]
    fn admin_only_classification() {
        assert!(CommandKind::Clear.is_admin_only());
        assert!(!CommandKind::Search.is_admin_only());
        assert!(!CommandKind::ImageToText.is_admin_only());
    }
}
