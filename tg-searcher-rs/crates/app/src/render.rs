//! HTML rendering for Telegram messages and inline keyboards.

use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tgsearcher_core::{
    brief_content, escape_html, ChatId, FileFilter, ImageSearchMode, ImageSearchResult,
    IndexedMessage, SearchResult,
};

use crate::messenger::{ButtonDef, InlineKeyboard};

pub const TELEGRAM_TEXT_LIMIT: usize = 4096;
const MAX_FILENAME_CHARS: usize = 60;
const MAX_TEXT_CHARS: usize = 120;

pub fn help_text(is_admin: bool) -> String {
    let mut text = String::from(
        "<b>可用命令</b>\n\
         /s <code>[--exact|--fuzzy|--threshold=0.4] 关键词</code> — 搜索消息（也可直接发送文本）\n\
         /chats <code>[关键词]</code> — 列出/筛选已索引的对话，并点击选择\n\
         /random — 随机返回一条已索引消息\n\
         /img2text <code>[--chat ID]</code> — 回复一张图片，按图搜索相关文字\n\
         /img2img <code>[--chat ID]</code> — 回复一张图片，按图搜索相似图片\n\
         /help — 显示帮助\n\n\
         <b>搜索语法</b>\n\
         <code>\"foo bar\"</code> 短语；<code>AND</code>、<code>OR</code>、<code>NOT</code>、括号；\n\
         <code>*</code> / <code>?</code> 通配符（建议少用，较慢）。\n\
         默认在精确搜索无结果时自动模糊匹配；阈值可在配置 <code>search.fuzzy.threshold</code> 或\n\
         环境变量 <code>TG_SEARCH_FUZZY_THRESHOLD</code> 中设置，也可用 <code>--threshold</code> 临时覆盖。\n\n\
         <b>选择对话后</b>\n\
         回复带 “☑️ 已选择” 的消息并发送搜索词，即可只搜索该对话；\n\
         管理员回复该消息并发送管理命令，可对该对话执行操作。",
    );
    if is_admin {
        text.push_str(
            "\n\n<b>管理员命令</b>\n\
             /download_chat <code>[--min ID] [--max ID] [对话...]</code> — 下载并索引历史消息\n\
             /monitor_chat <code>对话...</code> — 加入实时监控\n\
             /clear <code>[对话...|all]</code> — 清除索引（all 需二次确认）\n\
             /stat — 查看后端索引状态\n\
             /find_chat_id <code>关键词</code> — 查找对话 ID\n\
             /refresh_chat_names — 刷新对话名称缓存\n\
             /usage — 查看使用统计",
        );
    }
    text
}

pub fn format_time(timestamp: i64) -> String {
    DateTime::<Utc>::from_timestamp(timestamp, 0)
        .map(|time| time.format("%y-%m-%d %H:%M").to_string())
        .unwrap_or_else(|| "无效时间".to_string())
}

fn title_of(titles: &HashMap<ChatId, String>, chat_id: ChatId) -> String {
    titles
        .get(&chat_id)
        .cloned()
        .unwrap_or_else(|| format!("未知对话 ({chat_id})"))
}

fn message_link_text(message: &IndexedMessage, highlighted: &str) -> String {
    if let Some(filename) = &message.filename {
        format!(
            "📎 {}",
            escape_html(&brief_content(filename, MAX_FILENAME_CHARS))
        )
    } else if !highlighted.trim().is_empty() {
        highlighted.to_string()
    } else if !message.content.is_empty() {
        escape_html(&brief_content(&message.content, MAX_TEXT_CHARS))
    } else {
        "[查看消息]".to_string()
    }
}

pub fn render_search_result(
    result: &SearchResult,
    titles: &HashMap<ChatId, String>,
    elapsed: Duration,
) -> String {
    if result.total == 0 {
        return "没有找到相关的消息。".to_string();
    }
    let total_pages = result.total_pages();
    let mut out = format!(
        "共搜索到 {} 个结果 (第 {}/{} 页)，耗时 {:.3} 秒:",
        result.total,
        result.page,
        total_pages,
        elapsed.as_secs_f64()
    );
    if result.fuzzy_used {
        out.push_str(&format!(
            "\n🔍 模糊匹配已启用 (阈值 {:.2})",
            result.fuzzy_threshold.unwrap_or(0.0)
        ));
    }
    out.push_str("\n\n");

    let start_index = (result.page.saturating_sub(1)) * result.page_len + 1;
    for (offset, hit) in result.hits.iter().enumerate() {
        let index = start_index + offset;
        let message = &hit.message;
        let title = title_of(titles, message.chat_id);
        out.push_str(&format!(
            "<b>{index}. {}</b> <code>[{}]</code>\n",
            escape_html(&title),
            format_time(message.post_time)
        ));
        out.push_str(&format!(
            "<a href=\"{}\">{}</a>\n",
            escape_html(&message.url),
            message_link_text(message, &hit.highlighted)
        ));
        if message.filename.is_some() && !message.content.is_empty() {
            out.push_str(&format!(
                "{}\n",
                escape_html(&brief_content(&message.content, MAX_TEXT_CHARS))
            ));
        }
        out.push('\n');
    }
    truncate_telegram(&out, TELEGRAM_TEXT_LIMIT)
}

pub fn render_random_message(message: &IndexedMessage, title: &str, highlighted: &str) -> String {
    let mut out = String::from("🎲 <b>随机消息</b>\n\n");
    out.push_str(&format!(
        "<b>{}</b> <code>[{}]</code>\n",
        escape_html(title),
        format_time(message.post_time)
    ));
    out.push_str(&format!(
        "<a href=\"{}\">{}</a>\n",
        escape_html(&message.url),
        message_link_text(message, highlighted)
    ));
    if message.filename.is_some() && !message.content.is_empty() {
        out.push_str(&format!(
            "{}\n",
            escape_html(&brief_content(&message.content, MAX_TEXT_CHARS))
        ));
    }
    truncate_telegram(&out, TELEGRAM_TEXT_LIMIT)
}

pub fn search_buttons(result: &SearchResult, current_filter: FileFilter) -> Option<InlineKeyboard> {
    let mut rows: Vec<Vec<ButtonDef>> = Vec::new();
    let filters = [
        (FileFilter::All, "全部"),
        (FileFilter::TextOnly, "纯文本"),
        (FileFilter::FileOnly, "仅文件"),
    ];
    rows.push(
        filters
            .iter()
            .map(|(filter, label)| {
                let text = if *filter == current_filter {
                    format!("【{label}】")
                } else {
                    (*label).to_string()
                };
                ButtonDef::new(text, format!("search_filter={}", filter.as_str()))
            })
            .collect(),
    );

    if result.total > 0 {
        let total_pages = result.total_pages();
        if total_pages > 1 {
            let mut page_row = Vec::new();
            if result.page > 1 {
                page_row.push(ButtonDef::new(
                    "⬅️ 上一页",
                    format!("search_page={}", result.page - 1),
                ));
            }
            page_row.push(ButtonDef::new(
                format!("{}/{}", result.page, total_pages),
                "noop",
            ));
            if !result.is_last_page && result.page < total_pages {
                page_row.push(ButtonDef::new(
                    "下一页 ➡️",
                    format!("search_page={}", result.page + 1),
                ));
            }
            rows.push(page_row);
        }
    }
    let keyboard = InlineKeyboard::new(rows);
    if keyboard.is_empty() {
        None
    } else {
        Some(keyboard)
    }
}

pub struct ChatsRender {
    pub text: String,
    pub keyboard: Option<InlineKeyboard>,
    pub shown: usize,
    pub total: usize,
}

/// Render the `/chats` list: sorted by title, at most 90 buttons.
pub fn render_chats(
    chats: &[(ChatId, String)],
    filter_query: &str,
    truncated: bool,
) -> ChatsRender {
    const MAX_BUTTONS: usize = 90;
    let mut sorted: Vec<&(ChatId, String)> = chats.iter().collect();
    sorted.sort_by(|a, b| a.1.cmp(&b.1));
    let total = sorted.len();
    let mut rows: Vec<Vec<ButtonDef>> = Vec::new();
    let mut row: Vec<ButtonDef> = Vec::new();
    let mut shown = 0usize;
    for (chat_id, title) in sorted.into_iter().take(MAX_BUTTONS) {
        row.push(ButtonDef::new(
            brief_content(title, 30),
            format!("select_chat={chat_id}"),
        ));
        shown += 1;
        if row.len() == 2 {
            rows.push(std::mem::take(&mut row));
        }
    }
    if !row.is_empty() {
        rows.push(row);
    }

    let mut text = format!("找到 {total} 个匹配的已索引对话");
    if !filter_query.is_empty() {
        text.push_str(&format!(" (筛选条件: “{}”)", escape_html(filter_query)));
    }
    text.push_str(":\n请点击下方按钮选择一个对话以进行后续操作。");
    if truncated || total > MAX_BUTTONS {
        text.push_str("\n\n(列表过长，仅显示部分对话)");
    }
    ChatsRender {
        text,
        keyboard: if rows.is_empty() {
            None
        } else {
            Some(InlineKeyboard::new(rows))
        },
        shown,
        total,
    }
}

pub fn render_image_result(
    result: &ImageSearchResult,
    titles: &HashMap<ChatId, String>,
    mode: ImageSearchMode,
    elapsed: Duration,
) -> String {
    let header = match mode {
        ImageSearchMode::ImageToText => "🖼 <b>以图搜文</b>",
        ImageSearchMode::ImageToImage => "🖼 <b>以图搜图</b>",
    };
    if result.matches.is_empty() {
        return format!(
            "{header}\n未找到相似度达到 {:.0}% 的图片（已扫描 {} 条图片索引）。",
            result.min_similarity * 100.0,
            result.scanned
        );
    }
    let mut out = format!(
        "{header}\n找到 {} 个相似结果 (相似度阈值 {:.0}%，扫描 {} 条)，耗时 {:.3} 秒:\n\n",
        result.total_candidates,
        result.min_similarity * 100.0,
        result.scanned,
        elapsed.as_secs_f64()
    );
    for (index, item) in result.matches.iter().enumerate() {
        let message = &item.message;
        out.push_str(&format!(
            "<b>{}. {}</b> <code>[{}]</code>\n",
            index + 1,
            escape_html(&title_of(titles, message.chat_id)),
            format_time(message.post_time)
        ));
        out.push_str(&format!(
            "相似度 {:.0}% · ORB {} · pHash {}\n",
            item.similarity * 100.0,
            item.good_matches,
            item.phash_distance
        ));
        let label = if let Some(filename) = &message.filename {
            format!(
                "📎 {}",
                escape_html(&brief_content(filename, MAX_FILENAME_CHARS))
            )
        } else if !message.content.is_empty() {
            escape_html(&brief_content(&message.content, MAX_TEXT_CHARS))
        } else {
            "[查看消息]".to_string()
        };
        out.push_str(&format!(
            "<a href=\"{}\">{}</a>\n\n",
            escape_html(&message.url),
            label
        ));
        if message.filename.is_some() && !message.content.is_empty() {
            out.push_str(&format!(
                "{}\n\n",
                escape_html(&brief_content(&message.content, MAX_TEXT_CHARS))
            ));
        }
    }
    truncate_telegram(&out, TELEGRAM_TEXT_LIMIT)
}

pub fn image_caption(item: &tgsearcher_core::ImageMatch, chat_title: &str) -> String {
    let mut caption = format!(
        "🖼 相似度 {:.0}% · {}\n",
        item.similarity * 100.0,
        escape_html(chat_title)
    );
    if !item.message.content.is_empty() {
        caption.push_str(&escape_html(&brief_content(&item.message.content, 80)));
        caption.push('\n');
    } else if let Some(filename) = &item.message.filename {
        caption.push_str(&format!(
            "📎 {}\n",
            escape_html(&brief_content(filename, 60))
        ));
    }
    caption.push_str(&escape_html(&item.message.url));
    truncate_telegram(&caption, 1024)
}

/// Truncate a Telegram HTML message without leaving dangling tags.
pub fn truncate_telegram(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit {
        return text.to_string();
    }
    let suffix = "\n\n...(结果过多，仅显示部分)";
    let budget = limit.saturating_sub(suffix.chars().count() + 10);
    let chars: Vec<char> = text.chars().collect();
    let mut cutoff = budget.min(chars.len());
    // Prefer a newline close to the cutoff.
    if let Some(position) = chars[..cutoff].iter().rposition(|c| *c == '\n') {
        if position > budget / 2 {
            cutoff = position;
        }
    }
    let mut truncated: String = chars[..cutoff].iter().collect();
    // Close any tags that may have been cut in half.
    let open_b = truncated.matches("<b>").count();
    let close_b = truncated.matches("</b>").count();
    for _ in close_b..open_b {
        truncated.push_str("</b>");
    }
    let open_a = truncated.matches("<a ").count();
    let close_a = truncated.matches("</a>").count();
    for _ in close_a..open_a {
        truncated.push_str("</a>");
    }
    let open_code = truncated.matches("<code>").count();
    let close_code = truncated.matches("</code>").count();
    for _ in close_code..open_code {
        truncated.push_str("</code>");
    }
    let open_pre = truncated.matches("<pre>").count();
    let close_pre = truncated.matches("</pre>").count();
    for _ in close_pre..open_pre {
        truncated.push_str("</pre>");
    }
    truncated.push_str(suffix);
    truncated
}

#[cfg(test)]
mod tests {
    use super::*;
    use tgsearcher_core::{ImageMatch, ImageMessage, SearchHit, SearchResult};

    fn titles() -> HashMap<ChatId, String> {
        HashMap::from([(1, "测试 <群>".to_string())])
    }

    fn hit(url: &str, content: &str, highlighted: &str) -> SearchHit {
        SearchHit {
            message: IndexedMessage {
                url: url.into(),
                chat_id: 1,
                content: content.into(),
                post_time: 1_700_000_000,
                sender: "sender".into(),
                filename: None,
            },
            highlighted: highlighted.into(),
            score: 0.0,
        }
    }

    #[test]
    fn search_rendering_escapes_and_links() {
        let result = SearchResult {
            hits: vec![hit(
                "https://t.me/c/1/2?a=1&b=2",
                "hello <world>",
                "<b>hello</b> <world>",
            )],
            total: 1,
            page: 1,
            page_len: 10,
            is_last_page: true,
            fuzzy_used: true,
            fuzzy_threshold: Some(0.45),
        };
        let text = render_search_result(&result, &titles(), Duration::from_millis(12));
        assert!(text.contains("测试 &lt;群&gt;"));
        assert!(text.contains("https://t.me/c/1/2?a=1&amp;b=2"));
        assert!(text.contains("模糊匹配已启用 (阈值 0.45)"));
        assert!(text.contains("<b>hello</b>"));
    }

    #[test]
    fn empty_search_message() {
        let result = SearchResult {
            total: 0,
            page_len: 10,
            page: 1,
            is_last_page: true,
            ..Default::default()
        };
        assert_eq!(
            render_search_result(&result, &titles(), Duration::ZERO),
            "没有找到相关的消息。"
        );
    }

    #[test]
    fn buttons_include_filters_and_pages() {
        let result = SearchResult {
            hits: vec![hit("https://t.me/c/1/1", "x", "<b>x</b>")],
            total: 35,
            page: 2,
            page_len: 10,
            is_last_page: false,
            ..Default::default()
        };
        let keyboard = search_buttons(&result, FileFilter::FileOnly).unwrap();
        assert_eq!(keyboard.rows[0][0].data, "search_filter=all");
        assert_eq!(keyboard.rows[0][0].text, "全部");
        assert_eq!(keyboard.rows[0][2].text, "【仅文件】");
        assert_eq!(keyboard.rows[1][0].data, "search_page=1");
        assert_eq!(keyboard.rows[1][2].data, "search_page=3");
    }

    #[test]
    fn truncation_respects_limit_and_closes_tags() {
        let long = format!("<b>{}</b>", "很长的内容".repeat(2000));
        let truncated = truncate_telegram(&long, 1000);
        assert!(truncated.chars().count() <= 1000);
        assert_eq!(
            truncated.matches("<b>").count(),
            truncated.matches("</b>").count()
        );
        assert!(truncated.contains("结果过多"));
    }

    #[test]
    fn chats_rendering() {
        let chats = vec![(1, "B chat".to_string()), (2, "A chat".to_string())];
        let rendered = render_chats(&chats, "chat", false);
        assert_eq!(rendered.total, 2);
        let keyboard = rendered.keyboard.unwrap();
        // Sorted by title: A chat first.
        assert_eq!(keyboard.rows[0][0].data, "select_chat=2");
        assert!(rendered.text.contains("找到 2 个"));
    }

    #[test]
    fn image_rendering() {
        let result = ImageSearchResult {
            matches: vec![ImageMatch {
                message: ImageMessage {
                    url: "https://t.me/c/1/9".into(),
                    chat_id: 1,
                    post_time: 1_700_000_000,
                    sender: "a".into(),
                    content: "会议照片".into(),
                    filename: Some("meeting.jpg".into()),
                    width: 100,
                    height: 100,
                    thumbnail: None,
                },
                similarity: 0.82,
                good_matches: 31,
                phash_distance: 2,
                thumbnail: None,
            }],
            total_candidates: 1,
            min_similarity: 0.12,
            scanned: 5,
        };
        let text = render_image_result(
            &result,
            &titles(),
            ImageSearchMode::ImageToImage,
            Duration::from_millis(50),
        );
        assert!(text.contains("以图搜图"));
        assert!(text.contains("相似度 82%"));
        assert!(text.contains("会议照片"));
        assert!(text.contains("ORB 31"));
    }

    #[test]
    fn help_lists_image_commands() {
        let user = help_text(false);
        assert!(user.contains("/img2text"));
        assert!(user.contains("/img2img"));
        assert!(user.contains("TG_SEARCH_FUZZY_THRESHOLD"));
        assert!(!user.contains("/download_chat"));
        assert!(help_text(true).contains("/download_chat"));
    }
}
