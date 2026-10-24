//! System tests: drive the whole frontend through its public update interface
//! with a fake Telegram messenger and a real tantivy/OpenCV backend.

use std::path::Path;

use tgsearcher_app::testing::{fixture_image_message, seed_harness, RecordedCall, TestHarness};
use tgsearcher_app::{FrontendSettings, IncomingMessage, ReplyContext};
use tgsearcher_core::{ChatId, IndexedMessage};

const CHAT: ChatId = 777;
const ADMIN: ChatId = 42;
const USER: ChatId = 77;
const BOT: ChatId = 999;

fn asset_image() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("assets")
        .join("example.jpg")
}

/// Strip Telegram HTML tags for textual assertions.
fn plain(text: &str) -> String {
    let mut out = String::new();
    let mut in_tag = false;
    for ch in text.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(ch),
            _ => {}
        }
    }
    out
}

#[tokio::test]
async fn search_returns_cjk_hits_and_filter_buttons() {
    let harness = seed_harness().await.unwrap();
    harness.user_message(CHAT, USER, "/s 机器学习").await;
    let text = plain(&harness.messenger.last_text().unwrap());
    assert!(text.contains("机器学习入门教程"), "text was: {text}");
    assert!(text.contains("共搜索到 1 个结果"));
    let markup = harness.messenger.last_sent_markup().unwrap();
    assert!(markup.rows[0]
        .iter()
        .any(|button| button.data == "search_filter=file_only"));
    assert!(markup.rows[0]
        .iter()
        .any(|button| button.data == "search_filter=all"));
}

#[tokio::test]
async fn pagination_and_filter_callbacks() {
    let harness = seed_harness().await.unwrap();
    harness.user_message(CHAT, USER, "/s *").await;
    let text = harness.messenger.last_text().unwrap();
    assert!(text.contains("共搜索到 7 个结果"), "text was: {text}");
    assert!(text.contains("(第 1/3 页)"));
    let message_id = harness.last_sent_message_id().unwrap();
    let markup = harness.messenger.last_sent_markup().unwrap();
    assert!(markup.rows[1]
        .iter()
        .any(|button| button.data == "search_page=2"));

    harness
        .callback(CHAT, message_id, USER, "search_page=2")
        .await;
    let text = harness.messenger.last_text().unwrap();
    assert!(text.contains("(第 2/3 页)"), "text was: {text}");

    harness
        .callback(CHAT, message_id, USER, "search_filter=file_only")
        .await;
    let text = harness.messenger.last_text().unwrap();
    assert!(text.contains("共搜索到 2 个结果"), "text was: {text}");
    assert!(text.contains("(第 1/1 页)"));
}

#[tokio::test]
async fn fuzzy_threshold_is_decoupled_and_overridable() {
    let harness = seed_harness().await.unwrap();
    harness.user_message(CHAT, USER, "/s 机器学刁").await;
    let text = plain(&harness.messenger.last_text().unwrap());
    assert!(text.contains("模糊匹配已启用"), "text was: {text}");
    assert!(text.contains("机器学习入门教程"));

    harness
        .user_message(CHAT, USER, "/s --threshold=0.99 机器学刁")
        .await;
    let strict = harness.messenger.last_text().unwrap();
    assert!(strict.contains("没有找到相关的消息"), "text was: {strict}");

    harness
        .user_message(CHAT, USER, "/s --exact 机器学刁")
        .await;
    let exact = harness.messenger.last_text().unwrap();
    assert!(exact.contains("没有找到相关的消息"), "text was: {exact}");
}

#[tokio::test]
async fn chats_selection_scopes_follow_up_search() {
    let harness = seed_harness().await.unwrap();
    harness.user_message(CHAT, ADMIN, "/chats 中文").await;
    let text = harness.messenger.last_text().unwrap();
    assert!(text.contains("找到 1 个"), "text was: {text}");
    let markup = harness.messenger.last_sent_markup().unwrap();
    assert_eq!(markup.rows[0][0].data, "select_chat=1001");
    let chat_message_id = harness.last_sent_message_id().unwrap();

    harness
        .callback(CHAT, chat_message_id, ADMIN, "select_chat=1001")
        .await;
    let selected = harness.messenger.last_text().unwrap();
    assert!(selected.contains("已选择"), "text was: {selected}");

    harness
        .feed_message(IncomingMessage {
            chat_id: CHAT,
            message_id: 9001,
            sender_id: ADMIN,
            text: "深度学习".into(),
            is_private: true,
            mentioned_bot: false,
            reply: Some(ReplyContext {
                message_id: chat_message_id,
                sender_id: BOT,
                text: selected,
                image_path: None,
            }),
            image_path: None,
        })
        .await;
    let result = plain(&harness.messenger.last_text().unwrap());
    assert!(result.contains("深度学习与神经网络"), "text was: {result}");
    assert!(!result.contains("今天天气很好"));
}

#[tokio::test]
async fn admin_download_monitor_stat_clear_flow() {
    let harness = seed_harness().await.unwrap();
    harness.backend.add_chat(1004, "新群");

    harness
        .user_message(CHAT, ADMIN, "/download_chat 中文测试群")
        .await;
    let report = harness.messenger.last_text().unwrap();
    assert!(
        report.contains("下载任务完成 (1 成功"),
        "report was: {report}"
    );
    assert!(report.contains("索引 3 条"), "report was: {report}");

    harness.user_message(CHAT, USER, "/s 下载测试").await;
    let search = harness.messenger.last_text().unwrap();
    assert!(search.contains("下载测试"), "text was: {search}");

    harness
        .user_message(CHAT, ADMIN, "/monitor_chat 新群")
        .await;
    let monitor = harness.messenger.last_text().unwrap();
    assert!(monitor.contains("已添加监控"), "text was: {monitor}");

    harness.user_message(CHAT, ADMIN, "/stat").await;
    let stat = harness.messenger.last_text().unwrap();
    assert!(stat.contains("后端"), "text was: {stat}");
    assert!(stat.contains("新群"), "text was: {stat}");

    harness.user_message(CHAT, USER, "/clear all").await;
    assert!(harness
        .messenger
        .last_text()
        .unwrap()
        .contains("仅管理员可用"));

    harness.user_message(CHAT, ADMIN, "/clear all").await;
    let warning = harness.messenger.last_text().unwrap();
    assert!(warning.contains("警告"), "text was: {warning}");
    harness.user_message(CHAT, ADMIN, "/clear all").await;
    let cleared = harness.messenger.last_text().unwrap();
    assert!(cleared.contains("已清除所有索引"), "text was: {cleared}");

    harness.user_message(CHAT, USER, "/random").await;
    assert!(harness
        .messenger
        .last_text()
        .unwrap()
        .contains("没有任何消息"));
}

#[tokio::test]
async fn image_to_text_and_image_to_image_commands() {
    let harness = seed_harness().await.unwrap();
    let image = asset_image();
    assert!(image.exists(), "missing asset {}", image.display());
    harness
        .backend
        .add_image(
            &image,
            fixture_image_message(
                "https://t.me/c/1001/img-1",
                1001,
                "会议现场照片",
                Some("meeting.jpg"),
            ),
        )
        .unwrap();

    harness
        .user_message_with_image(CHAT, USER, "/img2text", image.clone())
        .await;
    let text = harness.messenger.last_text().unwrap();
    assert!(text.contains("以图搜文"), "text was: {text}");
    assert!(text.contains("会议现场照片"), "text was: {text}");

    harness
        .user_message_with_image(CHAT, USER, "/img2img", image.clone())
        .await;
    let text = harness.messenger.last_text().unwrap();
    assert!(text.contains("以图搜图"), "text was: {text}");
    let photos = harness.messenger.photos();
    assert_eq!(photos.len(), 1);
    match &photos[0] {
        RecordedCall::Photo { path, caption, .. } => {
            assert!(path.exists(), "thumbnail missing: {}", path.display());
            assert!(caption.contains("meeting.jpg") || caption.contains("会议现场照片"));
            assert!(caption.contains("相似度"));
        }
        _ => unreachable!(),
    }

    // The same command works when replying to an image.
    harness
        .reply_message_with_image(CHAT, USER, "/img2text", 4242, USER, "一张图片", image)
        .await;
    let text = harness.messenger.last_text().unwrap();
    assert!(text.contains("以图搜文"), "text was: {text}");

    // Without an image the command explains how to use it.
    harness.user_message(CHAT, USER, "/img2text").await;
    let usage = harness.messenger.last_text().unwrap();
    assert!(usage.contains("请发送图片"), "text was: {usage}");
}

#[tokio::test]
async fn non_command_text_and_unknown_commands() {
    let harness = seed_harness().await.unwrap();
    harness.user_message(CHAT, USER, "Rust").await;
    let text = plain(&harness.messenger.last_text().unwrap());
    assert!(text.contains("Rust async programming"), "text was: {text}");

    harness.user_message(CHAT, USER, "/nope").await;
    let text = harness.messenger.last_text().unwrap();
    assert!(text.contains("未知命令"), "text was: {text}");
}

#[tokio::test]
async fn help_stat_find_and_refresh() {
    let harness = seed_harness().await.unwrap();
    harness.user_message(CHAT, ADMIN, "/help").await;
    let help = harness.messenger.last_text().unwrap();
    assert!(help.contains("/download_chat"));
    assert!(help.contains("/img2text"));
    assert!(help.contains("/img2img"));

    harness.user_message(CHAT, USER, "/help").await;
    let user_help = harness.messenger.last_text().unwrap();
    assert!(!user_help.contains("/download_chat"));

    harness.user_message(CHAT, ADMIN, "/stat").await;
    let stat = harness.messenger.last_text().unwrap();
    assert!(stat.contains("总消息"));
    assert!(stat.contains("中文测试群"));

    harness
        .user_message(CHAT, ADMIN, "/find_chat_id Rust")
        .await;
    let found = harness.messenger.last_text().unwrap();
    assert!(found.contains("Rust Group"), "text was: {found}");

    harness
        .user_message(CHAT, ADMIN, "/refresh_chat_names")
        .await;
    let refreshed = harness.messenger.last_text().unwrap();
    assert!(refreshed.contains("已刷新"));
}

#[tokio::test]
async fn usage_tracking() {
    let harness = seed_harness().await.unwrap();
    harness.user_message(CHAT, USER, "/s Rust").await;
    harness.user_message(CHAT, ADMIN, "/usage").await;
    let usage = harness.messenger.last_text().unwrap();
    assert!(usage.contains("总互动用户数"), "text was: {usage}");
    assert!(
        usage.contains(": </b> 1") || usage.contains(":</b> 1"),
        "text was: {usage}"
    );
}

#[tokio::test]
async fn private_mode_blocks_non_whitelisted_users() {
    let settings = FrontendSettings {
        id: "private-frontend".into(),
        bot_username: "test_bot".into(),
        bot_user_id: BOT,
        admin_id: ADMIN,
        page_len: 3,
        private_mode: true,
        private_whitelist: [ADMIN].into_iter().collect(),
        no_redis: false,
        send_photo_limit: 2,
    };
    let harness = TestHarness::with_settings(settings).await.unwrap();
    harness.user_message(CHAT, USER, "/help").await;
    assert!(harness.messenger.calls().is_empty());

    harness.user_message(CHAT, ADMIN, "/help").await;
    assert!(harness.messenger.last_text().unwrap().contains("可用命令"));
}

#[tokio::test]
async fn search_escapes_html_content() {
    let harness = seed_harness().await.unwrap();
    harness
        .backend
        .add_message(IndexedMessage {
            url: "https://t.me/c/1001/999".into(),
            chat_id: 1001,
            content: "<script>alert('x') & 中文</script>".into(),
            post_time: 1_700_000_999,
            sender: "xss".into(),
            filename: None,
        })
        .unwrap();
    harness.user_message(CHAT, USER, "/s script").await;
    let text = harness.messenger.last_text().unwrap();
    assert!(!text.contains("<script>"), "raw HTML leaked: {text}");
    assert!(
        text.contains("&lt;script&gt;") || text.contains("alert"),
        "text was: {text}"
    );
}
