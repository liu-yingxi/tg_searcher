//! Frontend logic and test doubles for the Telegram search bot.

pub mod backend;
pub mod cache;
pub mod commands;
pub mod frontend;
pub mod messenger;
pub mod render;
pub mod testing;

pub use backend::{ProgressSink, SearchBackend};
pub use cache::{build_cache, Cache, MemoryCache, RedisCache};
pub use frontend::{Frontend, FrontendSettings};
pub use messenger::{
    ButtonDef, CallbackEvent, IncomingMessage, IncomingUpdate, InlineKeyboard, Messenger,
    ReplyContext, SentMessage,
};

pub use commands::{parse_command, CommandKind, ParsedCommand};
pub use render::{help_text, render_search_result};
