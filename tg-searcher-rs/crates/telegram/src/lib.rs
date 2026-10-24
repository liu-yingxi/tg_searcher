//! Telegram adapters: grammers userbot backend and teloxide bot frontend.

pub mod backend;
pub mod frontend;
pub mod session;

pub use backend::{spawn_update_dispatch, UserbotBackend};
pub use frontend::{bot_smoke_test, BotFrontend, BotFrontendConfig};
pub use session::{DialogInfo, SelfInfo, UserSession};
