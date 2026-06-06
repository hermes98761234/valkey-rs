pub mod config;
pub mod failover;
pub mod monitor;
pub mod server;
pub mod state;

pub use config::SentinelConfig;
pub use server::SentinelServer;
pub use state::{MasterInfo, MasterState, SentinelState};
