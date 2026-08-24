//! Service adapters and application composition for Sporos.

pub mod app;
pub mod arr;
pub mod completion;
pub mod config;
pub mod durable_ingress;
pub mod engine;
pub mod http;
pub mod inventory;
pub mod outbox;
pub mod preflight;
pub mod qbit_projection;
pub mod qbit_sync;
pub mod qbittorrent;
pub mod storage;
pub mod task_projection;
pub mod torrent;
pub mod torznab;

#[cfg(test)]
mod duroxide_phase0;
