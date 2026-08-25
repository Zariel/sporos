//! Service adapters and application composition for Sporos.

mod activity_failure;
pub mod app;
pub mod arr;
pub mod candidate;
pub mod candidate_workflow;
pub mod completion;
pub mod config;
mod data_scan;
pub mod durable_ingress;
pub mod engine;
mod error_report;
mod execution;
pub mod hardlink;
pub mod http;
mod injection;
pub mod inventory;
pub mod outbox;
pub mod preflight;
mod prowlarr;
pub mod qbit_projection;
pub mod qbit_sync;
pub mod qbittorrent;
mod retry;
mod search;
mod source_facts;
pub mod storage;
mod task_control;
pub mod task_projection;
mod template;
pub mod torrent;
pub mod torznab;

#[cfg(test)]
mod duroxide_phase0;
