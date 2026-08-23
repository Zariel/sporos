//! Service adapters and application composition for Sporos.

pub mod durable_ingress;
pub mod inventory;
pub mod qbittorrent;
pub mod storage;
pub mod torrent;
pub mod torznab;

#[cfg(test)]
mod duroxide_phase0;
