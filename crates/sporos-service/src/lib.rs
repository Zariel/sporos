//! Service adapters and application composition for Sporos.

pub mod durable_ingress;
pub mod storage;
pub mod torrent;

#[cfg(test)]
mod duroxide_phase0;
