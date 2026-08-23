//! Service adapters and application composition for Sporos.

pub mod durable_ingress;
pub mod storage;

#[cfg(test)]
mod duroxide_phase0;
