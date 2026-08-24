use serde::{Deserialize, Serialize};

macro_rules! byte_id {
    ($name:ident, $length:expr) => {
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        pub struct $name([u8; $length]);

        impl $name {
            pub const fn from_bytes(bytes: [u8; $length]) -> Self {
                Self(bytes)
            }

            pub const fn as_bytes(&self) -> &[u8; $length] {
                &self.0
            }
        }
    };
}

byte_id!(CandidateId, 16);
byte_id!(PolicySnapshotId, 16);
byte_id!(SourceId, 16);
byte_id!(TaskId, 16);
byte_id!(TaskKey, 32);
