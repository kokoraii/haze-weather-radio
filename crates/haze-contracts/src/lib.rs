//! Rust types and gRPC stubs for the protobuf contracts in `contracts/`.
//!
//! The code is generated at build time from the same `.proto` files that
//! `scripts/gen-proto.*` turns into the committed Go code, so both languages
//! always agree. See `docs/grpc-migration-plan.md`.

/// The daemon event bridge contract, package `haze.bridge.v1`.
pub mod bridge {
    pub mod v1 {
        tonic::include_proto!("haze.bridge.v1");
    }
}

/// The media bridge and media control contracts, package `haze.media.v1`.
pub mod media {
    pub mod v1 {
        tonic::include_proto!("haze.media.v1");
    }
}
