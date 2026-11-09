//! Compiles the protobuf contracts with the pure-Rust `protox` compiler, so no
//! native `protoc` is needed on any build host, and feeds the descriptors to
//! tonic's code generator.

use std::path::PathBuf;

const CONTRACT_FILES: &[&str] = &[
    "bridge/v1/bridge.proto",
    "media/v1/pcm.proto",
    "media/v1/control.proto",
];

fn main() {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("manifest dir"));
    let contracts = manifest_dir.join("../../contracts");
    println!("cargo:rerun-if-changed={}", contracts.display());
    for file in CONTRACT_FILES {
        println!("cargo:rerun-if-changed={}", contracts.join(file).display());
    }

    let descriptors = protox::compile(CONTRACT_FILES, [&contracts])
        .unwrap_or_else(|err| panic!("failed to compile the protobuf contracts: {err}"));
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_fds(descriptors)
        .unwrap_or_else(|err| panic!("failed to generate the gRPC stubs: {err}"));
}
