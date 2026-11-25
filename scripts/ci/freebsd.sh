#!/bin/sh
# Runs inside the FreeBSD CI VM: installs the toolchain, then runs one task.
#
#   sh scripts/ci/freebsd.sh test             Go and Rust tests
#   sh scripts/ci/freebsd.sh bundle NAME      portable bundle at dist/NAME.tar.xz
set -eu

install_toolchain() {
  pkg update -f
  # flvmux comes from -good and rtmp2sink from -bad. build-haze.sh requires both.
  pkg install -y alsa-lib bash ca_root_nss cmake fontconfig git gmake go llvm pkgconf rust ffmpeg opus \
    gstreamer1 gstreamer1-plugins gstreamer1-plugins-good gstreamer1-plugins-bad \
    gstreamer1-plugins-opus gstreamer1-plugins-x264 gstreamer1-libav
  pkgconf --exists libavcodec opus gstreamer-1.0 gstreamer-app-1.0

  # msdfgen-sys generates its bindings on FreeBSD, so bindgen needs libclang.
  libclang="$(find /usr/local -maxdepth 3 -type f -name 'libclang.so*' -print -quit)"
  test -n "$libclang" || { echo "libclang is required for the FreeBSD MSDF build" >&2; exit 1; }
  LIBCLANG_PATH="$(dirname "$libclang")"
  export LIBCLANG_PATH
  export CXX="${CXX:-c++}"
  export CXX_STDLIB=libc++

  # The synced checkout has a different owner than the VM's root user.
  git config --global --add safe.directory "$PWD"
  go version
  rustc --version
}

task="${1:?usage: freebsd.sh test | bundle NAME}"
install_toolchain

case "$task" in
  test)
    (cd services/go && go test -count=1 ./...)
    cargo test --workspace --locked
    cargo test -p haze-cgen --features gpu-wgpu --locked
    ;;
  bundle)
    name="${2:?bundle needs a bundle name}"
    bash scripts/build-haze.sh --profile release --output-dir "dist/$name"
    bash scripts/ci/package-bundle.sh "$name"
    # Copying the VM back to the host is slow, so leave only the archive behind.
    rm -rf target "dist/$name"
    ;;
  *)
    echo "Unknown task: $task" >&2
    exit 2
    ;;
esac
