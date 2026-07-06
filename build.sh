set -euo pipefail

if [ -z "$1" ]; then
    echo "Error: missing version argument." >&2
    echo "Usage: ./build.sh <version>" >&2
    exit 1
fi

if [ -f "./Cargo.toml" ]; then
    cargo build --release
    cargo build --release --target armv7-unknown-linux-gnueabihf
else
    echo "Cargo.toml not found. Please run this script from the root of your Rust project."
    exit 1
fi

ARM64=./target/release/dupcheckerrs
ARM32=./target/armv7-unknown-linux-gnueabihf/release/dupcheckerrs

if [ -f "$ARM64" ]; then
    echo "Copying ARM64 binary to current directory..."
    cp "$ARM64" ./dupcheckerrs-rpi4-"$1"
else
    echo "ARM64 binary not found. Please ensure it was built successfully."
fi

if [ -f "$ARM32" ]; then
    echo "Copying ARM32 binary to current directory..."
    cp "$ARM32" ./dupcheckerrs-rpi3b-"$1"
else
    echo "ARM32 binary not found. Please ensure it was built successfully."
fi