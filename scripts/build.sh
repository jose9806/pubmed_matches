#!/bin/bash
set -e

# Check if Rust is installed
if ! command -v cargo &> /dev/null; then
    echo "Rust is not installed. Installing Rust..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source $HOME/.cargo/env
fi

# Build release version
echo "Building pubmed-matcher in release mode..."
cargo build --release

echo "Build complete. The executable is at target/release/pubmed-matcher"