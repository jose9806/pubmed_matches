#!/bin/bash
set -e

# Check if Rust is installed
if ! command -v cargo &> /dev/null; then
    echo "Rust is not installed. Installing Rust..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source $HOME/.cargo/env
fi

# Install additional libraries needed
echo "Installing required system dependencies..."
if [ -f /etc/debian_version ]; then
    # Debian/Ubuntu
    sudo apt-get update
    sudo apt-get install -y libssl-dev pkg-config libpq-dev clang llvm
elif [ -f /etc/redhat-release ]; then
    # CentOS/RHEL/Fedora
    sudo yum install -y openssl-devel postgresql-devel clang llvm
elif [ -f /etc/arch-release ]; then
    # Arch Linux
    sudo pacman -Sy --noconfirm openssl postgresql clang llvm
elif [ -f /etc/alpine-release ]; then
    # Alpine
    apk add --no-cache openssl-dev postgresql-dev clang llvm
fi

# Set environment variables for optimization
export RUSTFLAGS="-C target-cpu=native -C link-arg=-s -C opt-level=3 -C codegen-units=1 -C lto=fat"

# Build with maximum optimization
echo "Building ultra-optimized pubmed-matcher in release mode..."
cargo build --release

# Create a symbolic link to the ultra-optimized version
echo "Creating symbolic link to the ultra-optimized binary..."
cd target/release
ln -sf pubmed-ultra-matcher pubmed-matcher

echo "Build complete. The ultra-optimized executable is at target/release/pubmed-ultra-matcher"
echo "You can run it using the standard './target/release/pubmed-matcher' command as well."