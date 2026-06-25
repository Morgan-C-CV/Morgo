#!/usr/bin/env sh
set -eu

REPO="${MORGO_REPO:-Morgan-C-CV/Morgo}"
VERSION="${MORGO_VERSION:-latest}"
INSTALL_DIR="${MORGO_INSTALL_DIR:-$HOME/.local/bin}"
BIN_NAME="${MORGO_BIN_NAME:-morgo}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "morgo installer: missing required command: $1" >&2
    exit 1
  fi
}

need uname
need tar
need mktemp

system="$(uname -s)"
machine="$(uname -m)"

case "$system" in
  Darwin) os="apple-darwin" ;;
  Linux) os="unknown-linux-gnu" ;;
  *)
    echo "morgo installer: unsupported OS: $system" >&2
    exit 1
    ;;
esac

if [ "$system" = "Darwin" ]; then
  case "$machine" in
    arm64 | aarch64) arch="aarch64" ;;
    x86_64 | amd64) arch="x86_64" ;;
    *)
      echo "morgo installer: unsupported macOS CPU architecture: $machine" >&2
      exit 1
      ;;
  esac
else
  case "$machine" in
    x86_64 | amd64) arch="x86_64" ;;
    *)
      echo "morgo installer: unsupported Linux CPU architecture: $machine" >&2
      exit 1
      ;;
  esac
fi

target="${arch}-${os}"
archive="morgo-${target}.tar.gz"

if [ "$VERSION" = "latest" ]; then
  url="https://github.com/${REPO}/releases/latest/download/${archive}"
else
  url="https://github.com/${REPO}/releases/download/${VERSION}/${archive}"
fi

tmp="$(mktemp -d)"
cleanup() {
  rm -rf "$tmp"
}
trap cleanup EXIT INT TERM

download() {
  if command -v curl >/dev/null 2>&1; then
    curl --fail --location --show-error --silent "$url" --output "$tmp/$archive"
  elif command -v wget >/dev/null 2>&1; then
    wget -q "$url" -O "$tmp/$archive"
  else
    echo "morgo installer: missing required command: curl or wget" >&2
    exit 1
  fi
}

echo "Downloading Morgo from $url"
download

tar -xzf "$tmp/$archive" -C "$tmp"
mkdir -p "$INSTALL_DIR"
mv "$tmp/morgo" "$INSTALL_DIR/$BIN_NAME"
chmod +x "$INSTALL_DIR/$BIN_NAME"

echo "Morgo installed to $INSTALL_DIR/$BIN_NAME"
case ":$PATH:" in
  *":$INSTALL_DIR:"*) ;;
  *)
    echo "Add this to your shell profile if morgo is not found:"
    echo "  export PATH=\"$INSTALL_DIR:\$PATH\""
    ;;
esac
echo "Start the TUI with:"
echo "  $BIN_NAME"
