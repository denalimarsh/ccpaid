#!/usr/bin/env sh
set -eu

repo="paid-ai/ccpaid"
version="${CCPAID_VERSION:-latest}"
bin_name="ccpaid"

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ccpaid install: missing required command: $1" >&2
    exit 1
  fi
}

detect_asset() {
  os="$(uname -s)"
  arch="$(uname -m)"

  case "$os:$arch" in
    Darwin:arm64)
      echo "ccpaid-darwin-arm64"
      ;;
    Linux:x86_64|Linux:amd64)
      echo "ccpaid-linux-x64"
      ;;
    Linux:aarch64|Linux:arm64)
      echo "ccpaid-linux-arm64"
      ;;
    *)
      echo "ccpaid install: unsupported platform: $os $arch" >&2
      exit 1
      ;;
  esac
}

download() {
  url="$1"
  output="$2"

  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$url" -o "$output"
  elif command -v wget >/dev/null 2>&1; then
    wget -q "$url" -O "$output"
  else
    echo "ccpaid install: curl or wget is required" >&2
    exit 1
  fi
}

checksum_cmd() {
  if command -v shasum >/dev/null 2>&1; then
    echo "shasum -a 256"
  elif command -v sha256sum >/dev/null 2>&1; then
    echo "sha256sum"
  else
    echo ""
  fi
}

verify_checksum() {
  asset="$1"
  dir="$2"
  sum_cmd="$(checksum_cmd)"

  if [ -z "$sum_cmd" ]; then
    echo "ccpaid install: checksum tool not found; skipping checksum verification" >&2
    return
  fi

  if ! grep "  $asset\$" "$dir/checksums.txt" > "$dir/checksums.expected"; then
    echo "ccpaid install: checksum for $asset not found; skipping checksum verification" >&2
    return
  fi

  (
    cd "$dir"
    $sum_cmd -c checksums.expected
  )
}

choose_install_dir() {
  if [ -n "${CCPAID_INSTALL_DIR:-}" ]; then
    echo "$CCPAID_INSTALL_DIR"
  elif [ -d /usr/local/bin ] && [ -w /usr/local/bin ]; then
    echo "/usr/local/bin"
  else
    echo "$HOME/.local/bin"
  fi
}

need_cmd uname
need_cmd chmod
need_cmd mkdir
need_cmd mktemp

asset="$(detect_asset)"

if [ "$version" = "latest" ]; then
  base_url="https://github.com/$repo/releases/latest/download"
else
  base_url="https://github.com/$repo/releases/download/$version"
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT INT TERM

echo "Downloading $asset from $repo ($version)"
download "$base_url/$asset" "$tmp_dir/$asset"
download "$base_url/checksums.txt" "$tmp_dir/checksums.txt"
verify_checksum "$asset" "$tmp_dir"
chmod +x "$tmp_dir/$asset"

install_dir="$(choose_install_dir)"
mkdir -p "$install_dir"

if ! mv "$tmp_dir/$asset" "$install_dir/$bin_name" 2>/dev/null; then
  echo "ccpaid install: cannot write to $install_dir" >&2
  echo "Try setting CCPAID_INSTALL_DIR to a writable directory." >&2
  exit 1
fi

echo "Installed ccpaid to $install_dir/$bin_name"

case ":$PATH:" in
  *":$install_dir:"*) ;;
  *)
    echo "Note: $install_dir is not on your PATH."
    echo "Add this to your shell profile:"
    echo "  export PATH=\"$install_dir:\$PATH\""
    ;;
esac

"$install_dir/$bin_name" -h
