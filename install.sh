#!/usr/bin/env bash
#
# FrankenSQLite installer
#
# One-line install (cache-busting query avoids stale proxy/CDN copies):
#   curl -fsSL "https://raw.githubusercontent.com/Dicklesworthstone/frankensqlite/main/install.sh?$(date +%s)" | bash
#
# Examples:
#   bash install.sh --version v0.1.19
#   bash install.sh --offline ./fsqlite-0.1.19-linux_amd64.tar.gz --checksum SHA256
#
set -euo pipefail
umask 022
shopt -s lastpipe 2>/dev/null || true

OWNER="${FSQLITE_GITHUB_OWNER:-Dicklesworthstone}"
REPO="${FSQLITE_GITHUB_REPO:-frankensqlite}"
VERSION="${FSQLITE_VERSION:-}"
DEST="${FSQLITE_INSTALL_DIR:-$HOME/.local/bin}"
OFFLINE_ARCHIVE=""
OFFLINE_CHECKSUM=""
FROM_SOURCE=0
POST_VERIFY=1
NO_VERIFY=0
EASY_MODE=0
QUIET=0
NO_GUM=0
SYSTEM=0
DEST_EXPLICIT=0
HAS_GUM=0
TMP_DIR=""
STAGED_BINARY=""
LOCK_DIR="${FSQLITE_INSTALL_LOCK_DIR:-${TMPDIR:-/tmp}/fsqlite-install.lock.d}"
LOCKED=0
PROXY_ARGS=()
MINISIGN_PUBLIC_KEY="RWTQoKUb0Ue4NsqTpPWnABCrIU0+m25zsMlbv6UcRClQ7jmRP3A7NmTB"

if command -v gum >/dev/null 2>&1 && [[ -t 1 ]]; then
  HAS_GUM=1
fi

info() {
  [[ "$QUIET" -eq 1 ]] && return 0
  if [[ "$HAS_GUM" -eq 1 && "$NO_GUM" -eq 0 ]]; then
    gum style --foreground 39 -- "-> $*"
  else
    printf '\033[0;34m->\033[0m %s\n' "$*"
  fi
}

ok() {
  [[ "$QUIET" -eq 1 ]] && return 0
  if [[ "$HAS_GUM" -eq 1 && "$NO_GUM" -eq 0 ]]; then
    gum style --foreground 42 "OK $*"
  else
    printf '\033[0;32mOK\033[0m %s\n' "$*"
  fi
}

warn() {
  [[ "$QUIET" -eq 1 ]] && return 0
  if [[ "$HAS_GUM" -eq 1 && "$NO_GUM" -eq 0 ]]; then
    gum style --foreground 214 "WARN $*"
  else
    printf '\033[1;33mWARN\033[0m %s\n' "$*" >&2
  fi
}

die() {
  if [[ "$HAS_GUM" -eq 1 && "$NO_GUM" -eq 0 ]]; then
    gum style --foreground 196 "ERROR $*" >&2
  else
    printf '\033[0;31mERROR\033[0m %s\n' "$*" >&2
  fi
  exit 1
}

run_with_spinner() {
  local title="$1"
  shift
  if [[ $(type -t "${1:-}" 2>/dev/null || true) == "function" ]]; then
    info "$title"
    "$@"
    return
  fi
  if [[ "$HAS_GUM" -eq 1 && "$NO_GUM" -eq 0 && "$QUIET" -eq 0 ]]; then
    gum spin --spinner dot --title "$title" -- "$@"
  else
    info "$title"
    "$@"
  fi
}

run_bounded() {
  local limit="$1"
  shift
  if command -v timeout >/dev/null 2>&1; then
    if timeout --version 2>/dev/null | grep -qi 'GNU coreutils'; then
      timeout --signal=TERM --kill-after=2 "${limit}s" "$@"
    else
      timeout "$limit" "$@"
    fi
  elif command -v gtimeout >/dev/null 2>&1; then
    gtimeout --signal=TERM --kill-after=2 "${limit}s" "$@"
  elif command -v perl >/dev/null 2>&1; then
    perl -MPOSIX -e '
      my $limit = shift @ARGV;
      my $pid = fork();
      die "fork failed: $!\n" unless defined $pid;
      if ($pid == 0) {
        my $rc = POSIX::setpgid(0, 0);
        die "setpgid failed: $!\n" unless defined $rc;
        exec {$ARGV[0]} @ARGV;
        exit 127;
      }
      POSIX::setpgid($pid, $pid);
      my $timed_out = 0;
      local $SIG{ALRM} = sub {
        $timed_out = 1;
        kill "TERM", -$pid;
        select undef, undef, undef, 0.25;
        kill "KILL", -$pid;
      };
      alarm $limit;
      waitpid($pid, 0);
      my $status = $?;
      alarm 0;
      kill "TERM", -$pid;
      select undef, undef, undef, 0.05;
      kill "KILL", -$pid;
      exit 124 if $timed_out;
      exit(128 + ($status & 127)) if $status & 127;
      exit($status >> 8);
    ' "$limit" "$@"
  else
    die "timeout, gtimeout, or perl is required for bounded binary verification"
  fi
}

draw_box() {
  local color="$1"
  shift
  local lines=("$@") line width=0 len border=""
  for line in "${lines[@]}"; do
    len=${#line}
    (( len > width )) && width=$len
  done
  for ((len=0; len<width+4; len++)); do border+="="; done
  printf '\033[%sm+%s+\033[0m\n' "$color" "$border"
  for line in "${lines[@]}"; do
    printf '\033[%sm|\033[0m  %-*s  \033[%sm|\033[0m\n' "$color" "$width" "$line" "$color"
  done
  printf '\033[%sm+%s+\033[0m\n' "$color" "$border"
}

usage() {
  cat <<'EOF'
Usage: install.sh [OPTIONS]

Options:
  --version vX.Y.Z      Install an exact release (default: latest)
  --dest DIR            Install directory (default: ~/.local/bin)
  --system              Install to /usr/local/bin
  --easy-mode           Add the install directory to shell PATH files
  --verify              Run version and SQL smoke tests (the default)
  --skip-post-verify    Skip the post-install version and SQL smoke tests
  --offline ARCHIVE     Install a previously downloaded .tar.gz archive
  --checksum SHA256     Expected checksum for --offline
  --from-source         Build the selected release with cargo
  --no-verify           Skip checksum/signature verification (unsafe)
  --quiet, -q           Suppress non-error output
  --no-gum              Disable gum formatting
  --help, -h            Show this help

Environment:
  FSQLITE_VERSION, FSQLITE_INSTALL_DIR, HTTPS_PROXY, HTTP_PROXY
  FSQLITE_KEEP_TEMP=1   Preserve the temporary directory for diagnostics
  FSQLITE_KEEP_LOCK=1   Preserve the installer lock for diagnostics
EOF
}

require_value() {
  [[ -n "${2:-}" && "${2:-}" != -* ]] || die "$1 requires a value"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version) require_value "$1" "${2:-}"; VERSION="$2"; shift 2 ;;
    --dest) require_value "$1" "${2:-}"; DEST="$2"; DEST_EXPLICIT=1; shift 2 ;;
    --system) SYSTEM=1; DEST="/usr/local/bin"; shift ;;
    --easy-mode) EASY_MODE=1; shift ;;
    --verify) POST_VERIFY=1; shift ;;
    --skip-post-verify) POST_VERIFY=0; shift ;;
    --offline) require_value "$1" "${2:-}"; OFFLINE_ARCHIVE="$2"; shift 2 ;;
    --checksum) require_value "$1" "${2:-}"; OFFLINE_CHECKSUM="$2"; shift 2 ;;
    --from-source) FROM_SOURCE=1; shift ;;
    --no-verify) NO_VERIFY=1; shift ;;
    --quiet|-q) QUIET=1; shift ;;
    --no-gum) NO_GUM=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

[[ -n "$OFFLINE_ARCHIVE" && "$FROM_SOURCE" -eq 1 ]] \
  && die "--offline and --from-source are mutually exclusive"
[[ -n "$OFFLINE_CHECKSUM" && -z "$OFFLINE_ARCHIVE" ]] \
  && die "--checksum requires --offline ARCHIVE"
[[ "$SYSTEM" -eq 1 && "$DEST_EXPLICIT" -eq 1 ]] \
  && die "--system and --dest are mutually exclusive"

setup_proxy() {
  if [[ -n "${HTTPS_PROXY:-}" ]]; then
    PROXY_ARGS=(--proxy "$HTTPS_PROXY")
  elif [[ -n "${HTTP_PROXY:-}" ]]; then
    PROXY_ARGS=(--proxy "$HTTP_PROXY")
  fi
}

curl_fetch() {
  # Bash 3.2 treats a declared-but-empty array as unset under `set -u`.
  # The `+` guard preserves zero arguments when no proxy is configured.
  curl --proto '=https' --tlsv1.2 --fail --silent --show-error --location \
    --connect-timeout 10 --retry 3 --retry-delay 1 \
    ${PROXY_ARGS[@]+"${PROXY_ARGS[@]}"} "$1" --output "$2"
}

resolve_version() {
  if [[ -n "$VERSION" ]]; then
    [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]] && VERSION="v$VERSION"
    [[ "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]] \
      || die "invalid version '$VERSION'; expected vX.Y.Z"
    return 0
  fi
  [[ -n "$OFFLINE_ARCHIVE" ]] && die "--offline requires --version vX.Y.Z"
  local api="https://api.github.com/repos/${OWNER}/${REPO}/releases/latest" tag=""
  tag=$(curl --proto '=https' --tlsv1.2 --fail --silent --show-error --location \
    --connect-timeout 5 ${PROXY_ARGS[@]+"${PROXY_ARGS[@]}"} \
    -H 'Accept: application/vnd.github+json' "$api" \
    | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1) || true
  if [[ ! "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]]; then
    tag=$(curl --proto '=https' --tlsv1.2 --fail --silent --show-error --location \
      --connect-timeout 5 ${PROXY_ARGS[@]+"${PROXY_ARGS[@]}"} \
      --output /dev/null --write-out '%{url_effective}' \
      "https://github.com/${OWNER}/${REPO}/releases/latest" | sed -E 's|.*/tag/||') || true
  fi
  [[ "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]] \
    || die "could not resolve the latest release; pass --version vX.Y.Z"
  VERSION="$tag"
}

detect_platform() {
  local os arch
  os=$(uname -s | tr '[:upper:]' '[:lower:]')
  arch=$(uname -m)
  case "$arch" in
    x86_64|amd64) arch="amd64" ;;
    arm64|aarch64) arch="arm64" ;;
    *) die "unsupported architecture: $arch" ;;
  esac
  case "$os" in
    linux) PLATFORM="linux_${arch}"; ARCHIVE_EXT="tar.gz"; BINARY="fsqlite" ;;
    darwin) PLATFORM="darwin_${arch}"; ARCHIVE_EXT="tar.gz"; BINARY="fsqlite" ;;
    mingw*|msys*|cygwin*) die "use install.ps1 from PowerShell on Windows" ;;
    *) die "unsupported operating system: $os" ;;
  esac
  if [[ "$os" == "linux" ]] && grep -qi microsoft /proc/version 2>/dev/null; then
    warn "WSL detected; installing the Linux artifact"
  fi
}

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    die "sha256sum or shasum is required"
  fi
}

verify_archive() {
  local archive="$1" expected="$2" actual
  actual=$(sha256_file "$archive")
  [[ "$actual" == "$expected" ]] \
    || die "checksum mismatch for $(basename "$archive"): expected $expected, got $actual"
  ok "SHA-256 verified"
}

cleanup() {
  if [[ -n "$STAGED_BINARY" && -e "$STAGED_BINARY" ]]; then
    warn "unpromoted candidate preserved for diagnostics: $STAGED_BINARY"
  fi
  if [[ "${FSQLITE_KEEP_TEMP:-0}" != "1" && -n "$TMP_DIR" ]]; then
    case "$TMP_DIR" in
      "${TMPDIR:-/tmp}"/fsqlite-install.*) rm -rf -- "$TMP_DIR" ;;
      *) warn "refusing to remove unexpected temporary path: $TMP_DIR" ;;
    esac
  elif [[ -n "$TMP_DIR" ]]; then
    warn "temporary directory preserved: $TMP_DIR"
  fi
  if [[ "$LOCKED" -eq 1 && "${FSQLITE_KEEP_LOCK:-0}" != "1" ]]; then
    rm -f -- "$LOCK_DIR/pid"
    rmdir -- "$LOCK_DIR" 2>/dev/null || true
  elif [[ "$LOCKED" -eq 1 ]]; then
    warn "installer lock preserved for diagnostics: $LOCK_DIR"
  fi
}

acquire_lock() {
  if mkdir "$LOCK_DIR" 2>/dev/null; then
    LOCKED=1
    printf '%s\n' "$$" > "$LOCK_DIR/pid"
  else
    local holder=""
    [[ -f "$LOCK_DIR/pid" ]] && holder=$(sed -n '1p' "$LOCK_DIR/pid" 2>/dev/null || true)
    if [[ -n "$holder" ]] && kill -0 "$holder" 2>/dev/null; then
      die "another installer is running (PID $holder)"
    fi
    die "stale installer lock at $LOCK_DIR; inspect it before removing it"
  fi
  trap cleanup EXIT
}

preflight() {
  command -v curl >/dev/null 2>&1 || [[ -n "$OFFLINE_ARCHIVE" || "$FROM_SOURCE" -eq 1 ]] \
    || die "curl is required"
  [[ "$SYSTEM" -eq 0 || $(id -u) -eq 0 || -n $(command -v sudo || true) ]] \
    || die "--system requires root or sudo"
  local parent="$DEST"
  while [[ ! -e "$parent" && "$parent" != "/" && "$parent" != "." ]]; do
    parent=$(dirname "$parent")
  done
  [[ -d "$parent" ]] || die "could not find an existing parent directory for $DEST"
  if command -v df >/dev/null 2>&1; then
    local free_kb
    free_kb=$(df -Pk "$parent" | awk 'NR==2 {print $4}')
    [[ -z "$free_kb" || "$free_kb" -ge 10240 ]] || die "at least 10 MiB of free space is required"
  fi
}

verify_binary() {
  local binary="$1" output version_clean="${VERSION#v}"
  output=$(run_bounded 15 "$binary" --version) \
    || die "version check failed or timed out for $binary"
  [[ "$output" == "fsqlite $version_clean" ]] \
    || die "binary version mismatch: expected fsqlite $version_clean, got $output"
  output=$(run_bounded 15 "$binary" --batch --command "SELECT 1 + 2;") \
    || die "SQL smoke test failed or timed out for $binary"
  [[ "$output" == "3" ]] || die "SQL smoke test failed: $output"
}

stage_binary() {
  local source="$1"
  STAGED_BINARY="${DEST}/.fsqlite.install.$$.${TMP_DIR##*.}"
  if [[ "$SYSTEM" -eq 1 && $(id -u) -ne 0 ]]; then
    sudo install -m 0755 "$source" "$STAGED_BINARY"
  else
    install -m 0755 "$source" "$STAGED_BINARY"
  fi
}

promote_binary() {
  local target="$DEST/fsqlite"
  if [[ "$SYSTEM" -eq 1 && $(id -u) -ne 0 ]]; then
    sudo mv -f "$STAGED_BINARY" "$target"
  else
    mv -f "$STAGED_BINARY" "$target"
  fi
  STAGED_BINARY=""
}

install_from_source() {
  command -v cargo >/dev/null 2>&1 || die "cargo is required for --from-source"
  local root="$TMP_DIR/cargo-root" version_clean="${VERSION#v}"
  if [[ "$QUIET" -eq 1 ]]; then
    if ! cargo +nightly install fsqlite-cli --version "=$version_clean" \
      --locked --root "$root" --bin fsqlite > "$TMP_DIR/cargo-install.log" 2>&1; then
      tail -n 40 "$TMP_DIR/cargo-install.log" >&2 || true
      FSQLITE_KEEP_TEMP=1
      die "source build failed; full log preserved at $TMP_DIR/cargo-install.log"
    fi
  else
    run_with_spinner "Building fsqlite-cli ${VERSION} from source" \
      cargo +nightly install fsqlite-cli --version "=$version_clean" \
        --locked --root "$root" --bin fsqlite
  fi
  stage_binary "$root/bin/fsqlite"
  [[ "$POST_VERIFY" -eq 1 ]] && verify_binary "$STAGED_BINARY"
  promote_binary
}

install_from_archive() {
  local archive="$1"
  case "$archive" in
    *.tar.gz|*.tgz)
      local archive_listing entry_count entry type_listing
      archive_listing=$(tar -tzf "$archive") || die "could not list release archive"
      entry_count=$(printf '%s\n' "$archive_listing" | awk 'END {print NR}')
      [[ "$entry_count" -eq 1 ]] || die "release archive must contain exactly one file"
      entry="${archive_listing#./}"
      [[ "$entry" == "fsqlite" ]] || die "unexpected release archive entry: $archive_listing"
      type_listing=$(tar -tvzf "$archive") || die "could not inspect release archive"
      [[ "${type_listing:0:1}" == "-" ]] || die "release archive entry must be a regular file"
      tar -xzf "$archive" -C "$TMP_DIR"
      ;;
    *) die "unsupported archive type: $archive" ;;
  esac
  local candidate="$TMP_DIR/$BINARY"
  [[ -n "$candidate" && -f "$candidate" ]] || die "$BINARY not found in archive"
  chmod 0755 "$candidate" 2>/dev/null || true
  stage_binary "$candidate"
  [[ "$POST_VERIFY" -eq 1 ]] && verify_binary "$STAGED_BINARY"
  promote_binary
}

maybe_add_path() {
  case ":$PATH:" in
    *:"$DEST":*) return 0 ;;
  esac
  if [[ "$EASY_MODE" -eq 1 ]]; then
    local rc quoted_dest path_line
    printf -v quoted_dest '%q' "$DEST"
    path_line="export PATH=${quoted_dest}:\$PATH"
    for rc in "$HOME/.zshrc" "$HOME/.bashrc"; do
      [[ -e "$rc" && -w "$rc" ]] || continue
      grep -F "$path_line" "$rc" >/dev/null 2>&1 \
        || printf '\n# FrankenSQLite\n%s\n' "$path_line" >> "$rc"
    done
    warn "PATH configuration updated where possible; restart your shell"
  else
    warn "$DEST is not on PATH; add it to use fsqlite by name"
  fi
}

post_install_verify() {
  verify_binary "$DEST/fsqlite"
  ok "version and SQL smoke tests passed"
}

setup_proxy
resolve_version
detect_platform

if [[ "$QUIET" -eq 0 ]]; then
  if [[ "$HAS_GUM" -eq 1 && "$NO_GUM" -eq 0 ]]; then
    gum style --border double --border-foreground 39 --padding '0 1' \
      "$(gum style --foreground 42 --bold 'FrankenSQLite installer')" \
      "$(gum style --foreground 245 "Installing ${VERSION} for ${PLATFORM}")"
  else
    draw_box '1;32' 'FrankenSQLite installer' "Installing ${VERSION} for ${PLATFORM}"
  fi
fi

preflight
if [[ "$SYSTEM" -eq 1 && $(id -u) -ne 0 ]]; then
  sudo mkdir -p "$DEST"
else
  mkdir -p "$DEST"
fi
acquire_lock
TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/fsqlite-install.XXXXXXXX")

if [[ "$FROM_SOURCE" -eq 1 ]]; then
  install_from_source
else
  archive_name="fsqlite-${VERSION#v}-${PLATFORM}.${ARCHIVE_EXT}"
  if [[ -n "$OFFLINE_ARCHIVE" ]]; then
    [[ -f "$OFFLINE_ARCHIVE" ]] || die "offline archive not found: $OFFLINE_ARCHIVE"
    archive="$OFFLINE_ARCHIVE"
    if [[ "$NO_VERIFY" -eq 0 ]]; then
      [[ "$OFFLINE_CHECKSUM" =~ ^[0-9a-fA-F]{64}$ ]] \
        || die "--offline requires --checksum SHA256 (or explicitly --no-verify)"
      verify_archive "$archive" "$(printf '%s' "$OFFLINE_CHECKSUM" | tr '[:upper:]' '[:lower:]')"
    else
      warn "archive verification disabled"
    fi
  else
    base="https://github.com/${OWNER}/${REPO}/releases/download/${VERSION}"
    archive="$TMP_DIR/$archive_name"
    run_with_spinner "Downloading $archive_name" curl_fetch "$base/$archive_name" "$archive" \
      || die "prebuilt artifact download failed; retry or explicitly use --from-source"
    if [[ "$NO_VERIFY" -eq 0 ]]; then
      manifest="$TMP_DIR/SHA256SUMS.txt"
      signature="$TMP_DIR/SHA256SUMS.txt.minisig"
      curl_fetch "$base/SHA256SUMS.txt" "$manifest"
      if command -v minisign >/dev/null 2>&1; then
        curl_fetch "$base/SHA256SUMS.txt.minisig" "$signature"
        minisign -Vm "$manifest" -x "$signature" -P "$MINISIGN_PUBLIC_KEY" >/dev/null \
          || die "release checksum signature verification failed"
        ok "release manifest signature verified"
      else
        warn "minisign is not installed; authenticity check skipped (SHA-256 still required)"
      fi
      expected=$(awk -v file="$archive_name" '$2 == file || $2 == "*" file {print $1; exit}' "$manifest")
      [[ "$expected" =~ ^[0-9a-fA-F]{64}$ ]] || die "checksum missing for $archive_name"
      verify_archive "$archive" "$(printf '%s' "$expected" | tr '[:upper:]' '[:lower:]')"
    else
      warn "archive verification disabled"
    fi
  fi
  install_from_archive "$archive"
fi

ok "installed fsqlite ${VERSION} to $DEST/fsqlite"
maybe_add_path
[[ "$POST_VERIFY" -eq 1 ]] && post_install_verify

if [[ "$QUIET" -eq 0 ]]; then
  draw_box '1;32' 'Installation complete' "Binary: $DEST/fsqlite" \
    "Verify: $DEST/fsqlite --version" "Uninstall: remove $DEST/fsqlite"
fi
