#!/bin/sh
# probe-rs CF mode installer. POSIX sh compatible, including:
#   curl -fsSL <url>/cf-install.sh | sh -s -- install -id=... -secret=... -url=...
# The shell only bootstraps the binary/service. Rust parses and validates all
# CF protocol options through configure-cf-compat.
set -eu

SCRIPT_VERSION=v0.1.4-beta.6
DEFAULT_UPDATE_REPOSITORY=ukuq/probe-rs

log() { printf '%s\n' "[probe-rs] $*"; }
die() { printf '%s\n' "[probe-rs] error: $*" >&2; exit 1; }

USER_SERVICE=false
if [ "$(id -u)" = 0 ]; then
    BIN_DIR=/usr/local/bin
    CONF_DIR=/etc/probe-rs
    DATA_DIR=/var/lib/probe-rs
    UNIT_DST=/etc/systemd/system/probe-rs.service
    JOURNAL_CMD='journalctl -u probe-rs'
else
    [ -n "${HOME:-}" ] || die "HOME is required for a non-root install"
    USER_SERVICE=true
    CONFIG_HOME=${XDG_CONFIG_HOME:-$HOME/.config}
    DATA_HOME=${XDG_DATA_HOME:-$HOME/.local/share}
    BIN_DIR=$HOME/.local/bin
    CONF_DIR=$CONFIG_HOME/probe-rs
    DATA_DIR=$DATA_HOME/probe-rs
    UNIT_DST=$CONFIG_HOME/systemd/user/probe-rs.service
    JOURNAL_CMD='journalctl --user -u probe-rs'
fi
BIN_DST=$BIN_DIR/probe-rs
CONFIG_PATH=$CONF_DIR/config.toml

service_ctl() {
    if [ "$USER_SERVICE" = true ]; then
        systemctl --user "$@"
    else
        systemctl "$@"
    fi
}

require_service_manager() {
    command -v systemctl >/dev/null 2>&1 || die "systemd is required"
    if [ "$USER_SERVICE" = true ] && ! service_ctl show-environment >/dev/null 2>&1; then
        die "a non-root install requires a running systemd user session (systemctl --user is unavailable)"
    fi
}

warn_linger() {
    [ "$USER_SERVICE" = true ] || return 0
    command -v loginctl >/dev/null 2>&1 || return 0
    linger=$(loginctl show-user "$(id -u)" --property=Linger --value 2>/dev/null || true)
    if [ "$linger" = no ]; then
        log "warning: user lingering is disabled; the service may stop after logout"
        log "ask an administrator to run: loginctl enable-linger $(id -un)"
    fi
}

systemd_escape() { printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g; s/%/%%/g'; }

usage() {
    printf '%s\n' \
        'Usage: sh cf-install.sh install -id=<UUID> -secret=<SECRET> -url=<HTTP(S) URL> [options]' \
        '       sh cf-install.sh uninstall [--purge]' \
        '' \
        'CF-compatible options:' \
        '  -collect_interval= / -collect=       collection seconds (0 keeps CF compatibility)' \
        '  -wss_report_interval=                WSS report seconds (1-5; default/preserved: 2)' \
        '  -interval=                           report seconds' \
        '  -connection_mode=                    auto (WSS + fallback) / http' \
        '  -ping_mode=                          tcp / icmp' \
        '  -reset_day=                          0-31' \
        '  -ct= -cu= -cm= -bd=                 ping targets' \
        '  -interface= / -interfaces= / -iface= comma-separated interface globs' \
        '  -auto_update= / -auto-update=        0/1' \
        '  -rx_correction= -tx_correction=      current billing-period totals in GiB' \
        '  -debug=                              0/1' \
        '  -install-version[=]                   release tag (default: script version)' \
        '  -update_repository[=]                 GitHub Release repository (owner/repo)' \
        '  -install-ghproxy[=]                   GitHub proxy URL prefix; persisted as update fallback' \
        '  -no_start / -no-start[=0|1]          install without starting' \
        '' \
        'probe-rs options:' \
        '  -reporter_id= / --reporter-id=       upsert one CF Reporter (default: cf)' \
        '  -update_channel=                     stable/prerelease' \
        '  -bin=                                local path or HTTP(S) binary URL'
}

parse_bool() {
    case "$2" in
        1|true|TRUE|yes|YES) eval "$1=true" ;;
        0|false|FALSE|no|NO) eval "$1=false" ;;
        # 空值按非法处理：-flag= 的笔误不该被静默当成 false。
        *) die "$3 must be 0 or 1" ;;
    esac
}

do_uninstall() {
    service_ctl disable --now probe-rs 2>/dev/null || true
    rm -f "$UNIT_DST" "$BIN_DST"
    service_ctl daemon-reload 2>/dev/null || true
    if [ "${1:-}" = "--purge" ]; then
        rm -rf "$CONF_DIR" "$DATA_DIR"
        log "uninstalled probe-rs and removed its config/data"
    else
        log "uninstalled probe-rs; config/data were kept"
    fi
}

COMMAND=${1:-}
[ "$#" -gt 0 ] && shift || true
case "$COMMAND" in
    uninstall)
        do_uninstall "${1:-}"
        exit 0
        ;;
    install) ;;
    *) usage; exit 1 ;;
esac

BIN= UPDATE_REPOSITORY= INSTALL_VERSION=$SCRIPT_VERSION GH_PROXY=
DEBUG=false NO_START=false
UPDATE_REPOSITORY_SET=false DEBUG_SET=false

# Only inspect options needed before the downloaded binary can run. The
# original argument vector remains untouched and is validated by Rust below.
parse_install_args() {
    while [ "$#" -gt 0 ]; do
        arg=$1
        shift
        case "$arg" in
            -update_repository=*|-update-repository=*|--update-repository=*)
                UPDATE_REPOSITORY=${arg#*=}; UPDATE_REPOSITORY_SET=true
                ;;
            -update_repository|-update-repository|--update-repository)
                [ "$#" -gt 0 ] || die "$arg requires a value"
                UPDATE_REPOSITORY=$1; UPDATE_REPOSITORY_SET=true
                shift
                ;;
            -debug=*)
                DEBUG=${arg#*=}; DEBUG_SET=true
                parse_bool DEBUG "$DEBUG" "debug"
                ;;
            -no_start=*|-no-start=*)
                NO_START=${arg#*=}
                parse_bool NO_START "$NO_START" "no_start"
                ;;
            -no_start|-no-start) NO_START=true ;;
            -install-version=*|--install-version=*) INSTALL_VERSION=${arg#*=} ;;
            -install-version|--install-version)
                [ "$#" -gt 0 ] || die "$arg requires a value"
                INSTALL_VERSION=$1
                shift
                ;;
            -install-ghproxy=*|--install-ghproxy=*) GH_PROXY=${arg#*=} ;;
            -install-ghproxy|--install-ghproxy)
                [ "$#" -gt 0 ] || die "$arg requires a value"
                GH_PROXY=$1
                shift
                ;;
            -bin=*|--bin=*) BIN=${arg#*=} ;;
            *) : ;;
        esac
    done
}

parse_install_args "$@"
require_service_manager

if [ "$UPDATE_REPOSITORY_SET" = true ]; then
    case "$UPDATE_REPOSITORY" in
        ''|/*|*/|*/*/*|./*|../*|*/.|*/..|*[!A-Za-z0-9_./-]*) die "update_repository must use owner/repo" ;;
    esac
fi
if [ -n "$GH_PROXY" ]; then
    case "$GH_PROXY" in http://*|https://*) ;; *) die "install-ghproxy must be an absolute HTTP(S) URL" ;; esac
    case "$GH_PROXY" in *'?'*|*'#'*|http://*@*|https://*@*)
        die "install-ghproxy must not contain credentials, a query string, or a fragment"
        ;;
    esac
fi
DOWNLOAD_REPOSITORY=${UPDATE_REPOSITORY:-$DEFAULT_UPDATE_REPOSITORY}
GITHUB_REPO=https://github.com/$DOWNLOAD_REPOSITORY
if [ "$DEBUG_SET" = false ] && [ -f "$UNIT_DST" ] && grep -q ' --debug' "$UNIT_DST"; then
    DEBUG=true
fi

install -d -m 0755 "$BIN_DIR"
install -d -m 0755 "$DATA_DIR"
install -d -m 0750 "$CONF_DIR"
install -d -m 0755 "$(dirname -- "$UNIT_DST")"

TMP_BIN=$(mktemp /tmp/probe-rs.XXXXXX)
TMP_SUM=
INSTALL_COMPLETE=false
WAS_ACTIVE=false
cleanup() {
    status=$?
    trap - 0 HUP INT TERM
    rm -f "$TMP_BIN"
    [ -z "$TMP_SUM" ] || rm -f "$TMP_SUM"
    if [ "$status" -ne 0 ] && [ "$WAS_ACTIVE" = true ] && [ "$INSTALL_COMPLETE" = false ]; then
        service_ctl start probe-rs 2>/dev/null || true
    fi
    exit "$status"
}
trap cleanup 0
trap 'exit 1' HUP INT TERM

arch=$(uname -m)
case "$arch" in
    x86_64|amd64) asset=probe-rs-linux-x86_64 ;;
    aarch64|arm64) asset=probe-rs-linux-aarch64 ;;
    loongarch64|loong64) asset=probe-rs-linux-loong64 ;;
    *) die "unsupported architecture: $arch" ;;
esac

if [ -z "$BIN" ]; then
    case "$INSTALL_VERSION" in
        latest) release_base=$GITHUB_REPO/releases/latest/download ;;
        '') die "install-version must not be empty" ;;
        *)
            case "$INSTALL_VERSION" in v*) tag=$INSTALL_VERSION ;; *) tag=v$INSTALL_VERSION ;; esac
            case "$tag" in *[!A-Za-z0-9._-]*) die "invalid install-version" ;; esac
            release_base=$GITHUB_REPO/releases/download/$tag
            ;;
    esac
    BIN=$release_base/$asset
    SUM_URL=$release_base/SHA256SUMS
    if [ -n "$GH_PROXY" ]; then
        PROXY_BIN=${GH_PROXY%/}/$BIN
        PROXY_SUM_URL=${GH_PROXY%/}/$SUM_URL
    fi
fi

if [ -f "$BIN" ]; then
    cp -f "$BIN" "$TMP_BIN"
else
    command -v curl >/dev/null 2>&1 || die "curl is required"
    log "downloading $BIN"
    if curl -fSL --connect-timeout 10 -o "$TMP_BIN" "$BIN"; then
        :
    elif [ -n "${PROXY_BIN:-}" ]; then
        log "direct binary download failed; trying proxy $GH_PROXY"
        curl -fSL --connect-timeout 10 -o "$TMP_BIN" "$PROXY_BIN" || die "binary download failed through direct and proxy URLs"
    else
        die "binary download failed"
    fi
    if [ -n "${SUM_URL:-}" ]; then
        command -v sha256sum >/dev/null 2>&1 || die "sha256sum is required"
        TMP_SUM=$(mktemp /tmp/probe-rs-sha.XXXXXX)
        if curl -fsSL --connect-timeout 10 -o "$TMP_SUM" "$SUM_URL"; then
            :
        elif [ -n "${PROXY_SUM_URL:-}" ]; then
            log "warning: direct SHA256SUMS download failed; falling back to proxy (checksum then only detects transfer corruption, not origin)"
            curl -fSL --connect-timeout 10 -o "$TMP_SUM" "$PROXY_SUM_URL" || die "checksum download failed through direct and proxy URLs"
        else
            die "checksum download failed"
        fi
        expected=$(awk -v name="$asset" '$2 == name || $2 == "*" name { print $1; exit }' "$TMP_SUM")
        [ -n "$expected" ] || die "checksum for $asset is missing"
        actual=$(sha256sum "$TMP_BIN" | awk '{print $1}')
        [ "$actual" = "$expected" ] || die "binary checksum mismatch"
    fi
fi
chmod 0755 "$TMP_BIN"

# configure-cf-compat owns the legacy CF argument surface. Check support before
# stopping the existing service or replacing its executable, so explicitly
# selected older releases fail without disturbing a working installation.
if ! "$TMP_BIN" configure-cf-compat --help >/dev/null 2>&1; then
    die "the selected probe-rs binary does not support this CF installer; use $SCRIPT_VERSION or a compatible -bin"
fi

if command -v pgrep >/dev/null 2>&1 && pgrep -x cf-probe >/dev/null 2>&1; then
    log "warning: official cf-probe is still running; using the same credentials will duplicate reports"
fi
if service_ctl is-active --quiet probe-rs 2>/dev/null; then
    WAS_ACTIVE=true
    service_ctl stop probe-rs
fi

install -m 0755 "$TMP_BIN" "$BIN_DST"

SELECTED_REPORTER=$("$BIN_DST" configure-cf-compat \
    --config "$CONFIG_PATH" \
    --net-static-path "$DATA_DIR/net_static.json" \
    -- \
    "$@")
[ -n "$SELECTED_REPORTER" ] || die "CF Reporter selection failed"
chmod 600 "$CONFIG_PATH"

DEBUG_ARG=
[ "$DEBUG" = false ] || DEBUG_ARG=' --debug'
if [ "$USER_SERVICE" = true ]; then
    unit_bin=$(systemd_escape "$BIN_DST")
    unit_config=$(systemd_escape "$CONFIG_PATH")
    cat > "$UNIT_DST" <<EOF
[Unit]
Description=probe-rs server monitoring agent

[Service]
Type=simple
ExecStart="$unit_bin" --config "$unit_config"$DEBUG_ARG
Restart=always
RestartSec=5
NoNewPrivileges=true
UMask=0077

[Install]
WantedBy=default.target
EOF
else
    cat > "$UNIT_DST" <<EOF
[Unit]
Description=probe-rs server monitoring agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=$BIN_DST$DEBUG_ARG
Restart=always
RestartSec=5
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=-$DATA_DIR -$CONF_DIR $BIN_DIR
ProtectHome=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOF
fi
chmod 0644 "$UNIT_DST"

service_ctl daemon-reload
if [ "$NO_START" = true ]; then
    service_ctl disable --now probe-rs >/dev/null 2>&1 || true
    INSTALL_COMPLETE=true
    log "installed without starting; Reporter '$SELECTED_REPORTER' is configured"
    warn_linger
    exit 0
fi
service_ctl enable probe-rs >/dev/null 2>&1 || true
service_ctl restart probe-rs
INSTALL_COMPLETE=true
sleep 1
if service_ctl is-active --quiet probe-rs; then
    log "installed and running; Reporter '$SELECTED_REPORTER' is configured"
    warn_linger
else
    die "service failed to start; run: $JOURNAL_CMD -n 20 --no-pager"
fi
