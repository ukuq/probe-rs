#!/bin/sh
# probe-rs CF mode installer. POSIX sh compatible, including:
#   curl -fsSL <url>/cf-install.sh | sh -s -- install -id=... -secret=... -url=...
set -eu

SCRIPT_VERSION=v0.1.4-beta.5
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

normalize_uint() {
    case "$2" in ''|*[!0-9]*) die "$1 must be a non-negative integer" ;; esac
    normalized=$(printf '%s' "$2" | sed 's/^0*//')
    [ -n "$normalized" ] || normalized=0
    printf '%s' "$normalized"
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

ID= SECRET= URL= BIN=
COLLECT= WSS_REPORT= REPORT= RESET_DAY= INTERFACES= CONNECTION_MODE= PING_MODE=
CT= CU= CM= BD=
AUTO_UPDATE= UPDATE_REPOSITORY= UPDATE_CHANNEL= RX_CORRECTION= TX_CORRECTION=
REPORTER_ID=cf INSTALL_VERSION=$SCRIPT_VERSION GH_PROXY=
DEBUG=false NO_START=false
ID_SET=false SECRET_SET=false URL_SET=false
COLLECT_SET=false WSS_REPORT_SET=false REPORT_SET=false RESET_SET=false INTERFACES_SET=false CONNECTION_MODE_SET=false PING_MODE_SET=false
CT_SET=false CU_SET=false CM_SET=false BD_SET=false
AUTO_UPDATE_SET=false UPDATE_REPOSITORY_SET=false UPDATE_CHANNEL_SET=false
RX_SET=false TX_SET=false DEBUG_SET=false

while [ "$#" -gt 0 ]; do
    arg=$1
    shift
    case "$arg" in
        -id=*) ID=${arg#*=}; ID_SET=true ;;
        -secret=*) SECRET=${arg#*=}; SECRET_SET=true ;;
        -url=*) URL=${arg#*=}; URL_SET=true ;;
        -collect_interval=*|-collect=*) COLLECT=${arg#*=}; COLLECT_SET=true ;;
        -wss_report_interval=*|-wss-report-interval=*) WSS_REPORT=${arg#*=}; WSS_REPORT_SET=true ;;
        -interval=*) REPORT=${arg#*=}; REPORT_SET=true ;;
        -connection_mode=*|-connection-mode=*) CONNECTION_MODE=${arg#*=}; CONNECTION_MODE_SET=true ;;
        -ping_mode=*|-ping-mode=*) PING_MODE=${arg#*=}; PING_MODE_SET=true ;;
        -reset_day=*) RESET_DAY=${arg#*=}; RESET_SET=true ;;
        -ct=*) CT=${arg#*=}; CT_SET=true ;;
        -cu=*) CU=${arg#*=}; CU_SET=true ;;
        -cm=*) CM=${arg#*=}; CM_SET=true ;;
        -bd=*) BD=${arg#*=}; BD_SET=true ;;
        -interface=*|-interfaces=*|-iface=*) INTERFACES=${arg#*=}; INTERFACES_SET=true ;;
        -auto_update=*|-auto-update=*)
            AUTO_UPDATE=${arg#*=}; AUTO_UPDATE_SET=true
            parse_bool AUTO_UPDATE "$AUTO_UPDATE" "auto_update"
            ;;
        -update_channel=*|--update-channel=*)
            UPDATE_CHANNEL=${arg#*=}; UPDATE_CHANNEL_SET=true
            ;;
        -update_repository=*|-update-repository=*|--update-repository=*)
            UPDATE_REPOSITORY=${arg#*=}; UPDATE_REPOSITORY_SET=true
            ;;
        -update_repository|-update-repository|--update-repository)
            [ "$#" -gt 0 ] || die "$arg requires a value"
            UPDATE_REPOSITORY=$1; UPDATE_REPOSITORY_SET=true
            shift
            ;;
        -rx_correction=*) RX_CORRECTION=${arg#*=}; RX_SET=true ;;
        -tx_correction=*) TX_CORRECTION=${arg#*=}; TX_SET=true ;;
        -reporter_id=*|--reporter-id=*)
            REPORTER_ID=${arg#*=}
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
        *) die "unknown option: $arg" ;;
    esac
done

[ "$ID_SET" = false ] || [ -n "$ID" ] || die "id must not be empty"
[ "$SECRET_SET" = false ] || [ -n "$SECRET" ] || die "secret must not be empty"
[ "$URL_SET" = false ] || [ -n "$URL" ] || die "url must not be empty"
require_service_manager

if [ "$COLLECT_SET" = true ]; then
    COLLECT=$(normalize_uint collect_interval "$COLLECT")
fi
if [ "$WSS_REPORT_SET" = true ]; then
    WSS_REPORT=$(normalize_uint wss_report_interval "$WSS_REPORT")
    [ "$WSS_REPORT" -ge 1 ] && [ "$WSS_REPORT" -le 5 ] || die "wss_report_interval must be between 1 and 5"
fi
if [ "$REPORT_SET" = true ]; then
    REPORT=$(normalize_uint interval "$REPORT")
    [ "$REPORT" -gt 0 ] || die "interval must be at least 1"
fi
if [ "$CONNECTION_MODE_SET" = true ]; then
    case "$CONNECTION_MODE" in auto|http) ;; *) die "connection_mode must be auto or http" ;; esac
fi
if [ "$PING_MODE_SET" = true ]; then
    case "$PING_MODE" in tcp|icmp) ;; *) die "ping_mode must be tcp or icmp" ;; esac
fi
if [ "$RESET_SET" = true ]; then
    RESET_DAY=$(normalize_uint reset_day "$RESET_DAY")
    [ "$RESET_DAY" -le 31 ] || die "reset_day must be between 0 and 31"
fi
case "$REPORTER_ID" in ''|*[!A-Za-z0-9_.-]*) die "invalid reporter_id" ;; esac
if [ "$UPDATE_CHANNEL_SET" = true ]; then
    case "$UPDATE_CHANNEL" in stable|prerelease) ;; *) die "invalid update_channel" ;; esac
fi
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

if command -v pgrep >/dev/null 2>&1 && pgrep -x cf-probe >/dev/null 2>&1; then
    log "warning: official cf-probe is still running; using the same credentials will duplicate reports"
fi
if service_ctl is-active --quiet probe-rs 2>/dev/null; then
    WAS_ACTIVE=true
    service_ctl stop probe-rs
fi

install -m 0755 "$TMP_BIN" "$BIN_DST"

set -- configure-cf --config "$CONFIG_PATH" --net-static-path "$DATA_DIR/net_static.json" \
    --reporter-id "$REPORTER_ID"
[ "$ID_SET" = false ] || set -- "$@" --server-id "$ID"
[ "$SECRET_SET" = false ] || set -- "$@" --secret "$SECRET"
[ "$URL_SET" = false ] || set -- "$@" --url "$URL"
[ "$COLLECT_SET" = false ] || set -- "$@" --collect "$COLLECT"
[ "$WSS_REPORT_SET" = false ] || set -- "$@" --wss-report-interval "$WSS_REPORT"
[ "$REPORT_SET" = false ] || set -- "$@" --report-interval "$REPORT"
[ "$CONNECTION_MODE_SET" = false ] || set -- "$@" --connection-mode "$CONNECTION_MODE"
[ "$PING_MODE_SET" = false ] || set -- "$@" --ping-mode "$PING_MODE"
[ "$RESET_SET" = false ] || set -- "$@" --reset-day "$RESET_DAY"
[ "$INTERFACES_SET" = false ] || set -- "$@" --interfaces "$INTERFACES"
[ "$CT_SET" = false ] || set -- "$@" --ct "$CT"
[ "$CU_SET" = false ] || set -- "$@" --cu "$CU"
[ "$CM_SET" = false ] || set -- "$@" --cm "$CM"
[ "$BD_SET" = false ] || set -- "$@" --bd "$BD"
[ "$AUTO_UPDATE_SET" = false ] || set -- "$@" --auto-update "$AUTO_UPDATE"
[ "$UPDATE_REPOSITORY_SET" = false ] || set -- "$@" --update-repository "$UPDATE_REPOSITORY"
[ "$UPDATE_CHANNEL_SET" = false ] || set -- "$@" --update-channel "$UPDATE_CHANNEL"
[ -z "$GH_PROXY" ] || set -- "$@" --update-proxy "$GH_PROXY"
SELECTED_REPORTER=$($BIN_DST "$@")
[ -n "$SELECTED_REPORTER" ] || die "CF Reporter selection failed"
chmod 600 "$CONFIG_PATH"

if [ "$RX_SET" = true ] || [ "$TX_SET" = true ]; then
    set -- set-traffic-correction --config "$CONFIG_PATH" --reporter-id "$SELECTED_REPORTER"
    [ "$RX_SET" = false ] || set -- "$@" --rx-gib "$RX_CORRECTION"
    [ "$TX_SET" = false ] || set -- "$@" --tx-gib "$TX_CORRECTION"
    $BIN_DST "$@"
    log "applied local traffic correction to Reporter '$SELECTED_REPORTER'"
fi

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
