#!/bin/sh
# probe-rs CF mode installer. POSIX sh compatible, including:
#   curl -fsSL <url>/cf-install.sh | sh -s -- install -id=... -secret=... -url=...
set -eu

BIN_DST=/usr/local/bin/probe-rs
CONF_DIR=/etc/probe-rs
DATA_DIR=/var/lib/probe-rs
UNIT_DST=/etc/systemd/system/probe-rs.service
CONFIG_PATH=$CONF_DIR/config.toml
SCRIPT_VERSION=v0.1.3-beta.3
GITHUB_REPO=https://github.com/ukuq/probe-rs

log() { printf '%s\n' "[probe-rs] $*"; }
die() { printf '%s\n' "[probe-rs] error: $*" >&2; exit 1; }

usage() {
    printf '%s\n' \
        'Usage: sh cf-install.sh install -id=<UUID> -secret=<SECRET> -url=<HTTP(S) URL> [options]' \
        '       sh cf-install.sh uninstall [--purge]' \
        '' \
        'CF-compatible options:' \
        '  -collect_interval= / -collect=       collection seconds (0 maps to 1)' \
        '  -interval=                           report seconds' \
        '  -reset_day=                          0-31' \
        '  -ct= -cu= -cm= -bd=                 ping targets' \
        '  -interface= / -interfaces= / -iface= comma-separated interface globs' \
        '  -auto_update= / -auto-update=        0/1' \
        '  -rx_correction= -tx_correction=      current billing-period totals in GiB' \
        '  -debug=                              0/1' \
        '  -install-version=                    release tag (default: script version)' \
        '  -install-ghproxy=                    GitHub proxy URL prefix' \
        '  -no_start= / -no-start=              0/1' \
        '' \
        'probe-rs options:' \
        '  -reporter_id= / --reporter-id=       upsert one CF Reporter' \
        '  -replace_cf=                         replace all CF Reporters only' \
        '  -update_channel=                     stable/prerelease' \
        '  -bin=                                local path or HTTP(S) binary URL'
}

parse_bool() {
    case "$2" in
        1|true|TRUE|yes|YES) eval "$1=true" ;;
        0|false|FALSE|no|NO|'') eval "$1=false" ;;
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
    systemctl disable --now probe-rs 2>/dev/null || true
    rm -f "$UNIT_DST" "$BIN_DST"
    systemctl daemon-reload 2>/dev/null || true
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
        [ "$(id -u)" = 0 ] || die "root is required"
        do_uninstall "${1:-}"
        exit 0
        ;;
    install) ;;
    *) usage; exit 1 ;;
esac

ID= SECRET= URL= BIN=
COLLECT= REPORT= RESET_DAY= INTERFACES=
CT= CU= CM= BD=
AUTO_UPDATE= UPDATE_CHANNEL= RX_CORRECTION= TX_CORRECTION=
REPORTER_ID= INSTALL_VERSION=$SCRIPT_VERSION GH_PROXY=
DEBUG=false NO_START=false REPLACE_CF=false
COLLECT_SET=false REPORT_SET=false RESET_SET=false INTERFACES_SET=false
CT_SET=false CU_SET=false CM_SET=false BD_SET=false
AUTO_UPDATE_SET=false UPDATE_CHANNEL_SET=false REPORTER_ID_SET=false
RX_SET=false TX_SET=false DEBUG_SET=false

for arg in "$@"; do
    case "$arg" in
        -id=*) ID=${arg#*=} ;;
        -secret=*) SECRET=${arg#*=} ;;
        -url=*) URL=${arg#*=} ;;
        -collect_interval=*|-collect=*) COLLECT=${arg#*=}; COLLECT_SET=true ;;
        -interval=*) REPORT=${arg#*=}; REPORT_SET=true ;;
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
        -rx_correction=*) RX_CORRECTION=${arg#*=}; RX_SET=true ;;
        -tx_correction=*) TX_CORRECTION=${arg#*=}; TX_SET=true ;;
        -reporter_id=*|--reporter-id=*)
            REPORTER_ID=${arg#*=}; REPORTER_ID_SET=true
            ;;
        -replace_cf=*|--replace-cf=*)
            REPLACE_CF=${arg#*=}
            parse_bool REPLACE_CF "$REPLACE_CF" "replace_cf"
            ;;
        -debug=*)
            DEBUG=${arg#*=}; DEBUG_SET=true
            parse_bool DEBUG "$DEBUG" "debug"
            ;;
        -no_start=*|-no-start=*)
            NO_START=${arg#*=}
            parse_bool NO_START "$NO_START" "no_start"
            ;;
        -install-version=*|--install-version=*) INSTALL_VERSION=${arg#*=} ;;
        -install-ghproxy=*|--install-ghproxy=*) GH_PROXY=${arg#*=} ;;
        -bin=*|--bin=*) BIN=${arg#*=} ;;
        *) die "unknown option: $arg" ;;
    esac
done

[ "$(id -u)" = 0 ] || die "root is required"
[ -n "$ID" ] || die "missing -id="
[ -n "$SECRET" ] || die "missing -secret="
[ -n "$URL" ] || die "missing -url="
command -v systemctl >/dev/null 2>&1 || die "systemd is required"

if [ "$COLLECT_SET" = true ]; then
    COLLECT=$(normalize_uint collect_interval "$COLLECT")
    [ "$COLLECT" -gt 0 ] || COLLECT=1
fi
if [ "$REPORT_SET" = true ]; then
    REPORT=$(normalize_uint interval "$REPORT")
    [ "$REPORT" -gt 0 ] || die "interval must be at least 1"
fi
if [ "$RESET_SET" = true ]; then
    RESET_DAY=$(normalize_uint reset_day "$RESET_DAY")
    [ "$RESET_DAY" -le 31 ] || die "reset_day must be between 0 and 31"
fi
if [ "$REPORTER_ID_SET" = true ]; then
    case "$REPORTER_ID" in ''|*[!A-Za-z0-9_.-]*) die "invalid reporter_id" ;; esac
fi
if [ "$UPDATE_CHANNEL_SET" = true ]; then
    case "$UPDATE_CHANNEL" in stable|prerelease) ;; *) die "invalid update_channel" ;; esac
fi
if [ "$DEBUG_SET" = false ] && [ -f "$UNIT_DST" ] && grep -q ' --debug' "$UNIT_DST"; then
    DEBUG=true
fi

install -d -m 0755 "$DATA_DIR"
install -d -m 0750 "$CONF_DIR"

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
        systemctl start probe-rs 2>/dev/null || true
    fi
    exit "$status"
}
trap cleanup 0
trap 'exit 1' HUP INT TERM

arch=$(uname -m)
case "$arch" in
    x86_64|amd64) asset=probe-rs-linux-x86_64 ;;
    aarch64|arm64) asset=probe-rs-linux-aarch64 ;;
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
        BIN=${GH_PROXY%/}/$BIN
        SUM_URL=${GH_PROXY%/}/$SUM_URL
    fi
fi

if [ -f "$BIN" ]; then
    cp -f "$BIN" "$TMP_BIN"
else
    command -v curl >/dev/null 2>&1 || die "curl is required"
    log "downloading $BIN"
    curl -fSL --connect-timeout 10 -o "$TMP_BIN" "$BIN" || die "binary download failed"
    if [ -n "${SUM_URL:-}" ]; then
        command -v sha256sum >/dev/null 2>&1 || die "sha256sum is required"
        TMP_SUM=$(mktemp /tmp/probe-rs-sha.XXXXXX)
        curl -fSL --connect-timeout 10 -o "$TMP_SUM" "$SUM_URL" || die "checksum download failed"
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
if systemctl is-active --quiet probe-rs 2>/dev/null; then
    WAS_ACTIVE=true
    systemctl stop probe-rs
fi

install -m 0755 "$TMP_BIN" "$BIN_DST"

set -- configure-cf --config "$CONFIG_PATH" --net-static-path "$DATA_DIR/net_static.json" \
    --server-id "$ID" --secret "$SECRET" --url "$URL"
[ "$COLLECT_SET" = false ] || set -- "$@" --collect "$COLLECT"
[ "$REPORT_SET" = false ] || set -- "$@" --report-interval "$REPORT"
[ "$RESET_SET" = false ] || set -- "$@" --reset-day "$RESET_DAY"
[ "$INTERFACES_SET" = false ] || set -- "$@" --interfaces "$INTERFACES"
[ "$CT_SET" = false ] || set -- "$@" --ct "$CT"
[ "$CU_SET" = false ] || set -- "$@" --cu "$CU"
[ "$CM_SET" = false ] || set -- "$@" --cm "$CM"
[ "$BD_SET" = false ] || set -- "$@" --bd "$BD"
[ "$AUTO_UPDATE_SET" = false ] || set -- "$@" --auto-update "$AUTO_UPDATE"
[ "$UPDATE_CHANNEL_SET" = false ] || set -- "$@" --update-channel "$UPDATE_CHANNEL"
[ "$REPORTER_ID_SET" = false ] || set -- "$@" --reporter-id "$REPORTER_ID"
[ "$REPLACE_CF" = false ] || set -- "$@" --replace-cf
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
ReadWritePaths=-$DATA_DIR -$CONF_DIR /usr/local/bin
ProtectHome=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
if [ "$NO_START" = true ]; then
    systemctl disable --now probe-rs >/dev/null 2>&1 || true
    INSTALL_COMPLETE=true
    log "installed without starting; Reporter '$SELECTED_REPORTER' is configured"
    exit 0
fi
systemctl enable probe-rs >/dev/null 2>&1 || true
systemctl restart probe-rs
INSTALL_COMPLETE=true
sleep 1
if systemctl is-active --quiet probe-rs; then
    log "installed and running; Reporter '$SELECTED_REPORTER' is configured"
else
    die "service failed to start; run: journalctl -u probe-rs -n 20 --no-pager"
fi
