#!/usr/bin/env bash
# probe-rs CF 模式一键安装脚本（自包含单文件）
#
# 与 CF-Server-Monitor 官方 install.sh 参数完全一致——把管道里的 URL 换成这个脚本即可：
#   curl -sL https://<你的地址>/cf-install.sh | bash -s install \
#     -id=<CF后台分配的UUID> -secret=<API_SECRET> -url=https://<worker>/update \
#     -collect_interval=0 -interval=60 -reset_day=1 \
#     -ct=gd-ct-dualstack.ip.zstaticcdn.com -cu=gd-cu-dualstack.ip.zstaticcdn.com \
#     -cm=gd-cm-dualstack.ip.zstaticcdn.com -bd=ip.zstaticcdn.com
#
# 额外参数（官方没有，可选）：
#   -bin=<路径或URL>   指定 probe-rs 二进制来源；缺省从 GitHub Releases 按架构下载
# 官方参数里的 -auto_update 映射为全局自动更新开关；
# -rx_correction/-tx_correction 忽略（校正由服务端运行时下发，agent 原生支持）。
#
# 卸载：bash cf-install.sh uninstall [--purge]
set -euo pipefail

BIN_DST=/usr/local/bin/probe-rs
CONF_DIR=/etc/probe-rs
DATA_DIR=/var/lib/probe-rs
UNIT_DST=/etc/systemd/system/probe-rs.service
RELEASE_BASE="https://github.com/ukuq/probe-rs/releases/latest/download"

usage() {
    cat <<'EOF'
用法: bash cf-install.sh install -id=<UUID> -secret=<SECRET> -url=<https://.../update> [选项]
      bash cf-install.sh uninstall [--purge]

选项（与官方一致）:
  -id=               CF 后台分配的服务器 UUID（必填）
  -secret=           API_SECRET（必填）
  -url=              上报地址，形如 https://<worker>/update（必填）
  -collect_interval= 采样间隔秒（0 兼容映射为 1 秒；缺省 0）
  -interval=         上报间隔秒（缺省 60）
  -reset_day=        月流量账期重置日 1-31，0 = 不重置（缺省 1）
  -ct= -cu= -cm= -bd=  电信/联通/移动/BGP 探测节点 host[:port]
  -auto_update=      自动更新开关：0/1（缺省 0）
  -rx_correction= -tx_correction=  忽略（校正由服务端运行时下发）
额外:
  -bin=              二进制来源（本地路径或 URL），缺省 GitHub Releases
  -reporter_id=      已有配置中追加/更新的 Reporter id（缺省 cf）
  -update_channel=   stable / prerelease（缺省 stable）
EOF
}

log() { echo "[probe-rs] $*"; }
die() { echo "[probe-rs] 错误: $*" >&2; exit 1; }

do_uninstall() {
    systemctl disable --now probe-rs 2>/dev/null || true
    rm -f "$UNIT_DST" "$BIN_DST"
    systemctl daemon-reload 2>/dev/null || true
    if [ "${1:-}" = "--purge" ]; then
        rm -rf "$CONF_DIR" "$DATA_DIR"
        log "已卸载并清除配置与数据"
    else
        log "已卸载（保留 $CONF_DIR 与 $DATA_DIR，加 --purge 全清）"
    fi
}

CMD="${1:-}"
[ $# -gt 0 ] && shift || true
case "$CMD" in
    uninstall) [ "$(id -u)" = 0 ] || die "需要 root"; do_uninstall "${1:-}"; exit 0 ;;
    install) ;;
    *) usage; exit 1 ;;
esac

ID=""; SECRET=""; URL=""; COLLECT=0; REPORT=60; RESET_DAY=1
CT=""; CU=""; CM=""; BD=""; BIN=""; REPORTER_ID="cf"
AUTO_UPDATE=false; UPDATE_CHANNEL=stable
UPDATE_SETTINGS_SET=false
for arg in "$@"; do
    case "$arg" in
        -id=*)              ID="${arg#-id=}" ;;
        -secret=*)          SECRET="${arg#-secret=}" ;;
        -url=*)             URL="${arg#-url=}" ;;
        -collect_interval=*) COLLECT="${arg#-collect_interval=}" ;;
        -interval=*)        REPORT="${arg#-interval=}" ;;
        -reset_day=*)       RESET_DAY="${arg#-reset_day=}" ;;
        -ct=*)              CT="${arg#-ct=}" ;;
        -cu=*)              CU="${arg#-cu=}" ;;
        -cm=*)              CM="${arg#-cm=}" ;;
        -bd=*)              BD="${arg#-bd=}" ;;
        -bin=*)             BIN="${arg#-bin=}" ;;
        -reporter_id=*|--reporter-id=*) REPORTER_ID="${arg#*=}" ;;
        -auto_update=*)
            UPDATE_SETTINGS_SET=true
            case "${arg#*=}" in
                1|true|TRUE|yes|YES) AUTO_UPDATE=true ;;
                0|false|FALSE|no|NO|'') AUTO_UPDATE=false ;;
                *) die "-auto_update must be 0 or 1" ;;
            esac ;;
        -update_channel=*|--update-channel=*) UPDATE_CHANNEL="${arg#*=}"; UPDATE_SETTINGS_SET=true ;;
        -rx_correction=*|-tx_correction=*)
            log "参数 $arg 忽略（见脚本头注释）" ;;
        *) die "未知参数: $arg" ;;
    esac
done

case "$REPORTER_ID" in
    ''|*[!A-Za-z0-9_.-]*) die "reporter id must use A-Z, a-z, 0-9, _, . or -" ;;
esac
case "$UPDATE_CHANNEL" in
    stable|prerelease) ;;
    *) die "update channel must be stable or prerelease" ;;
esac

[ "$(id -u)" = 0 ] || die "需要 root（sudo 或 root 执行）"
[ -n "$ID" ] || die "缺少 -id="
[ -n "$SECRET" ] || die "缺少 -secret="
[ -n "$URL" ] || die "缺少 -url="
command -v systemctl >/dev/null || die "仅支持 systemd 系统"
# 内部采集与上报严格分离，collect 至少 1 秒；0 映射为实时 1 秒，前导零需剥掉
case "$COLLECT" in ''|*[!0-9]*) COLLECT=1 ;; esac
case "$REPORT" in ''|*[!0-9]*) REPORT=60 ;; esac
case "$RESET_DAY" in ''|*[!0-9]*) RESET_DAY=1 ;; esac
COLLECT=$((10#$COLLECT)); REPORT=$((10#$REPORT)); RESET_DAY=$((10#$RESET_DAY))
[ "$COLLECT" -lt 1 ] && COLLECT=1
[ "$REPORT" -lt 1 ] && REPORT=60

# TOML 字符串转义（\ 与 " 防注入/解析失败）
toml_escape() { printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'; }
ID=$(toml_escape "$ID"); SECRET=$(toml_escape "$SECRET"); URL=$(toml_escape "$URL")
CT=$(toml_escape "$CT"); CU=$(toml_escape "$CU"); CM=$(toml_escape "$CM"); BD=$(toml_escape "$BD")

# ---- 二进制 ----
TMP_BIN=$(mktemp /tmp/probe-rs.XXXXXX)
if [ -z "$BIN" ]; then
    arch=$(uname -m)
    case "$arch" in
        x86_64|amd64)   arch=x86_64 ;;
        aarch64|arm64)  arch=aarch64 ;;
        *) die "不支持的架构: $arch" ;;
    esac
    BIN="$RELEASE_BASE/probe-rs-linux-$arch"
fi
if [ -f "$BIN" ]; then
    cp -f "$BIN" "$TMP_BIN"
else
    log "下载二进制: $BIN"
    curl -fSL --connect-timeout 10 -o "$TMP_BIN" "$BIN" || die "二进制下载失败（可用 -bin= 指定本地路径）"
fi
install -m 0755 "$TMP_BIN" "$BIN_DST"
rm -f "$TMP_BIN"

# ---- 配置 ----
install -d -m 0755 "$DATA_DIR"
install -d -m 0750 "$CONF_DIR"
CONFIG_PATH="$CONF_DIR/config.toml"

remove_reporter_block() {
    cfg="$1"; target="$2"; tmp=$(mktemp "$CONF_DIR/config.toml.XXXXXX")
    awk -v target="$target" '
        function flush() {
            if (in_reporter && !drop) printf "%s", block
            block=""; drop=0
        }
        /^[[:space:]]*\[\[reporters\]\][[:space:]]*$/ {
            if (in_reporter) flush()
            in_reporter=1; block=$0 ORS; next
        }
        {
            if (in_reporter && $0 ~ /^[[:space:]]*\[/ &&
                $0 !~ /^[[:space:]]*\[\[?reporters(\.|\]\])/) {
                flush(); in_reporter=0
            }
            if (in_reporter) {
                block=block $0 ORS
                if ($0 ~ /^[[:space:]]*id[[:space:]]*=/) {
                    value=$0
                    sub(/^[^=]*=[[:space:]]*/, "", value)
                    sub(/[[:space:]]*#.*$/, "", value)
                    sub(/[[:space:]]+$/, "", value)
                    double_quoted="\"" target "\""
                    single_quoted=sprintf("%c%s%c", 39, target, 39)
                    if (value == double_quoted || value == single_quoted) drop=1
                }
            } else print
        }
        END { if (in_reporter) flush() }
    ' "$cfg" > "$tmp"
    mv -f "$tmp" "$cfg"
}
strip_seeded_sample_reporters() {
    cfg="$1"; tmp=$(mktemp "$CONF_DIR/config.toml.XXXXXX")
    awk '
        function flush() {
            if (in_reporter && !seeded) printf "%s", block
            block=""; seeded=0
        }
        /^[[:space:]]*\[\[reporters\]\][[:space:]]*$/ {
            if (in_reporter) flush()
            in_reporter=1; block=$0 ORS; next
        }
        {
            if (in_reporter && $0 ~ /^[[:space:]]*\[/ &&
                $0 !~ /^[[:space:]]*\[\[?reporters(\.|\]\])/) {
                flush(); in_reporter=0
            }
            if (in_reporter) {
                block=block $0 ORS
                if ($0 ~ /^[[:space:]]*server_id[[:space:]]*=[[:space:]]*"cf-server-uuid"/) seeded=1
                if ($0 ~ /^[[:space:]]*worker_url[[:space:]]*=[[:space:]]*"https:\/\/monitor\.example\.com\/update"/) seeded=1
                if ($0 ~ /^[[:space:]]*worker_url[[:space:]]*=[[:space:]]*"https:\/\/komari\.example\.com"/) seeded=1
                if ($0 ~ /^[[:space:]]*worker_url[[:space:]]*=[[:space:]]*"http:\/\/127\.0\.0\.1:8080\/report"/) seeded=1
            } else print
        }
        END { if (in_reporter) flush() }
    ' "$cfg" > "$tmp"
    mv -f "$tmp" "$cfg"
}

upsert_auto_update_config() {
    cfg="$1"; tmp=$(mktemp "$CONF_DIR/config.toml.XXXXXX")
    if [ "$UPDATE_SETTINGS_SET" = false ] && grep -q '^[[:space:]]*\[auto_update\][[:space:]]*$' "$cfg"; then
        rm -f "$tmp"
        return
    fi
    awk -v enabled="$AUTO_UPDATE" -v channel="$UPDATE_CHANNEL" '
        BEGIN { in_auto=0; inserted=0 }
        /^[[:space:]]*\[auto_update\][[:space:]]*$/ { in_auto=1; next }
        in_auto && /^[[:space:]]*\[/ { in_auto=0 }
        in_auto { next }
        !inserted && /^[[:space:]]*\[\[reporters\]\][[:space:]]*$/ {
            print "[auto_update]"
            print "enabled = " enabled
            print "channel = \"" channel "\""
            print "check_interval = 21600"
            print ""
            inserted=1
        }
        { print }
        END {
            if (!inserted) {
                print ""
                print "[auto_update]"
                print "enabled = " enabled
                print "channel = \"" channel "\""
                print "check_interval = 21600"
            }
        }
    ' "$cfg" > "$tmp"
    mv -f "$tmp" "$cfg"
}

if [ -s "$CONFIG_PATH" ] && grep -q '^[[:space:]]*\[\[reporters\]\]' "$CONFIG_PATH"; then
    strip_seeded_sample_reporters "$CONFIG_PATH"
fi


if [ -s "$CONFIG_PATH" ] &&
   grep -q '^[[:space:]]*\[\[reporters\]\]' "$CONFIG_PATH" &&
   ! awk '
       /^[[:space:]]*\[intervals\][[:space:]]*$/ || /^[[:space:]]*\[\[pings\]\][[:space:]]*$/ { found=1; exit }
       /^[[:space:]]*\[/ { exit }
       /^[[:space:]]*(server_id|enable_gpu)[[:space:]]*=/ { found=1 }
       END { exit found ? 0 : 1 }
   ' "$CONFIG_PATH"; then
    # 新 schema 中每路声明需求，实际 worker 配置由所有 Reporter 聚合。
    remove_reporter_block "$CONFIG_PATH" "$REPORTER_ID"
    {
        echo ""
        echo "[[reporters]]"
        echo "id = \"$REPORTER_ID\""
        echo 'protocol = "cf"'
        echo "server_id = \"$ID\""
        echo "secret = \"$SECRET\""
        echo "worker_url = \"$URL\""
        echo 'config_version = ""'
        echo "report_interval = $REPORT"
        echo "reset_day = $RESET_DAY"
        echo "interfaces = []"
        echo "disks = []"
        echo "report_gpu = true"
        echo "report_errors = true"
        echo "report_self = false"
        echo ""
        echo "[reporters.intervals]"
        echo "collect = $COLLECT"
        echo "ping = 30"
        echo "slow = 60"
        echo "gpu = 60"
        echo "ip = 600"
        echo "diskio = 10"
        for pair in "ct:$CT" "cu:$CU" "cm:$CM" "bd:$BD"; do
            name="${pair%%:*}"; target="${pair#*:}"
            [ -n "$target" ] || continue
            echo ""
            echo "[[reporters.pings]]"
            echo "name = \"$name\""
            echo 'type = "tcp"'
            echo "target = \"$target\""
            echo "interval = 30"
        done
        echo ""
        echo "[reporters.ext.cf]"
        echo "correction = true"
        echo "batch = true"
    } >> "$CONFIG_PATH"
    log "preserved other Reporters and upserted CF Reporter '$REPORTER_ID'"
else
    # 缺失配置或旧的根连接 schema 都直接覆盖为新 canonical 格式，不做兼容迁移。
{
    echo "net_static_path = \"$DATA_DIR/net_static.json\""
    echo ""
    echo "[[reporters]]"
    echo "id = \"$REPORTER_ID\""
    echo 'protocol = "cf"'
    echo "server_id = \"$ID\""
    echo "secret = \"$SECRET\""
    echo "worker_url = \"$URL\""
    echo 'config_version = ""'
    echo "report_interval = $REPORT"
    echo "reset_day = $RESET_DAY"
    echo "interfaces = []"
    echo "disks = []"
    echo "report_gpu = true"
    echo "report_errors = true"
    echo "report_self = false"
    echo ""
    echo "[reporters.intervals]"
    echo "collect = $COLLECT"
    echo "ping = 30"
    echo "slow = 60"
    echo "gpu = 60"
    echo "ip = 600"
    echo "diskio = 10"
    for pair in "ct:$CT" "cu:$CU" "cm:$CM" "bd:$BD"; do
        name="${pair%%:*}"; target="${pair#*:}"
        [ -n "$target" ] || continue
        echo ""
        echo "[[reporters.pings]]"
        echo "name = \"$name\""
        echo 'type = "tcp"'
        echo "target = \"$target\""
        echo "interval = 30"
    done
    echo ""
    echo "[reporters.ext.cf]"
    echo "correction = true"
    echo "batch = true"
} > "$CONFIG_PATH"
    log "wrote a fresh canonical config with CF Reporter '$REPORTER_ID'"
fi
upsert_auto_update_config "$CONFIG_PATH"
chmod 600 "$CONFIG_PATH"

# ---- systemd unit ----
cat > "$UNIT_DST" <<'EOF'
[Unit]
Description=probe-rs server monitoring agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/probe-rs
Restart=always
RestartSec=5
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=-/var/lib/probe-rs -/etc/probe-rs -/usr/local/bin
ProtectHome=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable probe-rs >/dev/null 2>&1 || true
systemctl restart probe-rs
sleep 1
if systemctl is-active --quiet probe-rs; then
    log "安装完成，服务运行中。状态: systemctl status probe-rs；日志: journalctl -u probe-rs -f"
else
    die "服务启动失败，排查: journalctl -u probe-rs -n 20 --no-pager"
fi
