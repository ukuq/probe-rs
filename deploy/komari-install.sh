#!/usr/bin/env bash
# probe-rs komari 模式一键安装脚本（自包含单文件）
#
# 与 komari-agent 官方 install.sh 完全同参——把管道里的 URL 换掉即可：
#   wget -qO- https://<你的地址>/komari-install.sh | sudo bash -s -- \
#     -e http://<面板地址> -t <API token> -i 1 --month-rotate 1
#
# 参数映射：-e/-t/-i/--month-rotate/--gpu/--include-nics/--install-version 直接生效；
# 其余官方参数（--disable-web-ssh 等）兼容接收、提示后忽略——同一条命令永不过错。
# 说明：komari 探针没有 collect/report 之分（每 tick 直采直发），
# -i 是官方 interval 语义；脚本按建议值拆成 collect=1 / report=<interval>。
#
# 卸载：bash komari-install.sh uninstall [--purge]
set -euo pipefail

BIN_DST=/usr/local/bin/probe-rs
CONF_DIR=/etc/probe-rs
DATA_DIR=/var/lib/probe-rs
UNIT_DST=/etc/systemd/system/probe-rs.service
RELEASE_BASE="https://github.com/ukuq/probe-rs/releases"

usage() {
    cat <<'EOF'
用法: bash komari-install.sh -e <面板地址> -t <token> [官方参数...]
      bash komari-install.sh uninstall [--purge]

直接生效:
  -e, --endpoint        面板地址（必填）
  -t, --token           API token（必填）
  -i, --interval        上报间隔秒（缺省 3；collect 固定 1s）
  --month-rotate <日>   月流量重置日 1-31，0 = 不重置（缺省 1）
  --gpu                 开启 GPU 详细采集（缺省关）
  --include-nics <列表> 网卡白名单，逗号分隔（映射 interfaces）
  --install-version <v> 指定 probe-rs 版本（缺省 latest）
  --name <名称>         客户端名（缺省主机名）
  --reporter-id <id>   已有配置中追加/更新的 Reporter id（缺省 komari）
  --disable-auto-update 关闭自动更新（缺省开启）
  --update-channel <c> stable / prerelease（缺省 stable）
  -bin=<路径或URL>      二进制来源（缺省 GitHub Releases 按架构下载）
兼容忽略（仅提示）：--disable-web-ssh / --ignore-unsafe-cert 等其余官方参数
EOF
}

log() { echo "[probe-rs] $*"; }
warn() { echo "[probe-rs] 提示: 参数 $1 已忽略（probe-rs 不支持/不需要）"; }
has_word() { case " $1 " in *" $2 "*) return 0 ;; *) return 1 ;; esac; }
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

if [ "${1:-}" = "uninstall" ]; then
    [ "$(id -u)" = 0 ] || die "需要 root"
    do_uninstall "${2:-}"
    exit 0
fi
[ $# -gt 0 ] || { usage; exit 1; }

ENDPOINT=""; TOKEN=""; INTERVAL=3; RESET_DAY=1; NAME=""; BIN=""
ENABLE_GPU=false; INTERFACES=""; VERSION=""; REPORTER_ID="komari"
AUTO_UPDATE=true; UPDATE_CHANNEL=stable
# 需要吞掉一个值的官方参数（接受但忽略）
IGNORED_WITH_VALUE="--auto-discovery --max-retries -r --reconnect-interval -c --info-report-interval --exclude-nics --include-mountpoint --custom-dns --custom-ipv4 --custom-ipv6 --config --protocol-version --prefer-ip-version --install-dir --install-service-name --install-ghproxy"
# 纯标志位官方参数
IGNORED_FLAGS="--disable-web-ssh -u --ignore-unsafe-cert --memory-include-cache --memory-exclude-bcf --show-warning --get-ip-addr-from-nic --disable-compression"

while [ $# -gt 0 ]; do
    case "$1" in
        -e|--endpoint)       ENDPOINT="$2"; shift 2 ;;
        -t|--token)          TOKEN="$2"; shift 2 ;;
        -i|--interval)       INTERVAL="$2"; shift 2 ;;
        --month-rotate)      RESET_DAY="$2"; shift 2 ;;
        --gpu)               ENABLE_GPU=true; shift ;;
        --include-nics)      INTERFACES="$2"; shift 2 ;;
        --install-version)   VERSION="$2"; shift 2 ;;
        --name)              NAME="$2"; shift 2 ;;
        --reporter-id)       REPORTER_ID="$2"; shift 2 ;;
        --disable-auto-update) AUTO_UPDATE=false; shift ;;
        --enable-auto-update) AUTO_UPDATE=true; shift ;;
        --update-channel)    UPDATE_CHANNEL="$2"; shift 2 ;;
        -bin=*)              BIN="${1#-bin=}"; shift ;;
        -h|--help)           usage; exit 0 ;;
        *)
            if has_word "$IGNORED_WITH_VALUE" "$1"; then
                warn "$1"; shift 2
            elif has_word "$IGNORED_FLAGS" "$1"; then
                warn "$1"; shift
            else
                warn "$1（未知）"; shift
            fi ;;
    esac
done

case "$REPORTER_ID" in
    ''|*[!A-Za-z0-9_.-]*) die "--reporter-id must use A-Z, a-z, 0-9, _, . or -" ;;
esac
case "$UPDATE_CHANNEL" in
    stable|prerelease) ;;
    *) die "--update-channel must be stable or prerelease" ;;
esac

[ "$(id -u)" = 0 ] || die "需要 root（sudo 或 root 执行）"
[ -n "$ENDPOINT" ] || die "缺少 -e <面板地址>"
[ -n "$TOKEN" ] || die "缺少 -t <token>"
command -v systemctl >/dev/null || die "仅支持 systemd 系统"
# -i 官方是 float（如 1.5）；我们按整数秒处理，不足 1 抬到 1
INTERVAL="${INTERVAL%.*}"
case "$INTERVAL" in ''|*[!0-9]*) INTERVAL=3 ;; esac
[ "$INTERVAL" -lt 1 ] && INTERVAL=1
case "$RESET_DAY" in ''|*[!0-9]*) RESET_DAY=1 ;; esac
[ "$RESET_DAY" -gt 31 ] && RESET_DAY=1
[ -n "$NAME" ] || NAME=$(hostname)
# TOML 字符串转义（\\ 与 " 防止注入/解析失败）
toml_escape() { printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'; }
# 前导零（08 不是合法 TOML 整数）
INTERVAL=$((10#$INTERVAL)); RESET_DAY=$((10#$RESET_DAY))
NAME=$(toml_escape "$NAME"); TOKEN_ESC=$(toml_escape "$TOKEN"); ENDPOINT_ESC=$(toml_escape "$ENDPOINT")

# 逗号分隔 → TOML 数组
INTERFACES_TOML="[]"
if [ -n "$INTERFACES" ]; then
    INTERFACES_TOML="[$(printf '%s' "$INTERFACES" | tr ',' '\n' | sed 's/^ *//; s/ *$//; /^$/d; s/.*/"&"/' | paste -sd, -)]"
fi

# ---- 二进制 ----
TMP_BIN=$(mktemp /tmp/probe-rs.XXXXXX)
if [ -z "$BIN" ]; then
    arch=$(uname -m)
    case "$arch" in
        x86_64|amd64)   arch=x86_64 ;;
        aarch64|arm64)  arch=aarch64 ;;
        *) die "不支持的架构: $arch" ;;
    esac
    if [ -n "$VERSION" ]; then
        BIN="$RELEASE_BASE/download/$VERSION/probe-rs-linux-$arch"
    else
        BIN="$RELEASE_BASE/latest/download/probe-rs-linux-$arch"
    fi
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

# Remove one complete [[reporters]] subtree while preserving all other
# reporters and any root tables serialized after it.
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
    remove_reporter_block "$CONFIG_PATH" "$REPORTER_ID"
    cat >> "$CONFIG_PATH" <<EOF

[[reporters]]
id = "$REPORTER_ID"
protocol = "komari"
server_id = "$NAME"
secret = "$TOKEN_ESC"
worker_url = "$ENDPOINT_ESC"
config_version = ""
report_interval = $INTERVAL
reset_day = $RESET_DAY
interfaces = $INTERFACES_TOML
disks = []
report_gpu = $ENABLE_GPU
report_errors = true
report_self = false

[reporters.intervals]
collect = 1
ping = 30
slow = 60
gpu = 60
ip = 600
diskio = 10
EOF
    log "preserved other Reporters and upserted Komari Reporter '$REPORTER_ID'"
else
    # 缺失配置或旧的根连接 schema 都直接覆盖为新 canonical 格式，不做兼容迁移。
cat > "$CONFIG_PATH" <<EOF
net_static_path = "$DATA_DIR/net_static.json"

[[reporters]]
id = "$REPORTER_ID"
protocol = "komari"
server_id = "$NAME"
secret = "$TOKEN_ESC"
worker_url = "$ENDPOINT_ESC"
config_version = ""
report_interval = $INTERVAL
reset_day = $RESET_DAY
interfaces = $INTERFACES_TOML
disks = []
report_gpu = $ENABLE_GPU
report_errors = true
report_self = false

[reporters.intervals]
collect = 1
ping = 30
slow = 60
gpu = 60
ip = 600
diskio = 10
EOF
    log "wrote a fresh canonical config with Komari Reporter '$REPORTER_ID'"
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
    log "安装完成，服务运行中（komari 模式 → $ENDPOINT）。日志: journalctl -u probe-rs -f"
else
    die "服务启动失败，排查: journalctl -u probe-rs -n 20 --no-pager"
fi
