#!/usr/bin/env bash
# probe-rs komari 模式一键安装脚本（自包含单文件）
#
# 与 komari-agent 官方 install.sh 完全同参——把管道里的 URL 换掉即可：
#   wget -qO- https://<你的地址>/komari-install.sh | bash -s -- \
#     -e http://<面板地址> -t <API token> -i 1 --month-rotate 1
#
# 参数映射：-e/-t/-i/--month-rotate/--gpu/--include-nics/--install-version 直接生效；
# 其余官方参数（--disable-web-ssh 等）兼容接收、提示后忽略——同一条命令永不过错。
# 说明：komari 探针没有 collect/report 之分（每 tick 直采直发），
# -i 是官方 interval 语义；脚本按建议值拆成 collect=1 / report=<interval>。
#
# 卸载：bash komari-install.sh uninstall [--purge]
set -euo pipefail

RELEASE_BASE="https://github.com/ukuq/probe-rs/releases"

usage() {
    cat <<'EOF'
用法: bash komari-install.sh -e <面板地址> -t <token> [官方参数...]
      bash komari-install.sh uninstall [--purge]

直接生效:
  -e, --endpoint        面板地址（必填）
  -t, --token           API token（必填）
  -i, --interval        采集/上报间隔秒（缺省 3；komari 按采集周期上报）
  --month-rotate <日>   月流量重置日 1-31，0 = 不重置（缺省 1）
  --gpu                 开启 GPU 详细采集（缺省关）
  --include-nics <列表> 网卡白名单，逗号分隔通配符（写入 include_nics）
  --install-version <v> 指定 probe-rs 版本（缺省 latest）
  --install-ghproxy <URL> GitHub 代理前缀；直连失败后使用并持久化为更新兜底
  --name <名称>         已忽略（komari 段无 server_id 字段，面板按 token 识别）
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

USER_SERVICE=false
if [ "$(id -u)" = 0 ]; then
    BIN_DIR=/usr/local/bin
    CONF_DIR=/etc/probe-rs
    DATA_DIR=/var/lib/probe-rs
    UNIT_DST=/etc/systemd/system/probe-rs.service
    JOURNAL_CMD='journalctl -u probe-rs'
else
    [ -n "${HOME:-}" ] || die "普通用户安装需要 HOME"
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

service_ctl() {
    if [ "$USER_SERVICE" = true ]; then
        systemctl --user "$@"
    else
        systemctl "$@"
    fi
}

require_service_manager() {
    command -v systemctl >/dev/null || die "仅支持 systemd 系统"
    if [ "$USER_SERVICE" = true ] && ! service_ctl show-environment >/dev/null 2>&1; then
        die "普通用户安装需要正在运行的 systemd 用户会话（systemctl --user 不可用）"
    fi
}

warn_linger() {
    [ "$USER_SERVICE" = true ] || return 0
    command -v loginctl >/dev/null 2>&1 || return 0
    local linger
    linger=$(loginctl show-user "$(id -u)" --property=Linger --value 2>/dev/null || true)
    if [ "$linger" = no ]; then
        log "警告: 当前用户未启用 linger，退出登录后用户服务可能停止"
        log "如需后台常驻，请让管理员执行: loginctl enable-linger $(id -un)"
    fi
}

systemd_escape() { printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g; s/%/%%/g'; }

do_uninstall() {
    service_ctl disable --now probe-rs 2>/dev/null || true
    rm -f "$UNIT_DST" "$BIN_DST"
    service_ctl daemon-reload 2>/dev/null || true
    if [ "${1:-}" = "--purge" ]; then
        rm -rf "$CONF_DIR" "$DATA_DIR"
        log "已卸载并清除配置与数据"
    else
        log "已卸载（保留 $CONF_DIR 与 $DATA_DIR，加 --purge 全清）"
    fi
}

if [ "${1:-}" = "uninstall" ]; then
    do_uninstall "${2:-}"
    exit 0
fi
[ $# -gt 0 ] || { usage; exit 1; }

ENDPOINT=""; TOKEN=""; INTERVAL=3; RESET_DAY=1; BIN=""; GH_PROXY=""
ENABLE_GPU=false; INTERFACES=""; VERSION=""; REPORTER_ID="komari"
AUTO_UPDATE=true; UPDATE_CHANNEL=stable
# 需要吞掉一个值的官方参数（接受但忽略）
IGNORED_WITH_VALUE="--auto-discovery --max-retries -r --reconnect-interval -c --info-report-interval --exclude-nics --include-mountpoint --custom-dns --custom-ipv4 --custom-ipv6 --config --protocol-version --prefer-ip-version --install-dir --install-service-name --name"
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
        --install-ghproxy)   GH_PROXY="$2"; shift 2 ;;
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
if [ -n "$GH_PROXY" ]; then
    case "$GH_PROXY" in http://*|https://*) ;; *) die "--install-ghproxy must be an absolute HTTP(S) URL" ;; esac
    case "$GH_PROXY" in *'?'*|*'#'*|*'"'*|*'\'*|http://*@*|https://*@*)
        die "--install-ghproxy must not contain credentials, quotes, backslashes, a query string, or a fragment"
        ;;
    esac
    GH_PROXY=${GH_PROXY%/}
fi

[ -n "$ENDPOINT" ] || die "缺少 -e <面板地址>"
[ -n "$TOKEN" ] || die "缺少 -t <token>"
require_service_manager
# -i 官方是 float（如 1.5）；我们按整数秒处理，不足 1 抬到 1
INTERVAL="${INTERVAL%.*}"
case "$INTERVAL" in ''|*[!0-9]*) INTERVAL=3 ;; esac
[ "$INTERVAL" -lt 1 ] && INTERVAL=1
case "$RESET_DAY" in ''|*[!0-9]*) RESET_DAY=1 ;; esac
[ "$RESET_DAY" -gt 31 ] && RESET_DAY=1
# TOML 字符串转义（\\ 与 " 防止注入/解析失败）
toml_escape() { printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'; }
# 前导零（08 不是合法 TOML 整数）
INTERVAL=$((10#$INTERVAL)); RESET_DAY=$((10#$RESET_DAY))
TOKEN_ESC=$(toml_escape "$TOKEN"); ENDPOINT_ESC=$(toml_escape "$ENDPOINT"); NICS_ESC=$(toml_escape "$INTERFACES")
GH_PROXY_ESC=$(toml_escape "$GH_PROXY")


# ---- 二进制 ----
install -d -m 0755 "$BIN_DIR"
install -d -m 0755 "$(dirname -- "$UNIT_DST")"
TMP_BIN=$(mktemp /tmp/probe-rs.XXXXXX)
TMP_SUMS=
VERIFY_SUMS=0
cleanup_tmp() { rm -f "$TMP_BIN" ${TMP_SUMS:+$TMP_SUMS}; }
trap cleanup_tmp EXIT
if [ -z "$BIN" ]; then
    arch=$(uname -m)
    case "$arch" in
        x86_64|amd64)   arch=x86_64 ;;
        aarch64|arm64)  arch=aarch64 ;;
        loongarch64|loong64) arch=loong64 ;;
        *) die "不支持的架构: $arch" ;;
    esac
    if [ -z "$VERSION" ] || [ "$VERSION" = latest ]; then
        BIN="$RELEASE_BASE/latest/download/probe-rs-linux-$arch"
        SUM_URL="$RELEASE_BASE/latest/download/SHA256SUMS"
    else
        # 与 cf-install.sh 对齐：缺 v 前缀自动补，非法字符拒绝
        case "$VERSION" in v*) tag=$VERSION ;; *) tag=v$VERSION ;; esac
        case "$tag" in *[!A-Za-z0-9._-]*) die "invalid install-version" ;; esac
        BIN="$RELEASE_BASE/download/$tag/probe-rs-linux-$arch"
        SUM_URL="$RELEASE_BASE/download/$tag/SHA256SUMS"
    fi
    if [ -n "$GH_PROXY" ]; then
        PROXY_BIN=$GH_PROXY/$BIN
        PROXY_SUM_URL=$GH_PROXY/$SUM_URL
    fi
    # 脚本自动推导的 Release 源必须校验;用户显式 -bin=<路径或URL> 视为信任输入
    VERIFY_SUMS=1
fi
if [ -f "$BIN" ]; then
    cp -f "$BIN" "$TMP_BIN"
else
    log "下载二进制: $BIN"
    if curl -fSL --connect-timeout 10 -o "$TMP_BIN" "$BIN"; then
        :
    elif [ -n "${PROXY_BIN:-}" ]; then
        log "直连下载失败，尝试代理 $GH_PROXY"
        curl -fSL --connect-timeout 10 -o "$TMP_BIN" "$PROXY_BIN" || die "直连和代理下载均失败（可用 -bin= 指定本地路径）"
    else
        die "二进制下载失败（可用 -bin= 指定本地路径）"
    fi
    if [ "$VERIFY_SUMS" = 1 ]; then
        TMP_SUMS=$(mktemp /tmp/probe-rs-sums.XXXXXX)
        if ! curl -fSL --connect-timeout 10 -o "$TMP_SUMS" "$SUM_URL"; then
            if [ -n "${PROXY_SUM_URL:-}" ]; then
                log "直连 SHA256SUMS 失败，尝试代理（此时校验只能保证传输完整性）"
                curl -fSL --connect-timeout 10 -o "$TMP_SUMS" "$PROXY_SUM_URL" ||
                    die "直连和代理 SHA256SUMS 下载均失败，拒绝安装未校验的二进制"
            else
                die "SHA256SUMS 下载失败，拒绝安装未校验的二进制；可用 -bin= 指定本地路径"
            fi
        fi
        asset=$(basename "$BIN")
        expected=$(awk -v asset="$asset" '$2 == asset { print $1; exit }' "$TMP_SUMS")
        if [ -z "$expected" ]; then
            die "SHA256SUMS 缺少 $asset 的校验值，拒绝安装"
        fi
        actual=$(sha256sum "$TMP_BIN" | awk '{print $1}')
        if [ "$actual" != "$expected" ]; then
            die "二进制 SHA-256 校验失败（期望 $expected，实际 $actual）；旧版本保持不变"
        fi
        log "SHA-256 校验通过: $actual"
    fi
fi
# staged replace：先写同目录临时文件再原子替换，中途失败不破坏现有二进制
install -m 0755 "$TMP_BIN" "$BIN_DST.new" || die "安装二进制失败；旧版本保持不变"
mv -f "$BIN_DST.new" "$BIN_DST"
rm -f "$TMP_BIN" "$TMP_SUMS"
trap - EXIT

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
                if ($0 ~ /^[[:space:]]*url[[:space:]]*=[[:space:]]*"https:\/\/monitor\.example\.com\/update"/) seeded=1
                if ($0 ~ /^[[:space:]]*endpoint[[:space:]]*=[[:space:]]*"https:\/\/komari\.example\.com"/) seeded=1
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
    awk -v enabled="$AUTO_UPDATE" -v channel="$UPDATE_CHANNEL" -v proxy="$GH_PROXY_ESC" '
        function proxy_array() {
            return proxy == "" ? "[]" : "[\"" proxy "\"]"
        }
        function append_proxy(line,    bare, comment, hash) {
            if (proxy == "" || index(line, "\"" proxy "\"") > 0) return line
            bare=line; comment=""; hash=index(bare, "#")
            if (hash > 0) {
                comment=substr(bare, hash)
                bare=substr(bare, 1, hash - 1)
            }
            sub(/[[:space:]]+$/, "", bare)
            if (bare ~ /\[[[:space:]]*\]$/) sub(/\[[[:space:]]*\]$/, proxy_array(), bare)
            else if (bare ~ /\]$/) sub(/\]$/, ", \"" proxy "\"]", bare)
            else return line
            return bare (comment == "" ? "" : " " comment)
        }
        function finish_auto() {
            if (!seen_enabled) print "enabled = " enabled
            if (!seen_channel) print "channel = \"" channel "\""
            if (!seen_interval) print "check_interval = 21600"
            if (!seen_proxys) print "proxys = " proxy_array()
        }
        function print_auto() {
            print "[auto_update]"
            print "enabled = " enabled
            print "channel = \"" channel "\""
            print "check_interval = 21600"
            print "proxys = " proxy_array()
            print ""
        }
        BEGIN { in_auto=0; inserted=0 }
        /^[[:space:]]*\[auto_update\][[:space:]]*$/ {
            in_auto=1; inserted=1
            seen_enabled=seen_channel=seen_interval=seen_proxys=0
            print
            next
        }
        in_auto && /^[[:space:]]*\[/ {
            finish_auto()
            in_auto=0
        }
        in_auto {
            if ($0 ~ /^[[:space:]]*enabled[[:space:]]*=/) {
                if (!seen_enabled) print "enabled = " enabled
                seen_enabled=1
            } else if ($0 ~ /^[[:space:]]*channel[[:space:]]*=/) {
                if (!seen_channel) print "channel = \"" channel "\""
                seen_channel=1
            } else {
                if ($0 ~ /^[[:space:]]*check_interval[[:space:]]*=/) seen_interval=1
                if ($0 ~ /^[[:space:]]*proxys[[:space:]]*=/) {
                    seen_proxys=1
                    print append_proxy($0)
                } else print
            }
            next
        }
        !inserted && /^[[:space:]]*\[\[reporters\]\][[:space:]]*$/ {
            print_auto()
            inserted=1
        }
        { print }
        END {
            if (in_auto) finish_auto()
            else if (!inserted) { print ""; print_auto() }
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

[reporters.komari]
endpoint = "$ENDPOINT_ESC"
token = "$TOKEN_ESC"
interval = $INTERVAL
month_rotate = $RESET_DAY
enable_gpu = $ENABLE_GPU
include_nics = "$NICS_ESC"
include_mountpoints = ""
EOF
    log "preserved other Reporters and upserted Komari Reporter '$REPORTER_ID'"
else
    # 缺失配置或旧的根连接 schema 都直接覆盖为新 canonical 格式，不做兼容迁移。
cat > "$CONFIG_PATH" <<EOF
schema = 1

data_dir = "$DATA_DIR"

[[reporters]]
id = "$REPORTER_ID"

[reporters.komari]
endpoint = "$ENDPOINT_ESC"
token = "$TOKEN_ESC"
interval = $INTERVAL
month_rotate = $RESET_DAY
enable_gpu = $ENABLE_GPU
include_nics = "$NICS_ESC"
include_mountpoints = ""
EOF
    log "wrote a fresh canonical config with Komari Reporter '$REPORTER_ID'"
fi
upsert_auto_update_config "$CONFIG_PATH"
chmod 600 "$CONFIG_PATH"

# ---- systemd unit ----
if [ "$USER_SERVICE" = true ]; then
    unit_bin=$(systemd_escape "$BIN_DST")
    unit_config=$(systemd_escape "$CONFIG_PATH")
    cat > "$UNIT_DST" <<EOF
[Unit]
Description=probe-rs server monitoring agent

[Service]
Type=simple
ExecStart="$unit_bin" --config "$unit_config"
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
ExecStart=$BIN_DST
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
service_ctl enable probe-rs >/dev/null 2>&1 || true
service_ctl restart probe-rs
sleep 1
if service_ctl is-active --quiet probe-rs; then
    log "安装完成，服务运行中（komari 模式 → $ENDPOINT）。日志: $JOURNAL_CMD -f"
    warn_linger
else
    die "服务启动失败，排查: $JOURNAL_CMD -n 20 --no-pager"
fi
