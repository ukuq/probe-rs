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
# 官方参数里 -auto_update 忽略（probe-rs 不做自升级）；
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
  -collect_interval= 采样间隔秒（0 = 实时，按 1s 处理；缺省 0）
  -interval=         上报间隔秒（缺省 60）
  -reset_day=        月流量账期重置日 1-31，0 = 不重置（缺省 1）
  -ct= -cu= -cm= -bd=  电信/联通/移动/BGP 探测节点 host[:port]
  -auto_update=      忽略（probe-rs 不支持自升级）
  -rx_correction= -tx_correction=  忽略（校正由服务端运行时下发）
额外:
  -bin=              二进制来源（本地路径或 URL），缺省 GitHub Releases
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
CT=""; CU=""; CM=""; BD=""; BIN=""
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
        -auto_update=*|-rx_correction=*|-tx_correction=*)
            log "参数 $arg 忽略（见脚本头注释）" ;;
        *) die "未知参数: $arg" ;;
    esac
done

[ "$(id -u)" = 0 ] || die "需要 root（sudo 或 root 执行）"
[ -n "$ID" ] || die "缺少 -id="
[ -n "$SECRET" ] || die "缺少 -secret="
[ -n "$URL" ] || die "缺少 -url="
command -v systemctl >/dev/null || die "仅支持 systemd 系统"
# collect_interval=0 按 1s（实时）；其余非法值兜底
case "$COLLECT" in ''|*[!0-9]*) COLLECT=1 ;; esac
[ "$COLLECT" -lt 1 ] && COLLECT=1
case "$REPORT" in ''|*[!0-9]*) REPORT=60 ;; esac
[ "$REPORT" -lt 1 ] && REPORT=60

# ---- 二进制 ----
TMP_BIN=/tmp/probe-rs.bin
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
    cp "$BIN" "$TMP_BIN"
else
    log "下载二进制: $BIN"
    curl -fSL --connect-timeout 10 -o "$TMP_BIN" "$BIN" || die "二进制下载失败（可用 -bin= 指定本地路径）"
fi
install -m 0755 "$TMP_BIN" "$BIN_DST"
rm -f "$TMP_BIN"

# ---- 配置 ----
install -d -m 0755 "$DATA_DIR"
install -d -m 0750 "$CONF_DIR"
{
    echo "server_id = \"$ID\""
    echo "secret = \"$SECRET\""
    echo "worker_url = \"$URL\""
    echo 'protocol = "cf"'
    echo "net_static_path = \"$DATA_DIR/net_static.json\""
    echo "reset_day = $RESET_DAY"
    echo 'config_version = ""'
    echo "interfaces = []"
    echo "enable_gpu = false"
    echo "report_errors = true"
    echo "report_self = false"
    echo ""
    echo "[intervals]"
    echo "collect = $COLLECT"
    echo "report = $REPORT"
    echo "ping = 30"
    echo "slow = 60"
    echo "gpu = 60"
    echo "ip = 600"
    for pair in "ct:$CT" "cu:$CU" "cm:$CM" "bd:$BD"; do
        name="${pair%%:*}"; target="${pair#*:}"
        [ -n "$target" ] || continue
        echo ""
        echo "[[pings]]"
        echo "name = \"$name\""
        echo "target = \"$target\""
    done
    echo ""
    echo "[ext.cf]"
    echo "correction = true"
    echo "batch = true"
} > "$CONF_DIR/config.toml"
chmod 600 "$CONF_DIR/config.toml"

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
ReadWritePaths=-/var/lib/probe-rs -/etc/probe-rs
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
