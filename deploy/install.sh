#!/bin/sh
# probe-rs 安装/卸载脚本（Linux + systemd）
#
# 用法（root 或 sudo 执行，任意目录下均可）：
#   ./install.sh [二进制路径]     安装；默认 仓库根/target/release/probe-rs
#   ./install.sh uninstall        停用并移除 unit 与二进制（保留 /etc 配置与 /var 数据）
#   ./install.sh uninstall --purge  连同 /etc/probe-rs 与 /var/lib/probe-rs 一起删除
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
BIN_SRC="${1:-$SCRIPT_DIR/../target/release/probe-rs}"
BIN_DST=/usr/local/bin/probe-rs
CONF_DIR=/etc/probe-rs
DATA_DIR=/var/lib/probe-rs
UNIT_DST=/etc/systemd/system/probe-rs.service
EXAMPLE_CONF="$SCRIPT_DIR/../config.example.toml"

[ "$(id -u)" = 0 ] || { echo "需要 root 执行（sudo $0 $*）" >&2; exit 1; }
command -v systemctl >/dev/null || { echo "未找到 systemctl，本脚本仅支持 systemd 系统" >&2; exit 1; }

reload() { systemctl daemon-reload 2>/dev/null || echo "警告: daemon-reload 失败（systemd 未运行？）"; }

do_uninstall() {
    systemctl disable --now probe-rs 2>/dev/null || true
    rm -f "$UNIT_DST" "$BIN_DST"
    reload
    if [ "${1:-}" = "--purge" ]; then
        rm -rf "$CONF_DIR" "$DATA_DIR"
        echo "已卸载并清除配置与数据"
    else
        echo "已卸载（保留 $CONF_DIR 与 $DATA_DIR，加 --purge 可一并清除）"
    fi
}

is_placeholder() {
    # 示例配置未被编辑过的特征（身份/地址仍为占位值；secret 不算，可能是真实值）
    grep -q '^server_id = "srv-01"' "$1" \
        || grep -q '^worker_url = "https://monitor.example.com/report"' "$1"
}

do_install() {
    if [ ! -x "$BIN_SRC" ]; then
        echo "未找到二进制 $BIN_SRC，先执行: cargo build --release" >&2
        exit 1
    fi
    install -m 0755 "$BIN_SRC" "$BIN_DST"
    install -d -m 0755 "$DATA_DIR"
    install -d -m 0750 "$CONF_DIR"
    install -m 0644 "$SCRIPT_DIR/probe-rs.service" "$UNIT_DST"
    reload

    if [ -f "$CONF_DIR/config.toml" ]; then
        echo "保留已有配置 $CONF_DIR/config.toml"
        if is_placeholder "$CONF_DIR/config.toml"; then
            echo ""
            echo "配置仍是示例占位（srv-01 / change-me），暂不启动。"
            echo "编辑 $CONF_DIR/config.toml 后执行: systemctl enable --now probe-rs"
            exit 0
        fi
        systemctl enable probe-rs
        if systemctl restart probe-rs; then
            echo "已安装并启动。状态: systemctl status probe-rs"
        else
            echo ""
            echo "警告: 服务启动失败（多半是现有配置非法）。"
            echo "排查: journalctl -u probe-rs -n 20 --no-pager"
            echo "修好后: systemctl restart probe-rs"
        fi
    else
        # 含 secret，权限 600；示例文件缺失时写最小兜底配置
        if [ -f "$EXAMPLE_CONF" ]; then
            install -m 0600 "$EXAMPLE_CONF" "$CONF_DIR/config.toml"
        else
            cat > "$CONF_DIR/config.toml" <<'EOF'
server_id = "srv-01"
secret = "change-me"
worker_url = "https://monitor.example.com/report"
net_static_path = "/var/lib/probe-rs/net_static.json"
EOF
            chmod 600 "$CONF_DIR/config.toml"
        fi
        echo ""
        echo "首次安装：请编辑 $CONF_DIR/config.toml"
        echo "  必填: server_id / secret / worker_url"
        echo "然后启动: systemctl enable --now probe-rs"
    fi
}

case "${1:-}" in
    uninstall) do_uninstall "${2:-}" ;;
    *) do_install ;;
esac
