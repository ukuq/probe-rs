#!/bin/sh
# probe-rs 安装/卸载脚本（Linux + systemd）
#
# 用法（任意目录下均可；root 安装系统服务，普通用户安装用户服务）：
#   ./install.sh [二进制路径]     安装；默认 仓库根/target/release/probe-rs
#   ./install.sh uninstall        停用并移除当前用户范围的 unit 与二进制（保留配置和数据）
#   ./install.sh uninstall --purge  连同当前用户范围的配置和数据一起删除
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
BIN_SRC="${1:-$SCRIPT_DIR/../target/release/probe-rs}"
EXAMPLE_CONF="$SCRIPT_DIR/../config.example.toml"
USER_SERVICE=false

if [ "$(id -u)" = 0 ]; then
    BIN_DST=/usr/local/bin/probe-rs
    CONF_DIR=/etc/probe-rs
    DATA_DIR=/var/lib/probe-rs
    UNIT_DST=/etc/systemd/system/probe-rs.service
    SERVICE_CMD='systemctl'
    JOURNAL_CMD='journalctl -u probe-rs'
else
    [ -n "${HOME:-}" ] || { echo "普通用户安装需要 HOME" >&2; exit 1; }
    USER_SERVICE=true
    CONFIG_HOME=${XDG_CONFIG_HOME:-$HOME/.config}
    DATA_HOME=${XDG_DATA_HOME:-$HOME/.local/share}
    BIN_DST=$HOME/.local/bin/probe-rs
    CONF_DIR=$CONFIG_HOME/probe-rs
    DATA_DIR=$DATA_HOME/probe-rs
    UNIT_DST=$CONFIG_HOME/systemd/user/probe-rs.service
    SERVICE_CMD='systemctl --user'
    JOURNAL_CMD='journalctl --user -u probe-rs'
fi
CONFIG_PATH=$CONF_DIR/config.toml

command -v systemctl >/dev/null || { echo "未找到 systemctl，本脚本仅支持 systemd 系统" >&2; exit 1; }

service_ctl() {
    if [ "$USER_SERVICE" = true ]; then
        systemctl --user "$@"
    else
        systemctl "$@"
    fi
}

reload() { service_ctl daemon-reload 2>/dev/null || echo "警告: daemon-reload 失败（systemd 未运行？）"; }

warn_linger() {
    [ "$USER_SERVICE" = true ] || return 0
    command -v loginctl >/dev/null 2>&1 || return 0
    linger=$(loginctl show-user "$(id -u)" --property=Linger --value 2>/dev/null || true)
    if [ "$linger" = no ]; then
        echo "警告: 当前用户未启用 linger，退出登录后用户服务可能停止。"
        echo "如需后台常驻，请让管理员执行: loginctl enable-linger $(id -un)"
    fi
}

toml_escape() { printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'; }

systemd_escape() { printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g; s/%/%%/g'; }

install_unit() {
    install -d -m 0755 "$(dirname -- "$UNIT_DST")"
    if [ "$USER_SERVICE" = false ]; then
        install -m 0644 "$SCRIPT_DIR/probe-rs.service" "$UNIT_DST"
        return
    fi

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
    chmod 0644 "$UNIT_DST"
}

install_example_config() {
    if [ "$USER_SERVICE" = false ]; then
        install -m 0600 "$EXAMPLE_CONF" "$CONFIG_PATH"
        return
    fi

    data_dir_esc=$(toml_escape "$DATA_DIR")
    replacement=$(printf '%s' "$data_dir_esc" | sed 's/[\\&|]/\\&/g')
    tmp_config=$(mktemp "$CONF_DIR/config.toml.XXXXXX")
    sed "s|^[[:space:]]*data_dir[[:space:]]*=.*|data_dir = \"$replacement\"|" \
        "$EXAMPLE_CONF" > "$tmp_config"
    install -m 0600 "$tmp_config" "$CONFIG_PATH"
    rm -f "$tmp_config"
}

do_uninstall() {
    service_ctl disable --now probe-rs 2>/dev/null || true
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
    grep -Eq '^[[:space:]]*server_id[[:space:]]*=[[:space:]]*"(srv-01|cf-server-uuid)"' "$1" \
        || grep -Eq '^[[:space:]]*url[[:space:]]*=[[:space:]]*"https://monitor\.example\.com/update"' "$1" \
        || grep -Eq '^[[:space:]]*endpoint[[:space:]]*=[[:space:]]*"https://komari\.example\.com"' "$1" \
        || grep -Eq '^[[:space:]]*worker_url[[:space:]]*=[[:space:]]*"https://monitor\.example\.com/(report|update)"' "$1" \
        || grep -Eq '^[[:space:]]*worker_url[[:space:]]*=[[:space:]]*"https://komari\.example\.com"' "$1" \
        || grep -Eq '^[[:space:]]*worker_url[[:space:]]*=[[:space:]]*"http://127\.0\.0\.1:8080/report"' "$1"
}

do_install() {
    if [ "$USER_SERVICE" = true ] && ! service_ctl show-environment >/dev/null 2>&1; then
        echo "普通用户安装需要正在运行的 systemd 用户会话（systemctl --user 不可用）" >&2
        exit 1
    fi
    if [ ! -x "$BIN_SRC" ]; then
        echo "未找到二进制 $BIN_SRC，先执行: cargo build --release" >&2
        exit 1
    fi
    install -d -m 0755 "$(dirname -- "$BIN_DST")"
    install -m 0755 "$BIN_SRC" "$BIN_DST"
    install -d -m 0755 "$DATA_DIR"
    install -d -m 0750 "$CONF_DIR"
    install_unit
    reload

    if [ -f "$CONFIG_PATH" ]; then
        echo "保留已有配置 $CONFIG_PATH"
        if is_placeholder "$CONFIG_PATH"; then
            echo ""
            echo "配置仍包含示例占位身份或地址，暂不启动。"
            echo "编辑 $CONFIG_PATH 后执行: $SERVICE_CMD enable --now probe-rs"
            warn_linger
            exit 0
        fi
        service_ctl enable probe-rs
        if service_ctl restart probe-rs; then
            echo "已安装并启动。状态: $SERVICE_CMD status probe-rs"
            warn_linger
        else
            echo ""
            echo "错误: 服务启动失败（多半是现有配置非法）。" >&2
            echo "排查: $JOURNAL_CMD -n 20 --no-pager" >&2
            echo "修好后: $SERVICE_CMD restart probe-rs" >&2
            exit 1
        fi
    else
        # 含 secret，权限 600；示例文件缺失时写最小合法兜底配置（schema = 1）。
        if [ -f "$EXAMPLE_CONF" ]; then
            install_example_config
        else
            data_dir_esc=$(toml_escape "$DATA_DIR")
            cat > "$CONFIG_PATH" <<EOF
# probe-rs minimal fallback config（首次安装后必须编辑）
schema = 1

data_dir = "$data_dir_esc"

[auto_update]
enabled = false
channel = "stable"
check_interval = 21600

[[reporters]]
id = "primary"

[reporters.probe]
server_id = "srv-01"
secret = "change-me"
worker_url = "https://monitor.example.com/report"
report_interval = 60
reset_day = 1
interfaces = []
disks = []
report_gpu = true
report_errors = true
report_self = false

[reporters.probe.intervals]
collect = 10
ping = 30
slow = 60
gpu = 60
ip = 600
diskio = 10
EOF
            chmod 600 "$CONFIG_PATH"
        fi
        echo ""
        echo "首次安装：请编辑 $CONFIG_PATH"
        echo "  必填: server_id / secret / worker_url"
        echo "然后启动: $SERVICE_CMD enable --now probe-rs"
        warn_linger
    fi
}

case "${1:-}" in
    uninstall) do_uninstall "${2:-}" ;;
    *) do_install ;;
esac
