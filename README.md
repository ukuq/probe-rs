# probe-rs

使用 Rust 编写的跨平台服务器监控探针，支持 Linux 和 Windows，可对接原生 Probe、CF-Server-Monitor 与 Komari 协议。

## 安装

推荐使用 [安装脚本转换 / 部署命令生成器](https://htmlpreview.github.io/?https://raw.githubusercontent.com/ukuq/probe-rs/refs/heads/master/deploy/deploy-generator.html)。它可以将 CF-Server-Monitor 或 Komari 的官方安装命令转换为 probe-rs 安装命令，也可以直接填写参数生成原生、CF 或 Komari 模式的部署命令。

仓库内也提供了以下安装脚本：

- Linux：`deploy/install.sh`、`deploy/cf-install.sh`、`deploy/komari-install.sh`
- Windows：`deploy/install.ps1`、`deploy/cf-install.ps1`

Linux 支持普通用户的 systemd user service；Windows 默认安装到当前用户，无需管理员权限。需要机器级常驻时，可显式选择 root 或 Windows Machine 模式。

## 功能

- CPU、内存、磁盘、网络、负载、进程、连接数、GPU 和 Ping 等指标采集
- 多上报端独立运行，采集与上报周期解耦
- CF HTTP/WSS 上报及远程配置下发
- 流量账期统计、校正与持久化
- 自动更新与配置热加载

## 构建与测试

```bash
cargo build --release
cargo test
```

更多细节请参阅 [设计文档](DESIGN.md)、[上报协议](REPORT.md) 和 [实现说明](IMPL.md)。

## License

MIT
