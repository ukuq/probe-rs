# probe-rs 本地构建/检查流程
# CI 门禁（.github/workflows/ci.yml）与 make check 一致

.PHONY: fmt check test build install uninstall demo

# 格式化（提交前跑一次）
fmt:
	cargo fmt
	deno fmt server-demo/

# CI 同款检查：fmt 校验 + 类型检查 + 测试
check:
	cargo fmt --check
	deno fmt --check server-demo/
	deno check server-demo/server.ts
	cargo test

test:
	cargo test

# 本机构建（release：strip + lto，见 Cargo.toml [profile.release]）
build:
	cargo build --release

# Linux + systemd 安装（在目标机上执行）
install: build
	sudo ./deploy/install.sh

uninstall:
	sudo ./deploy/install.sh uninstall

# 本地演示：Deno 服务端（8080）+ 指定配置的 agent
demo:
	deno run --allow-net --allow-env=HOST server-demo/server.ts
