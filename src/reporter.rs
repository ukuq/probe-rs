//! 上报器：POST /report，X-Secret 认证，失败丢弃（由调用方 drain 后不重试），
//! 响应体非空时解析为远端配置

use std::time::Duration;

use anyhow::{Context, Result};

use crate::model::{RemoteConfig, Report};

const TIMEOUT: Duration = Duration::from_secs(8);

/// 响应信封：config 缺席 = 无配置变更；next.static = true 时下次上报强制带 static
#[derive(serde::Deserialize)]
struct ReportResponse {
    config: Option<RemoteConfig>,
    #[serde(default)]
    next: Option<NextDirective>,
}

/// 对下一次上报的指令
#[derive(serde::Deserialize)]
struct NextDirective {
    #[serde(rename = "static", default)]
    r#static: bool,
}

/// 上报响应中需要 agent 执行的动作
pub struct ResponseAction {
    pub config: Option<RemoteConfig>,
    /// 下次上报强制带 static
    pub next_static: bool,
}

pub struct Reporter {
    client: reqwest::Client,
    url: String,
    secret: String,
    agent_version: String,
}

impl Reporter {
    pub fn new(worker_url: &str, secret: &str, agent_version: &str) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(TIMEOUT)
            .pool_max_idle_per_host(1)
            .build()
            .context("构建 HTTP client 失败")?;
        Ok(Self {
            client,
            url: worker_url.to_string(),
            secret: secret.to_string(),
            agent_version: agent_version.to_string(),
        })
    }

    /// 成功返回响应中的动作；任何失败返回 Err，调用方直接保留数据待重发
    pub async fn send(&self, report: &Report) -> Result<ResponseAction> {
        let resp = self
            .client
            .post(&self.url)
            .header("X-Secret", &self.secret)
            .header("X-Agent-Version", &self.agent_version)
            .json(report)
            .send()
            .await
            .context("上报请求失败")?;
        let status = resp.status();
        if !status.is_success() {
            anyhow::bail!("上报响应 HTTP {status}");
        }
        let body = resp.text().await.context("读取上报响应失败")?;
        let body = body.trim();
        if body.is_empty() || body == "{}" {
            return Ok(ResponseAction { config: None, next_static: false });
        }
        let parsed: ReportResponse = serde_json::from_str(body).context("解析上报响应失败")?;
        Ok(ResponseAction {
            config: parsed.config,
            next_static: parsed.next.is_some_and(|n| n.r#static),
        })
    }
}
