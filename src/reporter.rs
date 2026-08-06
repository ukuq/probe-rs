//! 上报器：POST /report，X-Secret 认证，失败丢弃（由调用方 drain 后不重试），
//! 响应体非空时解析为远端配置

use std::time::Duration;

use anyhow::{Context, Result};

use crate::model::{RemoteConfig, Report};
use crate::reporter_cf::{CfCorrectionAck, CfResponse, CfUpdate};

const TIMEOUT: Duration = Duration::from_secs(8);
// 与 CF-Server-Monitor src/utils/agentConfig.js 的 AGENT_CONFIG_SCHEMA_VERSION 保持一致。
const CF_CONFIG_SCHEMA: &str = "3";

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
            return Ok(ResponseAction {
                config: None,
                next_static: false,
            });
        }
        let parsed: ReportResponse = serde_json::from_str(body).context("解析上报响应失败")?;
        Ok(ResponseAction {
            config: parsed.config,
            next_static: parsed.next.is_some_and(|n| n.r#static),
        })
    }

    fn cf_request<T: serde::Serialize + ?Sized>(
        &self,
        payload: &T,
        config_md5: &str,
    ) -> reqwest::RequestBuilder {
        let md5 = if config_md5.is_empty() {
            "none"
        } else {
            config_md5
        };
        let agent_ver = format!("probe-rs_{}", self.agent_version);
        self.client
            .post(&self.url)
            .header("X-Agent-Version", agent_ver)
            .header("X-Agent-Config-Schema", CF_CONFIG_SCHEMA)
            .header("X-Agent-Config-Md5", md5)
            .json(payload)
    }

    /// 单独确认已应用的 CF 流量校正。服务端会对该请求提前返回，因此请求体不能夹带指标。
    pub async fn send_cf_correction_ack(
        &self,
        ack: &CfCorrectionAck,
        config_md5: &str,
    ) -> Result<()> {
        let resp = self
            .cf_request(ack, config_md5)
            .send()
            .await
            .context("发送 CF 流量校正确认失败")?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("CF 流量校正确认响应 HTTP {status}: {}", body.trim());
        }
        Ok(())
    }

    /// CF 协议上报（POST /update）。config_md5 为当前已应用的 CF 配置 MD5（空 = none）。
    /// 204 = 无变更；200 = 解析 URL-encoded 配置/校正
    pub async fn send_cf(&self, update: &CfUpdate, config_md5: &str) -> Result<CfResponse> {
        let resp = self
            .cf_request(update, config_md5)
            .send()
            .await
            .context("上报请求失败")?;
        let status = resp.status();
        if status.as_u16() == 204 {
            return Ok(CfResponse::default());
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("上报响应 HTTP {status}: {}", body.trim());
        }
        let resp_md5 = resp
            .headers()
            .get("x-agent-config-md5")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let body = resp.text().await.context("读取上报响应失败")?;
        Ok(crate::reporter_cf::parse_response_body(
            &body,
            resp_md5.as_deref(),
        ))
    }
}
