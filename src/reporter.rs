//! 上报器：POST /report，X-Secret 认证，失败丢弃（由调用方 drain 后不重试），
//! 响应体非空时解析为远端配置

use std::time::Duration;

use anyhow::{Context, Result};

use crate::model::{RemoteConfig, Report};
use crate::reporter_cf::{CfConfirm, CfResponse, CfUpdate};

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
    reporter_id: String,
    protocol: String,
}

impl Reporter {
    pub fn new(
        worker_url: &str,
        secret: &str,
        agent_version: &str,
        reporter_id: &str,
        protocol: &str,
    ) -> Result<Self> {
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
            reporter_id: reporter_id.to_string(),
            protocol: protocol.to_string(),
        })
    }

    /// 成功返回响应中的动作；任何失败返回 Err，调用方直接保留数据待重发
    pub async fn send(&self, report: &Report) -> Result<ResponseAction> {
        let resp = self
            .client
            .post(&self.url)
            .header("X-Secret", &self.secret)
            .header("X-Agent-Version", &self.agent_version)
            // Optional metadata: old servers ignore these headers. Values are
            // percent-encoded so arbitrary Unicode Reporter ids remain valid.
            .header("X-Reporter-Id", encode_header_value(&self.reporter_id))
            .header("X-Reporter-Protocol", encode_header_value(&self.protocol))
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

    /// CF 协议公共 POST：统一 agent_version 头与错误包装
    async fn post_cf(
        &self,
        body: &impl serde::Serialize,
        extra: &[(&str, &str)],
    ) -> Result<reqwest::Response> {
        // CF 服务端 agent_version 取值优先用该头——带官方兼容版本前缀以区分探针实现
        let agent_ver = crate::reporter_cf::cf_agent_version(&self.agent_version);
        let mut req = self
            .client
            .post(&self.url)
            .header("X-Agent-Version", &agent_ver);
        for (k, v) in extra {
            req = req.header(*k, *v);
        }
        req.json(body).send().await.context("CF 请求失败")
    }

    /// CF 协议上报（POST /update）。config_md5 为当前已应用的 CF 配置 MD5（空 = none）。
    /// 204 = 无变更；200 = 解析 URL-encoded 配置/校正
    pub async fn send_cf(&self, update: &CfUpdate, config_md5: &str) -> Result<CfResponse> {
        let md5 = if config_md5.is_empty() {
            "none"
        } else {
            config_md5
        };
        let resp = self
            .post_cf(
                update,
                &[("X-Agent-Config-Schema", "3"), ("X-Agent-Config-Md5", md5)],
            )
            .await?;
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

    /// CF 校正确认（独立请求，不带 metrics）：成功 200 纯文本 OK
    pub async fn send_cf_confirm(&self, confirm: &CfConfirm) -> Result<()> {
        let resp = self.post_cf(confirm, &[]).await?;
        if !resp.status().is_success() {
            anyhow::bail!("校正确认响应 HTTP {}", resp.status());
        }
        Ok(())
    }
}

fn encode_header_value(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(byte as char);
        } else {
            use std::fmt::Write;
            write!(&mut encoded, "%{byte:02X}").expect("writing to String cannot fail");
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    #[test]
    fn reporter_header_value_percent_encodes_unicode() {
        let encoded = super::encode_header_value("本地 demo/一");
        assert_eq!(encoded, "%E6%9C%AC%E5%9C%B0%20demo%2F%E4%B8%80");
    }
}
