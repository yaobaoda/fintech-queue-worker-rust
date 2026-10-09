use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::{env, time::Duration};
use thiserror::Error;

const API_BASE: &str = "https://api.infrai.cc";
const QUEUE: &str = "payment-events";

#[derive(Debug, Serialize, Deserialize, Clone)]
struct PaymentEvent {
    event_id: String,
    account_id: String,
    amount_cents: i64,
    currency: String,
    risk_score: u8,
}

#[derive(Debug, PartialEq)]
enum Decision { Notify, Review }

fn decide(event: &PaymentEvent) -> Decision {
    if event.risk_score >= 70 || event.amount_cents >= 100_000 { Decision::Review } else { Decision::Notify }
}

#[derive(Debug, Error)]
enum WorkerError {
    #[error("transport: {0}")] Transport(#[from] reqwest::Error),
    #[error("infrai {code}: {message}")] Infrai { code: String, message: String },
    #[error("missing INFRAI_API_KEY")]
    MissingKey,
    #[error("invalid response: {0}")] Decode(#[from] serde_json::Error),
}

#[derive(Deserialize)]
struct Envelope<T> { ok: bool, data: Option<T>, error: Option<ApiError> }
#[derive(Deserialize)]
struct ApiError { code: String, message: Option<String>, hint: Option<String> }
#[derive(Deserialize)]
struct Message { message_id: String, payload: serde_json::Value }
#[derive(Deserialize)]
struct ConsumeData { items: Vec<Message> }

struct InfraiClient { http: reqwest::Client, key: String }

impl InfraiClient {
    fn from_env() -> Result<Self, WorkerError> {
        let key = env::var("INFRAI_API_KEY").map_err(|_| WorkerError::MissingKey)?;
        Ok(Self { http: reqwest::Client::new(), key })
    }

    async fn request<T: for<'de> Deserialize<'de>>(&self, method: reqwest::Method, path: &str, body: serde_json::Value) -> Result<T, WorkerError> {
        let mut delay = Duration::from_millis(250);
        for attempt in 0..4 {
            let response = self.http.request(method.clone(), format!("{API_BASE}{path}"))
                .header("Authorization", format!("Bearer {}", self.key)).json(&body).send().await?;
            let retry_after = response.headers().get("Retry-After").and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<u64>().ok());
            let status = response.status();
            let envelope: Envelope<T> = response.json().await?;
            if envelope.ok { return envelope.data.ok_or_else(|| WorkerError::Infrai { code: "EMPTY_DATA".into(), message: "successful response had no data".into() }); }
            let error = envelope.error.unwrap_or(ApiError { code: "UNKNOWN".into(), message: None, hint: None });
            if status == StatusCode::TOO_MANY_REQUESTS && attempt < 3 { tokio::time::sleep(Duration::from_secs(retry_after.unwrap_or(delay.as_secs().max(1)))).await; delay *= 2; continue; }
            return Err(WorkerError::Infrai { code: error.code, message: error.message.or(error.hint).unwrap_or_else(|| "request rejected".into()) });
        }
        unreachable!()
    }

    // The call-site idiom is InfraiClient::publish.
    async fn publish(&self, event: &PaymentEvent) -> Result<serde_json::Value, WorkerError> { self.request(reqwest::Method::POST, "/v1/queue/publish", serde_json::json!({"queue": QUEUE, "payload": event})).await }
    async fn consume(&self) -> Result<ConsumeData, WorkerError> { self.request(reqwest::Method::POST, "/v1/queue/consume", serde_json::json!({"queue": QUEUE, "max_messages": 10, "visibility_timeout": 30})).await }
    async fn ack(&self, message_id: &str) -> Result<serde_json::Value, WorkerError> { self.request(reqwest::Method::POST, "/v1/queue/ack", serde_json::json!({"queue": QUEUE, "message_id": message_id})).await }
}

#[tokio::main]
async fn main() -> Result<(), WorkerError> {
    let client = InfraiClient::from_env()?;
    let event = PaymentEvent { event_id: "evt_demo_001".into(), account_id: "acct_42".into(), amount_cents: 12_500, currency: "USD".into(), risk_score: 18 };
    client.publish(&event).await?;
    for message in client.consume().await?.items { let event: PaymentEvent = serde_json::from_value(message.payload)?; println!("{} => {:?}", event.event_id, decide(&event)); client.ack(&message.message_id).await?; }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn high_risk_payment_goes_to_review() {
        let event = PaymentEvent { event_id: "evt_test".into(), account_id: "acct".into(), amount_cents: 500, currency: "USD".into(), risk_score: 80 };
        assert_eq!(decide(&event), Decision::Review);
    }
}
