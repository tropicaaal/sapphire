use twilight_model::{
    channel::Message,
    id::{marker::ChannelMarker, Id},
};
use worker::{Fetch, Headers, Method, Request, RequestInit, Response};

const API_V10_BASE: &str = "https://discord.com/api/v10";

pub enum ChannelMessagesFilter {
    Before(u64),
    After(u64),
    Around(u64),
}

// this is like really not the whole payload but it doesn't matter since we don't
// care about other message components.
#[derive(serde::Serialize)]
struct CreateMessage<'a> {
    content: &'a str,
}

pub struct Bot {
    token: String,
}

impl Bot {
    const USER_AGENT: &str = "DiscordBot (https://tropical.sh, 1.0)";

    pub fn new(token: &str) -> Self {
        Self {
            token: token.to_string(),
        }
    }

    pub async fn create_message(
        &self,
        channel: Id<ChannelMarker>,
        content: &str,
    ) -> worker::Result<Response> {
        let body = serde_json::to_string(&CreateMessage { content })
            .map_err(|e| worker::Error::RustError(e.to_string()))?;

        let req = Request::new_with_init(
            &format!("{API_V10_BASE}/channels/{}/messages", channel),
            &RequestInit::new()
                .with_method(Method::Post)
                .with_headers({
                    #[allow(unused_mut)]
                    let mut headers = Headers::new();

                    headers.set("Content-Type", "application/json").unwrap();
                    headers
                        .set("Authorization", &format!("Bot {}", self.token))
                        .unwrap();
                    headers.set("User-Agent", Self::USER_AGENT).unwrap();

                    headers
                })
                .with_body(Some(body.into())),
        )?;

        Fetch::Request(req).send().await
    }

    pub async fn channel_messages(
        &self,
        channel: Id<ChannelMarker>,
        filter: Option<ChannelMessagesFilter>,
        limit: usize,
    ) -> worker::Result<Vec<Message>> {
        let url = format!(
            "{API_V10_BASE}/channels/{}/messages?limit={limit}{}",
            channel,
            match filter {
                Some(ChannelMessagesFilter::Before(id)) => format!("&before={id}"),
                Some(ChannelMessagesFilter::After(id)) => format!("&after={id}"),
                Some(ChannelMessagesFilter::Around(id)) => format!("&around={id}"),
                None => String::new(),
            }
        );

        const MAX_RETRIES: u32 = 5;
        let mut attempt = 0;

        loop {
            let req = Request::new_with_init(
                &url,
                &RequestInit::new().with_method(Method::Get).with_headers({
                    #[allow(unused_mut)]
                    let mut headers = Headers::new();

                    headers
                        .set("Authorization", &format!("Bot {}", self.token))
                        .unwrap();
                    headers.set("User-Agent", Self::USER_AGENT).unwrap();

                    headers
                }),
            )?;

            let mut res = Fetch::Request(req).send().await?;

            match res.status_code() {
                200 => return res.json::<Vec<Message>>().await,
                429 => {
                    // attempt at a somewhat sane ratelimit handler
                    attempt += 1;
                    if attempt > MAX_RETRIES {
                        return Err(worker::Error::RustError(format!(
                            "rate limited after {MAX_RETRIES} retries fetching {url}"
                        )));
                    }

                    let retry_after = res
                        .headers()
                        .get("Retry-After")
                        .ok()
                        .flatten()
                        .and_then(|v| v.parse::<f64>().ok())
                        .unwrap_or(1.0);

                    log::warn!(
                    "[bot] rate limited on channel {channel}, retrying in {retry_after}s (attempt {attempt})"
                );
                    worker::Delay::from(std::time::Duration::from_millis(
                        (retry_after * 1000.0) as u64,
                    ))
                    .await;
                }
                status => {
                    let body = res.text().await.unwrap_or_default();
                    return Err(worker::Error::RustError(format!(
                        "discord API error {status} fetching {url}: {body}"
                    )));
                }
            }
        }
    }
}
