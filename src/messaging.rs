// This is the handrolled version of what our wit bindgen stuff should generate
#![allow(clippy::redundant_clone)]
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct BrokerMessage {
    pub subject: String,
    pub body: Option<Vec<u8>>,
    pub reply_to: Option<String>,
}

pub struct Handler<'a> {
    ld: &'a ::provider_sdk::core::LinkDefinition,
}

impl<'a> Handler<'a> {
    pub fn new(ld: &'a ::provider_sdk::core::LinkDefinition) -> Self {
        Self { ld }
    }

    pub async fn handle(
        &self,
        msg: BrokerMessage,
    ) -> Result<(), ::provider_sdk::error::ProviderInvocationError> {
        let connection = provider_sdk::provider_main::get_connection();

        let client = connection.get_rpc_client();

        let response = client
            .send(
                ::provider_sdk::core::WasmCloudEntity {
                    public_key: self.ld.provider_id.clone(),
                    link_name: self.ld.link_name.clone(),
                    contract_id: "wasmcloud:messaging".to_string(),
                },
                ::provider_sdk::core::WasmCloudEntity {
                    public_key: self.ld.actor_id.clone(),
                    ..Default::default()
                },
                "Message.Handle",
                ::provider_sdk::serialize(&msg)?,
            )
            .await?;

        if let Some(err) = response.error {
            // Please note that all errors used should implement ToString in order for this to work
            Err(::provider_sdk::error::ProviderInvocationError::Provider(
                err.to_string(),
            ))
        } else {
            Ok(())
        }
    }
}

// NOTE(thomastaylor312): In order for the provider to get what it needs to do work, it needs to
// pass the context with the actor ID on it (which means the signatures are slightly different than
// what is expressed in wit, but will be entirely contained to our generators). I'm guessing we can
// come up with a cleaner way to maybe do this so we aren't generating special functions here, but
// not going to do it for now.
#[async_trait::async_trait]
pub trait Consumer {
    async fn request(
        &self,
        ctx: ::provider_sdk::Context,
        subject: String,
        body: Option<Vec<u8>>,
        timeout_ms: u32,
    ) -> Result<BrokerMessage, String>;

    async fn request_multi(
        &self,
        ctx: ::provider_sdk::Context,
        subject: String,
        body: Option<Vec<u8>>,
        timeout_ms: u32,
        max_results: u32,
    ) -> Result<Vec<BrokerMessage>, String>;

    async fn publish(&self, ctx: ::provider_sdk::Context, msg: BrokerMessage)
        -> Result<(), String>;
}

// NOTE(thomastaylor312): I tried to implement this for T where T: Consumer, but because provider
// sdk is a foreign type, it didn't work. So the macro that generates this will need to take the
// type name of the provider implementation. We can probably find a better way.
#[async_trait::async_trait]
impl ::provider_sdk::MessageDispatch for super::NatsMessagingProvider {
    async fn dispatch<'a>(
        &'a self,
        ctx: ::provider_sdk::Context,
        method: String,
        body: std::borrow::Cow<'a, [u8]>,
    ) -> Result<Vec<u8>, ::provider_sdk::error::ProviderInvocationError> {
        match method.as_str() {
            "Message.Request" => {
                let input: RequestBody = ::provider_sdk::deserialize(&body)?;
                let result = self
                    .request(ctx, input.subject, input.body, input.timeout_ms)
                    .await
                    .map_err(|e| {
                        ::provider_sdk::error::ProviderInvocationError::Provider(e.to_string())
                    })?;
                Ok(::provider_sdk::serialize(&result)?)
            }
            "Message.RequestMulti" => {
                let input: RequestMultiBody = ::provider_sdk::deserialize(&body)?;
                let result = self
                    .request_multi(
                        ctx,
                        input.subject,
                        input.body,
                        input.timeout_ms,
                        input.max_results,
                    )
                    .await
                    .map_err(|e| {
                        ::provider_sdk::error::ProviderInvocationError::Provider(e.to_string())
                    })?;
                Ok(::provider_sdk::serialize(&result)?)
            }
            "Message.Publish" => {
                let input: PublishBody = ::provider_sdk::deserialize(&body)?;
                let result = self.publish(ctx, input.msg).await.map_err(|e| {
                    ::provider_sdk::error::ProviderInvocationError::Provider(e.to_string())
                })?;
                Ok(::provider_sdk::serialize(&result)?)
            }
            _ => Err(::provider_sdk::error::InvocationError::Malformed(format!(
                "Invalid method name {method}",
            ))
            .into()),
        }
    }
}

// Same note here about type
impl ::provider_sdk::Provider for super::NatsMessagingProvider {}

#[derive(Debug, Serialize, Deserialize)]
struct RequestBody {
    subject: String,
    body: Option<Vec<u8>>,
    timeout_ms: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct RequestMultiBody {
    subject: String,
    body: Option<Vec<u8>>,
    timeout_ms: u32,
    max_results: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct PublishBody {
    msg: BrokerMessage,
}
