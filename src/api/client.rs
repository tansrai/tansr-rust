use super::{operations, schema, *};
use crate::{canonical, sse};
use futures_util::StreamExt;
use reqwest::{
    Method,
    header::{HeaderMap, HeaderValue},
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime},
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio_util::sync::CancellationToken;

/// 同一登录主体的短期令牌更新；不得在已有绑定中切换主体。
pub type TokenProvider =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<String>> + Send>> + Send + Sync>;

pub struct ClientBuilder {
    base_url: String,
    token: String,
    provider: Option<TokenProvider>,
    family: String,
    timeout: Duration,
    idle_timeout: Duration,
    max_response: usize,
    max_frame: usize,
    roots: Vec<reqwest::Certificate>,
}
impl ClientBuilder {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            token: String::new(),
            provider: None,
            family: String::new(),
            timeout: Duration::from_secs(60),
            idle_timeout: Duration::from_secs(120),
            max_response: 2 * 1024 * 1024,
            max_frame: 2 * 1024 * 1024,
            roots: Vec::new(),
        }
    }
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = token.into();
        self
    }
    pub fn token_provider(mut self, provider: TokenProvider) -> Self {
        self.provider = Some(provider);
        self
    }
    pub fn session_family(mut self, family: impl Into<String>) -> Self {
        self.family = family.into();
        self
    }
    pub fn timeout(mut self, value: Duration) -> Self {
        self.timeout = value;
        self
    }
    pub fn idle_timeout(mut self, value: Duration) -> Self {
        self.idle_timeout = value;
        self
    }
    pub fn max_response_bytes(mut self, value: usize) -> Self {
        self.max_response = value;
        self
    }
    pub fn max_event_frame_bytes(mut self, value: usize) -> Self {
        self.max_frame = value;
        self
    }
    /// 显式添加宿主信任的企业 CA；仍执行主机名、有效期及证书链检查。
    pub fn root_certificate_pem(mut self, pem: &[u8]) -> Result<Self> {
        self.roots.push(
            reqwest::Certificate::from_pem(pem).map_err(|_| invalid("invalid CA certificate"))?,
        );
        Ok(self)
    }
    pub fn build(self) -> Result<ApiClient> {
        let base =
            reqwest::Url::parse(&self.base_url).map_err(|_| invalid("invalid Serve origin"))?;
        if !matches!(base.scheme(), "http" | "https")
            || base.host_str().is_none()
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
            || base.path() != "/"
        {
            return Err(invalid(
                "base URL must be an HTTP(S) origin without credentials/path/query/fragment",
            ));
        }
        if !matches!(self.family.as_str(), "sdk1" | "sdk2-offload-v1") {
            return Err(invalid("unsupported session family"));
        }
        if self.provider.is_none() && !visible(&self.token, 1, 16384, false) {
            return Err(invalid("a valid token or provider is required"));
        }
        if self.max_response < 1024
            || self.max_frame < 1024
            || self.timeout.is_zero()
            || self.idle_timeout.is_zero()
        {
            return Err(invalid("invalid size/timeout limit"));
        }
        let mut http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(self.timeout)
            .read_timeout(self.idle_timeout);
        for root in self.roots {
            http = http.add_root_certificate(root);
        }
        let http = http
            .build()
            .map_err(|_| invalid("could not create HTTPS transport"))?;
        Ok(ApiClient {
            inner: Arc::new(Inner {
                base: base.as_str().trim_end_matches('/').into(),
                http,
                token: self.token,
                provider: self.provider,
                family: self.family,
                timeout: self.timeout,
                idle_timeout: self.idle_timeout,
                max_response: self.max_response,
                max_frame: self.max_frame,
                closed: CancellationToken::new(),
                identity: Arc::new(()),
            }),
        })
    }
}
struct Inner {
    base: String,
    http: reqwest::Client,
    token: String,
    provider: Option<TokenProvider>,
    family: String,
    timeout: Duration,
    idle_timeout: Duration,
    max_response: usize,
    max_frame: usize,
    closed: CancellationToken,
    identity: Arc<()>,
}
impl Drop for Inner {
    fn drop(&mut self) {
        self.closed.cancel();
    }
}
#[derive(Clone)]
pub struct ApiClient {
    inner: Arc<Inner>,
}
impl ApiClient {
    pub fn builder(base_url: impl Into<String>) -> ClientBuilder {
        ClientBuilder::new(base_url)
    }
    pub fn family(&self) -> &str {
        &self.inner.family
    }
    pub fn base_url(&self) -> &str {
        &self.inner.base
    }
    /// 停止共享实例的网络等待；这不是 Serve interrupt 或业务回滚。
    pub async fn shutdown(&self) {
        self.inner.closed.cancel();
    }

    pub async fn call(&self, operation: &str, options: CallOptions) -> Result<ApiResponse> {
        if options.max_response_bytes == Some(0) {
            return Err(invalid("response limit must be positive"));
        }
        let op = operations::get(operation).ok_or_else(|| invalid("unknown operation"))?;
        if op.kind == "stream" {
            return Err(invalid("use events for stream operations"));
        }
        let work = async {
            let request = self.prepare(op, &options, false).await?;
            let replay = ReplayIdentity {
                client: self.inner.identity.clone(),
                operation: operation.to_string(),
                digest: request_digest(&request),
            };
            let response = self.inner.http.execute(request).await.map_err(transport)?;
            if response.status().is_redirection() {
                return Err(Error::Contract("redirect refused".into()));
            }
            let status = response.status().as_u16();
            let meta = metadata(response.headers())?;
            let maximum = options
                .max_response_bytes
                .unwrap_or(self.inner.max_response);
            if maximum == 0 {
                return Err(invalid("response limit must be positive"));
            }
            let body = read_body(response, maximum).await?;
            if status >= 400 {
                let mut error = decode_error(status, &body, &meta, op)?;
                if let Error::Api(api) = &mut error {
                    api.replay = Some(replay);
                }
                return Err(error);
            }
            if !(200..300).contains(&status) {
                return Err(Error::Contract("unexpected HTTP status".into()));
            }
            if status != 204
                && !matches!(
                    meta.content_type.as_str(),
                    "application/json" | "application/octet-stream"
                )
            {
                return Err(Error::Contract("expected JSON or octet-stream".into()));
            }
            if meta.content_type == "application/octet-stream"
                && operation != "session.checkpoint.export"
            {
                return Err(Error::Contract(
                    "byte response is not declared for this operation".into(),
                ));
            }
            if op.response_schema.is_some()
                && (status == 204 || meta.content_type != "application/json" || body.is_empty())
            {
                return Err(Error::Contract(
                    "schema response requires a JSON body".into(),
                ));
            }
            if meta.content_type == "application/json" && status != 204 {
                let value = canonical::parse_json(&body, maximum)?;
                validate_reference(op.response_schema, &value)?;
                match operation {
                    "discovery.manifest" => {
                        schema::validate("Manifest", &value)?;
                        if value["revision"].as_u64() != Some(meta.manifest_revision)
                            || format!("sha256:{}", value["schemaHash"].as_str().unwrap_or(""))
                                != meta.schema_hash
                            || !value["runtime"].is_object()
                        {
                            return Err(Error::Contract("manifest/header mismatch".into()));
                        }
                    }
                    "discovery.capabilities" => {
                        schema::validate("Capabilities", &value)?;
                        if value["manifestRevision"].as_u64() != Some(meta.manifest_revision)
                            || value["schemaHash"].as_str() != Some(&meta.schema_hash)
                        {
                            return Err(Error::Contract("capabilities/header mismatch".into()));
                        }
                    }
                    "discovery.session.capabilities" => {
                        schema::validate("CapabilityClosure", &value)?;
                        if value["closureId"].as_str() != meta.closure_id.as_deref() {
                            return Err(Error::Contract("closure/header mismatch".into()));
                        }
                    }
                    _ => {}
                }
            }
            Ok(ApiResponse { status, body, meta })
        };
        self.with_cancellation(&options, work).await
    }

    /// 建连成功后返回惰性流，消费方驱动读取；丢弃流即可释放连接。
    pub async fn events(&self, operation: &str, options: CallOptions) -> Result<EventStream> {
        let op = operations::get(operation).ok_or_else(|| invalid("unknown operation"))?;
        if op.kind != "stream" {
            return Err(invalid("not a stream operation"));
        }
        let response = self
            .with_cancellation(&options, async {
                let request = self.prepare(op, &options, true).await?;
                self.inner.http.execute(request).await.map_err(transport)
            })
            .await?;
        if response.status().is_redirection() {
            return Err(Error::Contract("redirect refused".into()));
        }
        let meta = metadata(response.headers())?;
        let status = response.status().as_u16();
        if status >= 400 {
            let body = self
                .with_cancellation(&options, read_body(response, self.inner.max_response))
                .await?;
            return Err(decode_error(status, &body, &meta, op)?);
        }
        if status != 200
            || meta.content_type != "text/event-stream"
            || header(response.headers(), "tansr-event-envelope")? != Some("unified-v1")
        {
            return Err(Error::Contract("unified SSE was not negotiated".into()));
        }
        let mut chunks = response.bytes_stream();
        let mut parser = sse::Parser::new(self.inner.max_frame);
        let closed = self.inner.closed.clone();
        let cancel = options.cancellation.clone();
        let idle = self.inner.idle_timeout;
        let deadline = options.deadline;
        let stream = async_stream::try_stream! {
            loop {
                let remaining = match deadline { Some(at) => at.duration_since(SystemTime::now()).map_err(|_| invalid("deadline exceeded"))?.min(idle), None => idle };
                let chunk = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => Err(Error::Cancelled),
                    _ = closed.cancelled() => Err(Error::Cancelled),
                    value = tokio::time::timeout(remaining, chunks.next()) => value.map_err(|_| Error::Transport("stream read timeout".into())),
                }?;
                match chunk {
                    Some(bytes) => {
                        let bytes = bytes.map_err(transport)?;
                        for frame in parser.feed(bytes.as_ref())? {
                            if let Some(event) = envelope(frame)? { yield event; }
                        }
                    },
                    None => {
                        for frame in parser.finish()? { if let Some(event) = envelope(frame)? { yield event; } }
                        break;
                    },
                }
            }
        };
        Ok(Box::pin(stream))
    }

    /// 仅重放服务端明确许可的原请求；调用者保留原 options，不得延长截止时间。
    pub async fn retry_same_request(
        &self,
        operation: &str,
        options: CallOptions,
        previous: &ApiError,
    ) -> Result<ApiResponse> {
        if previous.retry_action != "same-request"
            || previous.code == "result_unknown"
            || matches!(
                previous.detail["domainCode"].as_str(),
                Some("result_unknown" | "commit_unknown")
            )
        {
            return Err(invalid(
                "retry action does not permit replay; query original status",
            ));
        }
        let replay = previous
            .replay
            .as_ref()
            .ok_or_else(|| invalid("retry requires the original client error"))?;
        if !Arc::ptr_eq(&replay.client, &self.inner.identity) || replay.operation != operation {
            return Err(invalid("retry belongs to a different client or operation"));
        }
        let op = operations::get(operation).ok_or_else(|| invalid("unknown operation"))?;
        let body_id = options
            .body
            .as_ref()
            .and_then(|v| at_path(v, op.request_id_path))
            .and_then(Value::as_str);
        let request_id = options
            .idempotency_key
            .as_deref()
            .or(body_id)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| invalid("replay requires original request identity"))?;
        if previous
            .request_id
            .as_deref()
            .is_some_and(|id| id != request_id)
        {
            return Err(invalid("replay identity differs"));
        }
        let request = self
            .with_cancellation(&options, self.prepare(op, &options, false))
            .await?;
        if request_digest(&request) != replay.digest {
            return Err(invalid(
                "retry body, headers, or deadline differs from original request",
            ));
        }
        let delay = Duration::from_millis(previous.retry_after_ms.unwrap_or(0));
        self.with_cancellation(&options, async {
            tokio::time::sleep(delay).await;
            Ok(())
        })
        .await?;
        self.call(operation, options).await
    }

    async fn with_cancellation<T>(
        &self,
        opts: &CallOptions,
        work: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        let timeout = match opts.deadline {
            Some(at) => at
                .duration_since(SystemTime::now())
                .map_err(|_| invalid("deadline exceeded before sending"))?
                .min(self.inner.timeout),
            None => self.inner.timeout,
        };
        tokio::select! {
            biased;
            _ = opts.cancellation.cancelled() => Err(Error::Cancelled),
            _ = self.inner.closed.cancelled() => Err(Error::Cancelled),
            result = tokio::time::timeout(timeout, work) => result.map_err(|_| Error::Transport("request timeout; outcome must be reconciled".into()))?,
        }
    }

    async fn prepare(
        &self,
        op: &operations::Operation,
        opts: &CallOptions,
        stream: bool,
    ) -> Result<reqwest::Request> {
        let path = instantiate_path(op.path, &opts.params)?;
        let query = build_query(op, &opts.query)?;
        if opts.body.is_some() && opts.raw_body.is_some() {
            return Err(invalid("JSON and byte bodies are mutually exclusive"));
        }
        if opts.raw_body.is_some() && op.name != "session.checkpoint.import" {
            return Err(invalid("byte request is not declared for this operation"));
        }
        if stream && (opts.body.is_some() || opts.raw_body.is_some()) {
            return Err(invalid("stream body is not allowed"));
        }
        let mut headers = HeaderMap::new();
        insert(
            &mut headers,
            "accept",
            if stream {
                "text/event-stream"
            } else {
                "application/json, application/octet-stream"
            },
        )?;
        insert(&mut headers, "tansr-session-family", &self.inner.family)?;
        if stream {
            insert(&mut headers, "tansr-event-envelope", "unified-v1")?;
        }
        if let Some(closure) = &opts.closure_id {
            if !hex64(closure) || op.kind != "write" || !op.path.starts_with("/api/sessions/:id/") {
                return Err(invalid("closure ID not applicable or invalid"));
            }
            insert(&mut headers, "tansr-closure-id", closure)?;
        }
        // 门面先将三头映射到正文再校验。这里只构造等价视图校验，原正文与请求键仍按原样发送。
        let mut effective = opts.body.clone();
        if let Some(key) = &opts.idempotency_key {
            if op.kind != "write" || !visible(key, 1, 128, false) {
                return Err(invalid("invalid/not applicable idempotency key"));
            }
            if !op.request_id_path.is_empty() {
                map_header(
                    &mut effective,
                    op.request_id_path,
                    Value::String(key.clone()),
                )?;
            }
            insert(&mut headers, "idempotency-key", key)?;
        }
        if let Some(value) = &opts.if_match {
            let revision = normalize_revision(value)
                .ok_or_else(|| invalid("invalid strong If-Match revision"))?;
            let expected = op
                .expected_revision
                .as_ref()
                .filter(|_| op.kind == "write")
                .ok_or_else(|| invalid("If-Match not applicable"))?;
            let mapped = if expected.kind == "integer" {
                let number: u64 = revision.parse().map_err(|_| invalid("invalid revision"))?;
                if number > 9_007_199_254_740_991 {
                    return Err(invalid("unsafe revision"));
                }
                Value::from(number)
            } else {
                Value::String(revision.to_string())
            };
            map_header(&mut effective, expected.path, mapped)?;
            insert(&mut headers, "if-match", &format!("\"{revision}\""))?;
        }
        if let Some(deadline) = opts.deadline {
            if deadline <= SystemTime::now() {
                return Err(invalid("deadline exceeded before sending"));
            }
            let text = OffsetDateTime::from(deadline)
                .format(&Rfc3339)
                .map_err(|_| invalid("deadline out of range"))?;
            insert(&mut headers, "deadline", &text)?;
        }
        if let Some(id) = &opts.last_event_id {
            if !stream || !visible(id, 1, 256, true) {
                return Err(invalid("invalid Last-Event-ID"));
            }
            insert(&mut headers, "last-event-id", id)?;
        }
        if let Some(body) = &effective {
            validate_reference(op.request_schema, body)?;
        }
        let mut request = self.inner.http.request(
            Method::from_bytes(op.method.as_bytes())
                .map_err(|_| invalid("invalid generated method"))?,
            format!("{}{path}{query}", self.inner.base),
        );
        if let Some(raw) = &opts.raw_body {
            if raw.len() > self.inner.max_response {
                return Err(invalid("request too large"));
            }
            insert(&mut headers, "content-type", "application/octet-stream")?;
            request = request.body(raw.clone());
        } else if let Some(value) = &opts.body {
            let bytes = if op
                .family
                .is_some_and(|f| f != "agent-session-v1" && f != "unified-v1")
            {
                canonical::encode(value)?
            } else {
                serde_json::to_vec(value)?
            };
            if bytes.len() > self.inner.max_response {
                return Err(invalid("request too large"));
            }
            insert(&mut headers, "content-type", "application/json")?;
            request = request.body(bytes);
        }
        let token = match &self.inner.provider {
            Some(provider) => provider().await?,
            None => self.inner.token.clone(),
        };
        if !visible(&token, 1, 16384, false) {
            return Err(invalid("invalid token"));
        }
        let mut auth = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| invalid("invalid token"))?;
        auth.set_sensitive(true);
        headers.insert("authorization", auth);
        request
            .headers(headers)
            .build()
            .map_err(|_| invalid("request build failed"))
    }
}

fn request_digest(request: &reqwest::Request) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(request.method().as_str());
    digest.update([0]);
    digest.update(request.url().as_str());
    digest.update([0]);
    let ordered: BTreeMap<_, _> = request
        .headers()
        .iter()
        .filter(|(name, _)| name.as_str() != "authorization")
        .map(|(name, value)| (name.as_str(), value.as_bytes()))
        .collect();
    for (name, value) in ordered {
        digest.update(name);
        digest.update([0]);
        digest.update(value);
        digest.update([0]);
    }
    if let Some(bytes) = request.body().and_then(reqwest::Body::as_bytes) {
        digest.update(bytes);
    }
    digest.finalize().into()
}

fn invalid(message: &str) -> Error {
    Error::InvalidInput(message.into())
}
fn transport(error: reqwest::Error) -> Error {
    Error::Transport(
        if error.is_timeout() {
            "timeout"
        } else if error.is_connect() {
            "connection failed"
        } else {
            "HTTP transport failed"
        }
        .into(),
    )
}
fn visible(value: &str, min: usize, max: usize, space: bool) -> bool {
    (min..=max).contains(&value.len())
        && value
            .bytes()
            .all(|b| ((if space { 0x20 } else { 0x21 })..=0x7e).contains(&b))
}
fn hex64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn insert(headers: &mut HeaderMap, name: &'static str, value: &str) -> Result<()> {
    headers.insert(
        name,
        HeaderValue::from_str(value).map_err(|_| invalid("invalid header value"))?,
    );
    Ok(())
}
fn header<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a str>> {
    let mut values = headers.get_all(name).iter();
    let value = values.next();
    if values.next().is_some() {
        return Err(Error::Contract("duplicate control header".into()));
    }
    value
        .map(|v| {
            v.to_str()
                .map_err(|_| Error::Contract("invalid header encoding".into()))
        })
        .transpose()
}
fn metadata(headers: &HeaderMap) -> Result<ResponseMeta> {
    if header(headers, "tansr-contract")? != Some("unified-v1") {
        return Err(Error::Contract(
            "missing or wrong unified contract header".into(),
        ));
    }
    let revision = header(headers, "tansr-manifest-revision")?.unwrap_or("");
    let domain = header(headers, "tansr-domain")?.unwrap_or("");
    let hash = header(headers, "tansr-schema-hash")?.unwrap_or("");
    if revision.is_empty()
        || revision.len() > 10
        || revision.starts_with('0')
        || !revision.bytes().all(|b| b.is_ascii_digit())
        || domain.is_empty()
        || domain.len() > 64
        || !domain.as_bytes()[0].is_ascii_lowercase()
        || !domain.bytes().all(|b| b.is_ascii_lowercase() || b == b'-')
        || (hash != "none" && !hash.strip_prefix("sha256:").is_some_and(hex64))
    {
        return Err(Error::Contract("invalid unified headers".into()));
    }
    let closure = header(headers, "tansr-closure-id")?;
    if closure.is_some_and(|c| !hex64(c)) {
        return Err(Error::Contract("invalid closure header".into()));
    }
    if header(headers, "tansr-event-envelope")?.is_some_and(|v| v != "unified-v1") {
        return Err(Error::Contract("invalid envelope negotiation".into()));
    }
    let content_type = header(headers, "content-type")?
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    let etag = header(headers, "etag")?
        .filter(|v| v.starts_with('"') && v.ends_with('"') && normalize_revision(v).is_some())
        .map(String::from);
    let retry_after_ms = header(headers, "retry-after")?.and_then(|text| {
        if let Ok(seconds) = text.trim().parse::<f64>() {
            if seconds.is_finite() && (0.0..=1e9).contains(&seconds) {
                return Some((seconds * 1000.0).ceil() as u64);
            }
        }
        httpdate::parse_http_date(text).ok().map(|at| {
            at.duration_since(SystemTime::now())
                .unwrap_or_default()
                .as_millis()
                .min(u64::MAX as u128) as u64
        })
    });
    Ok(ResponseMeta {
        manifest_revision: revision
            .parse()
            .map_err(|_| Error::Contract("invalid revision".into()))?,
        schema_hash: hash.into(),
        domain: domain.into(),
        etag,
        closure_id: closure.map(String::from),
        content_type,
        retry_after_ms,
    })
}
async fn read_body(response: reqwest::Response, maximum: usize) -> Result<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|n| n > maximum as u64)
    {
        return Err(Error::Contract("response too large".into()));
    }
    let mut body = Vec::new();
    let mut chunks = response.bytes_stream();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.map_err(transport)?;
        if chunk.len() > maximum.saturating_sub(body.len()) {
            return Err(Error::Contract("response too large".into()));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}
fn decode_error(
    status: u16,
    bytes: &[u8],
    meta: &ResponseMeta,
    op: &operations::Operation,
) -> Result<Error> {
    if meta.content_type != "application/json" {
        return Err(Error::Contract("non-JSON error body".into()));
    }
    let value = canonical::parse_json(bytes, bytes.len().max(1))?;
    if value["contract"] == "unified-v1" {
        schema::validate("UnifiedError", &value)?;
        if value["status"].as_u64() != Some(status.into()) {
            return Err(Error::Contract("error status mismatch".into()));
        }
        let mut error: ApiError = serde_json::from_value(value)?;
        error.status = status;
        if error.retry_after_ms.is_none() {
            error.retry_after_ms = meta.retry_after_ms;
        }
        Ok(Error::Api(Box::new(error)))
    } else if op.family == Some("archive-sync-v1") && value.is_object() {
        Ok(Error::Domain {
            family: "archive-sync-v1".into(),
            status,
            body: value,
        })
    } else {
        Err(Error::Contract("unknown error envelope".into()))
    }
}

fn envelope(frame: sse::Frame) -> Result<Option<EventEnvelope>> {
    if frame.data.is_empty() && frame.id.is_none() && frame.event.is_none() {
        return Ok(None);
    }
    let value = canonical::parse_json(frame.data.as_bytes(), frame.data.len().max(1))?;
    schema::validate("EventEnvelope", &value)?;
    let envelope: EventEnvelope = serde_json::from_value(value)?;
    if frame.id != envelope.event_id
        || envelope.cursor_set["eventCursor"].as_str() != envelope.event_id.as_deref()
    {
        return Err(Error::Contract("SSE frame/envelope cursor mismatch".into()));
    }
    Ok(Some(envelope))
}
fn validate_reference(reference: Option<&str>, value: &Value) -> Result<()> {
    if let Some((family, definition)) = reference.and_then(|r| r.split_once('#')) {
        if family != "agent-session-v1" {
            schema::validate_family(family, definition, value)?;
        }
    }
    Ok(())
}
fn at_path<'a>(mut value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    if path.is_empty() {
        return None;
    }
    for key in path {
        value = value.get(*key)?;
    }
    Some(value)
}
fn map_header(body: &mut Option<Value>, path: &[&str], mapped: Value) -> Result<()> {
    let Some(root) = body else {
        return Ok(());
    };
    let mut parent = root;
    for (index, key) in path.iter().enumerate() {
        let object = parent
            .as_object_mut()
            .ok_or_else(|| invalid("header mapping requires object body"))?;
        if index == path.len() - 1 {
            if object.get(*key).is_some_and(|v| v != &mapped) {
                return Err(invalid("header/body conflict"));
            }
            object.insert((*key).into(), mapped);
            return Ok(());
        }
        parent = object
            .entry((*key).to_string())
            .or_insert_with(|| Value::Object(Default::default()));
    }
    Ok(())
}
fn normalize_revision(value: &str) -> Option<&str> {
    let value = value.trim();
    let value = value
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .unwrap_or(value);
    if value.is_empty()
        || value.len() > 19
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|c| c.is_ascii_digit())
    {
        None
    } else {
        Some(value)
    }
}
fn instantiate_path(template: &str, params: &BTreeMap<String, String>) -> Result<String> {
    let expected: Vec<_> = template
        .split('/')
        .filter_map(|s| s.strip_prefix(':'))
        .collect();
    if params.len() != expected.len() || params.keys().any(|k| !expected.contains(&k.as_str())) {
        return Err(invalid("path parameters mismatch"));
    }
    let mut segments = Vec::new();
    for segment in template.split('/') {
        if let Some(name) = segment.strip_prefix(':') {
            let value = params
                .get(name)
                .ok_or_else(|| invalid("missing path parameter"))?;
            if value.is_empty()
                || value.len() > 512
                || matches!(value.as_str(), "." | "..")
                || value
                    .chars()
                    .any(|c| c.is_control() || c == '/' || c == '\\')
            {
                return Err(invalid("invalid path segment"));
            }
            segments.push(canonical::encode_path_segment(value));
        } else {
            segments.push(segment.to_string());
        }
    }
    Ok(segments.join("/"))
}
fn build_query(op: &operations::Operation, query: &BTreeMap<String, String>) -> Result<String> {
    if query.keys().any(|k| !op.query.contains(&k.as_str())) {
        return Err(invalid("unknown query key"));
    }
    let mut parts = Vec::new();
    for key in op.query {
        let fallback = match (*key, op.family) {
            ("protocol", Some("agent-session-v1" | "sdk2-ext-v1" | "sdk2-archive-recovery-v1")) => {
                Some("sdk2-ext-v1")
            }
            ("protocol", Some("sdk2-cache-v1")) => Some("sdk2-cache-v1"),
            ("protocol", Some("sdk2-cache-core-v1")) => Some("sdk2-cache-core-v1"),
            (
                "contract",
                Some(
                    f
                    @ ("terminal-services-v1" | "terminal-observation-v1" | "terminal-profile-v1"),
                ),
            ) => Some(f),
            _ => None,
        };
        if let Some(value) = query.get(*key).map(String::as_str).or(fallback) {
            if value.len() > 8192 {
                return Err(invalid("query value too large"));
            }
            parts.push(format!(
                "{}={}",
                canonical::encode_path_segment(key),
                canonical::encode_path_segment(value)
            ));
        }
    }
    Ok(if parts.is_empty() {
        String::new()
    } else {
        format!("?{}", parts.join("&"))
    })
}

#[cfg(test)]
mod error_matrix_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn frozen_error_matrix_preserves_types_identity_and_domain_facts() {
        let matrix: Value =
            serde_json::from_str(include_str!("../../contract/api-error-map.json")).unwrap();
        let mut codes = matrix["unifiedCodes"].as_object().unwrap().clone();
        codes.insert(
            "precondition_failed".into(),
            json!({"status":412,"retryAction":"refresh"}),
        );
        codes.insert(
            "not_canonical".into(),
            json!({"status":400,"retryAction":"none"}),
        );
        assert_eq!(codes.len(), 19);
        let meta = ResponseMeta {
            manifest_revision: 7,
            schema_hash: operations::SCHEMA_HASH.into(),
            domain: "agent".into(),
            etag: None,
            closure_id: None,
            content_type: "application/json".into(),
            retry_after_ms: None,
        };
        let op = operations::get("session.list").unwrap();
        let parse = |value: &Value, status: u16| {
            decode_error(status, &serde_json::to_vec(value).unwrap(), &meta, op)
        };
        let body = |code: &str, row: &Value| {
            json!({"contract":"unified-v1","traceId":"observation-only",
            "requestId":"original-business-key","code":code,"status":row["status"],
            "retryAction":row["retryAction"],"message":"synthetic-secret-body"})
        };
        for (code, row) in &codes {
            let value = body(code, row);
            let status = row["status"].as_u64().unwrap() as u16;
            let Error::Api(error) = parse(&value, status).unwrap() else {
                panic!("wrong error class")
            };
            assert_eq!(error.code.as_str(), code);
            assert_eq!(
                error.retry_action.as_str(),
                row["retryAction"].as_str().unwrap()
            );
            assert_eq!(error.request_id.as_deref(), Some("original-business-key"));
            assert_eq!(error.trace_id.as_deref(), Some("observation-only"));
            assert!(error.replay.is_none());
            assert!(!error.to_string().contains("synthetic-secret-body"));
            assert!(parse(&value, 200).is_err());
            for field in [
                "contract",
                "code",
                "retryAction",
                "requestId",
                "traceId",
                "message",
                "status",
            ] {
                let mut malformed = value.clone();
                malformed.as_object_mut().unwrap().remove(field);
                assert!(parse(&malformed, status).is_err(), "missing {field}");
            }
        }
        let actions = matrix["retryActions"].as_array().unwrap();
        assert_eq!(actions.len(), 6);
        for action in actions {
            let mut value = body("conflict", &codes["conflict"]);
            value["retryAction"] = action.clone();
            let Error::Api(error) = parse(&value, 409).unwrap() else {
                panic!("wrong error class")
            };
            assert_eq!(error.retry_action.as_str(), action.as_str().unwrap());
        }
        for family in matrix["families"].as_object().unwrap().values() {
            for (domain_code, code) in family["errorMap"].as_object().unwrap() {
                let code = code.as_str().unwrap();
                let mut value = body(code, &codes[code]);
                value["detail"] = json!({"domainCode":domain_code,"syntheticRetained":true});
                let Error::Api(error) =
                    parse(&value, codes[code]["status"].as_u64().unwrap() as u16).unwrap()
                else {
                    panic!("wrong error class")
                };
                assert_eq!(error.code.as_str(), code);
                assert_eq!(error.detail["domainCode"], *domain_code);
            }
        }
        for (field, value) in [
            ("code", json!("new_guessed_code")),
            ("retryAction", json!("retry")),
            ("retryAfterMs", json!(0)),
        ] {
            let mut malformed = body("conflict", &codes["conflict"]);
            malformed[field] = value;
            assert!(parse(&malformed, 409).is_err());
        }
    }
}
