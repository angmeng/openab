use anyhow::{anyhow, Context, Result};
use axum::{body::Body, extract::State, http::Request, response::Response, routing::any, Router};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use clap::{Parser, Subcommand};
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    sync::RwLock,
};
use tracing::{error, info};

const REFRESH_SKEW_SECONDS: u64 = 120;

// === Provider Config ===

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProviderConfig {
    name: String,
    discovery_url: String,
    client_id: String,
    scopes: String,
    upstream_base_url: String,
    #[serde(default = "default_redirect_port")]
    redirect_port: u16,
    #[serde(default)]
    device_authorization_endpoint: Option<String>,
}

fn default_redirect_port() -> u16 {
    56121
}

impl ProviderConfig {
    fn xai() -> Self {
        Self {
            name: "xAI".to_string(),
            discovery_url: "https://auth.x.ai/.well-known/openid-configuration".to_string(),
            client_id: "b1a00492-073a-47ea-816f-4c329264a828".to_string(),
            scopes: "openid profile email offline_access grok-cli:access api:access".to_string(),
            upstream_base_url: "https://api.x.ai".to_string(),
            redirect_port: 56121,
            device_authorization_endpoint: None,
        }
    }

    fn upstream_host(&self) -> &str {
        self.upstream_base_url
            .strip_prefix("https://")
            .or_else(|| self.upstream_base_url.strip_prefix("http://"))
            .unwrap_or(&self.upstream_base_url)
            .split('/')
            .next()
            .unwrap_or("localhost")
    }
}

#[derive(Debug, Deserialize)]
struct ConfigFile {
    provider: ProviderConfig,
}

fn load_config(path: Option<&PathBuf>) -> Result<ProviderConfig> {
    if let Some(p) = path {
        let content = std::fs::read_to_string(p)
            .with_context(|| format!("Cannot read config file: {}", p.display()))?;
        let cfg: ConfigFile = toml::from_str(&content)?;
        return Ok(cfg.provider);
    }
    // Check default locations
    let candidates = [
        PathBuf::from("auth-proxy.toml"),
        dirs::config_dir()
            .unwrap_or_default()
            .join("openab-auth-proxy/config.toml"),
    ];
    for c in &candidates {
        if c.exists() {
            let content = std::fs::read_to_string(c)?;
            let cfg: ConfigFile = toml::from_str(&content)?;
            return Ok(cfg.provider);
        }
    }
    // Default to xAI
    Ok(ProviderConfig::xai())
}

// === CLI ===

#[derive(Parser)]
#[command(name = "openab-auth-proxy", about = "Generic OAuth proxy sidecar for LLM APIs")]
struct Cli {
    /// Path to config file (default: auth-proxy.toml or xAI preset)
    #[arg(short, long, global = true)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Authenticate via browser OAuth (PKCE)
    Login,
    /// Authenticate via device-code flow (headless/K8s/ECS)
    LoginDevice,
    /// Start the proxy server
    Serve {
        #[arg(short, long, default_value = "9090")]
        port: u16,
        #[arg(long, default_value = "127.0.0.1")]
        bind: String,
    },
}

// === Token Storage ===

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TokenStore {
    access_token: String,
    refresh_token: String,
    #[serde(default)]
    expires_at: u64,
    #[serde(default)]
    token_endpoint: String,
}

fn token_path(provider: &ProviderConfig) -> PathBuf {
    if let Ok(p) = std::env::var("AUTH_PROXY_TOKEN_PATH") {
        let path = PathBuf::from(p);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).ok();
        }
        return path;
    }
    // Legacy env var for backward compat
    if let Ok(p) = std::env::var("XAI_PROXY_TOKEN_PATH") {
        let path = PathBuf::from(p);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).ok();
        }
        return path;
    }
    let dir = dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".openab-auth-proxy")
        .join(provider.name.to_lowercase().replace(' ', "-"));
    std::fs::create_dir_all(&dir).ok();
    dir.join("tokens.json")
}

fn load_tokens(provider: &ProviderConfig) -> Result<TokenStore> {
    let path = token_path(provider);
    let data = std::fs::read_to_string(&path)
        .with_context(|| format!("No token file at {}. Run `openab-auth-proxy login` first.", path.display()))?;
    serde_json::from_str(&data).context("Invalid token file")
}

fn save_tokens(provider: &ProviderConfig, store: &TokenStore) -> Result<()> {
    let path = token_path(provider);
    let data = serde_json::to_string_pretty(store)?;
    std::fs::write(&path, data)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

// === OIDC Discovery ===

#[derive(Deserialize)]
struct OidcDiscovery {
    authorization_endpoint: String,
    token_endpoint: String,
    #[serde(default)]
    device_authorization_endpoint: String,
}

async fn discover_endpoints(provider: &ProviderConfig) -> Result<OidcDiscovery> {
    let client = reqwest::Client::new();
    let resp = client
        .get(&provider.discovery_url)
        .send()
        .await?
        .error_for_status()?;
    resp.json().await.context("Failed to parse OIDC discovery")
}

// === PKCE ===

fn pkce_verifier() -> String {
    let mut buf = [0u8; 64];
    rand::thread_rng().fill_bytes(&mut buf);
    URL_SAFE_NO_PAD.encode(buf)
}

fn pkce_challenge(verifier: &str) -> String {
    let digest = Sha256::digest(verifier.as_bytes());
    URL_SAFE_NO_PAD.encode(digest)
}

// === OAuth Login (browser PKCE) ===

async fn do_login(provider: &ProviderConfig) -> Result<()> {
    info!("Starting {} OAuth PKCE login...", provider.name);
    let discovery = discover_endpoints(provider).await?;

    let code_verifier = pkce_verifier();
    let code_challenge = pkce_challenge(&code_verifier);
    let state = uuid::Uuid::new_v4().to_string();
    let nonce = uuid::Uuid::new_v4().to_string();
    let redirect_uri = format!("http://127.0.0.1:{}/callback", provider.redirect_port);

    let authorize_url = format!(
        "{}?response_type=code&client_id={}&redirect_uri={}&scope={}&code_challenge={}&code_challenge_method=S256&state={}&nonce={}",
        discovery.authorization_endpoint,
        urlencoding::encode(&provider.client_id),
        urlencoding::encode(&redirect_uri),
        urlencoding::encode(&provider.scopes),
        urlencoding::encode(&code_challenge),
        urlencoding::encode(&state),
        urlencoding::encode(&nonce),
    );

    let listener = TcpListener::bind(format!("127.0.0.1:{}", provider.redirect_port))
        .await
        .with_context(|| format!("Failed to bind callback port {}", provider.redirect_port))?;

    println!("\nOpen this URL to authorize:\n\n  {}\n", authorize_url);
    if open::that(&authorize_url).is_ok() {
        println!("Browser opened. Waiting for callback...");
    } else {
        println!("Could not open browser. Please open the URL above manually.");
    }

    let (mut stream, _) = listener.accept().await?;
    let mut reader = BufReader::new(&mut stream);
    let mut request_line = String::new();
    reader.read_line(&mut request_line).await?;

    let path = request_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| anyhow!("Invalid HTTP request"))?;
    let url = url::Url::parse(&format!("http://localhost{}", path))?;
    let params: std::collections::HashMap<_, _> = url.query_pairs().collect();

    loop {
        let mut line = String::new();
        reader.read_line(&mut line).await?;
        if line.trim().is_empty() { break; }
    }

    let body = "<html><body><h1>Authorization received.</h1>You can close this tab.</body></html>";
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(), body
    );
    stream.write_all(response.as_bytes()).await?;

    let received_state = params.get("state").ok_or_else(|| anyhow!("No state in callback"))?;
    if received_state.as_ref() != state {
        return Err(anyhow!("State mismatch — possible CSRF"));
    }
    let code = params
        .get("code")
        .ok_or_else(|| anyhow!("No code in callback. Error: {:?}", params.get("error")))?;

    info!("Exchanging authorization code for tokens...");
    let client = reqwest::Client::new();
    let resp = client
        .post(&discovery.token_endpoint)
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", code.as_ref()),
            ("redirect_uri", redirect_uri.as_str()),
            ("client_id", provider.client_id.as_str()),
            ("code_verifier", code_verifier.as_str()),
            ("code_challenge", code_challenge.as_str()),
            ("code_challenge_method", "S256"),
        ])
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!("Token exchange failed (HTTP {}): {}", status, body));
    }

    let token_resp: serde_json::Value = resp.json().await?;
    let access_token = token_resp["access_token"].as_str().ok_or_else(|| anyhow!("No access_token"))?;
    let refresh_token = token_resp["refresh_token"].as_str().ok_or_else(|| anyhow!("No refresh_token"))?;
    let expires_in = token_resp["expires_in"].as_u64().unwrap_or(3600);
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

    let store = TokenStore {
        access_token: access_token.to_string(),
        refresh_token: refresh_token.to_string(),
        expires_at: now + expires_in,
        token_endpoint: discovery.token_endpoint,
    };
    save_tokens(provider, &store)?;
    println!("\n✅ Login successful! Token saved to {:?}", token_path(provider));
    Ok(())
}

// === Device-Code Login ===

async fn do_login_device(provider: &ProviderConfig) -> Result<()> {
    info!("Starting {} device-code login...", provider.name);
    let discovery = discover_endpoints(provider).await?;

    let device_endpoint = provider
        .device_authorization_endpoint
        .clone()
        .unwrap_or_else(|| {
            if discovery.device_authorization_endpoint.is_empty() {
                // Fallback: derive from discovery URL
                let base = provider.discovery_url.trim_end_matches("/.well-known/openid-configuration");
                format!("{}/oauth2/device/code", base)
            } else {
                discovery.device_authorization_endpoint.clone()
            }
        });

    let client = reqwest::Client::new();
    let resp = client
        .post(&device_endpoint)
        .form(&[("client_id", provider.client_id.as_str()), ("scope", provider.scopes.as_str())])
        .send()
        .await?;

    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!("Device authorization failed: {}", body));
    }

    let device_resp: serde_json::Value = resp.json().await?;
    let device_code = device_resp["device_code"].as_str().ok_or_else(|| anyhow!("No device_code"))?;
    let user_code = device_resp["user_code"].as_str().ok_or_else(|| anyhow!("No user_code"))?;
    let verification_uri = device_resp["verification_uri"]
        .as_str()
        .or_else(|| device_resp["verification_url"].as_str())
        .unwrap_or("(see provider docs)");
    let interval = device_resp["interval"].as_u64().unwrap_or(5);

    println!("\n  Go to:     {}", verification_uri);
    println!("  Enter code: {}\n", user_code);
    println!("Waiting for authorization...");

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(interval)).await;
        let resp = client
            .post(&discovery.token_endpoint)
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                ("client_id", provider.client_id.as_str()),
                ("device_code", device_code),
            ])
            .send()
            .await?;

        let status = resp.status();
        let payload: serde_json::Value = resp.json().await?;

        if status.is_success() {
            let access_token = payload["access_token"].as_str().ok_or_else(|| anyhow!("No access_token"))?;
            let refresh_token = payload["refresh_token"].as_str().ok_or_else(|| anyhow!("No refresh_token"))?;
            let expires_in = payload["expires_in"].as_u64().unwrap_or(3600);
            let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

            let store = TokenStore {
                access_token: access_token.to_string(),
                refresh_token: refresh_token.to_string(),
                expires_at: now + expires_in,
                token_endpoint: discovery.token_endpoint,
            };
            save_tokens(provider, &store)?;
            println!("\n✅ Login successful! Token saved to {:?}", token_path(provider));
            return Ok(());
        }

        match payload["error"].as_str().unwrap_or_default() {
            "authorization_pending" | "slow_down" => continue,
            "expired_token" => return Err(anyhow!("Device code expired. Try again.")),
            "access_denied" => return Err(anyhow!("Authorization denied by user.")),
            e => return Err(anyhow!("Device-code error: {} — {:?}", e, payload)),
        }
    }
}

// === Token Refresh ===

async fn refresh_token(provider: &ProviderConfig, store: &TokenStore) -> Result<TokenStore> {
    let client = reqwest::Client::new();
    let resp = client
        .post(&store.token_endpoint)
        .form(&[
            ("grant_type", "refresh_token"),
            ("refresh_token", &store.refresh_token),
            ("client_id", &provider.client_id),
        ])
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!("Token refresh failed (HTTP {}): {}", status, body));
    }

    let token_resp: serde_json::Value = resp.json().await?;
    let access_token = token_resp["access_token"].as_str().ok_or_else(|| anyhow!("No access_token"))?;
    let new_refresh = token_resp["refresh_token"].as_str().unwrap_or(&store.refresh_token);
    let expires_in = token_resp["expires_in"].as_u64().unwrap_or(3600);
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

    Ok(TokenStore {
        access_token: access_token.to_string(),
        refresh_token: new_refresh.to_string(),
        expires_at: now + expires_in,
        token_endpoint: store.token_endpoint.clone(),
    })
}

// === Proxy ===

struct ProxyState {
    provider: ProviderConfig,
    tokens: RwLock<TokenStore>,
    http_client: Client<hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>, Body>,
}

impl ProxyState {
    async fn get_valid_token(&self) -> Result<String> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        {
            let tokens = self.tokens.read().await;
            if tokens.expires_at > now + REFRESH_SKEW_SECONDS {
                return Ok(tokens.access_token.clone());
            }
        }
        let mut tokens = self.tokens.write().await;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        if tokens.expires_at > now + REFRESH_SKEW_SECONDS {
            return Ok(tokens.access_token.clone());
        }
        info!("Refreshing OAuth token...");
        let new_tokens = refresh_token(&self.provider, &tokens).await?;
        save_tokens(&self.provider, &new_tokens)?;
        *tokens = new_tokens;
        Ok(tokens.access_token.clone())
    }
}

async fn proxy_handler(State(state): State<Arc<ProxyState>>, mut req: Request<Body>) -> Response<Body> {
    let token = match state.get_valid_token().await {
        Ok(t) => t,
        Err(e) => {
            error!("Failed to get token: {}", e);
            return Response::builder()
                .status(502)
                .body(Body::from(format!("Token error: {}", e)))
                .unwrap();
        }
    };

    let path_and_query = req.uri().path_and_query().map(|pq| pq.as_str()).unwrap_or("/");
    let target_uri = format!("{}{}", state.provider.upstream_base_url, path_and_query);
    *req.uri_mut() = target_uri.parse().unwrap();

    req.headers_mut().insert(
        hyper::header::AUTHORIZATION,
        format!("Bearer {}", token).parse().unwrap(),
    );
    req.headers_mut().insert(
        hyper::header::HOST,
        state.provider.upstream_host().parse().unwrap(),
    );

    match state.http_client.request(req).await {
        Ok(resp) => {
            let (parts, body) = resp.into_parts();
            Response::from_parts(parts, Body::new(body))
        }
        Err(e) => {
            error!("Upstream error: {}", e);
            Response::builder()
                .status(502)
                .body(Body::from(format!("Upstream error: {}", e)))
                .unwrap()
        }
    }
}

async fn do_serve(provider: &ProviderConfig, bind: &str, port: u16) -> Result<()> {
    let store = load_tokens(provider)?;

    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let store = if store.expires_at <= now + REFRESH_SKEW_SECONDS {
        info!("Token expired, refreshing...");
        let new_store = refresh_token(provider, &store).await?;
        save_tokens(provider, &new_store)?;
        new_store
    } else {
        store
    };

    let https = hyper_rustls::HttpsConnectorBuilder::new()
        .with_native_roots()?
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();
    let http_client = Client::builder(TokioExecutor::new()).build(https);

    let state = Arc::new(ProxyState {
        provider: provider.clone(),
        tokens: RwLock::new(store),
        http_client,
    });

    let app = Router::new()
        .route("/{*path}", any(proxy_handler))
        .route("/", any(proxy_handler))
        .with_state(state);

    let addr: SocketAddr = format!("{}:{}", bind, port).parse()?;
    let listener = TcpListener::bind(addr).await?;
    info!("openab-auth-proxy ({}) listening on http://{}", provider.name, addr);
    println!("openab-auth-proxy ({}) listening on http://{}", provider.name, addr);
    println!("Upstream: {}", provider.upstream_base_url);

    axum::serve(listener, app).await?;
    Ok(())
}

// === Main ===

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "openab_auth_proxy=info".into()),
        )
        .init();

    let cli = Cli::parse();
    let provider = load_config(cli.config.as_ref())?;
    info!("Provider: {} (upstream: {})", provider.name, provider.upstream_base_url);

    match cli.command {
        Commands::Login => do_login(&provider).await,
        Commands::LoginDevice => do_login_device(&provider).await,
        Commands::Serve { port, bind } => do_serve(&provider, &bind, port).await,
    }
}
