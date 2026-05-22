// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    env::args,
    error::Error as StdError,
    net::{SocketAddr, ToSocketAddrs},
    str,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};

use hyper::{
    self, header,
    server::{accept::Accept, conn::AddrIncoming, Builder as HyperBuilder},
    service::{make_service_fn, service_fn},
    Body, Method, Request, Response, Server, StatusCode,
};
use security::SecurityManager;
use serde::Deserialize;
use serde_json::Value;
use tikv_util::{
    logger::{get_level_by_string, set_log_level},
    metrics::dump,
    Either,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    runtime::{Builder, Runtime},
    sync::oneshot::{self, Receiver, Sender},
    time::sleep,
};

use crate::{
    engine_store_helper::{get_engine_store_server_helper, EngineStoreServerHelperExt},
    interfaces_ffi::RaftProxyStatus,
    metrics::STATUS_SERVER_REQUEST_DURATION,
    profile::{
        dump_heap_profile_pprof, dump_heap_profile_svg, set_heap_profile_active,
        start_one_cpu_profile,
    },
};

const SERVER_READ_TIMEOUT: Duration = Duration::from_secs(600);
const SERVER_TCP_KEEPALIVE: Duration = Duration::from_secs(120);
const PROTOBUF_CONTENT_TYPE: &str = "application/protobuf";

#[cfg(feature = "failpoints")]
const FAIL_POINTS_REQUEST_PATH: &str = "/fail";
#[cfg(feature = "failpoints")]
const MISSING_NAME: &[u8] = b"Missing param name";
#[cfg(feature = "failpoints")]
const MISSING_ACTIONS: &[u8] = b"Missing param actions";

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct LogLevelRequest {
    log_level: String,
}

pub struct HubStatusServer {
    thread_pool: Runtime,
    close_tx: Sender<()>,
    close_rx: Option<Receiver<()>>,
    addr: Option<SocketAddr>,
    security_mgr: Arc<SecurityManager>,
    status: Arc<AtomicU8>,
    config_json: Arc<String>,
}

impl HubStatusServer {
    pub fn new(
        security_mgr: Arc<SecurityManager>,
        status: Arc<AtomicU8>,
        config_json: Arc<String>,
    ) -> std::result::Result<Self, Box<dyn StdError + Send + Sync>> {
        let thread_pool = Builder::new_multi_thread()
            .enable_all()
            .worker_threads(1)
            .thread_name("status-server")
            .build()?;
        let (close_tx, close_rx) = oneshot::channel();
        Ok(Self {
            thread_pool,
            close_tx,
            close_rx: Some(close_rx),
            addr: None,
            security_mgr,
            status,
            config_json,
        })
    }

    pub fn start(
        &mut self,
        status_addr: String,
    ) -> std::result::Result<(), Box<dyn StdError + Send + Sync>> {
        let addr = status_addr
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| format!("invalid status addr: {}", status_addr))?;

        let mut incoming = {
            let _enter = self.thread_pool.enter();
            AddrIncoming::bind(&addr)?
        };
        incoming.set_keepalive(Some(SERVER_TCP_KEEPALIVE));
        incoming.set_nodelay(true);
        self.addr = Some(incoming.local_addr());

        match self.security_mgr.acceptor(incoming)? {
            Either::Left(addr_incoming) => {
                let server =
                    Server::builder(addr_incoming).http1_header_read_timeout(SERVER_READ_TIMEOUT);
                self.start_serve(server);
            }
            Either::Right(tls_acceptor) => {
                let server =
                    Server::builder(tls_acceptor).http1_header_read_timeout(SERVER_READ_TIMEOUT);
                self.start_serve(server);
            }
        }

        info!(
            "TiFlash Columnar Hub status server listening";
            "addr" => self.addr.unwrap().to_string()
        );
        Ok(())
    }

    pub fn stop(self) {
        let _ = self.close_tx.send(());
        self.thread_pool.shutdown_timeout(Duration::from_secs(3));
    }

    fn start_serve<I, C>(&mut self, builder: HyperBuilder<I>)
    where
        I: Accept<Conn = C, Error = std::io::Error> + Send + 'static,
        I::Error: Into<Box<dyn StdError + Send + Sync>>,
        I::Conn: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        C: Send + 'static,
    {
        let status = self.status.clone();
        let config_json = self.config_json.clone();
        let close_rx = self.close_rx.take().unwrap();

        let server = builder.serve(make_service_fn(move |_conn: &C| {
            let status = status.clone();
            let config_json = config_json.clone();
            async move {
                Ok::<_, hyper::Error>(service_fn(move |req: Request<Body>| {
                    let status = status.clone();
                    let config_json = config_json.clone();
                    async move { handle_request(req, status, config_json).await }
                }))
            }
        }));

        self.thread_pool.spawn(async move {
            if let Err(err) = server
                .with_graceful_shutdown(async {
                    let _ = close_rx.await;
                })
                .await
            {
                error!(
                    "TiFlash Columnar Hub status server exited with error: {}",
                    err
                );
            }
        });
    }
}

async fn handle_request(
    req: Request<Body>,
    status: Arc<AtomicU8>,
    config_json: Arc<String>,
) -> hyper::Result<Response<Body>> {
    let path = req.uri().path().to_owned();
    let method = req.method().clone();
    let start = std::time::Instant::now();
    let mut path_label = path.clone();

    #[cfg(feature = "failpoints")]
    if path.starts_with(FAIL_POINTS_REQUEST_PATH) {
        path_label = FAIL_POINTS_REQUEST_PATH.to_owned();
        let res = handle_fail_points_request(req).await;
        STATUS_SERVER_REQUEST_DURATION
            .with_label_values(&[method.as_str(), &path_label])
            .observe(start.elapsed().as_secs_f64());
        return res;
    }

    let res = match (method.clone(), path.as_ref()) {
        (Method::GET, "/metrics") => handle_metrics(&req),
        (Method::GET, "/status") => Ok(Response::default()),
        (Method::GET, "/ready") => handle_ready(&req, status),
        (Method::GET, "/config") => handle_config_request(&req, config_json),
        (Method::POST, "/config") => Ok(make_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "online config update is unsupported in TiFlash Columnar Hub",
        )),
        (Method::PUT, "/config/reload") => {
            path_label = "/config/reload".to_owned();
            Ok(make_response(
                StatusCode::METHOD_NOT_ALLOWED,
                "config reload is unsupported in TiFlash Columnar Hub",
            ))
        }
        (Method::GET, "/debug/pprof/heap_list") => {
            path_label = "/debug/pprof/heap_list".to_owned();
            Ok(make_response(
                StatusCode::GONE,
                "Deprecated, periodic heap profile snapshots are unsupported in TiFlash Columnar Hub; use /debug/pprof/heap to dump the current heap profile when needed",
            ))
        }
        (Method::GET, "/debug/pprof/heap_activate") => {
            path_label = "/debug/pprof/heap_activate".to_owned();
            set_heap_profile_active_to_resp(true)
        }
        (Method::GET, "/debug/pprof/heap_deactivate") => {
            path_label = "/debug/pprof/heap_deactivate".to_owned();
            set_heap_profile_active_to_resp(false)
        }
        (Method::GET, "/debug/pprof/heap") => {
            path_label = "/debug/pprof/heap".to_owned();
            dump_heap_prof_to_resp(&req)
        }
        (Method::GET, "/debug/pprof/cmdline") => {
            path_label = "/debug/pprof/cmdline".to_owned();
            Ok(get_cmdline())
        }
        (Method::GET, "/debug/pprof/symbol") => {
            path_label = "/debug/pprof/symbol".to_owned();
            Ok(get_symbol_count())
        }
        (Method::POST, "/debug/pprof/symbol") => {
            path_label = "/debug/pprof/symbol".to_owned();
            get_symbol(req).await
        }
        (Method::GET, "/debug/pprof/profile") => {
            path_label = "/debug/pprof/profile".to_owned();
            dump_cpu_prof_to_resp(req).await
        }
        #[cfg(feature = "failpoints")]
        (Method::GET, "/debug/fail_point") => {
            path_label = "/debug/fail_point".to_owned();
            info!("debug fail point API start");
            fail::fail_point!("debug_fail_point");
            info!("debug fail point API finish");
            Ok(Response::default())
        }
        (Method::PUT, path) if path.starts_with("/log-level") => {
            path_label = "/log-level".to_owned();
            change_log_level(req).await
        }
        _ => {
            let helper = get_engine_store_server_helper();
            if method == Method::GET && helper.check_http_uri_available(path.as_ref()) {
                let (resp, api_prefix) = handle_engine_store_http_request(req, helper).await;
                path_label = if api_prefix.is_empty() {
                    "/engine-store".to_owned()
                } else {
                    api_prefix
                };
                resp
            } else {
                path_label = "unknown".to_owned();
                Ok(make_response(
                    StatusCode::NOT_FOUND,
                    format!("path not found: {} {}", method, path),
                ))
            }
        }
    };

    STATUS_SERVER_REQUEST_DURATION
        .with_label_values(&[method.as_str(), &path_label])
        .observe(start.elapsed().as_secs_f64());
    res
}

fn handle_metrics(req: &Request<Body>) -> hyper::Result<Response<Body>> {
    let simplify = req
        .uri()
        .query()
        .is_some_and(|query| query.contains("simplify=true"));
    Ok(Response::builder()
        .header(header::CONTENT_TYPE, prometheus::TEXT_FORMAT)
        .body(Body::from(dump(simplify)))
        .unwrap())
}

fn handle_ready(req: &Request<Body>, status: Arc<AtomicU8>) -> hyper::Result<Response<Body>> {
    let verbose = req
        .uri()
        .query()
        .is_some_and(|query| query.contains("verbose"));
    let current = status.load(Ordering::SeqCst);
    let ready = current == RaftProxyStatus::Running as u8;
    let status_code = if ready {
        StatusCode::OK
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
    let body = if verbose {
        format!(
            "{{\"ready\":{},\"status\":\"{}\"}}",
            ready,
            proxy_status_name(current)
        )
    } else {
        String::new()
    };
    Ok(make_response(status_code, body))
}

fn handle_config_request(
    req: &Request<Body>,
    config_json: Arc<String>,
) -> hyper::Result<Response<Body>> {
    let full = match parse_bool_query(req, "full", false) {
        Ok(v) => v,
        Err(err) => return Ok(make_response(StatusCode::BAD_REQUEST, err)),
    };

    let hub_config: Value = match serde_json::from_str(config_json.as_ref()) {
        Ok(config) => config,
        Err(err) => {
            return Ok(make_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to parse TiFlash Columnar Hub config JSON: {}", err),
            ));
        }
    };

    let mut response = serde_json::Map::new();
    // Keep the old key for compatibility with existing proxy status tooling.
    response.insert("raftstore-proxy".to_owned(), hub_config);

    let helper = get_engine_store_server_helper();
    if let Some(raw_engine_store_config) = helper.get_config(full) {
        if !raw_engine_store_config.is_empty() {
            let raw_engine_store_config = match str::from_utf8(&raw_engine_store_config) {
                Ok(config) => config,
                Err(err) => {
                    return Ok(make_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("engine-store config is not valid UTF-8: {}", err),
                    ));
                }
            };
            let engine_store_config: toml::Value = match toml::from_str(raw_engine_store_config) {
                Ok(config) => config,
                Err(_) => {
                    return Ok(make_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Internal Server Error: fail to parse config from engine-store",
                    ));
                }
            };
            let engine_store_config = match serde_json::to_value(engine_store_config) {
                Ok(config) => config,
                Err(err) => {
                    return Ok(make_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("failed to encode engine-store config: {}", err),
                    ));
                }
            };
            response.insert("engine-store".to_owned(), engine_store_config);
        }
    }

    match serde_json::to_vec(&Value::Object(response)) {
        Ok(body) => Ok(json_response(body)),
        Err(err) => Ok(make_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to encode config response: {}", err),
        )),
    }
}

fn dump_heap_prof_to_resp(req: &Request<Body>) -> hyper::Result<Response<Body>> {
    let use_svg = match parse_bool_query(req, "svg", false) {
        Ok(v) => v,
        Err(err) => return Ok(make_response(StatusCode::BAD_REQUEST, err)),
    };
    let result = if use_svg {
        dump_heap_profile_svg()
    } else {
        dump_heap_profile_pprof()
    };

    match result {
        Ok(body) => {
            info!("dump or get heap profile successfully");
            let file_name = if use_svg {
                "profile.svg"
            } else {
                "profile.pprof"
            };
            let content_type = if use_svg {
                "image/svg+xml"
            } else {
                "application/octet-stream"
            };
            Ok(Response::builder()
                .header("X-Content-Type-Options", "nosniff")
                .header(
                    "Content-Disposition",
                    format!("attachment; filename=\"{}\"", file_name),
                )
                .header("Content-Length", body.len())
                .header(header::CONTENT_TYPE, content_type)
                .body(Body::from(body))
                .unwrap())
        }
        Err(err) => {
            info!("dump or get heap profile fail: {}", err);
            Ok(make_response(StatusCode::INTERNAL_SERVER_ERROR, err))
        }
    }
}

fn set_heap_profile_active_to_resp(active: bool) -> hyper::Result<Response<Body>> {
    match set_heap_profile_active(active) {
        Ok(()) => {
            let body = if active {
                "heap profiling activated"
            } else {
                "heap profiling deactivated"
            };
            Ok(make_response(StatusCode::OK, body))
        }
        Err(err) => Ok(make_response(StatusCode::INTERNAL_SERVER_ERROR, err)),
    }
}

async fn dump_cpu_prof_to_resp(req: Request<Body>) -> hyper::Result<Response<Body>> {
    let seconds = match parse_u64_query(&req, "seconds", 10) {
        Ok(v) => v,
        Err(err) => return Ok(make_response(StatusCode::BAD_REQUEST, err)),
    };
    let frequency = match parse_i32_query(&req, "frequency", 99) {
        Ok(v) => v,
        Err(err) => return Ok(make_response(StatusCode::BAD_REQUEST, err)),
    };
    let output_protobuf = req
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        == Some(PROTOBUF_CONTENT_TYPE);

    let end = async move {
        sleep(Duration::from_secs(seconds)).await;
        Ok(())
    };
    match start_one_cpu_profile(end, frequency, output_protobuf).await {
        Ok(body) => {
            info!("dump cpu profile successfully");
            let content_type = if output_protobuf {
                "application/octet-stream"
            } else {
                "image/svg+xml"
            };
            Ok(Response::builder()
                .header(
                    "Content-Disposition",
                    "attachment; filename=\"cpu_profile\"",
                )
                .header("Content-Length", body.len())
                .header(header::CONTENT_TYPE, content_type)
                .body(Body::from(body))
                .unwrap())
        }
        Err(err) => {
            info!("dump cpu profile fail: {}", err);
            Ok(make_response(StatusCode::INTERNAL_SERVER_ERROR, err))
        }
    }
}

fn get_cmdline() -> Response<Body> {
    let args = args().fold(String::new(), |mut acc, arg| {
        acc.push_str(&arg);
        acc.push('\0');
        acc
    });
    text_response(args)
}

fn get_symbol_count() -> Response<Body> {
    text_response("num_symbols: 1\n")
}

async fn get_symbol(req: Request<Body>) -> hyper::Result<Response<Body>> {
    let mut text = String::new();
    let body_bytes = hyper::body::to_bytes(req.into_body()).await?;
    let body = match String::from_utf8(body_bytes.to_vec()) {
        Ok(body) => body,
        Err(_) => {
            return Ok(make_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Body is not a valid UTF8",
            ));
        }
    };

    for pc in body.split('+') {
        let addr = usize::from_str_radix(pc.trim_start_matches("0x"), 16).unwrap_or(0);
        if addr == 0 {
            info!("invalid addr: {}", addr);
            continue;
        }

        let mut syms = vec![];
        backtrace::resolve(addr as *mut std::ffi::c_void, |sym| {
            let name = sym
                .name()
                .unwrap_or_else(|| backtrace::SymbolName::new(b"<unknown>"));
            syms.push(name.to_string());
        });

        if !syms.is_empty() {
            text.push_str(format!("{:#x} {}\n", addr, syms.join("--")).as_str());
        } else {
            info!("can't resolve mapped addr: {:#x}", addr);
            text.push_str(format!("{:#x} ??\n", addr).as_str());
        }
    }

    Ok(text_response(text))
}

async fn change_log_level(req: Request<Body>) -> hyper::Result<Response<Body>> {
    let body = hyper::body::to_bytes(req.into_body()).await?;
    let log_level_request: std::result::Result<LogLevelRequest, serde_json::Error> =
        serde_json::from_slice(&body);
    match log_level_request {
        Ok(req) => match get_level_by_string(&req.log_level) {
            Some(level) => {
                set_log_level(level);
                Ok(Response::new(Body::empty()))
            }
            None => Ok(make_response(
                StatusCode::BAD_REQUEST,
                format!("invalid log level: {}", req.log_level),
            )),
        },
        Err(err) => Ok(make_response(StatusCode::BAD_REQUEST, err.to_string())),
    }
}

async fn handle_engine_store_http_request(
    req: Request<Body>,
    engine_store_server_helper: &'static crate::interfaces_ffi::EngineStoreServerHelper,
) -> (hyper::Result<Response<Body>>, String) {
    let (head, body) = req.into_parts();
    match hyper::body::to_bytes(body).await {
        Ok(body) => match engine_store_server_helper.handle_http_request(
            head.uri.path(),
            head.uri.query(),
            &body,
        ) {
            Some(res) => {
                let resp_code = match StatusCode::from_u16(res.status as u16) {
                    Ok(code) => code,
                    Err(_) => {
                        return (
                            Ok(make_response(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "engine-store returned invalid status code",
                            )),
                            String::new(),
                        );
                    }
                };
                let data = res.res.view.to_slice().to_vec();
                let api_prefix = str::from_utf8(res.api_name.view.to_slice())
                    .unwrap_or("")
                    .to_string();
                match Response::builder().status(resp_code).body(Body::from(data)) {
                    Ok(resp) => (Ok(resp), api_prefix),
                    Err(err) => (
                        Ok(make_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("failed to build response: {}", err),
                        )),
                        api_prefix,
                    ),
                }
            }
            None => (
                Ok(make_response(
                    StatusCode::NOT_FOUND,
                    "engine-store HTTP API is unavailable",
                )),
                String::new(),
            ),
        },
        Err(err) => (
            Ok(make_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to read request body: {}", err),
            )),
            String::new(),
        ),
    }
}

#[cfg(feature = "failpoints")]
async fn handle_fail_points_request(req: Request<Body>) -> hyper::Result<Response<Body>> {
    let path = req.uri().path();
    let method = req.method();
    let fail_path = format!("{}/", FAIL_POINTS_REQUEST_PATH);
    let fail_path_has_sub_path = path.starts_with(&fail_path);

    match (method, fail_path_has_sub_path) {
        (&Method::PUT, true) => {
            let (_, name) = path.split_at(fail_path.len());
            if name.is_empty() {
                return Ok(make_response(StatusCode::BAD_REQUEST, MISSING_NAME));
            }

            let body = hyper::body::to_bytes(req.into_body()).await?;
            if body.is_empty() {
                return Ok(make_response(StatusCode::BAD_REQUEST, MISSING_ACTIONS));
            }

            let actions = String::from_utf8_lossy(&body);
            if let Err(err) = fail::cfg(name.to_owned(), &actions) {
                return Ok(make_response(
                    StatusCode::BAD_REQUEST,
                    format!("Failed to add fail point: {}", err),
                ));
            }
            Ok(make_response(
                StatusCode::OK,
                format!("Added fail point with name: {}, actions: {}", name, actions),
            ))
        }
        (&Method::DELETE, true) => {
            let (_, name) = path.split_at(fail_path.len());
            if name.is_empty() {
                return Ok(make_response(StatusCode::BAD_REQUEST, MISSING_NAME));
            }
            fail::remove(name);
            Ok(make_response(
                StatusCode::OK,
                format!("Deleted fail point with name: {}", name),
            ))
        }
        (&Method::GET, false) => {
            if path != FAIL_POINTS_REQUEST_PATH && path != fail_path {
                return Ok(make_response(StatusCode::NOT_FOUND, "Not Found"));
            }
            let list: Vec<String> = fail::list()
                .into_iter()
                .map(|(name, actions)| format!("{}={}", name, actions))
                .collect();
            Ok(text_response(list.join("\n")))
        }
        _ => Ok(make_response(StatusCode::NOT_FOUND, "Not Found")),
    }
}

fn parse_bool_query(req: &Request<Body>, key: &str, default: bool) -> Result<bool, String> {
    match get_query_value(req, key) {
        Some(value) => value
            .parse::<bool>()
            .map_err(|err| format!("invalid query parameter {}: {}", key, err)),
        None => Ok(default),
    }
}

fn parse_u64_query(req: &Request<Body>, key: &str, default: u64) -> Result<u64, String> {
    match get_query_value(req, key) {
        Some(value) => value
            .parse::<u64>()
            .map_err(|err| format!("invalid query parameter {}: {}", key, err)),
        None => Ok(default),
    }
}

fn parse_i32_query(req: &Request<Body>, key: &str, default: i32) -> Result<i32, String> {
    match get_query_value(req, key) {
        Some(value) => value
            .parse::<i32>()
            .map_err(|err| format!("invalid query parameter {}: {}", key, err)),
        None => Ok(default),
    }
}

fn get_query_value(req: &Request<Body>, key: &str) -> Option<String> {
    let query = req.uri().query()?;
    url::form_urlencoded::parse(query.as_bytes())
        .find(|(current_key, _)| current_key == key)
        .map(|(_, value)| value.into_owned())
}

fn json_response(body: Vec<u8>) -> Response<Body> {
    Response::builder()
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap()
}

fn text_response<T>(body: T) -> Response<Body>
where
    T: Into<Body>,
{
    Response::builder()
        .header(header::CONTENT_TYPE, "text/plain")
        .header("X-Content-Type-Options", "nosniff")
        .body(body.into())
        .unwrap()
}

fn make_response<T>(status_code: StatusCode, message: T) -> Response<Body>
where
    T: Into<Body>,
{
    Response::builder()
        .status(status_code)
        .body(message.into())
        .unwrap()
}

fn proxy_status_name(status: u8) -> &'static str {
    match status {
        x if x == RaftProxyStatus::Idle as u8 => "idle",
        x if x == RaftProxyStatus::Running as u8 => "running",
        x if x == RaftProxyStatus::Stopped as u8 => "stopped",
        _ => "unknown",
    }
}
