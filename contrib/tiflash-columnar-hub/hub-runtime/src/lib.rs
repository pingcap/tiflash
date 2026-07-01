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

macro_rules! error {
    ($($args:tt)+) => {{
        ::slog::error!(&**::slog_global::borrow_global(), $($args)+)
    }};
}

macro_rules! warn {
    ($($args:tt)+) => {{
        ::slog::warn!(&**::slog_global::borrow_global(), $($args)+)
    }};
}

macro_rules! info {
    ($($args:tt)+) => {{
        ::slog::info!(&**::slog_global::borrow_global(), $($args)+)
    }};
}

mod basic_ffi_impls;
mod cloud_helper;
mod columnar_impls;
mod domain_impls;
mod engine_store_helper;
mod hub;
mod interfaces;
mod metrics;
mod profile;
mod run;
mod server_info;
mod status_server;

pub use self::{
    basic_ffi_impls::*, cloud_helper::*, domain_impls::*, hub::*,
    interfaces::root::DB as interfaces_ffi,
};

use std::os::raw::{c_char, c_int};

pub(crate) fn proxy_version_info() -> String {
    let fallback = "Unknown";
    format!(
        "Git Commit Hash:   {}\nRust Version:      {}\nPrometheus Prefix: {}\nProfile:           {}",
        option_env!("CLOUD_STORAGE_ENGINE_GIT_HASH").unwrap_or(fallback),
        option_env!("PROXY_BUILD_RUSTC_VERSION").unwrap_or(fallback),
        option_env!("PROMETHEUS_METRIC_NAME_PREFIX").unwrap_or(fallback),
        option_env!("PROXY_PROFILE").unwrap_or(fallback),
    )
}

#[no_mangle]
pub unsafe extern "C" fn print_raftstore_proxy_version() {
    println!("{}", proxy_version_info());
}

#[no_mangle]
pub unsafe extern "C" fn run_raftstore_proxy_ffi(
    argc: c_int,
    argv: *const *const c_char,
    helper: *const u8,
) {
    run::run_proxy(argc, argv, helper);
}
