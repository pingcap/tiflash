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

use lazy_static::lazy_static;
use prometheus::*;

lazy_static! {
    pub static ref COLUMNAR_PREFETCH_HISTOGRAM: Histogram = register_histogram!(
        "tikv_proxy_columnar_prefetch_duration_seconds",
        "Bucketed histogram of columnar prefetch segments duration",
        exponential_buckets(0.1, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref COLUMNAR_PREFETCH_CACHE_HIT_HISTOGRAM: Histogram = register_histogram!(
        "tikv_proxy_columnar_prefetch_cache_hit",
        "Bucketed histogram of columnar prefetch cache hit",
        linear_buckets(50.0, 2.0, 25).unwrap()
    )
    .unwrap();
    pub static ref COLUMNAR_FETCH_SNAPSHOT_HISTOGRAM: Histogram = register_histogram!(
        "tikv_proxy_columnar_fetch_snapshot_duration_seconds",
        "Bucketed histogram of columnar fetch snapshot duration",
        exponential_buckets(0.1, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref COLUMNAR_FETCH_SNAPSHOT_RETRY_COUNTER: Counter = register_counter!(
        "tikv_proxy_columnar_fetch_snapshot_retry_count",
        "Count of columnar fetch snapshot retry",
    )
    .unwrap();
    pub static ref STATUS_SERVER_REQUEST_DURATION: HistogramVec = register_histogram_vec!(
        "tikv_status_server_proxy_request_duration_seconds",
        "Bucketed histogram of TiKV status server request duration",
        &["method", "path"],
        exponential_buckets(0.0001, 2.0, 24).unwrap()
    )
    .unwrap();
}
