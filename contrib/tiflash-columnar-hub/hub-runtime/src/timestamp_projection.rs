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

use std::sync::Arc;

use kvengine::table::columnar::filter::TimestampProjector;

use crate::interfaces_ffi::TimestampProjection;

/// Owns timezone data used after ffi_make_columnar_reader returns. The callback
/// points into the process-lifetime TiFlash executable, and borrows no Context.
#[derive(Debug)]
struct RowTimestampProjector {
    name: String,
    offset: i64,
    callback: TimestampProjection,
}

impl TimestampProjector for RowTimestampProjector {
    fn project(&self, min: u64, max: u64) -> Option<(u64, u64)> {
        let callback = self.callback?;
        let mut lower = 0;
        let mut upper = 0;
        // SAFETY: name lives through the call and both output pointers are valid.
        // The FFI contract requires a thread-safe, non-unwinding callback.
        let status = unsafe {
            callback(
                self.name.as_ptr() as *const _,
                self.name.len() as u64,
                self.offset,
                min,
                max,
                &mut lower,
                &mut upper,
            )
        };
        (status == 1 && lower <= upper).then_some((lower, upper))
    }
}

pub fn timestamp_projector(
    request: &tipb::DagRequest,
    callback: TimestampProjection,
) -> Option<Arc<dyn TimestampProjector>> {
    callback.map(|callback| {
        Arc::new(RowTimestampProjector {
            name: request.get_time_zone_name().to_owned(),
            offset: request.get_time_zone_offset(),
            callback: Some(callback),
        }) as Arc<dyn TimestampProjector>
    })
}
