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

use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};

use crate::{
    cloud_helper::CloudHelper,
    interfaces_ffi::{ConstRawVoidPtr, RaftProxyStatus, RaftStoreProxyPtr},
};

/// Thin runtime host for the columnar-only hub component.
/// It only keeps status, config summary, and the kvengine-backed cloud helper.
pub struct ColumnarHub {
    status: Arc<AtomicU8>,
    pub cloud_helper: CloudHelper,
    hub_config_str: String,
}

impl ColumnarHub {
    pub fn new(status: Arc<AtomicU8>, cloud_helper: CloudHelper, hub_config_str: String) -> Self {
        Self {
            status,
            cloud_helper,
            hub_config_str,
        }
    }

    pub fn get_config_str(&self) -> &String {
        &self.hub_config_str
    }

    pub fn status(&self) -> RaftProxyStatus {
        unsafe { std::mem::transmute(self.status.load(Ordering::SeqCst)) }
    }

    pub fn set_status(&self, status: RaftProxyStatus) {
        self.status.store(status as u8, Ordering::SeqCst);
    }

    pub fn status_handle(&self) -> Arc<AtomicU8> {
        self.status.clone()
    }
}

impl RaftStoreProxyPtr {
    pub unsafe fn as_ref(&self) -> &ColumnarHub {
        &*(self.inner as *const ColumnarHub)
    }

    pub fn is_null(&self) -> bool {
        self.inner.is_null()
    }
}

impl From<&ColumnarHub> for RaftStoreProxyPtr {
    fn from(value: &ColumnarHub) -> Self {
        Self {
            inner: value as *const _ as ConstRawVoidPtr,
        }
    }
}
