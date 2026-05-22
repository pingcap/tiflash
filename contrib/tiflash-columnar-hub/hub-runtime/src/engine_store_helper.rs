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

use std::sync::atomic::{AtomicPtr, Ordering};

use crate::{
    interfaces_ffi::{
        BaseBuffView, EngineStoreServerHelper, EngineStoreServerStatus, HttpRequestRes, MsgPBType,
        RaftStoreProxyFFIHelper, RawCppStringPtr, RawVoidPtr, RAFT_STORE_PROXY_MAGIC_NUMBER,
        RAFT_STORE_PROXY_VERSION,
    },
    UnwrapExternCFunc,
};

static ENGINE_STORE_SERVER_HELPER_PTR: AtomicPtr<EngineStoreServerHelper> =
    AtomicPtr::new(std::ptr::null_mut());

pub fn init_engine_store_server_helper(ptr: *const u8) {
    ENGINE_STORE_SERVER_HELPER_PTR.store(ptr as *mut EngineStoreServerHelper, Ordering::Release);
}

pub fn get_engine_store_server_helper() -> &'static EngineStoreServerHelper {
    let ptr = ENGINE_STORE_SERVER_HELPER_PTR.load(Ordering::Acquire);
    assert!(!ptr.is_null());
    unsafe { &*(ptr as *const EngineStoreServerHelper) }
}

unsafe impl Sync for EngineStoreServerHelper {}

pub trait EngineStoreServerHelperExt {
    fn check(&self);
    fn set_proxy(&self, proxy: &mut RaftStoreProxyFFIHelper);
    fn handle_get_engine_store_server_status(&self) -> EngineStoreServerStatus;
    fn handle_http_request(
        &self,
        path: &str,
        query: Option<&str>,
        body: &[u8],
    ) -> Option<HttpRequestRes>;
    fn check_http_uri_available(&self, path: &str) -> bool;
    fn get_config(&self, full: bool) -> Option<Vec<u8>>;
    fn gen_cpp_string(&self, data: &[u8]) -> RawCppStringPtr;
    fn set_pb_msg_by_bytes(&self, type_: MsgPBType, ptr: RawVoidPtr, buff: BaseBuffView);
}

impl EngineStoreServerHelperExt for EngineStoreServerHelper {
    fn check(&self) {
        assert_eq!(self.magic_number, RAFT_STORE_PROXY_MAGIC_NUMBER);
        assert_eq!(self.version, RAFT_STORE_PROXY_VERSION);
    }

    fn set_proxy(&self, proxy: &mut RaftStoreProxyFFIHelper) {
        unsafe {
            self.fn_atomic_update_proxy.into_inner()(self.inner, proxy as *mut _);
        }
    }

    fn handle_get_engine_store_server_status(&self) -> EngineStoreServerStatus {
        unsafe { self.fn_handle_get_engine_store_server_status.into_inner()(self.inner) }
    }

    fn handle_http_request(
        &self,
        path: &str,
        query: Option<&str>,
        body: &[u8],
    ) -> Option<HttpRequestRes> {
        let query = query.map_or(
            BaseBuffView {
                data: std::ptr::null(),
                len: 0,
            },
            |s| s.as_bytes().into(),
        );
        self.fn_handle_http_request
            .map(|func| unsafe { func(self.inner, path.as_bytes().into(), query, body.into()) })
    }

    fn check_http_uri_available(&self, path: &str) -> bool {
        self.fn_check_http_uri_available
            .map(|func| unsafe { func(path.as_bytes().into()) != 0 })
            .unwrap_or(false)
    }

    fn get_config(&self, full: bool) -> Option<Vec<u8>> {
        self.fn_get_config.map(|func| unsafe {
            let config = func(self.inner, full.into());
            config.view.to_slice().to_vec()
        })
    }

    fn gen_cpp_string(&self, data: &[u8]) -> RawCppStringPtr {
        unsafe { self.fn_gen_cpp_string.into_inner()(data.into()).ptr.cast() }
    }

    fn set_pb_msg_by_bytes(&self, type_: MsgPBType, ptr: RawVoidPtr, buff: BaseBuffView) {
        unsafe {
            self.fn_set_pb_msg_by_bytes.into_inner()(type_, ptr, buff);
        }
    }
}
