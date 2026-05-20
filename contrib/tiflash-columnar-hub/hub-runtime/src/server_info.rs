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

use std::pin::Pin;

use kvproto::diagnosticspb::{
    ServerInfoItem, ServerInfoPair, ServerInfoRequest, ServerInfoResponse,
};
use protobuf::Message;
use tikv_util::sys::SysQuota;

use crate::{
    engine_store_helper::{get_engine_store_server_helper, EngineStoreServerHelperExt},
    interfaces_ffi::{BaseBuffView, MsgPBType, RaftStoreProxyPtr, RawVoidPtr},
    ProtoMsgBaseBuff,
};

fn pair(key: &str, value: impl ToString) -> ServerInfoPair {
    let mut pair = ServerInfoPair::default();
    pair.set_key(key.to_string());
    pair.set_value(value.to_string());
    pair
}

fn build_server_info_response(_req: &ServerInfoRequest) -> ServerInfoResponse {
    let logical = num_cpus::get();
    let physical = num_cpus::get_physical().max(1);
    let mut cpu = ServerInfoItem::default();
    cpu.set_tp("cpu".to_string());
    cpu.set_name("cpu".to_string());
    cpu.set_pairs(
        vec![
            pair("cpu-logical-cores", logical),
            pair("cpu-physical-cores", physical),
            pair("cpu-arch", std::env::consts::ARCH),
        ]
        .into(),
    );

    let mut memory = ServerInfoItem::default();
    memory.set_tp("memory".to_string());
    memory.set_name("virtual".to_string());
    memory.set_pairs(vec![pair("capacity", SysQuota::memory_limit_in_bytes())].into());

    let mut response = ServerInfoResponse::default();
    response.set_items(vec![cpu, memory].into());
    response
}

pub extern "C" fn ffi_server_info(
    proxy_ptr: RaftStoreProxyPtr,
    view: BaseBuffView,
    res: RawVoidPtr,
) -> u32 {
    assert!(!proxy_ptr.is_null());
    let mut req = ServerInfoRequest::default();
    req.merge_from_bytes(view.to_slice()).unwrap();
    let response = build_server_info_response(&req);
    let buff = ProtoMsgBaseBuff::new(&response);
    get_engine_store_server_helper().set_pb_msg_by_bytes(
        MsgPBType::ServerInfoResponse,
        res,
        Pin::new(&buff).into(),
    );
    0
}

#[no_mangle]
pub extern "C" fn ffi_get_server_info_from_proxy(
    server_helper_ptr: isize,
    view: BaseBuffView,
    res: RawVoidPtr,
) -> u32 {
    assert_ne!(server_helper_ptr, 0);
    let mut req = ServerInfoRequest::default();
    req.merge_from_bytes(view.to_slice()).unwrap();
    let response = build_server_info_response(&req);
    let buff = ProtoMsgBaseBuff::new(&response);
    let server_helper =
        unsafe { &*(server_helper_ptr as *const crate::interfaces_ffi::EngineStoreServerHelper) };
    server_helper.set_pb_msg_by_bytes(MsgPBType::ServerInfoResponse, res, Pin::new(&buff).into());
    0
}
