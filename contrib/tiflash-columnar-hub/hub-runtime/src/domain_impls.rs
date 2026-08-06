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

use std::{pin::Pin, sync::Mutex};

use kvengine::CloudColumnarReaders;

use crate::interfaces_ffi::{
    BaseBuffView, RawRustPtr, RawVoidPtr, RustStrWithView, RustStrWithViewVec,
};

#[repr(u32)]
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub enum RawRustPtrType {
    None = 0,
    ReadIndexTask = 1,
    ArcFutureWaker = 2,
    TimerTask = 3,
    String = 4,
    VecOfString = 5,
    ColumnarReader = 6,
}

impl From<u32> for RawRustPtrType {
    fn from(x: u32) -> Self {
        unsafe { std::mem::transmute(x) }
    }
}

impl From<RawRustPtrType> for u32 {
    fn from(value: RawRustPtrType) -> Self {
        value as u32
    }
}

pub extern "C" fn ffi_gc_rust_ptr(data: RawVoidPtr, type_: crate::interfaces_ffi::RawRustPtrType) {
    if data.is_null() {
        return;
    }
    match RawRustPtrType::from(type_) {
        RawRustPtrType::String => unsafe {
            drop(Box::from_raw(data as *mut RustStrWithViewInner));
        },
        RawRustPtrType::VecOfString => unsafe {
            drop(Box::from_raw(data as *mut RustStrWithViewVecInner));
        },
        RawRustPtrType::ColumnarReader => unsafe {
            drop(Box::from_raw(data as *mut CloudColumnarReaders));
        },
        RawRustPtrType::None => {}
        other => panic!("unexpected rust ptr type {other:?}"),
    }
}

impl Default for RawRustPtr {
    fn default() -> Self {
        Self {
            ptr: std::ptr::null_mut(),
            type_: RawRustPtrType::None.into(),
        }
    }
}

impl Default for RustStrWithView {
    fn default() -> Self {
        Self {
            buff: BaseBuffView::default(),
            inner: RawRustPtr::default(),
        }
    }
}

impl Default for RustStrWithViewVec {
    fn default() -> Self {
        Self {
            buffs: std::ptr::null(),
            len: 0,
            inner: RawRustPtr::default(),
        }
    }
}

struct RustStrWithViewInner {
    #[allow(clippy::box_collection)]
    _data: Pin<Box<Vec<u8>>>,
}

pub fn build_from_string(data: Vec<u8>) -> RustStrWithView {
    let data = Box::pin(data);
    let buff = BaseBuffView {
        data: data.as_ptr() as *const _,
        len: data.len() as u64,
    };
    let inner = Box::new(RustStrWithViewInner { _data: data });
    let raw = RawRustPtr {
        ptr: inner.as_ref() as *const _ as RawVoidPtr,
        type_: RawRustPtrType::String.into(),
    };
    std::mem::forget(inner);
    RustStrWithView { buff, inner: raw }
}

struct RustStrWithViewVecInner {
    #[allow(clippy::box_collection)]
    _data: Pin<Box<Vec<Vec<u8>>>>,
    #[allow(clippy::box_collection)]
    buff_views: Pin<Box<Vec<BaseBuffView>>>,
}

pub fn build_from_vec_string(data: Vec<Vec<u8>>) -> RustStrWithViewVec {
    let len = data.len() as u64;
    let data = Box::pin(data);
    let views = data
        .iter()
        .map(|item| BaseBuffView {
            data: item.as_ptr() as *const _,
            len: item.len() as u64,
        })
        .collect::<Vec<_>>();
    let inner = Box::new(RustStrWithViewVecInner {
        _data: data,
        buff_views: Box::pin(views),
    });
    let raw = RawRustPtr {
        ptr: inner.as_ref() as *const _ as RawVoidPtr,
        type_: RawRustPtrType::VecOfString.into(),
    };
    let buffs = inner.buff_views.as_ptr();
    std::mem::forget(inner);
    RustStrWithViewVec {
        buffs,
        len,
        inner: raw,
    }
}

#[derive(Default)]
pub struct TestGcObjectMonitor {
    _rust: Mutex<std::collections::HashMap<crate::interfaces_ffi::RawRustPtrType, isize>>,
}
