// Copyright 2023 PingCAP, Inc.
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

/// Address Symbolization
/// Refer [this document](https://docs.rs/findshlibs/latest/findshlibs/#addresses) for explanations
/// of what is AVMA, SVMA, BIAS.

mod object;
use std::os::raw::{c_char, c_void};
use object::{locate_symbol_in_object, LocationInObject};

/// We pass raw pointers in `SymbolInfo`.
/// The assumption is that `backtrace` will resolve symbols to image maps (mmap of binary files).
/// These maps have static life-time.
#[repr(C)]
pub struct SymbolInfo {
    /// corresponding function name
    pub symbol_name: *const c_char,
    /// binary file containing the symbol
    pub object_name: *const c_char,
    /// filename of the source.
    pub source_filename: *const c_char,
    /// length of source filename.
    /// this is needed because source filename may not have terminate nul.
    pub source_filename_length: usize,
    /// source line number.
    pub lineno: usize,
    /// symbol offset to the beginning of the binary file.
    pub svma: usize
}

impl Default for SymbolInfo {
    fn default() -> Self {
        SymbolInfo {
            symbol_name: std::ptr::null(),
            object_name: std::ptr::null(),
            source_filename: std::ptr::null(),
            source_filename_length: 0,
            lineno: 0,
            svma: 0,
        }
    }
}

impl SymbolInfo {
    pub fn new(avma: *mut c_void) -> Self {
        let mut info = SymbolInfo::default();
        if let Some(LocationInObject {name, svma}) = locate_symbol_in_object(avma) {
            info.object_name = name;
            info.svma = svma;
        }
        backtrace::resolve(avma, |sym| {
            info.symbol_name = sym
                .name()
                .map(|x|x.as_bytes().as_ptr() as *const _)
                .unwrap_or_else(std::ptr::null);
            if let Some(src) = sym
                .filename()
                .and_then(|x|x.to_str()) {
                info.source_filename = src.as_ptr() as _;
                info.source_filename_length = src.len();
            }
            info.lineno = sym.lineno()
                .map(|x| x as _)
                .unwrap_or(0)
        });
        info

    }
}
/// # Safety
/// `_tiflash_symbolize` is thread-safe. However, one should avoid reentrancy.
///
/// # Note
/// `_tiflash_symbolize` starts with `_`, indicating that this function is designed for internal
/// usage. Make sure you understand the mechanism behind this function before put it into your code!
#[no_mangle]
pub unsafe extern "C" fn _tiflash_symbolize(avma: *mut c_void) -> SymbolInfo {
    SymbolInfo::new(avma)
}