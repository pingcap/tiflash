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

use std::ffi::{c_void, CString};
use std::os::raw::c_char;
use std::os::unix::ffi::OsStringExt;
use lazy_static::lazy_static;
use findshlibs::{SharedLibrary, TargetSharedLibrary};

struct CachedObject {
    filename: CString,
    avma: usize,
    size: usize,
    bias: usize,
}

pub struct LocationInObject {
    pub name: *const c_char,
    pub svma: usize,
}

impl CachedObject {
    fn contains(&self, avma: usize) -> bool {
        self.avma <= avma && self.avma + self.size > avma
    }

    fn locate(&self, avma: usize) -> Option<LocationInObject> {
        if self.contains(avma) {
            Some(LocationInObject {
                name: self.filename.as_ptr() as _,
                svma: avma - self.bias,
            })
        } else {
            None
        }
    }
}

fn init_object_cache() -> Vec<CachedObject> {
    let mut cache = Vec::new();
    TargetSharedLibrary::each(|obj| {
        let raw_data = obj.name().to_os_string().into_vec();
        cache.push(CachedObject {
            filename: unsafe { CString::from_vec_unchecked(raw_data) },
            avma: obj.actual_load_addr().0,
            size: obj.len(),
            bias: obj.virtual_memory_bias().0,
        });
    });
    cache.sort_by_key(|obj| std::cmp::Reverse(obj.avma));
    cache
}

lazy_static! {
    static ref CACHED_OBJECTS : Vec<CachedObject> = init_object_cache();
}

pub fn locate_symbol_in_object(avma: *const c_void) -> Option<LocationInObject> {
    let address = avma as usize;
    let key = std::cmp::Reverse(address);
    let obj = match CACHED_OBJECTS
        .binary_search_by_key(&key, |obj| std::cmp::Reverse(obj.avma)) {
        Ok(idx) | Err(idx) => {
            CACHED_OBJECTS.get(idx)
        }
    };
    obj.and_then(|x| x.locate(address))
}