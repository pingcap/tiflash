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
    ffi::{c_char, c_int, c_void, CString},
    fs::File,
    io::BufReader,
    pin::Pin,
};

use futures::{
    future::BoxFuture,
    task::{Context, Poll},
    Future, FutureExt,
};
use lazy_static::lazy_static;
use pprof::protos::Message;
#[cfg(feature = "external-jemalloc")]
use pprof_util::{parse_jeheap, FlamegraphOptions};
use regex::Regex;
#[cfg(feature = "external-jemalloc")]
use tempfile::NamedTempFile;
use tokio::sync::{Mutex, MutexGuard};

lazy_static! {
    // Only allow a single profiling session at a time.
    static ref PROFILE_MUTEX: Mutex<()> = Mutex::new(());
    static ref THREAD_NAME_RE: Regex =
        Regex::new(r"^(?P<thread_name>[a-z-_ :]+?)(-?\d)*$").unwrap();
    static ref THREAD_NAME_REPLACE_SEPARATOR_RE: Regex = Regex::new(r"[_ ]").unwrap();
}

type OnEndFn<I, T> = Box<dyn FnOnce(I) -> Result<T, String> + Send + 'static>;

#[cfg(feature = "external-jemalloc")]
unsafe extern "C" {
    fn mallctl(
        name: *const c_char,
        oldp: *mut c_void,
        oldlenp: *mut usize,
        newp: *mut c_void,
        newlen: usize,
    ) -> c_int;
}

struct ProfileGuard<'a, I, T> {
    _guard: MutexGuard<'a, ()>,
    item: Option<I>,
    on_end: Option<OnEndFn<I, T>>,
    end: BoxFuture<'static, Result<(), String>>,
}

impl<I, T> Unpin for ProfileGuard<'_, I, T> {}

impl<'a, I, T> ProfileGuard<'a, I, T> {
    fn new<F1, F2>(
        on_start: F1,
        on_end: F2,
        end: BoxFuture<'static, Result<(), String>>,
    ) -> Result<ProfileGuard<'a, I, T>, String>
    where
        F1: FnOnce() -> Result<I, String>,
        F2: FnOnce(I) -> Result<T, String> + Send + 'static,
    {
        let _guard = match PROFILE_MUTEX.try_lock() {
            Ok(guard) => guard,
            Err(_) => return Err("Already in Profiling".to_owned()),
        };
        let item = on_start()?;
        Ok(ProfileGuard {
            _guard,
            item: Some(item),
            on_end: Some(Box::new(on_end) as OnEndFn<I, T>),
            end,
        })
    }
}

impl<I, T> Future for ProfileGuard<'_, I, T> {
    type Output = Result<T, String>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.end.as_mut().poll(cx) {
            Poll::Ready(res) => {
                let item = self.item.take().unwrap();
                let on_end = self.on_end.take().unwrap();
                let result = match (res, on_end(item)) {
                    (Ok(_), r) => r,
                    (Err(err), _) => Err(err),
                };
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

pub async fn start_one_cpu_profile<F>(
    end: F,
    frequency: i32,
    protobuf: bool,
) -> Result<Vec<u8>, String>
where
    F: Future<Output = Result<(), String>> + Send + 'static,
{
    let on_start = || {
        let guard = pprof::ProfilerGuardBuilder::default()
            .frequency(frequency)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .map_err(|e| format!("pprof::ProfilerGuardBuilder::build fail: {}", e))?;
        Ok(guard)
    };

    let on_end = move |guard: pprof::ProfilerGuard<'static>| {
        let report = guard
            .report()
            .frames_post_processor(move |frames| {
                frames.thread_name = extract_thread_name(&frames.thread_name);
            })
            .build()
            .map_err(|e| format!("create cpu profiling report fail: {}", e))?;
        let mut body = Vec::new();
        if protobuf {
            let profile = report
                .pprof()
                .map_err(|e| format!("generate pprof from report fail: {}", e))?;
            profile
                .write_to_vec(&mut body)
                .map_err(|e| format!("encode pprof into bytes fail: {}", e))?;
        } else {
            report
                .flamegraph(&mut body)
                .map_err(|e| format!("generate flamegraph from report fail: {}", e))?;
        }
        Ok(body)
    };

    ProfileGuard::new(on_start, on_end, end.boxed())?.await
}

fn extract_thread_name(thread_name: &str) -> String {
    THREAD_NAME_RE
        .captures(thread_name)
        .and_then(|cap| {
            cap.name("thread_name").map(|thread_name| {
                THREAD_NAME_REPLACE_SEPARATOR_RE
                    .replace_all(thread_name.as_str(), "-")
                    .into_owned()
            })
        })
        .unwrap_or_else(|| thread_name.to_owned())
}

#[cfg(feature = "external-jemalloc")]
fn mallctl_error(command: &str, code: c_int) -> String {
    format!(
        "mallctl({}) failed: {}",
        command,
        std::io::Error::from_raw_os_error(code)
    )
}

#[cfg(feature = "external-jemalloc")]
fn issue_mallctl_args(
    command: &str,
    oldptr: *mut c_void,
    oldsize: *mut usize,
    newptr: *mut c_void,
    newsize: usize,
) -> Result<(), String> {
    let command = CString::new(command)
        .map_err(|err| format!("invalid mallctl command {}: {}", command, err))?;
    let code = unsafe { mallctl(command.as_ptr(), oldptr, oldsize, newptr, newsize) };
    if code == 0 {
        Ok(())
    } else {
        Err(mallctl_error(
            command.to_str().unwrap_or("<invalid command>"),
            code,
        ))
    }
}

#[cfg(feature = "external-jemalloc")]
fn read_mallctl_bool(command: &str) -> Result<bool, String> {
    let mut value = false;
    let mut len = std::mem::size_of::<bool>();
    issue_mallctl_args(
        command,
        (&mut value as *mut bool).cast(),
        &mut len,
        std::ptr::null_mut(),
        0,
    )?;
    if len != std::mem::size_of::<bool>() {
        return Err(format!(
            "mallctl({}) returned unexpected bool size {}",
            command, len
        ));
    }
    Ok(value)
}

#[cfg(feature = "external-jemalloc")]
fn write_mallctl_bool(command: &str, value: bool) -> Result<(), String> {
    let mut value = value;
    issue_mallctl_args(
        command,
        std::ptr::null_mut(),
        std::ptr::null_mut(),
        (&mut value as *mut bool).cast(),
        std::mem::size_of::<bool>(),
    )
}

#[cfg(feature = "external-jemalloc")]
fn ensure_heap_profiling_enabled() -> Result<(), String> {
    if read_mallctl_bool("opt.prof")? {
        Ok(())
    } else {
        Err("host jemalloc heap profiling is disabled (opt.prof=false)".to_owned())
    }
}

#[cfg(feature = "external-jemalloc")]
fn dump_heap_profile_file() -> Result<File, String> {
    ensure_heap_profiling_enabled()?;

    let file = NamedTempFile::new().map_err(|err| format!("create temp file fail: {}", err))?;
    let path = CString::new(file.path().as_os_str().as_encoded_bytes())
        .map_err(|err| format!("invalid temp heap profile path: {}", err))?;
    let mut path_ptr = path.as_ptr();
    issue_mallctl_args(
        "prof.dump",
        std::ptr::null_mut(),
        std::ptr::null_mut(),
        (&mut path_ptr as *mut *const c_char).cast(),
        std::mem::size_of::<*const c_char>(),
    )?;
    File::open(file.path()).map_err(|err| format!("open dumped heap profile fail: {}", err))
}

#[cfg(feature = "external-jemalloc")]
fn read_heap_profile() -> Result<pprof_util::StackProfile, String> {
    let file = dump_heap_profile_file()?;
    parse_jeheap(BufReader::new(file), None)
        .map_err(|err| format!("parse heap profile fail: {}", err))
}

#[cfg(feature = "external-jemalloc")]
pub fn dump_heap_profile_pprof() -> Result<Vec<u8>, String> {
    let profile = read_heap_profile()?;
    Ok(profile.to_pprof(("inuse_space", "bytes"), ("space", "bytes"), None))
}

#[cfg(not(feature = "external-jemalloc"))]
pub fn dump_heap_profile_pprof() -> Result<Vec<u8>, String> {
    Err("heap profiling requires the external-jemalloc feature".to_owned())
}

#[cfg(feature = "external-jemalloc")]
pub fn dump_heap_profile_svg() -> Result<Vec<u8>, String> {
    let profile = read_heap_profile()?;
    let mut options = FlamegraphOptions::default();
    options.title = "inuse_space".to_owned();
    options.count_name = "bytes".to_owned();
    profile
        .to_flamegraph(&mut options)
        .map_err(|err| format!("render heap flamegraph fail: {}", err))
}

#[cfg(not(feature = "external-jemalloc"))]
pub fn dump_heap_profile_svg() -> Result<Vec<u8>, String> {
    Err("heap profiling requires the external-jemalloc feature".to_owned())
}

#[cfg(feature = "external-jemalloc")]
pub fn set_heap_profile_active(active: bool) -> Result<(), String> {
    ensure_heap_profiling_enabled()?;
    write_mallctl_bool("prof.active", active)
}

#[cfg(not(feature = "external-jemalloc"))]
pub fn set_heap_profile_active(_: bool) -> Result<(), String> {
    Err("heap profiling requires the external-jemalloc feature".to_owned())
}
