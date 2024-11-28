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

#[repr(C)]
#[derive(Default)]
pub struct ProcessMetricsInfo {
    pub cpu_total: u64,
    pub vsize: u64,
    pub rss: u64,
    pub rss_anon: u64,
    pub rss_file: u64,
    pub rss_shared: u64,
    pub start_time: i64,
}

#[cfg(target_os = "linux")]
#[inline]
pub fn ticks_per_second() -> i64 {
    *CLOCK_TICK
}

#[cfg(target_os = "macos")]
#[inline]
pub fn ticks_per_second() -> i64 {
    const MICRO_SEC_PER_SEC: i64 = 1_000_000;
    MICRO_SEC_PER_SEC
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
#[inline]
pub fn ticks_per_second() -> i64 {
    1
}

#[cfg(not(target_os = "linux"))]
#[no_mangle]
pub extern "C" fn get_process_metrics() -> ProcessMetricsInfo {
    return ProcessMetricsInfo::default();
}

#[cfg(target_os = "linux")]
fn get_proces_metrics_linux() -> procfs::ProcResult<ProcessMetricsInfo> {
    let p = procfs::process::Process::myself()?;
    let stat = p.stat()?;
    let status = p.status()?;
 
    let mut start_time = 0i64;
    if let Ok(boot_time) = procfs::boot_time_secs() {
        start_time = stat.starttime as i64 / ticks_per_second() + boot_time as i64;
    }
    
    Ok(ProcessMetricsInfo {
        cpu_total: (stat.utime + stat.stime) / ticks_per_second() as u64,
        vsize: status.vmsize.unwrap_or_default() * 1024,
        rss: status.vmrss.unwrap_or_default() * 1024,
        rss_anon: status.rssanon.unwrap_or_default() * 1024,
        rss_file: status.rssfile.unwrap_or_default() * 1024,
        rss_shared: status.rssshmem.unwrap_or_default() * 1024,
        start_time,
    })
}

#[cfg(target_os = "linux")]
#[no_mangle]
pub extern "C" fn get_process_metrics() -> ProcessMetricsInfo {
    get_proces_metrics_linux().unwrap_or_default()
}

lazy_static::lazy_static! {
    // getconf CLK_TCK
    static ref CLOCK_TICK: i64 = {
        unsafe {
            libc::sysconf(libc::_SC_CLK_TCK)
        }
    };
}
