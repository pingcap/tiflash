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
pub struct ProcessMetricsInfo {
    pub cpu_total: u64,
    pub vsize: i64,
    pub rss: u64,
    pub rss_anon: u64,
    pub rss_file: u64,
    pub rss_shared: u64,
    pub start_time: i64,
}

impl Default for ProcessMetricsInfo {
    fn default() -> Self {
        ProcessMetricsInfo {
            cpu_total: 0,
            vsize: 0,
            rss: 0,
            rss_anon: 0,
            rss_file: 0,
            rss_shared: 0,
            start_time: 0,
        }
    }
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
#[no_mangle]
pub extern "C" fn get_process_metrics() -> ProcessMetricsInfo {
    use procfs;
    let p = match procfs::process::Process::myself() {
        Ok(p) => p,
        Err(..) => {
            // we can't construct a Process object, so there's no stats to gather
            return ProcessMetricsInfo::default();
        }
    };

    let mut start_time: i64 = 0;
    if let Ok(boot_time) = procfs::boot_time_secs() {
        start_time = p.stat.starttime as i64 / ticks_per_second() + boot_time as i64;
    }
    ProcessMetricsInfo {
        cpu_total: (p.stat.utime + p.stat.stime) / ticks_per_second() as u64,
        vsize: (p.status.vm_size * 1024) as u64,
        rss: (p.status.vm_rss * 1024) as u64,
        rss_anon: (p.status.vm_rss_anon * 1024) as u64,
        rss_file: (p.status.vm_rss_file * 1024) as u64,
        rss_shared: (p.status.vm_rss_shared * 1024) as u64,
        start_time,
    }
}

lazy_static::lazy_static! {
    // getconf CLK_TCK
    static ref CLOCK_TICK: i64 = {
        unsafe {
            libc::sysconf(libc::_SC_CLK_TCK)
        }
    };
}
