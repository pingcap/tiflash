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

/** Allows to build on MacOS X
  *
  * Highly experimental, not recommended, disabled by default.
  *
  * To use, include this file with -include compiler parameter.
  */

#include <common/apple_rt.h>

#if APPLE_SIERRA_OR_NEWER == 0

#include <mach/mach_init.h>
#include <mach/mach_port.h>
#include <mach/thread_act.h>
#include <stdlib.h>
#include <sys/time.h>
#include <time.h>


int clock_gettime_thread(timespec * spec)
{
    thread_port_t thread = mach_thread_self();

    mach_msg_type_number_t count = THREAD_BASIC_INFO_COUNT;
    thread_basic_info_data_t info;
    if (KERN_SUCCESS != thread_info(thread, THREAD_BASIC_INFO, reinterpret_cast<thread_info_t>(&info), &count))
        return -1;

    spec->tv_sec = info.user_time.seconds + info.system_time.seconds;
    spec->tv_nsec = info.user_time.microseconds * 1000 + info.system_time.microseconds * 1000;
    mach_port_deallocate(mach_task_self(), thread);

    return 0;
}

int clock_gettime(int clk_id, struct timespec * t)
{
    if (clk_id == CLOCK_THREAD_CPUTIME_ID)
        return clock_gettime_thread(t);

    struct timeval now;
    int rv = gettimeofday(&now, NULL);

    if (rv)
        return rv;
    t->tv_sec = now.tv_sec;
    t->tv_nsec = now.tv_usec * 1000;

    return 0;
}

#endif
