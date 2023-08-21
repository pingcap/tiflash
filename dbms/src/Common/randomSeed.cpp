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

#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/randomSeed.h>
#include <Core/Types.h>
#include <port/unistd.h>
#include <sys/types.h>
#include <time.h>
#ifdef __APPLE__
#include <common/apple_rt.h>
#endif


namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_CLOCK_GETTIME;
}
} // namespace DB


DB::UInt64 randomSeed()
{
    struct timespec times;
    if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &times))
        DB::throwFromErrno("Cannot clock_gettime.", DB::ErrorCodes::CANNOT_CLOCK_GETTIME);

    /// Not cryptographically secure as time, pid and stack address can be predictable.

    SipHash hash;
    hash.update(times.tv_nsec);
    hash.update(times.tv_sec);
    hash.update(getpid());
    hash.update(&times);
    return hash.get64();
}
