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

#if defined(__APPLE__)
#include <pthread.h>
#elif defined(__FreeBSD__)
#include <pthread.h>
#include <pthread_np.h>
#else
#include <sys/prctl.h>
#endif
#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <IO/WriteHelpers.h>

#include <cstring>
#include <iostream>

namespace DB
{
namespace ErrorCodes
{
extern const int PTHREAD_ERROR;
}
} // namespace DB

void setThreadName(const char * tname)
{
    constexpr auto max_len = 15; // thread name will be tname[:MAX_LEN]
    // TODO: Replace strlen for safety
    if (std::strlen(tname) > max_len)
        std::cerr << "set thread name " << tname << " is too long and will be truncated by system\n";

#if defined(__FreeBSD__)
    pthread_set_name_np(pthread_self(), tname);
    return;

#elif defined(__APPLE__)
    if (0 != pthread_setname_np(tname))
#else
    if (0 != prctl(PR_SET_NAME, tname, 0, 0, 0))
#endif
    DB::throwFromErrno("Cannot set thread name " + std::string(tname), DB::ErrorCodes::PTHREAD_ERROR);
}

std::string getThreadName()
{
    constexpr auto max_len = 15;
    std::string name(max_len + 1, '\0'); // '\0' terminated

#if defined(__APPLE__)
    if (pthread_getname_np(pthread_self(), name.data(), name.size()))
        throw DB::Exception("Cannot get thread name with pthread_getname_np()", DB::ErrorCodes::PTHREAD_ERROR);
#elif defined(__FreeBSD__)
// TODO: make test. freebsd will have this function soon https://freshbsd.org/commit/freebsd/r337983
//    if (pthread_get_name_np(pthread_self(), name.data(), name.size()))
//        throw DB::Exception("Cannot get thread name with pthread_get_name_np()", DB::ErrorCodes::PTHREAD_ERROR);
#else
    if (0 != prctl(PR_GET_NAME, name.data(), 0, 0, 0))
        DB::throwFromErrno("Cannot get thread name with prctl(PR_GET_NAME)", DB::ErrorCodes::PTHREAD_ERROR);
#endif

    name.resize(std::strlen(name.data()));
    return name;
}

std::string getThreadNameAndID()
{
    uint64_t thread_id;
#if defined(__APPLE__)
    int err = pthread_threadid_np(pthread_self(), &thread_id);
    if (err)
        DB::throwFromErrno("Cannot get thread id with pthread_threadid_np()", DB::ErrorCodes::PTHREAD_ERROR, err);
#elif defined(__FreeBSD__)
    thread_id = pthread_getthreadid_np();
#else
    thread_id = pthread_self();
#endif
    return getThreadName() + "_" + DB::toString(thread_id);
}
