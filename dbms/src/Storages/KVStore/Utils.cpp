// Copyright 2024 PingCAP, Inc.
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

#include <Storages/KVStore/Utils.h>

#pragma GCC diagnostic push
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
// include to suppress warnings on NO_THREAD_SAFETY_ANALYSIS. clang can't work without this include, don't know why
#include <grpcpp/security/credentials.h>
#pragma GCC diagnostic pop

namespace DB
{

std::lock_guard<MutexLockWrap::Mutex> MutexLockWrap::genLockGuard() const NO_THREAD_SAFETY_ANALYSIS
{
    return std::lock_guard(*mutex);
}

std::unique_lock<MutexLockWrap::Mutex> MutexLockWrap::tryToLock() const NO_THREAD_SAFETY_ANALYSIS
{
    return std::unique_lock(*mutex, std::try_to_lock);
}

std::unique_lock<MutexLockWrap::Mutex> MutexLockWrap::genUniqueLock() const NO_THREAD_SAFETY_ANALYSIS
{
    return std::unique_lock(*mutex);
}
} // namespace DB
