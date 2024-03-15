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

// #pragma once

// #include <tuple>
// #ifdef WITH_JEMALLOC
// #include <jemalloc/jemalloc.h>
// #endif

// namespace DB
// {
// std::tuple<uint64_t *, uint64_t *> getAllocDeallocPtr()
// {
// #ifdef WITH_JEMALLOC
//     uint64_t * ptr1 = nullptr;
//     uint64_t size1 = sizeof ptr1;
//     mallctl("thread.allocatedp", (void *)&ptr1, &size1, NULL, 0);
//     uint64_t * ptr2 = nullptr;
//     uint64_t size2 = sizeof ptr2;
//     mallctl("thread.deallocatedp", (void *)&ptr2, &size2, NULL, 0);
//     return std::make_tuple(ptr1, ptr2);
// #else
//     return std::make_tuple(nullptr, nullptr);
// #endif
// }
} // namespace DB