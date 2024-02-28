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

#include <Storages/KVStore/Utils/NotNull.h>
#include <common/defines.h>
#include <gtest/gtest.h>

namespace DB::tests
{

void mustNotNull(NotNullRaw<int*> nn) {
    UNUSED(nn);
}

void mustNotNullPtr(NotNullShared<int*> nnp) {
    UNUSED(nnp);
}

void mustNotNullUPtr(NotNullUnique<int*> nnp) {
    UNUSED(nnp);
}

// Use volatile to prevent optimization.
static volatile int MAYBE_NOT_ZERO = 0;

template <typename T>
T* getNullPtr() {
    return reinterpret_cast<T*>(MAYBE_NOT_ZERO);
}

TEST(NotNullTest, Raw)
{
    auto p1 = newNotNullRaw(new int(1));
    UNUSED(p1);
    // p1 = nullptr;
    // auto p2 = newNotNullRaw(nullptr);
    auto p3 = newNotNullRaw(getNullPtr<int>());
    UNUSED(p3);
    // mustNotNull(p1);
    // mustNotNull(newNotNullRaw(nullptr));
}

}