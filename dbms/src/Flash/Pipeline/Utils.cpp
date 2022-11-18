// Copyright 2022 PingCAP, Ltd.
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

#include <Flash/Pipeline/Utils.h>

#include <thread>

namespace DB
{
void doCpuPart(size_t & count, size_t loop)
{
    count = 0;
    for (size_t i = 0; i < loop; ++i)
        count += (i + random() % 100);
}

void doIOPart()
{
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
}
} // namespace DB
