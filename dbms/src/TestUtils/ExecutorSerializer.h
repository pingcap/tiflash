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

#pragma once

#include <Common/FmtUtils.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <TestUtils/TiFlashTestException.h>
#include <common/types.h>

namespace DB
{
namespace tests
{
class ExecutorSerializer
{
public:
    String serialize(const tipb::DAGRequest * dag_request);

private:
    void serializeListStruct(const tipb::DAGRequest * dag_request);
    void serializeTreeStruct(const tipb::Executor & root_executor, size_t level);
    void addPrefix(size_t level) { buf.append(String(level, ' ')); }

private:
    FmtBuffer buf;
};
} // namespace tests

} // namespace DB