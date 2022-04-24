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

#pragma once

#include <Common/FmtUtils.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Interpreters/Context.h>
#include <TestUtils/TiFlashTestException.h>
namespace DB
{
class Context;

namespace tests
{
struct ExecutorSerializerContext
{
    Context & context;
    FmtBuffer & buf;

    explicit ExecutorSerializerContext(Context & context_, FmtBuffer & fmt_buf)
        : context(context_)
        , buf(fmt_buf)
    {}
};

class ExecutorSerializer
{
public:
    ExecutorSerializer(Context & context_, FmtBuffer & fmt_buf)
        : context(ExecutorSerializerContext(context_, fmt_buf))
    {
    }

    String serialize(const tipb::DAGRequest * dag_request);

private:
    void serialize(const tipb::Executor & root_executor, size_t level);
    void addPrefix(size_t level) { context.buf.append(String(level, ' ')); }

private:
    ExecutorSerializerContext context;
};
} // namespace tests

} // namespace DB