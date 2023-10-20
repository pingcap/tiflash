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

#include <DataStreams/IProfilingBlockInputStream.h>
#include <TiDB/Decode/TypeMapping.h>
#include <tipb/executor.pb.h>

namespace DB
{
/// Currently, this operator do nothing with the block.
/// TODO: Mock the sender process
class MockExchangeSenderInputStream : public IProfilingBlockInputStream
{
private:
    static constexpr auto NAME = "MockExchangeSender";

public:
    MockExchangeSenderInputStream(const BlockInputStreamPtr & input, const String & req_id);

    String getName() const override { return NAME; }
    Block getHeader() const override;

protected:
    Block readImpl() override;

private:
    const LoggerPtr log;
};

} // namespace DB
