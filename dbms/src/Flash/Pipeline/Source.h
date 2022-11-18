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

#include <common/types.h>
#include <Common/DynamicThreadPool.h>
#include <Core/Block.h>
#include <Flash/Pipeline/PStatus.h>
#include <TestUtils/ColumnGenerator.h>
#include <DataTypes/DataTypesNumber.h>

#include <memory>

namespace DB
{
namespace
{
Block prepareRandomBlock(size_t rows)
{
    Block block;
    for (size_t i = 0; i < 10; ++i)
    {
        DataTypePtr int64_data_type = std::make_shared<DataTypeInt64>();
        auto int64_column = tests::ColumnGenerator::instance().generate({rows, "Int64", tests::RANDOM}).column;
        block.insert(ColumnWithTypeAndName{
            std::move(int64_column),
            int64_data_type,
            String("col") + std::to_string(i)});
    }
    return block;
}
}

class Source
{
public:
    virtual ~Source() = default;

    virtual Block read() = 0;

    virtual bool isBlocked() = 0;
};
using SourcePtr = std::unique_ptr<Source>;

class CPUSource : public Source
{
public:
    Block read() override
    {
        if (block_count > 0)
        {
            --block_count;
            return prepareRandomBlock(50000); // 5w
        }
        return {};
    }

    bool isBlocked() override
    {
        return false;
    }

private:
    int block_count = 100;
};

class IOSource : public Source
{
public:
    Block read() override
    {
        if (block_count > 0)
        {
            --block_count;
            return prepareRandomBlock(50000); // 5w
        }
        return {};
    }

    bool isBlocked() override
    {
        if (!io_future)
        {
            io_future.emplace(DynamicThreadPool::global_instance->schedule(false, []() { doIOPart(); }));
            return true;
        }

        bool is_ready = io_future->wait_for(std::chrono::seconds(0)) == std::future_status::ready;
        if (is_ready)
            io_future.reset();
        return !is_ready;
    }
private:
    std::optional<std::future<void>> io_future;
    int block_count = 100;
};
} // namespace DB
