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

#include <common/logger_useful.h>
#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{

class DeletingDeletedBlockInputStream : public IProfilingBlockInputStream
{
private:
    Block readImpl() override;

    Block getHeader() const override
    {
        return input->getHeader();
    }

public:
    DeletingDeletedBlockInputStream(BlockInputStreamPtr & input_, const std::string & delmark_column_) :
        input(input_), delmark_column(delmark_column_)
    {
        log = &Poco::Logger::get(getName());
        children.emplace_back(input_);
    }

    String getName() const override
    {
        return "DeletingDeletedInput";
    }

    bool isGroupedOutput() const override
    {
        return input->isGroupedOutput();
    }

    bool isSortedOutput() const override
    {
        return input->isSortedOutput();
    }

    const SortDescription & getSortDescription() const override
    {
        return input->getSortDescription();
    }

private:
    Poco::Logger * log;
    BlockInputStreamPtr input;
    const std::string delmark_column;
};

}
