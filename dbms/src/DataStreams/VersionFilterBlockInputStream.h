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

#include <DataStreams/IProfilingBlockInputStream.h>
#include <common/logger_useful.h>

namespace DB
{

class VersionFilterBlockInputStream : public IProfilingBlockInputStream
{
public:
    VersionFilterBlockInputStream(const BlockInputStreamPtr & input_, const size_t version_column_index_, UInt64 filter_greater_version_)
        : input(input_), version_column_index(version_column_index_), filter_greater_version(filter_greater_version_)
    {}

protected:
    Block getHeader() const override { return input->getHeader(); }

    bool isGroupedOutput() const override { return input->isGroupedOutput(); }

    bool isSortedOutput() const override { return input->isSortedOutput(); }

    const SortDescription & getSortDescription() const override { return input->getSortDescription(); }

    String getName() const override { return "VersionFilter"; }

    Block readImpl() override;

private:
    BlockInputStreamPtr input;
    const size_t version_column_index;
    const UInt64 filter_greater_version;
    Poco::Logger * log = &Poco::Logger::get("VersionFilterBlockInputStream");
};

} // namespace DB
