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

#include <Common/Logger.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Flash/Coprocessor/DAGResponseWriter.h>
#include <Flash/Mpp/MppVersion.h>

namespace DB
{
/// read blocks directly from Union, then broadcast or partition blocks and encode them, later put them into sending tunnels
/// it should be an output stream but derived from the inputSteam
class ExchangeSenderBlockInputStream : public IProfilingBlockInputStream
{
public:
    ExchangeSenderBlockInputStream(
        const BlockInputStreamPtr & input,
        std::unique_ptr<DAGResponseWriter> writer,
        MppVersion mpp_version,
        const String & req_id)
        : writer(std::move(writer))
        , header(getHeaderByMppVersion(input->getHeader(), mpp_version))
        , log(Logger::get(req_id))
    {
        children.push_back(input);
    }
    static constexpr auto name = "ExchangeSender";
    String getName() const override { return name; }
    Block getHeader() const override { return header; }

    bool canHandleSelectiveBlock() const override { return true; }

protected:
    Block readImpl() override;
    void readPrefixImpl() override { writer->prepare(getHeader()); }
    void readSuffixImpl() override { LOG_DEBUG(log, "finish write with {} rows", total_rows); }

private:
    Block getHeaderByMppVersion(Block && header, MppVersion mpp_version) const
    {
        if (mpp_version > MppVersion::MppVersionV2)
        {
            return std::move(header);
        }

        for (auto & column : header)
        {
            if (removeNullable(column.type)->getTypeId() != TypeIndex::String)
                continue;

            if (column.type->isNullable())
                column.type = DataTypeFactory::instance().DataTypeFactory::instance().getOrSet(
                    DataTypeString::NullableLegacyName);
            else
                column.type
                    = DataTypeFactory::instance().DataTypeFactory::instance().getOrSet(DataTypeString::LegacyName);
        }
        return header;
    }


    std::unique_ptr<DAGResponseWriter> writer;
    Block header;
    const LoggerPtr log;
    size_t total_rows = 0;
};

} // namespace DB
