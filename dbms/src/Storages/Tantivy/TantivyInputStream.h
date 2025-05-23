// Copyright 2025 PingCAP, Ltd.
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
#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataTypes/DataTypeNullable.h>
#include <common/logger_useful.h>
#include <common/types.h>
#include <fcntl.h>
#include <fmt/os.h>
#include <tici-search-lib/src/lib.rs.h>

namespace DB::TS
{
class TantivyInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "TantivyInputStream";

public:
    TantivyInputStream(
        LoggerPtr log_,
        Int64 table_id_,
        Int64 index_id_,
        NamesAndTypes query_columns_,
        NamesAndTypes return_columns_,
        String query_json_str_,
        UInt64 limit_)
        : log(log_)
        , table_id(table_id_)
        , index_id(index_id_)
        , query_columns(query_columns_)
        , return_columns(return_columns_)
        , query_json_str(query_json_str_)
        , limit(limit_)
    {}

    String getName() const override { return NAME; }

    Block getHeader() const override { return header; }

    Block readImpl() override
    {
        if (done)
        {
            return {};
        }
        done = true;
        return readFromS3();
    }

protected:
    Block readFromS3()
    {
        auto query_fields = getFields(query_columns);
        auto return_fields = getFields(return_columns);

        auto search_param = SearchParam{static_cast<size_t>(limit)};
        rust::Vec<IdDocument> documents
            = search(table_id, index_id, query_fields, return_fields, query_json_str, search_param);

        Block res(return_columns);
        int i = 0;
        for (auto & name_and_type : return_columns)
        {
            auto col = res.getByName(name_and_type.name).column->assumeMutable();
            if (removeNullable(name_and_type.type)->isStringOrFixedString())
            {
                for (auto & doc : documents)
                {
                    col->insert(Field(String(doc.fieldValues[i].string_value.c_str())));
                }
            }
            if (removeNullable(name_and_type.type)->isInteger())
            {
                for (auto & doc : documents)
                {
                    col->insert(Field(doc.fieldValues[i].int_value));
                }
            }
            i++;
        }
        return res;
    }

private:
    Block header;
    bool done = false;
    LoggerPtr log;
    Int64 table_id;
    Int64 index_id;
    NamesAndTypes query_columns;
    NamesAndTypes return_columns;
    String query_json_str;
    UInt64 limit;

    rust::Vec<rust::String> getFields(NamesAndTypes & columns)
    {
        rust::Vec<rust::String> fields;
        for (auto & name_and_type : columns)
        {
            LOG_INFO(log, "name: {}, type: {}", name_and_type.name, name_and_type.type->getName());
            fields.push_back(name_and_type.name);
        }
        return fields;
    }
};

} // namespace DB::TS
