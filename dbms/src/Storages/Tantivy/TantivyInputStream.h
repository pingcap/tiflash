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

#include <Core/Block.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <fcntl.h>
#include <fmt/os.h>

#include <filesystem>
#include <iostream>
#include <memory>

#include "Columns/ColumnString.h"
#include "Columns/ColumnVector.h"
#include "Common/Exception.h"
#include "Common/Logger.h"
#include "Common/assert_cast.h"
#include "Core/ColumnsWithTypeAndName.h"
#include "DataTypes/DataTypeString.h"
#include "DataTypes/DataTypesNumber.h"
#include "common/logger_useful.h"
#include "tici-search-lib/src/lib.rs.h"

namespace DB::TS
{
class TantivyInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "TantivyInputStream";

public:
    TantivyInputStream(LoggerPtr log_, const String & uri_, const String & file_type_)
        : uri(uri_)
        , file_type(file_type_)
        , log(log_)
    {}

    String getName() const override { return NAME; }

    Block getHeader() const override { return header; }

    Block readImpl() override
    {
        LOG_INFO(log, "7777777777777777777777");
        if (done)
        {
            return {};
        }
        done = true;
        return readFromFile();
    }

protected:
    Block readFromFile()
    {
        rust::Vec<FieldMapping> field_mappings;
        field_mappings.push_back(FieldMapping{"_docId", FieldType::IntField});
        field_mappings.push_back(FieldMapping{"title", FieldType::TextField});
        field_mappings.push_back(FieldMapping{"body", FieldType::TextField});
        rust::Vec<rust::String> search_fields = {"title", "body"};

        auto search_param = SearchParam{20};
        rust::Vec<IdDocument> documents = search2(
            "/home/wshwsh12/project/ticilib/tmp/searcher/",
            field_mappings,
            "sea task",
            search_fields,
            search_param);

        Block res;

        {
            auto internal_type = std::make_shared<DataTypeString>();
            auto internal_column = internal_type->createColumn();
            PaddedPODArray<UInt8> & column_chars_t = assert_cast<ColumnString &>(*internal_column).getChars();
            PaddedPODArray<UInt64> & column_offsets = assert_cast<ColumnString &>(*internal_column).getOffsets();

            for (IdDocument & doc : documents)
            {
                auto t = doc.fieldValues[0];
                LOG_INFO(log, t.field_name.c_str());
                LOG_INFO(log, t.field_value.c_str());
                column_chars_t.insert(t.field_value.begin(), t.field_value.end());
                column_chars_t.emplace_back('\0');
                column_offsets.emplace_back(column_chars_t.size());
            }

            res.insert({std::move(internal_column), std::move(internal_type), "test"});
        }

        {
            auto internal_type = std::make_shared<DataTypeString>();
            auto internal_column = internal_type->createColumn();
            PaddedPODArray<UInt8> & column_chars_t = assert_cast<ColumnString &>(*internal_column).getChars();
            PaddedPODArray<UInt64> & column_offsets = assert_cast<ColumnString &>(*internal_column).getOffsets();

            for (IdDocument & doc : documents)
            {
                auto t = doc.fieldValues[1];
                LOG_INFO(log, t.field_name.c_str());
                LOG_INFO(log, t.field_value.c_str());
                column_chars_t.insert(t.field_value.begin(), t.field_value.end());
                column_chars_t.emplace_back('\0');
                column_offsets.emplace_back(column_chars_t.size());
            }

            res.insert({std::move(internal_column), std::move(internal_type), "test2"});
        }


        return res;
    }

private:
    [[maybe_unused]] const String & uri;
    [[maybe_unused]] const String & file_type;


    Block header;
    bool done = false;
    LoggerPtr log;
};

} // namespace DB::TS
