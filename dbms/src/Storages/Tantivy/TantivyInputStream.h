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

#include "Common/Logger.h"
#include "Core/NamesAndTypes.h"
#include "common/logger_useful.h"
#include "common/types.h"
#include "tici-search-lib/src/lib.rs.h"

namespace DB::TS
{
class TantivyInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "TantivyInputStream";

public:
    TantivyInputStream(LoggerPtr log_, const String &, const String &, NamesAndTypes name_and_types_)
        : log(log_)
        , names_and_types(name_and_types_)
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
        rust::Vec<rust::String> query_fields;
        query_fields.push_back("column_id_2");
        query_fields.push_back("column_id_3");

        rust::Vec<rust::String> search_fields = {};
        for (auto & name_and_type : names_and_types)
        {
            LOG_INFO(log, name_and_type.name);
            search_fields.push_back(name_and_type.name);
        }

        auto search_param = SearchParam{20};
        rust::Vec<IdDocument> documents
            = search("/home/wshwsh12/project/ticilib/tmp/searcher/", "sea", query_fields, search_fields, search_param);

        Block res(names_and_types);
        int i = 0;
        for (auto & name_and_type : names_and_types)
        {
            auto col = res.getByName(name_and_type.name).column->assumeMutable();
            if (name_and_type.type->isStringOrFixedString())
            {
                for (auto & doc : documents)
                {
                    col->insert(Field(String(doc.fieldValues[i].string_value.c_str())));
                }
            }
            if (name_and_type.type->isInteger())
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
    NamesAndTypes names_and_types;
};

} // namespace DB::TS
