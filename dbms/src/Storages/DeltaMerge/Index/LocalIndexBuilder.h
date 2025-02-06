// Copyright 2024 PingCAP, Inc.
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

#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <Storages/DeltaMerge/Index/LocalIndexInfo.h>
#include <Storages/DeltaMerge/Index/LocalIndex_fwd.h>


namespace DB::DM
{

/// Builds a LocalIndex.
class LocalIndexBuilder
{
public:
    using ProceedCheckFn = std::function<bool()>;

public:
    static LocalIndexBuilderPtr create(const LocalIndexInfo & index_info);

public:
    explicit LocalIndexBuilder(const LocalIndexInfo & index_info)
        : index_info(index_info)
    {}

    virtual ~LocalIndexBuilder() = default;

    virtual void addBlock(const IColumn & column, const ColumnVector<UInt8> * del_mark, ProceedCheckFn should_proceed)
        = 0;

    virtual void saveToFile(std::string_view path) const = 0;
    virtual void saveToBuffer(WriteBuffer & write_buf) const = 0;

public:
    const LocalIndexInfo index_info;
};

} // namespace DB::DM
