// Copyright 2025 PingCAP, Inc.
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
#include <Storages/DeltaMerge/Index/LocalIndexWriter_fwd.h>
#include <Storages/DeltaMerge/dtpb/index_file.pb.h>


namespace DB::DM
{

/// Builds a LocalIndex.
class LocalIndexWriter
{
public:
    using ProceedCheckFn = std::function<bool()>;

public:
    explicit LocalIndexWriter(IndexID index_id_)
        : index_id(index_id_)
    {}

    static LocalIndexWriterPtr createInMemory(const LocalIndexInfo & index_info);
    static LocalIndexWriterPtr createOnDisk(std::string_view index_file, const LocalIndexInfo & index_info);

    virtual ~LocalIndexWriter() = default;

    virtual void addBlock(const IColumn & column, const ColumnVector<UInt8> * del_mark, ProceedCheckFn should_proceed)
        = 0;

    dtpb::IndexFilePropsV2 finalize(std::string_view path) const;
    dtpb::IndexFilePropsV2 finalize(WriteBuffer & write_buf) const;

protected:
    virtual void saveToFile(std::string_view path) const = 0;
    virtual void saveToBuffer(WriteBuffer & write_buf) const = 0;

    virtual void saveFilePros(dtpb::IndexFilePropsV2 * pb_idx) const = 0;

    virtual dtpb::IndexFileKind kind() const = 0;

private:
    IndexID index_id;
};

} // namespace DB::DM
