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

class LocalIndexWriterInMemory;
using LocalIndexWriterInMemoryPtr = std::shared_ptr<LocalIndexWriterInMemory>;
class LocalIndexWriterOnDisk;
using LocalIndexWriterOnDiskPtr = std::shared_ptr<LocalIndexWriterOnDisk>;

class LocalIndexWriter
{
public:
    using ProceedCheckFn = std::function<bool()>;

public:
    explicit LocalIndexWriter(IndexID index_id_)
        : index_id(index_id_)
    {}

    static LocalIndexWriterInMemoryPtr createInMemory(const LocalIndexInfo & index_info);
    static LocalIndexWriterOnDiskPtr createOnDisk(std::string_view index_file, const LocalIndexInfo & index_info);

    virtual ~LocalIndexWriter() = default;

    virtual void addBlock(const IColumn & column, const ColumnVector<UInt8> * del_mark, ProceedCheckFn should_proceed)
        = 0;

protected:
    virtual void saveFilePros(dtpb::IndexFilePropsV2 * pb_idx) const = 0;

    virtual dtpb::IndexFileKind kind() const = 0;

protected:
    IndexID index_id;
};

class LocalIndexWriterInMemory : public LocalIndexWriter
{
public:
    explicit LocalIndexWriterInMemory(IndexID index_id_)
        : LocalIndexWriter(index_id_)
    {}

    ~LocalIndexWriterInMemory() override = default;

    dtpb::IndexFilePropsV2 finalize(WriteBuffer & write_buf);

protected:
    virtual void saveToBuffer(WriteBuffer & write_buf) = 0;
};

class LocalIndexWriterOnDisk : public LocalIndexWriter
{
public:
    explicit LocalIndexWriterOnDisk(IndexID index_id_, std::string_view index_file_)
        : LocalIndexWriter(index_id_)
        , index_file(index_file_)
    {}

    ~LocalIndexWriterOnDisk() override = default;

    dtpb::IndexFilePropsV2 finalize();

protected:
    virtual void saveToFile() const = 0;

protected:
    String index_file;
};

} // namespace DB::DM
