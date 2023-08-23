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

#include <Core/NamesAndTypes.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Storages/IStorage.h>

#include <ext/shared_ptr_helper.h>
#include <mutex>


namespace DB
{
/** Implements storage in the RAM.
  * Suitable for temporary data.
  * It does not support keys.
  * Data is stored as a set of blocks and is not stored anywhere else.
  */
class StorageMemory
    : public ext::SharedPtrHelper<StorageMemory>
    , public IStorage
{
    friend class MemoryBlockInputStream;
    friend class MemoryBlockOutputStream;

public:
    std::string getName() const override { return "Memory"; }
    std::string getTableName() const override { return table_name; }

    size_t getSize() const { return data.size(); }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Settings & settings) override;

    void drop() override;
    void rename(const String & /*new_path_to_db*/, const String & /*new_database_name*/, const String & new_table_name)
        override
    {
        table_name = new_table_name;
    }

private:
    String table_name;

    /// The data itself. `list` - so that when inserted to the end, the existing iterators are not invalidated.
    BlocksList data;

    std::mutex mutex;

protected:
    StorageMemory(String table_name_, ColumnsDescription columns_description_);
};

} // namespace DB
