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

#include <Common/Exception.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMemory.h>

#include <map>


namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


class MemoryBlockInputStream : public IProfilingBlockInputStream
{
public:
    MemoryBlockInputStream(
        const Names & column_names_,
        BlocksList::iterator begin_,
        BlocksList::iterator end_,
        const StorageMemory & storage_)
        : column_names(column_names_)
        , begin(begin_)
        , end(end_)
        , it(begin)
        , storage(storage_)
    {}

    String getName() const override { return "Memory"; }

    Block getHeader() const override { return storage.getSampleBlockForColumns(column_names); }

protected:
    Block readImpl() override
    {
        if (it == end)
        {
            return Block();
        }
        else
        {
            Block src = *it;
            Block res;

            /// Add only required columns to `res`.
            for (const auto & name : column_names)
                res.insert(src.getByName(name));

            ++it;
            return res;
        }
    }

private:
    Names column_names;
    BlocksList::iterator begin;
    BlocksList::iterator end;
    BlocksList::iterator it;
    const StorageMemory & storage;
};


class MemoryBlockOutputStream : public IBlockOutputStream
{
public:
    explicit MemoryBlockOutputStream(StorageMemory & storage_)
        : storage(storage_)
    {}

    Block getHeader() const override { return storage.getSampleBlock(); }

    void write(const Block & block) override
    {
        storage.check(block, true);
        std::lock_guard lock(storage.mutex);
        storage.data.push_back(block);
    }

private:
    StorageMemory & storage;
};


StorageMemory::StorageMemory(String table_name_, ColumnsDescription columns_description_)
    : IStorage{std::move(columns_description_)}
    , table_name(std::move(table_name_))
{}


BlockInputStreams StorageMemory::read(
    const Names & column_names,
    const SelectQueryInfo & /*query_info*/,
    const Context & /*context*/,
    QueryProcessingStage::Enum & processed_stage,
    size_t /*max_block_size*/,
    unsigned num_streams)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    std::lock_guard lock(mutex);

    size_t size = data.size();

    if (num_streams > size)
        num_streams = size;

    BlockInputStreams res;

    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        auto begin = data.begin();
        auto end = data.begin();

        std::advance(begin, stream * size / num_streams);
        std::advance(end, (stream + 1) * size / num_streams);

        res.push_back(std::make_shared<MemoryBlockInputStream>(column_names, begin, end, *this));
    }

    return res;
}


BlockOutputStreamPtr StorageMemory::write(const ASTPtr & /*query*/, const Settings & /*settings*/)
{
    return std::make_shared<MemoryBlockOutputStream>(*this);
}


void StorageMemory::drop()
{
    std::lock_guard lock(mutex);
    data.clear();
}


void registerStorageMemory(StorageFactory & factory)
{
    factory.registerStorage("Memory", [](const StorageFactory::Arguments & args) {
        if (!args.engine_args.empty())
            throw Exception(
                "Engine " + args.engine_name + " doesn't support any arguments (" + toString(args.engine_args.size())
                    + " given)",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return StorageMemory::create(args.table_name, args.columns);
    });
}

} // namespace DB
