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

#include <Common/HashTable/HashMap.h>
#include <Core/SortDescription.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Interpreters/Context.h>

namespace DB
{
// TODO before review: add toggle about whether merge block or not, and whether split block or not.
class HashOrderBlockInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "HashOrder";

public:
    HashOrderBlockInputStream(
        const BlockInputStreamPtr & input_,
        SortDescription & description_,
        const String & req_id,
        const Context & context_,
        const Block & sample_block,
        size_t limit_ = 0);

    String getName() const override { return NAME; }

    bool isGroupedOutput() const override { return true; }
    bool isSortedOutput() const override { return true; }
    const SortDescription & getSortDescription() const override { return description; }

    Block getHeader() const override { return children.at(0)->getHeader(); }

    /// Reference to the row in block.
    struct RowRef
    {
        const Block * block;
        size_t row_num;

        RowRef() = default;
        RowRef(const Block * block_, size_t row_num_)
            : block(block_)
            , row_num(row_num_)
        {}
    };

    /// Single linked list of references to rows. Used for ALL JOINs (non-unique JOINs)
    struct RowRefList : RowRef
    {
        RowRefList * next = nullptr;

        RowRefList() = default;
        RowRefList(const Block * block_, size_t row_num_)
            : RowRef(block_, row_num_)
        {}
    };

    static void insertRowToList(RowRefList * list, RowRefList * elem, Block * stored_block, size_t index)
    {
        elem->next = list->next; // NOLINT(clang-analyzer-core.NullDereference)
        list->next = elem;
        elem->block = stored_block;
        elem->row_num = index;
    }

    /// Different types of keys for maps.
#define APPLY_FOR_HASH_ORDER_VARIANTS(M) \
    M(key8)                              \
    M(key16)                             \
    M(key32)                             \
    M(key64)                             \
    M(key_string)                        \
    M(key_fixed_string)                  \
    M(keys128)                           \
    M(keys256)                           \
    M(serialized)

    enum class Type
    {
#define M(NAME) NAME,
        APPLY_FOR_HASH_ORDER_VARIANTS(M)
#undef M
    };

    struct Maps
    {
        std::unique_ptr<HashMap<UInt8, RowRefList, TrivialHash, HashTableFixedGrower<8>>> key8;
        std::unique_ptr<HashMap<UInt16, RowRefList, TrivialHash, HashTableFixedGrower<16>>> key16;
        std::unique_ptr<HashMap<UInt32, RowRefList, HashCRC32<UInt32>>> key32;
        std::unique_ptr<HashMap<UInt64, RowRefList, HashCRC32<UInt64>>> key64;
        std::unique_ptr<HashMapWithSavedHash<StringRef, RowRefList>> key_string;
        std::unique_ptr<HashMapWithSavedHash<StringRef, RowRefList>> key_fixed_string;
        std::unique_ptr<HashMap<UInt128, RowRefList, HashCRC32<UInt128>>> keys128;
        std::unique_ptr<HashMap<UInt256, RowRefList, HashCRC32<UInt256>>> keys256;
        std::unique_ptr<HashMap<StringRef, RowRefList>> serialized;
    };

    struct MapIterators
    {
        HashMap<UInt8, RowRefList, TrivialHash, HashTableFixedGrower<8>>::const_iterator key8;
        HashMap<UInt16, RowRefList, TrivialHash, HashTableFixedGrower<16>>::const_iterator key16;
        HashMap<UInt32, RowRefList, HashCRC32<UInt32>>::const_iterator key32;
        HashMap<UInt64, RowRefList, HashCRC32<UInt64>>::const_iterator key64;
        HashMapWithSavedHash<StringRef, RowRefList>::const_iterator key_string;
        HashMapWithSavedHash<StringRef, RowRefList>::const_iterator key_fixed_string;
        HashMap<UInt128, RowRefList, HashCRC32<UInt128>>::const_iterator keys128;
        HashMap<UInt256, RowRefList, HashCRC32<UInt256>>::const_iterator keys256;
        HashMap<StringRef, RowRefList>::const_iterator serialized;
    };

    Maps maps;
    MapIterators iters;


protected:
    Block readImpl() override;
    Block readImplInternal();
    void appendInfo(FmtBuffer & buffer) const override;

    bool executed = false;

private:
    SortDescription description;
    size_t limit;
    LoggerPtr log;
    Type type;

    TiDB::TiDBCollators collators;

    BlocksList blocks;

    using Sizes = std::vector<size_t>;
    static Type chooseMethod(const ColumnRawPtrs & key_columns, Sizes & key_sizes);
    void initMap(size_t capacity);
    void initIter();

    template <typename Map, typename KeyGetter>
    void insert(Map & map, size_t rows, KeyGetter key_getter, std::vector<std::string> & sort_key_container, Block * block);

    template <typename Map, typename MapIterator>
    Block output(Map & map, MapIterator & iter);

    Sizes key_sizes;

    std::shared_ptr<Arena> pool = std::make_shared<Arena>();

    const Context & context;

    size_t call_count = 0;

    void insertFromBlock(Block * block);
};

} // namespace DB
