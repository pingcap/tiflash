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

#include <Core/SortDescription.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{
/** Sorts each block individually by the values of the specified columns.
  * At the moment, not very optimal algorithm is used.
  */
class HashOrderBlockInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "HashOrder";

public:
    /// limit - if not 0, then you can sort each block not completely, but only `limit` first rows by order.
    HashOrderBlockInputStream(
        const BlockInputStreamPtr & input_,
        SortDescription & description_,
        const String & req_id,
        size_t limit_ = 0)
        : description(description_)
        , limit(limit_)
        , log(Logger::get(NAME, req_id))
    {
        children.push_back(input_);
    }

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

    void insertRowToList(RowRefList * list, RowRefList * elem, Block * stored_block, size_t index)
    {
        elem->next = list->next; // NOLINT(clang-analyzer-core.NullDereference)
        list->next = elem;
        elem->block = stored_block;
        elem->row_num = index;
    }

    /// Different types of keys for maps.
#define APPLY_FOR_HASH_ORDER_VARIANTS(M) \
    M(key8)                        \
    M(key16)                       \
    M(key32)                       \
    M(key64)                       \
    M(key_string)                  \
    M(key_fixed_string)            \
    M(keys128)                     \
    M(keys256)                     \
    M(serialized)

    enum class Type
    {
        EMPTY,
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

    Maps maps;


protected:
    Block readImpl() override;
    void appendInfo(FmtBuffer & buffer) const override;

    bool executed = false;

private:
    SortDescription description;
    size_t limit;
    LoggerPtr log;

    TiDB::TiDBCollators collators;

    Blocks blocks;

    using Sizes = std::vector<size_t>;
    static Type chooseMethod(const ColumnRawPtrs & key_columns, Sizes & key_sizes);
    void initMapImpl(Type type_);

    template <typename Map, typename KeyGetter>
    void insert(Map & map, size_t rows, KeyGetter key_getter, Block * block);

    Sizes key_sizes;

    std::shared_ptr<Arena> pool = std::make_shared<Arena>();
};

} // namespace DB
