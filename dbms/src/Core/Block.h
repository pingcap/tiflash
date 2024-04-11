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

#include <Core/BlockInfo.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/NamesAndTypes.h>

#include <initializer_list>
#include <list>
#include <map>
#include <vector>


namespace DB
{
/** Container for set of columns for bunch of rows in memory.
  * This is unit of data processing.
  * Also contains metadata - data types of columns and their names
  *  (either original names from a table, or generated names during temporary calculations).
  * Allows to insert, remove columns in arbitary position, to change order of columns.
  */

class Context;

class Block
{
private:
    using Container = ColumnsWithTypeAndName;
    using IndexByName = std::map<String, size_t>;

    Container data;
    IndexByName index_by_name;

    // `start_offset` is the offset of first row in this Block.
    // It is used for calculating `segment_row_id`.
    UInt64 start_offset = 0;
    // `segment_row_id_col` is a virtual column that represents the records' row id in the corresponding segment.
    // Only used for calculating MVCC-bitmap-filter.
    ColumnPtr segment_row_id_col;

public:
    BlockInfo info;

    Block() = default;
    Block(std::initializer_list<ColumnWithTypeAndName> il);
    explicit Block(const ColumnsWithTypeAndName & data_);
    explicit Block(const NamesAndTypes & names_and_types);

    /// insert the column at the specified position
    void insert(size_t position, const ColumnWithTypeAndName & elem);
    void insert(size_t position, ColumnWithTypeAndName && elem);
    /// insert the column to the end
    void insert(const ColumnWithTypeAndName & elem);
    void insert(ColumnWithTypeAndName && elem);
    /// insert the column to the end, if there is no column with that name yet
    void insertUnique(const ColumnWithTypeAndName & elem);
    void insertUnique(ColumnWithTypeAndName && elem);
    /// remove the column at the specified position
    void erase(size_t position);
    /// remove the column with the specified name
    void erase(const String & name);

    /// References are invalidated after calling functions above.

    ColumnWithTypeAndName & getByPosition(size_t position) { return data[position]; }
    const ColumnWithTypeAndName & getByPosition(size_t position) const { return data[position]; }

    ColumnWithTypeAndName & safeGetByPosition(size_t position);
    const ColumnWithTypeAndName & safeGetByPosition(size_t position) const;

    ColumnWithTypeAndName & getByName(const std::string & name);
    const ColumnWithTypeAndName & getByName(const std::string & name) const;

    Container::iterator begin() { return data.begin(); }
    Container::iterator end() { return data.end(); }
    Container::const_iterator begin() const { return data.begin(); }
    Container::const_iterator end() const { return data.end(); }
    Container::const_iterator cbegin() const { return data.cbegin(); }
    Container::const_iterator cend() const { return data.cend(); }

    bool has(const std::string & name) const;

    size_t getPositionByName(const std::string & name) const;

    const ColumnsWithTypeAndName & getColumnsWithTypeAndName() const;
    NamesAndTypesList getNamesAndTypesList() const;
    Names getNames() const;

    /// Returns number of rows from first column in block, not equal to nullptr. If no columns, returns 0.
    size_t rows() const;

    size_t columns() const { return data.size(); }

    /// Checks that every column in block is not nullptr and has same number of elements.
    void checkNumberOfRows() const;

    /// Approximate number of bytes in memory - for profiling and limits.
    size_t bytes() const;

    size_t estimateBytesForSpill() const;

    /// Approximate number of bytes between [offset, offset+limit) in memory - for profiling and limits.
    size_t bytes(size_t offset, size_t limit) const;

    /// Approximate number of allocated bytes in memory - for profiling and limits.
    size_t allocatedBytes() const;

    explicit operator bool() const { return !data.empty() || segment_row_id_col != nullptr; }
    bool operator!() const { return data.empty() && segment_row_id_col == nullptr; }

    /** Get a list of column names separated by commas. */
    std::string dumpNames() const;

    /** List of names, types and lengths of columns. Designed for debugging. */
    std::string dumpStructure() const;

    std::string dumpJsonStructure() const;

    /** Get the same block, but empty. */
    Block cloneEmpty() const;

    /** Get empty columns with the same types as in block. */
    MutableColumns cloneEmptyColumns() const;

    /** Get columns from block for mutation. Columns in block will be nullptr. */
    MutableColumns mutateColumns() const;

    Columns getColumns() const;

    /** Replace columns in a block */
    void setColumns(MutableColumns && columns);
    Block cloneWithColumns(MutableColumns && columns) const;
    Block cloneWithColumns(Columns && columns) const;

    /** Get a block with columns that have been rearranged in the order of their names. */
    Block sortColumns() const;

    void clear();
    void swap(Block & other) noexcept;

    /** Updates SipHash of the Block, using update method of columns.
      * Returns hash for block, that could be used to differentiate blocks
      *  with same structure, but different data.
      */
    void updateHash(SipHash & hash) const;

    void setStartOffset(UInt64 offset) { start_offset = offset; }
    UInt64 startOffset() const { return start_offset; }
    void setSegmentRowIdCol(ColumnPtr && col) { segment_row_id_col = col; }
    ColumnPtr segmentRowIdCol() const { return segment_row_id_col; }

private:
    void eraseImpl(size_t position);
    void initializeIndexByName();
};

using Blocks = std::vector<Block>;
using BlocksList = std::list<Block>;
using BucketBlocksListMap = std::map<Int32, BlocksList>;

/// Join blocks by columns
/// The schema of the output block is the same as the header block.
/// The columns not in the header block will be ignored.
/// For example:
/// header: (a UInt32, b UInt32, c UInt32, d UInt32)
/// block1: (a UInt32, b UInt32, c UInt32, e UInt32), rows: 3
/// block2: (d UInt32), rows: 3
/// result: (a UInt32, b UInt32, c UInt32, d UInt32), rows: 3
Block hstackBlocks(Blocks && blocks, const Block & header);

/// Join blocks by rows
/// For example:
/// block1: (a UInt32, b UInt32, c UInt32), rows: 2
/// block2: (a UInt32, b UInt32, c UInt32), rows: 3
/// result: (a UInt32, b UInt32, c UInt32), rows: 5
template <bool check_reserve = false>
Block vstackBlocks(Blocks && blocks);

Block popBlocksListFront(BlocksList & blocks);

/// Compare number of columns, data types, column types, column names, and values of constant columns.
bool blocksHaveEqualStructure(const Block & lhs, const Block & rhs);

/// Throw exception when blocks are different.
void assertBlocksHaveEqualStructure(const Block & lhs, const Block & rhs, const std::string & context_description);

/// Calculate difference in structure of blocks and write description into output strings. NOTE It doesn't compare values of constant columns.
void getBlocksDifference(const Block & lhs, const Block & rhs, std::string & out_lhs_diff, std::string & out_rhs_diff);


/** Additional data to the blocks. They are only needed for a query
  * DESCRIBE TABLE with Distributed tables.
  */
struct BlockExtraInfo
{
    BlockExtraInfo() = default;
    explicit operator bool() const { return is_valid; }
    bool operator!() const { return !is_valid; }

    std::string host;
    std::string resolved_address;
    std::string user;
    UInt16 port = 0;

    bool is_valid = false;
};

} // namespace DB
