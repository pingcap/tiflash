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

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Core/SortCursor.h>
#include <Core/SortDescription.h>
#include <DataStreams/IBlockInputStream.h>
#include <Storages/MutableSupport.h>


namespace DB
{
inline void deleteRows(Block & block, const IColumn::Filter & filter)
{
    for (size_t i = 0; i < block.columns(); i++)
    {
        ColumnWithTypeAndName column = block.getByPosition(i);
        column.column = column.column->filter(filter, 0);
        block.erase(i);
        block.insert(i, column);
    }
}

inline Block filterBlock(const Block & block, const IColumn::Filter & filter)
{
    Block res;
    for (size_t i = 0, num_columns = block.columns(); i < num_columns; ++i)
    {
        ColumnWithTypeAndName column = block.getByPosition(i);
        column.column = column.column->filter(filter, 0);
        res.insert(std::move(column));
    }
    return res;
}


inline size_t setFilterByDelMarkColumn(const Block & block, IColumn::Filter & filter)
{
    if (!block.has(MutableSupport::delmark_column_name))
        return 0;

    const ColumnWithTypeAndName & delmark_column = block.getByName(MutableSupport::delmark_column_name);
    const ColumnUInt8 * column = typeid_cast<const ColumnUInt8 *>(delmark_column.column.get());
    if (!column)
        throw("Del-mark column should be type ColumnUInt8.");

    size_t rows = block.rows();
    if (filter.size() != rows)
        throw("Filter array not fully inited, filter.size() != rows");

    size_t sum = 0;
    for (size_t i = 0; i < rows; i++)
    {
        UInt8 val = column->getElement(i);
        if (MutableSupport::DelMark::isDel(val))
        {
            filter[i] = 0;
            sum += 1;
        }
    }

    return sum;
}


class VersionColumn
{
public:
    VersionColumn(const Block & block)
        : column(0)
    {
        if (!block.has(MutableSupport::version_column_name))
            return;
        const ColumnWithTypeAndName & version_column = block.getByName(MutableSupport::version_column_name);
        column = typeid_cast<const ColumnUInt64 *>(version_column.column.get());
    }

    UInt64 operator[](size_t row) const
    {
        return column->getElement(row);
    }

private:
    const ColumnUInt64 * column;
};


class DedupingBlock
{
private:
    DedupingBlock(const DedupingBlock &);
    DedupingBlock & operator=(const DedupingBlock &);

public:
    DedupingBlock(const Block & block_, const size_t stream_position_, bool set_deleted_rows)
        : stream_position(stream_position_)
        , block(block_)
        , filter(block_.rows(), 1)
        , deleted_rows(0)
    {
        if (set_deleted_rows)
            deleted_rows = setFilterByDelMarkColumn(block, filter);
    }

    operator bool()
    {
        return bool(block);
    }

    operator const Block &()
    {
        return block;
    }

    size_t rows()
    {
        return block.rows();
    }

    size_t deleteds()
    {
        return deleted_rows;
    }

    void setDeleted(size_t i)
    {
        filter[i] = 0;
        deleted_rows += 1;
    }

    VersionColumn & versions()
    {
        if (!version_column)
            version_column = std::make_shared<VersionColumn>(block);
        return *version_column;
    }

    Block finalize()
    {
        deleteRows(block, filter);
        if (block.rows() == 0)
            return Block();
        return block;
    }

    String str()
    {
        std::stringstream ostr;
        ostr << "#";
        if (stream_position == size_t(-1))
            ostr << "?";
        else
            ostr << stream_position;
        ostr << ":";
        if (block)
            ostr << block.rows() << "-" << deleted_rows;
        else
            ostr << "?";
        return ostr.str();
    }

    friend std::ostream & operator<<(std::ostream & out, DedupingBlock & self)
    {
        return out << self.str();
    }

public:
    const size_t stream_position;

private:
    Block block;

    IColumn::Filter filter;
    size_t deleted_rows;
    std::shared_ptr<VersionColumn> version_column;
};

using DedupingBlockPtr = std::shared_ptr<DedupingBlock>;


template <typename T>
class SmallObjectFifo : public ConcurrentBoundedQueue<T>
{
    using Self = ConcurrentBoundedQueue<T>;

public:
    SmallObjectFifo(size_t size)
        : Self(size)
    {}

    T pop()
    {
        T value;
        Self::pop(value);
        return value;
    }
};


template <typename Fifo>
class FifoPtrs : public std::vector<std::shared_ptr<Fifo>>
{
    using Self = std::vector<std::shared_ptr<Fifo>>;
    using FifoPtr = std::shared_ptr<Fifo>;

public:
    FifoPtrs(size_t size, size_t queue_max_)
        : Self(size)
        , queue_max(queue_max_)
    {
        for (size_t i = 0; i < Self::size(); ++i)
            Self::operator[](i) = std::make_shared<Fifo>(queue_max_);
    }

    String str()
    {
        std::stringstream ostr;
        ostr << Self::size() << "*" << queue_max << "Q";
        for (size_t i = 0; i < Self::size(); ++i)
            ostr << ":" << Self::operator[](i)->size();
        return ostr.str();
    }

    friend std::ostream & operator<<(std::ostream & out, FifoPtrs & self)
    {
        return out << self.str();
    }

private:
    const size_t queue_max;
};


class BlocksFifo : public SmallObjectFifo<DedupingBlockPtr>
{
    using Self = SmallObjectFifo<DedupingBlockPtr>;

public:
    BlocksFifo(size_t size)
        : Self(size)
    {}

    // Auto finished: return empty blocks when finished.
    DedupingBlockPtr pop()
    {
        std::lock_guard lock(mutex);
        if (last)
            return last;
        DedupingBlockPtr block = Self::pop();
        if (!block || !*block)
        {
            last = std::make_shared<DedupingBlock>(Block(), size_t(-1), 0);
            block = last;
        }
        return block;
    }

private:
    DedupingBlockPtr last;
    std::mutex mutex;
};

using BlocksFifoPtr = std::shared_ptr<BlocksFifo>;
using BlocksFifoPtrs = FifoPtrs<BlocksFifo>;


class DedupCursor
{
public:
    DedupCursor() {}

    DedupCursor(const DedupCursor & rhs)
        : block(rhs.block)
        , cursor(rhs.cursor)
    {
        if (block)
            cursor.order = block->versions()[cursor.pos];
    }

    DedupCursor & operator=(const DedupCursor & rhs)
    {
        block = rhs.block;
        cursor = rhs.cursor;
        if (block)
            cursor.order = block->versions()[cursor.pos];
        return *this;
    }

    DedupCursor(const SortCursorImpl & cursor_, const DedupingBlockPtr & block_)
        : block(block_)
        , cursor(cursor_)
    {
        cursor.order = block->versions()[cursor.pos];
    }

    operator bool()
    {
        return bool(block) && block->rows();
    }

    size_t setMaxOrder()
    {
        size_t order = cursor.order;
        cursor.order = size_t(-1);
        return order;
    }

    UInt64 version()
    {
        return block->versions()[cursor.pos];
    }

    void setDeleted(size_t row)
    {
        block->setDeleted(row);
    }

    size_t assignCursorPos(const DedupCursor & rhs)
    {
        size_t skipped = rhs.cursor.pos - cursor.pos;
        cursor.pos = rhs.cursor.pos;
        cursor.order = block->versions()[cursor.pos];
        return skipped;
    }

    // TODO: May have bug: should ignore cursor.order.
    size_t skipToGreaterEqualBySearch(DedupCursor & bound)
    {
        size_t origin_pos = cursor.pos;
        size_t low = cursor.pos;
        size_t high = rows() - 1;
        while (low < high)
        {
            cursor.pos = ((high - low) >> 1) + low;
            if (bound.greater(*this))
                low = cursor.pos + 1;
            else
                high = cursor.pos;
        }
        cursor.pos = high;
        cursor.order = block->versions()[cursor.pos];
        return cursor.pos - origin_pos;
    }

    // TODO: May have bug: should ignore cursor.order.
    size_t skipToGreaterEqualByNext(DedupCursor & bound)
    {
        size_t origin_pos = cursor.pos;
        while (bound.greater(*this))
            cursor.next();
        cursor.order = block->versions()[cursor.pos];
        return cursor.pos - origin_pos;
    }

    bool isTheSame(const DedupCursor & rhs)
    {
        return block->stream_position == rhs.block->stream_position && cursor.pos == rhs.cursor.pos;
    }

    size_t position()
    {
        if (!block)
            return size_t(-1);
        return block->stream_position;
    }

    size_t row()
    {
        return cursor.pos;
    }

    size_t order()
    {
        return cursor.order;
    }

    size_t rows()
    {
        return block->rows();
    }

    bool isLast()
    {
        return cursor.isLast();
    }

    operator Block()
    {
        return (Block)*block;
    }

    void next()
    {
        cursor.next();
        cursor.order = block->versions()[cursor.pos];
    }

    void backward()
    {
        cursor.pos = cursor.pos > 0 ? cursor.pos - 1 : 0;
        cursor.order = block->versions()[cursor.pos];
    }

    bool greater(const DedupCursor & rhs)
    {
        if (block->stream_position == rhs.block->stream_position)
            return (cursor.pos == rhs.cursor.pos) ? (cursor.order > rhs.cursor.order) : (cursor.pos > rhs.cursor.pos);
        SortCursorImpl * lc = const_cast<SortCursorImpl *>(&cursor);
        SortCursorImpl * rc = const_cast<SortCursorImpl *>(&rhs.cursor);
        if (!lc || !rc)
            throw("SortCursorImpl const_cast Failed!");
        return SortCursor(lc).greater(SortCursor(rc));
    }

    bool equal(const DedupCursor & rhs)
    {
        SortCursorImpl * lc = const_cast<SortCursorImpl *>(&cursor);
        SortCursorImpl * rc = const_cast<SortCursorImpl *>(&rhs.cursor);
        if (!lc || !rc)
            throw("SortCursorImpl const_cast Failed!");
        return SortCursor(lc).equalIgnOrder(SortCursor(rc));
    }

    // Inverst for pririoty queue
    bool operator<(const DedupCursor & rhs) const
    {
        return const_cast<DedupCursor *>(this)->greater(rhs);
    }

    String str()
    {
        std::stringstream ostr;

        if (!block)
        {
            ostr << "#?";
            return ostr.str();
        }
        else
            ostr << block->str();
        ostr << "/" << cursor.pos << "\\" << cursor.order;
        return ostr.str();
    }

    friend std::ostream & operator<<(std::ostream & out, DedupCursor & self)
    {
        return out << self.str();
    }

public:
    DedupingBlockPtr block;

protected:
    SortCursorImpl cursor;
};


// For easy copy and sharing cursor.pos
struct CursorPlainPtr
{
    DedupCursor * ptr;

    CursorPlainPtr()
        : ptr(0)
    {}

    CursorPlainPtr(DedupCursor * ptr_)
        : ptr(ptr_)
    {}

    operator bool() const
    {
        return ptr != 0;
    }

    DedupCursor & operator*()
    {
        return *ptr;
    }

    DedupCursor * operator->()
    {
        return ptr;
    }

    bool operator<(const CursorPlainPtr & rhs) const
    {
        return (*ptr) < (*rhs.ptr);
    }

    friend std::ostream & operator<<(std::ostream & out, CursorPlainPtr & self)
    {
        return (self.ptr == 0) ? (out << "null") : (out << (*self.ptr));
    }
};


class CursorQueue : public std::priority_queue<CursorPlainPtr>
{
public:
    String str()
    {
        std::stringstream ostr;
        ostr << "Q:" << size();

        CursorQueue copy = *this;
        while (!copy.empty())
        {
            CursorPlainPtr it = copy.top();
            copy.pop();
            ostr << "|" << it->str();
        }
        return ostr.str();
    }

    friend std::ostream & operator<<(std::ostream & out, CursorQueue & self)
    {
        return out << self.str();
    }
};


struct DedupBound : public DedupCursor
{
    bool is_bottom;

    DedupBound()
        : DedupCursor()
        , is_bottom(false)
    {}

    DedupBound(const DedupCursor & rhs)
        : DedupCursor(rhs)
        , is_bottom(false)
    {}

    DedupBound(const SortCursorImpl & cursor_, const DedupingBlockPtr & block_)
        : DedupCursor(cursor_, block_)
        , is_bottom(false)
    {}

    void setToBottom()
    {
        if (block->rows() > 1)
            cursor.pos = block->rows() - 1;
        else
            cursor.pos = 0;
        cursor.order = block->versions()[cursor.pos];
        is_bottom = true;
    }

    bool greater(const DedupBound & rhs)
    {
        return DedupCursor::greater(rhs);
    }

    bool greater(const DedupCursor & rhs)
    {
        return DedupCursor::greater(rhs);
    }

    String str()
    {
        std::stringstream ostr;
        ostr << DedupCursor::str();
        ostr << (is_bottom ? "L" : "F");
        return ostr.str();
    }

    friend std::ostream & operator<<(std::ostream & out, DedupBound & self)
    {
        return out << self.str();
    }
};

using DedupBoundPtr = std::shared_ptr<DedupBound>;


class BoundQueue : public std::priority_queue<DedupBound>
{
public:
    String str()
    {
        std::stringstream ostr;
        ostr << "Q:" << size();

        BoundQueue copy = *this;
        while (!copy.empty())
        {
            DedupBound it = copy.top();
            copy.pop();
            ostr << "|" << it.str();
        }
        return ostr.str();
    }

    friend std::ostream & operator<<(std::ostream & out, BoundQueue & self)
    {
        return out << self.str();
    }
};

using DedupCursorPtr = std::shared_ptr<DedupCursor>;
using DedupCursors = std::vector<DedupCursorPtr>;


class StreamMasks
{
public:
    using Data = BoolVec;

    StreamMasks(size_t size = 0)
        : data(size, 0)
        , sum(0)
    {}

    void assign(const Data & data_)
    {
        sum = 0;
        data = data_;
        for (Data::iterator it = data.begin(); it != data.end(); ++it)
            sum += (*it) ? 1 : 0;
    }

    size_t flags()
    {
        return sum;
    }

    void flag(size_t i)
    {
        if (!data[i])
            sum++;
        data[i] = true;
    }

    bool flaged(size_t i)
    {
        return data[i];
    }

    String str()
    {
        std::stringstream ostr;
        ostr << "[";
        for (Data::iterator it = data.begin(); it != data.end(); ++it)
            ostr << ((*it) ? "+" : "-");
        ostr << "]";
        return ostr.str();
    }

    friend std::ostream & operator<<(std::ostream & out, StreamMasks & self)
    {
        return out << self.str();
    }

private:
    Data data;
    size_t sum;
};


class IdGen
{
public:
    IdGen(size_t begin = 10000)
        : id(begin)
    {}

    void reset(size_t begin = 10000)
    {
        id = begin;
    }

    size_t operator++(int)
    {
        return id++;
    }

private:
    std::atomic<size_t> id;
};


template <class DedupCursor>
inline DedupCursor * dedupCursor(DedupCursor & lhs, DedupCursor & rhs)
{
    if (!lhs.equal(rhs))
        return 0;

    DedupCursor * deleted = 0;

    UInt64 version_lhs = lhs.block->versions()[lhs.row()];
    UInt64 version_rhs = rhs.block->versions()[rhs.row()];

    if (version_lhs > version_rhs)
        deleted = &rhs;
    else
        deleted = &lhs;

    deleted->block->setDeleted(deleted->row());
    return deleted;
}


inline Block dedupInBlock(Block block, const SortDescription & description, size_t stream_position = size_t(-1))
{
    if (!block)
        return block;
    if (!block.rows())
        return {};

    DedupingBlockPtr deduping_block = std::make_shared<DedupingBlock>(block, stream_position, false);
    SortCursorImpl cursor_impl(*deduping_block, description);
    DedupCursor cursor(cursor_impl, deduping_block);

    DedupCursor max;
    while (true)
    {
        if (max)
            dedupCursor(max, cursor);

        max = cursor;
        if (cursor.isLast())
            break;
        else
            cursor.next();
    }

    return deduping_block->finalize();
}


inline void splitBlockByGreaterThanKey(const SortDescription description, SortCursorImpl & key, const Block & block, Block & head, Block & tail)
{
    head = block.cloneEmpty();
    tail = block.cloneEmpty();

    SortCursorImpl cursor(block, description);
    size_t low = 0;
    size_t high = block.rows() - 1;

    while (low < high)
    {
        cursor.pos = ((high - low) >> 1) + low;
        if (!SortCursor(const_cast<SortCursorImpl *>(&cursor)).greater(SortCursor(const_cast<SortCursorImpl *>(&key))))
            low = cursor.pos + 1;
        else
            high = cursor.pos;
    }

    cursor.pos = high;
    if (!SortCursor(const_cast<SortCursorImpl *>(&cursor)).greater(SortCursor(const_cast<SortCursorImpl *>(&key))))
        throw Exception("Split block by key (greater than) failed, matched data not found.");

    size_t tail_first_row = high;

    for (size_t col = 0; col < block.columns(); ++col)
    {
        head.getByPosition(col).column = block.getByPosition(col).column->cut(0, tail_first_row);
        tail.getByPosition(col).column = block.getByPosition(col).column->cut(tail_first_row, block.rows() - tail_first_row);
    }
}


inline void readStreamToList(BlockInputStreamPtr input, BlocksList & output)
{
    input->readPrefix();
    while (true)
    {
        Block block = input->read();
        if (!block)
            break;
        output.emplace_back(block);
    }
    input->readSuffix();
}


inline void readStreamToBlock(BlockInputStreamPtr input, Block & output)
{
    input->readPrefix();
    while (true)
    {
        Block block = input->read();
        if (!block)
        {
            if (!output)
                throw Exception("Stream should have one block, but not found.");
            break;
        }
        else
        {
            if (output)
                throw Exception("Stream should have one block, too many found.");
            output = block;
        }
    }
    input->readSuffix();
}


// Not a effective implement, for test/dev only.
struct DebugPrinter
{
    template <class T>
    static void print(std::ostream & writer, const ColumnRawPtrs & columns)
    {
        for (size_t i = 0; i < columns.size(); i++)
        {
            print<T>(writer, columns, i);
            writer << std::endl;
        }
    }

    template <class T>
    static void print(std::ostream & writer, const ColumnRawPtrs & columns, size_t i)
    {
        for (size_t j = 0; j < columns.size(); j++)
        {
            const auto column = typeid_cast<const T *>(columns[j]);
            if (column)
                writer << column->getElement(i) << ", ";
        }
    }

    static void print(std::ostream & writer, const Block & block)
    {
        for (size_t i = 0; i < block.rows(); i++)
        {
            print(writer, block, i);
            writer << std::endl;
        }
    }

    static void print(std::ostream & writer, const Block & block, size_t i)
    {
        for (size_t j = 0; j < block.columns(); j++)
        {
            ColumnWithTypeAndName data = block.getByPosition(j);
            print(writer, data.column.get(), data.type.get(), i);
        }
    }

    static void print(std::ostream & writer, const IColumn * column, const IDataType * type, size_t i)
    {
        auto name = type->getName();

        if (std::string(type->getFamilyName()) == "FixedString")
            print(writer, typeid_cast<const ColumnFixedString *>(column)->getDataAt(i).toString());
        else if (name == "String")
            print(writer, typeid_cast<const ColumnString *>(column)->getDataAt(i).toString());
        else if (name == "DateTime")
            print(writer, (int64_t)typeid_cast<const ColumnUInt32 *>(column)->getElement(i));
        else if (name == "Int8")
            print(writer, (int64_t)typeid_cast<const ColumnInt8 *>(column)->getElement(i));
        else if (name == "Int16")
            print(writer, (int64_t)typeid_cast<const ColumnInt16 *>(column)->getElement(i));
        else if (name == "Int32")
            print(writer, (int64_t)typeid_cast<const ColumnInt32 *>(column)->getElement(i));
        else if (name == "Int64")
            print(writer, (int64_t)typeid_cast<const ColumnInt64 *>(column)->getElement(i));
        else if (name == "UInt8")
            print(writer, (int64_t)typeid_cast<const ColumnUInt8 *>(column)->getElement(i));
        else if (name == "UInt16")
            print(writer, (int64_t)typeid_cast<const ColumnUInt16 *>(column)->getElement(i));
        else if (name == "UInt32")
            print(writer, (int64_t)typeid_cast<const ColumnUInt32 *>(column)->getElement(i));
        else if (name == "UInt64")
            print(writer, (uint64_t)typeid_cast<const ColumnUInt64 *>(column)->getElement(i));
        else if (name == "Float32")
            print(writer, typeid_cast<const ColumnFloat32 *>(column)->getElement(i));
        else if (name == "Float64")
            print(writer, typeid_cast<const ColumnFloat64 *>(column)->getElement(i));
        else if (name == "Date")
            print(writer, typeid_cast<const ColumnUInt16 *>(column)->getElement(i));
        else
            throw Exception("Unknown type name: " + name);
    }

private:
    template <typename T>
    static void print(std::ostream & writer, const T v)
    {
        writer << v << ", ";
    }
};

} // namespace DB
