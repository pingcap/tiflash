#pragma once

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <IO/Operators.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Range.h>
#include <Storages/Transaction/Types.h>

#include <sstream>


namespace DB
{
namespace DM
{

using Handle        = DB::HandleID;
using PrimaryKey    = ColumnDefines;
using PrimaryKeyPtr = std::shared_ptr<PrimaryKey>;
class PKRange;
using PKRangePtr = std::shared_ptr<PKRange>;
using PKRanges   = std::vector<PKRange>;

/// PKValue is a pointer to a specific row of a column group.
struct PKValue
{
    PrimaryKey * pk;

    Block * block;
    size_t  row_id;

    String toString()
    {
        WriteBufferFromOwnString buf;

        if (pk->size() != 1)
            buf << "<";
        for (size_t i = 0; i < pk->size(); ++i)
        {
            (*pk)[i].type->serializeTextEscaped(*(block->getByPosition(i).column), row_id, buf);
            if (i != pk->size() - 1)
                buf << ",";
        }
        if ((*pk).size() != 1)
            buf << ">";

        return buf.str();
    }
};

namespace
{
inline int compareValuesAt(const PrimaryKey & pk, const Columns & left, size_t left_row_id, const Columns & right, size_t right_row_id)
{
    for (size_t i = 0; i < left.size(); ++i)
    {
        int res;
        if (pk[i].collator)
            res = left[i]->compareAtWithCollation(left_row_id, right_row_id, *(right[i]), 1, *pk[i].collator);
        else
            res = left[i]->compareAt(left_row_id, right_row_id, *(right[i]), 1);
        if (res != 0)
            return res;
    }
    return 0;
}

inline int compareValuesAt(const PrimaryKey & pk, const Columns & left, size_t left_row_id, const Block & right, size_t right_row_id)
{
    for (size_t i = 0; i < left.size(); ++i)
    {
        int res;
        if (pk[i].collator)
            res = left[i]->compareAtWithCollation(left_row_id, right_row_id, *(right.getByPosition(i).column), 1, *pk[i].collator);
        else
            res = left[i]->compareAt(left_row_id, right_row_id, *(right.getByPosition(i).column), 1);
        if (res != 0)
            return res;
    }
    return 0;
}

// https://en.cppreference.com/w/cpp/algorithm/lower_bound
size_t lowerBound(const PrimaryKey & pk, const Block & block, size_t first, size_t last, const Columns & columns, size_t edge_id)
{
    size_t count = last - first;
    while (count > 0)
    {
        size_t step  = count / 2;
        size_t index = first + step;
        if (compareValuesAt(pk, columns, edge_id, block, index) > 0)
        {
            first = index + 1;
            count -= step + 1;
        }
        else
            count = step;
    }
    return first;
}

} // namespace

/// PKRange is a range of primary values, which consisted by [start, end). Start is inclusive and end is exclusive.
class PKRange
{
private:
    /// Definition of primary key.
    PrimaryKeyPtr pk;

    /// Whether start or end is infinite or not. e.g. [true, false] means the start of this range is negative infinite,
    /// while the end of this range is some value defined in columns.
    bool is_infinite[2];
    /// In each column, value at index 0 represents start value, while end represents end value.
    Columns columns;

private:
    PKRange(const PrimaryKeyPtr & pk_, bool is_infinite_[2], Columns columns_) : pk(pk_), columns(columns_)
    {
        is_infinite[0] = is_infinite_[0];
        is_infinite[1] = is_infinite_[1];
    }

public:
    static const size_t START_INDEX = 0;
    static const size_t END_INDEX   = 1;

    struct Start
    {
        static const size_t Index = START_INDEX;

        const PKRange * range;
    };

    struct End
    {
        static const size_t Index = END_INDEX;

        const PKRange * range;
    };

    Start getStart() const { return {this}; }
    End   getEnd() const { return {this}; }

    template <size_t index>
    static int compareEdge(const PKRange & left, const PKRange & right)
    {
        if (left.is_infinite[index] && right.is_infinite[index])
            return 0;
        else if (left.is_infinite[index])
            if constexpr (index == START_INDEX)
                return -1;
            else
                return 1;
        else if (right.is_infinite[index])
            if constexpr (index == START_INDEX)
                return 1;
            else
                return -1;
        else
            return compareValuesAt(*left.pk, left.columns, index, right.columns, index);
    }

    class Creator
    {
    private:
        PrimaryKeyPtr  pk;
        bool           is_infinite[2];
        MutableColumns columns;

        template <size_t INDEX>
        void setEdge(const Columns & values, size_t row_id)
        {
            if (unlikely(values.size() != columns.size()))
                throw Exception("PKRangeCreator: Unexpected columns size " + DB::toString(values.size()) + ", expected "
                                + DB::toString(columns.size()));
            if (unlikely(columns[0]->size() != INDEX))
                throw Exception("PKRangeCreator: Cannot set edge when values's size is not " + DB::toString(INDEX));
            is_infinite[INDEX] = false;
            for (size_t i = 0; i < columns.size(); ++i)
                columns[i]->insertFrom(*(values[i]), row_id);
        }

        template <size_t INDEX>
        void setEdge(const Block & block, size_t row_id)
        {
            if (unlikely(columns[0]->size() != INDEX))
                throw Exception("PKRangeCreator: Cannot set edge when values's size is not " + DB::toString(INDEX));
            is_infinite[INDEX] = false;
            for (size_t i = 0; i < columns.size(); ++i)
                columns[i]->insertFrom(*(block.getByPosition(i).column), row_id);
        }

        template <class ValueType, size_t INDEX>
        void setEdge(const ValueType & value)
        {
            if (unlikely(columns[0]->size() != INDEX))
                throw Exception("PKRangeCreator: Cannot set edge when values's size is not " + DB::toString(INDEX));
            is_infinite[INDEX] = value.range->is_infinite[ValueType::Index];
            for (size_t i = 0; i < columns.size(); ++i)
                columns[i]->insertFrom(*(value.range->columns[i]), ValueType::Index);
        }

        template <size_t INDEX>
        void setEdgeInfinite()
        {
            if (unlikely(columns[0]->size() != INDEX))
                throw Exception("PKRangeCreator: Cannot set edge when values's size is not " + DB::toString(INDEX));

            is_infinite[INDEX] = true;
            for (size_t i = 0; i < columns.size(); ++i)
                columns[i]->insertDefault();
        }

    public:
        explicit Creator(const PrimaryKeyPtr & pk_) : pk(pk_), columns(pk->size())
        {
            for (size_t i = 0; i < pk->size(); ++i)
            {
                columns[i] = (*pk)[i].type->createColumn();
            }
        }

        Creator & setStart(const Columns & values, size_t row_id)
        {
            setEdge<START_INDEX>(values, row_id);
            return *this;
        }

        Creator & setStart(const Block & block, size_t row_id)
        {
            setEdge<START_INDEX>(block, row_id);
            return *this;
        }

        Creator & setStart(const Start & start)
        {
            setEdge<Start, START_INDEX>(start);
            return *this;
        }

        Creator & setStart(const End & end)
        {
            if (end.range->is_infinite[END_INDEX])
                throw Exception("PKRangeCreator: Cannot set start to max infinite");
            setEdge<End, START_INDEX>(end);
            return *this;
        }

        Creator & setStartInfinite()
        {
            setEdgeInfinite<START_INDEX>();
            return *this;
        }

        Creator & setEnd(const Columns & values, size_t row_id)
        {
            setEdge<END_INDEX>(values, row_id);
            return *this;
        }

        Creator & setEnd(const Block & block, size_t row_id)
        {
            setEdge<END_INDEX>(block, row_id);
            return *this;
        }

        Creator & setEnd(const Start & start)
        {
            if (start.range->is_infinite[START_INDEX])
                throw Exception("PKRangeCreator: Cannot set end to min infinite");
            setEdge<Start, END_INDEX>(start);
            return *this;
        }

        Creator & setEnd(const End & end)
        {
            setEdge<End, END_INDEX>(end);
            return *this;
        }

        Creator & setEndInfinite()
        {
            setEdgeInfinite<END_INDEX>();
            return *this;
        }

        PKRange getRange()
        {
            if (unlikely(columns[0]->size() != 2))
                throw Exception("Creator: Uncompleted range, with values's size " + DB::toString(columns[0]->size()) + ", expected 2");
            Columns columns_(columns.size());
            for (size_t i = 0; i < columns.size(); ++i)
                columns_[i] = std::move(columns[i]);
            return PKRange(pk, is_infinite, columns_);
        }
    };


public:
    static PKRange newAll(const PKRange & pk, const ICollatorPtr & collator);

    HandleRange toHandleRange() const
    {
        return {is_infinite[START_INDEX] ? HandleRange::MIN : columns[0]->getInt(0),
                is_infinite[END_INDEX] ? HandleRange::MAX : columns[0]->getInt(1)};
    }

    bool isStartInfinite() const { return is_infinite[START_INDEX]; }
    bool isEndInfinite() const { return is_infinite[END_INDEX]; }

    inline bool isAll() const { return is_infinite[START_INDEX] && is_infinite[END_INDEX]; }
    inline bool isEmpty() const
    {
        if (is_infinite[START_INDEX] || is_infinite[END_INDEX])
            return false;
        return compareValuesAt(*pk, columns, START_INDEX, columns, END_INDEX) >= 0;
    }

    static PKRange intersect(const PKRange & a, const PKRange & b)
    {
        const PKRange * start_use = compareEdge<START_INDEX>(a, b) >= 0 ? &a : &b;
        const PKRange * end_use   = compareEdge<END_INDEX>(a, b) <= 0 ? &a : &b;

        Creator creator(a.pk);
        creator.setStart(start_use->getStart());
        creator.setEnd(end_use->getEnd());
        return creator.getRange();
    }

    static PKRange fromHandleRange(const HandleRange & handle_range)
    {
        auto & handle_def = getExtraHandleColumnDefine();
        auto   column     = handle_def.type->createColumn();
        column->insert(handle_range.start);
        column->insert(handle_range.end);
        Columns columns{std::move(column)};

        PKRange::Creator pk_creator(std::make_shared<PrimaryKey>(ColumnDefines{handle_def}));
        if (handle_range.start == HandleRange::MIN)
            pk_creator.setStartInfinite();
        else
            pk_creator.setStart(columns, 0);
        if (handle_range.end == HandleRange::MAX)
            pk_creator.setEndInfinite();
        else
            pk_creator.setEnd(columns, 1);

        return pk_creator.getRange();
    }

    bool isIntersect(const PKRange & other) const { return !intersect(*this, other).isEmpty(); }

    static PKRange merge(const PKRange & a, const PKRange & b)
    {
        const PKRange * start_use = compareEdge<START_INDEX>(a, b) <= 0 ? &a : &b;
        const PKRange * end_use   = compareEdge<END_INDEX>(a, b) >= 0 ? &a : &b;

        Creator creator(a.pk);
        creator.setStart(start_use->getStart());
        creator.setEnd(end_use->getEnd());
        return creator.getRange();
    }

    static PKRange merge(const PKRanges & ranges)
    {
        const PKRange * start_use = &(ranges.at(0));
        const PKRange * end_use   = &(ranges.at(0));
        for (auto & range : ranges)
        {
            if (compareEdge<START_INDEX>(*start_use, range) <= 0)
                start_use = &range;
            if (compareEdge<END_INDEX>(*end_use, range) <= 0)
                end_use = &range;
        }

        Creator creator(start_use->pk);
        creator.setStart(start_use->getStart());
        creator.setEnd(end_use->getEnd());
        return creator.getRange();
    }

    int startCompareWith(const Start & value) const { return compareEdge<START_INDEX>(*this, *(value.range)); }

    int startCompareWith(const End & value) const
    {
        if (is_infinite[START_INDEX])
            return -1;
        else if (value.range->is_infinite[END_INDEX])
            return -1;
        else
            return compareValuesAt(*pk, columns, START_INDEX, value.range->columns, END_INDEX);
    }

    int endCompareWith(const Start & value) const
    {
        if (is_infinite[END_INDEX])
            return 1;
        else if (value.range->is_infinite[START_INDEX])
            return 1;
        else
            return compareValuesAt(*pk, columns, END_INDEX, value.range->columns, START_INDEX);
    }

    int endCompareWith(const End & value) const { return compareEdge<END_INDEX>(*this, *(value.range)); }

    bool checkStart(const Block & block, size_t row_id) const
    {
        return is_infinite[START_INDEX] || compareValuesAt(*pk, columns, START_INDEX, block, row_id) <= 0;
    }

    bool checkEnd(const Block & block, size_t row_id) const
    {
        return is_infinite[END_INDEX] || compareValuesAt(*pk, columns, END_INDEX, block, row_id) > 0;
    }

    bool endLessThan(const Block & block, size_t row_id) const
    {
        return !is_infinite[END_INDEX] && compareValuesAt(*pk, columns, END_INDEX, block, row_id) < 0;
    }

    bool check(const Block & block, size_t row_id) const { return checkStart(block, row_id) && checkEnd(block, row_id); }

    std::pair<size_t, size_t> getPosRange(const Block & block, const size_t offset, const size_t limit) const
    {
        size_t start_index
            = (is_infinite[START_INDEX] || check(block, offset)) ? offset : lowerBound(*pk, block, offset, limit, columns, START_INDEX);
        size_t end_index = (is_infinite[END_INDEX] || check(block, offset + limit))
            ? offset + limit
            : lowerBound(*pk, block, offset, limit, columns, END_INDEX);

        return {start_index, end_index - start_index};
    }

    String toString() const
    {
        WriteBufferFromOwnString buf;

        auto print_value = [&](size_t index) {
            if (columns.size() != 1)
                buf << "<";
            for (size_t i = 0; i < columns.size(); ++i)
            {
                (*pk)[i].type->serializeTextEscaped(*(columns[i]), index, buf);
                if (i != columns.size() - 1)
                    buf << ",";
            }
            if (columns.size() != 1)
                buf << ">";
        };

        buf << "[";
        if (is_infinite[START_INDEX])
            buf << "-Inf";
        else
        {
            print_value(START_INDEX);
        }
        buf << ",";
        if (is_infinite[END_INDEX])
            buf << "+Inf";
        else
        {
            print_value(END_INDEX);
        }
        buf << ")";

        return buf.str();
    }

    bool operator==(const PKRange & other) const
    {
        return compareEdge<START_INDEX>(*this, other) == 0 && compareEdge<END_INDEX>(*this, other) == 0;
    }
    bool operator!=(const PKRange & other) const { return !(*this == other); }
};

inline bool operator<(const PKRange::Start & a, const PKRange::Start & b)
{
    return a.range->startCompareWith(b) < 0;
}

inline bool operator<(const PKRange::End & a, const PKRange::End & b)
{
    return a.range->endCompareWith(b) < 0;
}

inline bool operator<(const PKRange::End & a, const PKRange::Start & b)
{
    return a.range->endCompareWith(b) < 0;
}

inline bool operator<(const PKRange::Start & a, const PKRange::End & b)
{
    return a.range->startCompareWith(b) < 0;
}

inline bool operator<(const PKValue & a, const PKRange::End & b)
{
    return b.range->checkEnd(*(a.block), a.row_id);
}

inline bool operator<(const PKRange::End & a, const PKValue & b)
{
    return a.range->endLessThan(*(b.block), b.row_id);
}

} // namespace DM
} // namespace DB