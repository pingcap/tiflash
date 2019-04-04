#pragma once

#include <algorithm>
#include <set>

#include <common/logger_useful.h>

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Core/SortDescription.h>

#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Tuple.h>

namespace DB
{

/// Store the tuple values of delta tree.
/// Garbage collection is poorly supported.
class MemoryValueSpace
{
    using ValueSpace = MemoryValueSpace;

public:
    MemoryValueSpace(String name, const NamesAndTypesList & name_types, const SortDescription & sort_desc);

    ~MemoryValueSpace() { LOG_TRACE(log, "MM free"); }

    Ids       addFromInsert(const Block & block);
    RefTuples addFromModify(const Block & block);

    void removeFromInsert(UInt64 id);
    void removeFromModify(UInt64 id, size_t column_id);

    UInt64 withModify(UInt64 old_tuple_id, const ValueSpace & modify_value_space, const RefTuple & tuple);

    void gc();

    struct SplitColumn
    {
        static constexpr size_t SPLIT_SIZE = 65536;

        using DeleteMark  = std::vector<bool>;
        using DeleteMarks = std::vector<DeleteMark>;

        String         name;
        DataTypePtr    type;
        MutableColumns columns;
        DeleteMarks    delete_marks;

        SplitColumn(const String & name_, const DataTypePtr type_) : name(name_), type(type_)
        {
            columns.push_back(type->createColumn());
            delete_marks.emplace_back();
        }

        void append(const IColumn & src)
        {
            size_t remaining = src.size();
            while (remaining)
            {
                auto & last      = columns.back();
                auto & last_mark = delete_marks.back();

                auto append = std::min(SPLIT_SIZE - last->size(), remaining);

                last->insertRangeFrom(src, src.size() - remaining, append);
                for (size_t i = 0; i < append; ++i)
                    last_mark.push_back(false);

                remaining -= append;

                if (last->size() >= SPLIT_SIZE)
                {
                    columns.push_back(last->cloneEmpty());
                    delete_marks.emplace_back();
                }
            }
        }

        void append(const IColumn & src, size_t n)
        {
            auto & last      = columns.back();
            auto & last_mark = delete_marks.back();

            last->insertFrom(src, n);
            last_mark.push_back(false);

            if (last->size() >= SPLIT_SIZE)
            {
                columns.push_back(last->cloneEmpty());
                delete_marks.emplace_back();
            }
        }

        void append(const SplitColumn & split_column, size_t id)
        {
            const auto & column = split_column.columns[id / SPLIT_SIZE];
            append(*column, id % SPLIT_SIZE);
        }

        void remove(UInt64 id)
        {
            auto & mark           = delete_marks[id / SPLIT_SIZE];
            mark[id % SPLIT_SIZE] = true;
        }

        void gc()
        {
            /// Don't gc the last one
            for (size_t i = 0; i < delete_marks.size() - 1; ++i)
            {
                auto & mark = delete_marks[i];
                if (!mark.size())
                    continue;
                bool has_deleted = true;
                for (bool v : mark)
                    has_deleted &= v;
                if (has_deleted)
                {
                    DeleteMark empty_mark;
                    mark.swap(empty_mark);
                    columns[i] = type->createColumn();
                }
            }
        }

        IColumn & chunk(UInt64 id) const { return *(columns[id / SPLIT_SIZE]); }
        UInt64    offsetInChunk(UInt64 id) const { return id % SPLIT_SIZE; }

        size_t size() const { return (columns.size() - 1) * SPLIT_SIZE + columns.back()->size(); }
    };

    const SplitColumn &   columnAt(size_t column_id) const { return split_columns[column_id]; }
    const NamesAndTypes & namesAndTypes() const { return names_and_types; }

private:
    NamesAndTypes            names_and_types;
    std::vector<SplitColumn> split_columns;
    std::set<String>         sort_column_names;
    size_t                   num_columns;

    Logger * log;
};

} // namespace DB