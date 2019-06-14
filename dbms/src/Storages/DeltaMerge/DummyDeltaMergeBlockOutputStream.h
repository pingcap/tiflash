#pragma once

#include <type_traits>

#include <Common/Exception.h>

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>

#include <Interpreters/sortBlock.h>

#include <common/logger_useful.h>

#include <Storages/DeltaMerge/DummyValueSpace.h>

namespace DB
{

enum class Action : UInt8
{
    Insert = 1,
    Upsert = 2,
    Delete = 3,
    Update = 4
};

inline int compareTuple(const MyValueSpacePtr &   left,
                        size_t                  l_pos,
                        const MyValueSpacePtr &   right,
                        size_t                  r_pos,
                        const SortDescription & sort_desc,
                        const Ids &             vs_column_offsets)
{
    auto num_sort_columns = sort_desc.size();
    for (size_t i = 0; i < num_sort_columns; ++i)
    {
        int direction       = sort_desc[i].direction;
        int nulls_direction = sort_desc[i].nulls_direction;

        auto col_offset = vs_column_offsets[i];

        auto & left_col        = left->columnAt(col_offset).chunk(l_pos);
        auto   left_col_offset = left->columnAt(col_offset).offsetInChunk(l_pos);

        auto & right_col        = right->columnAt(col_offset).chunk(r_pos);
        auto   right_col_offset = right->columnAt(col_offset).offsetInChunk(r_pos);

        int res = direction * left_col.compareAt(left_col_offset, right_col_offset, right_col, nulls_direction);
        if (res != 0)
            return res;
    }
    return 0;
}

inline int compareTuple(const Columns & left, size_t l_pos, const Columns & right, size_t r_pos, const SortDescription & sort_desc)
{
    auto num_sort_columns = sort_desc.size();
    for (size_t i = 0; i < num_sort_columns; ++i)
    {
        int direction       = sort_desc[i].direction;
        int nulls_direction = sort_desc[i].nulls_direction;
        int res             = direction * left[i]->compareAt(l_pos, r_pos, *(right[i]), nulls_direction);
        if (res != 0)
            return res;
    }
    return 0;
}

struct RidGenerator
{
    BlockInputStreamPtr     stable_stream;
    const SortDescription & sort_desc;
    const size_t            num_sort_columns;

    Columns modify_block_columns;
    size_t  modify_block_rows;
    size_t  modify_block_pos = 0;
    // Whether this row's pk duplicates with the next one, if so, they share the same rid.
    std::vector<bool> modify_block_dup_next;
    // Whether current row's pk duplicates with the previous one. Used by Upsert.
    bool dup_prev = false;

    Columns stable_block_columns;
    size_t  stable_block_rows = 0;
    size_t  stable_block_pos  = 0;
    bool    stable_finished   = false;

    UInt64 rid = 0;

    RidGenerator(const BlockInputStreamPtr & stable_stream_, const SortDescription & sort_desc_, Action action, const Block & modify_block)
        : stable_stream(stable_stream_),
          sort_desc(sort_desc_),
          num_sort_columns(sort_desc.size()),
          modify_block_rows(modify_block.rows()),
          modify_block_dup_next(modify_block_rows, false)
    {
        stable_stream->readPrefix();

        for (size_t i = 0; i < num_sort_columns; ++i)
            modify_block_columns.push_back(modify_block.getByName(sort_desc[i].column_name).column);

        // Check continually tuples with identical primary key.
        for (size_t row = 0; row < modify_block_rows - 1; ++row)
        {
            auto res = compareTuple(modify_block_columns, row, modify_block_columns, row + 1, sort_desc);
            if (unlikely(res > 0))
                throw Exception("Algorithm broken: res should not > 0");
            if (res == 0 && (action == Action::Insert || action == Action::Delete))
                throw Exception("Duplicate primary key exist in modify block");
            modify_block_dup_next[row] = (res == 0);
        }
    }

    ~RidGenerator() { stable_stream->readSuffix(); }

    inline int compareModifyToStable() const
    {
        return compareTuple(modify_block_columns, modify_block_pos, stable_block_columns, stable_block_pos, sort_desc);
    }

    inline bool fillStableBlockIfNeed()
    {
        if (stable_finished)
            return false;
        if (!stable_block_columns.empty() && stable_block_pos < stable_block_rows)
            return true;
        stable_block_columns.clear();
        stable_block_rows = 0;
        stable_block_pos  = 0;
        auto block        = stable_stream->read();
        if (!block)
        {
            stable_finished = true;
            return false;
        }
        stable_block_rows = block.rows();
        for (size_t column_id = 0; column_id < num_sort_columns; ++column_id)
            stable_block_columns.push_back(block.getByName(sort_desc[column_id].column_name).column);

#ifndef NDEBUG
        for (size_t row = 0; row < stable_block_rows - 1; ++row)
        {
            auto res = compareTuple(stable_block_columns, row, stable_block_columns, row + 1, sort_desc);
            if (unlikely(res >= 0))
                throw Exception("Algorithm broken: res should not >= 0");
        }
#endif
        return true;
    }

    UInt64 nextForInsert()
    {
        while (fillStableBlockIfNeed())
        {
            auto res = compareModifyToStable();
            if (res == 0)
                throw Exception("Duplicate primary key already exists in table");
            else if (res < 0)
            {
                ++modify_block_pos;
                return rid++;
            }
            else
            {
                ++stable_block_pos;
                ++rid;
            }
        }

        ++modify_block_pos;
        return rid++;
    }

    std::pair<UInt64, bool> nextForUpsert()
    {
        while (fillStableBlockIfNeed())
        {
            auto res = compareModifyToStable();
            if (res > 0)
            {
                ++stable_block_pos;
                ++rid;
                continue;
            }

            auto cur_dup_prev = dup_prev;
            dup_prev          = modify_block_dup_next[modify_block_pos++];
            if (res == 0)
            {
                if (dup_prev)
                    return {rid, true};
                else
                {
                    ++stable_block_pos;
                    return {rid++, true};
                }
            }
            else if (res < 0)
            {
                if (dup_prev)
                    return {rid, cur_dup_prev};
                else
                    return {rid++, cur_dup_prev};
            }
        }

        auto cur_dup_prev = dup_prev;
        dup_prev          = modify_block_dup_next[modify_block_pos];
        ++modify_block_pos;
        if (dup_prev)
            return {rid, cur_dup_prev};
        else
            return {rid++, cur_dup_prev};
    }

    UInt64 nextForDelete()
    {
        while (fillStableBlockIfNeed())
        {
            auto res = compareModifyToStable();
            if (res == 0)
            {
                ++modify_block_pos;
                ++stable_block_pos;
                return rid;
            }
            else if (res > 0)
            {
                ++stable_block_pos;
                ++rid;
            }
            else // res < 0
            {
                throw Exception("Rows not found");
            }
        }
        throw Exception("Rows not found");
    }

    UInt64 nextForUpdate()
    {
        while (fillStableBlockIfNeed())
        {
            auto res = compareModifyToStable();
            if (res == 0)
            {
                if (modify_block_dup_next[modify_block_pos++])
                    return rid;
                else
                {
                    ++stable_block_pos;
                    return rid++;
                }
            }
            else if (res > 0)
            {
                ++stable_block_pos;
                ++rid;
            }
            else // res < 0
            {
                throw Exception("Rows not found");
            }
        }
        throw Exception("Rows not found");
    }
};

class DummyDeltaMergeBlockOutputStream final : public IBlockOutputStream
{
public:
    using InputStreamCreator = std::function<BlockInputStreamPtr()>;
    using Flusher            = std::function<void()>;

    DummyDeltaMergeBlockOutputStream( //
        const InputStreamCreator & stable_input_stream_creator_,
        MyDeltaTreePtr &           delta_tree_,
        const Action               action_,
        const SortDescription &    primary_sort_descr_,
        const Flusher              flusher_,
        const size_t               flush_limit_)
        : stable_input_stream_creator(stable_input_stream_creator_),
          delta_tree(delta_tree_),
          action(action_),
          primary_sort_descr(primary_sort_descr_),
          flusher(flusher_),
          flush_limit(flush_limit_),
          log(&Logger::get("DeltaMergeBlockOutputStream"))
    {
    }

    Block getHeader() const override
    {
        Block res;
        for (auto & nt : delta_tree->insert_value_space->namesAndTypes())
        {
            res.insert({nt.type, nt.name});
        }
        return res;
    }

    void writeSuffix() override
    {
        delta_tree->insert_value_space->gc();
        delta_tree->modify_value_space->gc();
    }

    Block sortBlock(const Block & block)
    {
        if (isAlreadySorted(block, primary_sort_descr))
            return block;

        Block                res;
        IColumn::Permutation perm;
        stableGetPermutation(block, primary_sort_descr, perm);
        for (const auto & col : block)
            res.insert(ColumnWithTypeAndName(col.column->permute(perm, 0), col.type, col.name));
        return res;
    }

    void write(const Block & block_) override
    {
        if (!block_.rows())
            return;

        Block block      = sortBlock(block_);
        auto  block_rows = block.rows();

        RidGenerator rid_gen(stable_input_stream_creator(), primary_sort_descr, action, block);

        switch (action)
        {
        case Action::Insert:
        {
            auto value_ids = delta_tree->insert_value_space->addFromInsert(block);
            Ids  rids(block_rows);
            for (size_t i = 0; i < block_rows; ++i)
                rids[i] = rid_gen.nextForInsert();
            for (size_t i = 0; i < block_rows; ++i)
                delta_tree->addInsert(rids[i], value_ids[i]);
            break;
        }
        case Action::Upsert:
        {
            auto value_ids = delta_tree->insert_value_space->addFromInsert(block);
            using Rids     = std::vector<std::pair<UInt64, bool>>;
            Rids rids(block_rows);
            for (size_t i = 0; i < block_rows; ++i)
                rids[i] = rid_gen.nextForUpsert();
            for (size_t i = 0; i < block_rows; ++i)
            {
                const auto & rid_dup = rids[i];
                if (rid_dup.second)
                    delta_tree->addDelete(rid_dup.first);
                delta_tree->addInsert(rid_dup.first, value_ids[i]);
            }
            break;
        }
        case Action::Delete:
        {
            Ids rids(block_rows);
            for (size_t i = 0; i < block_rows; ++i)
                rids[i] = rid_gen.nextForDelete();
            for (size_t i = 0; i < block_rows; ++i)
                delta_tree->addDelete(rids[i]);
            break;
        }
        case Action::Update:
        {
            auto tuples = delta_tree->insert_value_space->addFromModify(block);
            Ids  rids(block_rows);
            for (size_t i = 0; i < block_rows; ++i)
                rids[i] = rid_gen.nextForUpdate();
            for (size_t i = 0; i < block_rows; ++i)
                delta_tree->addModify(rids[i], tuples[i]);
            break;
        }
        default:
            throw Exception("Illegal action");
        }

        if (delta_tree->numEntries() >= flush_limit)
            flusher();
    }

private:
    InputStreamCreator stable_input_stream_creator;

    MyDeltaTreePtr & delta_tree; // it could be changed by flusher.
    Action           action;
    SortDescription  primary_sort_descr;

    Flusher flusher;
    size_t  flush_limit;

    Logger * log;
};
} // namespace DB