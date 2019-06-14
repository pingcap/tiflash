#pragma once

#include <Common/Exception.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>

#include <Core/Block.h>

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaTree.h>

namespace DB
{

// Note that the columns in stable input stream and value space must exactly the same, include the name, type, and id.
template <class DeltaTree, class DeltaValueSpace>
class DeltaMergeBlockInputStream final : public IProfilingBlockInputStream
{
    static constexpr size_t UNLIMITED = std::numeric_limits<UInt64>::max();

private:
    using DeltaValueSpacePtr = std::shared_ptr<DeltaValueSpace>;
    using SharedLock         = std::shared_lock<std::shared_mutex>;

    BlockInputStreamPtr stable_input_stream;

    DeltaValueSpacePtr delta_value_space;
    size_t             expected_block_size;

    EntryIterator entry_it;
    EntryIterator entry_end;

    Block  header;
    size_t num_columns;

    size_t skip_rows;

    bool    finished = false;
    Columns stable_block_columns;
    size_t  stable_block_rows = 0;
    size_t  stable_block_pos  = 0;

    size_t sid = 0;

    SharedLock lock;

public:
    DeltaMergeBlockInputStream(const BlockInputStreamPtr & stable_input_stream_,
                               DeltaTree &                 delta_tree,
                               const DeltaValueSpacePtr &  delta_value_space_,
                               size_t                      expected_block_size_,
                               SharedLock &&               lock)
        : stable_input_stream(stable_input_stream_),
          delta_value_space(delta_value_space_),
          expected_block_size(expected_block_size_),
          entry_it(delta_tree.begin()),
          entry_end(delta_tree.end()),
          lock(std::move(lock))
    {
        header      = stable_input_stream->getHeader();
        num_columns = header.columns();

        skip_rows = entry_it != entry_end ? entry_it.getSid() : UNLIMITED;
    }

    String getName() const override { return "DeltaMerge"; }
    Block  getHeader() const override { return header.cloneEmpty(); }

protected:
    Block readImpl() override
    {
        if (finished)
            return {};
        MutableColumns columns;
        initOutputColumns(columns);
        if (columns.empty())
            return {};

        while (columns[0]->size() < expected_block_size)
        {
            if (!next(columns))
            {
                finished = true;
                break;
            }
        }
        if (!columns.at(0)->size())
            return {};

        return header.cloneWithColumns(std::move(columns));
    }

private:
    inline void initOutputColumns(MutableColumns & columns)
    {
        columns.resize(num_columns);

        for (size_t i = 0; i < num_columns; ++i)
        {
            columns[i] = header.safeGetByPosition(i).column->cloneEmpty();
            columns[i]->reserve(expected_block_size);
        }
    }

    inline bool fillStableBlockIfNeed()
    {
        if (!stable_block_columns.empty() && stable_block_pos < stable_block_rows)
            return true;

        stable_block_columns.clear();
        stable_block_rows = 0;
        stable_block_pos  = 0;
        auto block        = stable_input_stream->read();
        if (!block)
            return false;

        stable_block_rows = block.rows();
        for (size_t column_id = 0; column_id < num_columns; ++column_id)
            stable_block_columns.push_back(block.getByPosition(column_id).column);
        return true;
    }

    inline void ignoreStableTuples(size_t n)
    {
        while (n)
        {
            if (!fillStableBlockIfNeed())
                throw Exception("Not more rows to ignore!");
            auto skip = std::min(stable_block_columns.at(0)->size() - stable_block_pos, n);
            stable_block_pos += skip;
            n -= skip;
        }
    }

    bool next(MutableColumns & output_columns)
    {
        while (skip_rows || (entry_it != entry_end && entry_it.getType() == DT_DEL))
        {
            if (skip_rows)
            {
                if (!fillStableBlockIfNeed())
                    return false;
                auto in_output_rows = output_columns.at(0)->size();
                auto in_input_rows  = stable_block_columns.at(0)->size();
                if (!in_output_rows && !stable_block_pos && in_input_rows <= skip_rows)
                {
                    // Simply return stable_input_stream block.
                    for (size_t column_id = 0; column_id < output_columns.size(); ++column_id)
                        output_columns[column_id] = (*std::move(stable_block_columns[column_id])).mutate();

                    stable_block_pos += in_input_rows;
                    skip_rows -= in_input_rows;
                    sid += in_input_rows;
                }
                else
                {
                    auto copy_rows = std::min(expected_block_size - in_output_rows, in_input_rows - stable_block_pos);
                    copy_rows      = std::min(copy_rows, skip_rows);

                    for (size_t column_id = 0; column_id < num_columns; ++column_id)
                        output_columns[column_id]->insertRangeFrom(*stable_block_columns[column_id], stable_block_pos, copy_rows);

                    stable_block_pos += copy_rows;
                    skip_rows -= copy_rows;
                    sid += copy_rows;
                }
                return true;
            }
            else
            {
                if (!fillStableBlockIfNeed())
                    throw Exception("No more rows to delete!");
                if (unlikely(sid != entry_it.getSid()))
                    throw Exception("Algorithm broken!");

                ignoreStableTuples(entry_it.getValue());
                sid += entry_it.getValue();

                ++entry_it;
                skip_rows = (entry_it != entry_end ? entry_it.getSid() : UNLIMITED) - sid;
            }
        }

        if (unlikely(sid != entry_it.getSid()))
            throw Exception("Algorithm broken!");

        if (entry_it.getType() == DT_INS)
        {
            auto value_id = entry_it.getValue();
            for (size_t index = 0; index < num_columns; ++index)
                delta_value_space->insertValue(*output_columns[index], index, value_id);
        }
        else
        {
            throw Exception("Entry type " + DTTypeString(entry_it.getType()) + " is not supported");
        }

        ++entry_it;
        skip_rows = (entry_it != entry_end ? entry_it.getSid() : UNLIMITED) - sid;

        return true;
    }
};
} // namespace DB