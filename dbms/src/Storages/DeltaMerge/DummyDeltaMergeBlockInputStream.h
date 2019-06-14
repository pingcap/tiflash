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
#include <Storages/DeltaMerge/DummyValueSpace.h>

namespace DB
{

class DummyDeltaMergeBlockInputStream final : public IProfilingBlockInputStream
{
    static constexpr size_t UNLIMITED = std::numeric_limits<UInt64>::max();

private:
    MyDeltaTreePtr delta_tree;

    MyValueSpacePtr insert_value_space;
    MyValueSpacePtr modify_value_space;

    EntryIterator entry_it;
    EntryIterator entry_end;

    Block  header;
    size_t num_columns;
    size_t expected_block_size;
    Ids    vs_column_offsets;

    size_t skip_rows;

    bool    finished = false;
    Columns stable_block_columns;
    size_t  stable_block_rows = 0;
    size_t  stable_block_pos  = 0;

    size_t sid = 0;

public:
    DummyDeltaMergeBlockInputStream(const BlockInputStreamPtr & stable_input_stream,
                                    const MyDeltaTreePtr &      delta_tree_,
                                    size_t                      expected_block_size_)
        : delta_tree(delta_tree_),
          insert_value_space(delta_tree->insert_value_space),
          modify_value_space(delta_tree->modify_value_space),
          entry_it(delta_tree->begin()),
          entry_end(delta_tree->end()),
          expected_block_size(expected_block_size_)
    {
        children.push_back(stable_input_stream);
        header      = stable_input_stream->getHeader();
        num_columns = header.columns();

        const auto & names_and_types = insert_value_space->namesAndTypes();
        for (const auto & c : header)
        {
            for (size_t i = 0; i < names_and_types.size(); ++i)
            {
                if (c.name == names_and_types[i].name)
                    vs_column_offsets.push_back(i);
            }
        }

        skip_rows = entry_it != entry_end ? entry_it.getSid() : UNLIMITED;
    }

    String getName() const override { return "DeltaMerge"; }
    Block  getHeader() const override { return children.back()->getHeader(); }

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
        auto block        = children.back()->read();
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
            for (size_t column_id = 0; column_id < num_columns; ++column_id)
            {
                const auto & split_column = insert_value_space->columnAt(vs_column_offsets[column_id]);
                output_columns[column_id]->insertFrom(split_column.chunk(value_id), split_column.offsetInChunk(value_id));
            }
        }
        else
        {
            // Modify
            if (!fillStableBlockIfNeed())
                return false;
            std::vector<size_t> has_modifies(num_columns, INVALID_ID);

            if (entry_it.getType() == DT_MULTI_MOD)
            {
                DTModifiesPtr modifies = reinterpret_cast<DTModifiesPtr>(entry_it.getValue());
                for (const auto & m : *modifies)
                    has_modifies[m.column_id] = m.value;
            }
            else
            {
                has_modifies[entry_it.getType()] = entry_it.getValue();
            }

            for (size_t column_id = 0; column_id < num_columns; ++column_id)
            {
                auto offset = vs_column_offsets[column_id];
                if (has_modifies[offset] == INVALID_ID)
                {
                    output_columns[column_id]->insertFrom(*stable_block_columns[column_id], stable_block_pos);
                }
                else
                {
                    auto & split_column = insert_value_space->columnAt(offset);
                    auto   value_id     = has_modifies[offset];
                    output_columns[column_id]->insertFrom(split_column.chunk(value_id), split_column.offsetInChunk(value_id));
                }
            }

            ++stable_block_pos;
            ++sid;
        }

        ++entry_it;
        skip_rows = (entry_it != entry_end ? entry_it.getSid() : UNLIMITED) - sid;

        return true;
    }
};
} // namespace DB