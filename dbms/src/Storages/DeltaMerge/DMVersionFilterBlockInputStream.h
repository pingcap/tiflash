#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>

namespace DB
{

/// Use the latest rows. For rows with the same handle, only take the rows with biggest version and version <= version_limit.
static constexpr int DM_VESION_FILTER_MODE_MVCC = 0;
/// Remove the outdated rows. For rows with the same handle, take all rows with version >= version_limit. And if all of them are smaller than version_limit, then take the biggest one, if it is not deleted.
static constexpr int DM_VESION_FILTER_MODE_COMPACT = 1;

template <int MODE>
class DMVersionFilterBlockInputStream : public IProfilingBlockInputStream
{
public:
    DMVersionFilterBlockInputStream(const BlockInputStreamPtr & input, const ColumnDefine & handle_define, UInt64 version_limit_)
        : version_limit(version_limit_),
          header(input->getHeader()),
          handle_col_pos(header.getPositionByName(handle_define.name)),
          version_col_pos(header.getPositionByName(VERSION_COLUMN_NAME)),
          delete_col_pos(header.getPositionByName(TAG_COLUMN_NAME))
    {
        children.push_back(input);
    }

    String getName() const override { return "DeltaMergeVersionFilter"; }
    Block  getHeader() const override { return header; }

protected:
    Block readImpl() override
    {
        while (true)
        {
            if (!raw_block)
            {
                if (!initNextBlock())
                    return {};
            }

            Block  cur_raw_block = raw_block;
            size_t rows          = cur_raw_block.rows();

            IColumn::Filter filter(rows);

            size_t passed_count = 0;
            for (size_t i = 0; i < rows - 1; ++i)
            {
#define cur_handle (*handle_col_data)[i]
#define next_handle (*handle_col_data)[i + 1]
#define cur_version (*version_col_data)[i]
#define next_version (*version_col_data)[i + 1]
#define deleted (*delete_col_data)[i]
                bool ok;
                if constexpr (MODE == DM_VESION_FILTER_MODE_MVCC)
                {
                    ok = !deleted && cur_version <= version_limit && (cur_handle != next_handle || next_version > version_limit);
                }
                else if constexpr (MODE == DM_VESION_FILTER_MODE_COMPACT)
                {
                    ok = cur_version >= version_limit || (cur_handle != next_handle && !deleted);
                }
                else
                {
                    throw Exception("Unsupported mode");
                }
#undef cur_handle
#undef next_handle
#undef cur_version
#undef next_version
#undef deleted
                filter[i] = ok;
                passed_count += ok;
            }

            {
                // Now let's handle the last row of current block.
                auto cur_handle  = (*handle_col_data)[rows - 1];
                auto cur_version = (*version_col_data)[rows - 1];
                auto deleted     = (*delete_col_data)[rows - 1];
                if (!initNextBlock())
                {
                    // No more block.
                    bool ok;
                    if constexpr (MODE == DM_VESION_FILTER_MODE_MVCC)
                    {
                        ok = !deleted && cur_version <= version_limit;
                    }
                    else if (MODE == DM_VESION_FILTER_MODE_COMPACT)
                    {
                        ok = cur_version >= version_limit || !deleted;
                    }
                    else
                    {
                        throw Exception("Unsupported mode");
                    }

                    filter[rows - 1] = ok;
                    passed_count += ok;
                }
                else
                {
                    auto next_handle  = (*handle_col_data)[0];
                    auto next_version = (*version_col_data)[0];
                    bool ok;
                    if constexpr (MODE == DM_VESION_FILTER_MODE_MVCC)
                    {
                        ok = !deleted && cur_version <= version_limit && (cur_handle != next_handle || next_version > version_limit);
                    }
                    else if (MODE == DM_VESION_FILTER_MODE_COMPACT)
                    {
                        ok = cur_version >= version_limit || (cur_handle != next_handle && !deleted);
                    }
                    else
                    {
                        throw Exception("Unsupported mode");
                    }
                    filter[rows - 1] = ok;
                    passed_count += ok;
                }
            }

            if (!passed_count)
                continue;
            if (passed_count == rows)
                return cur_raw_block;

            for (size_t i = 0; i < cur_raw_block.columns(); ++i)
            {
                auto & column = cur_raw_block.getByPosition(i);
                column.column = column.column->filter(filter, passed_count);
            }
            return cur_raw_block;
        }
    }

    bool initNextBlock()
    {
        raw_block = readNextBlock();
        if (!raw_block)
        {
            handle_col_data  = nullptr;
            version_col_data = nullptr;
            delete_col_data  = nullptr;
            return false;
        }
        else
        {
            handle_col_data  = getColumnVectorDataPtr<Handle>(raw_block, handle_col_pos);
            version_col_data = getColumnVectorDataPtr<UInt64>(raw_block, version_col_pos);
            delete_col_data  = getColumnVectorDataPtr<UInt8>(raw_block, delete_col_pos);
            return true;
        }
    }

    /// This method guarantees that the returned valid block is not empty.
    Block readNextBlock()
    {
        while (true)
        {
            Block res = children.back()->read();
            if (!res)
                return {};
            if (!res.rows())
                continue;
            return res;
        }
    }

private:
    UInt64 version_limit;
    Block  header;

    size_t handle_col_pos;
    size_t version_col_pos;
    size_t delete_col_pos;

    Block raw_block;

    PaddedPODArray<Handle> const * handle_col_data  = nullptr;
    PaddedPODArray<UInt64> const * version_col_data = nullptr;
    PaddedPODArray<UInt8> const *  delete_col_data  = nullptr;
};
} // namespace DB