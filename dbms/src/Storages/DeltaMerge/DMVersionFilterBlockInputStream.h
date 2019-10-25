#pragma once

#include <common/logger_useful.h>

#include <Columns/ColumnsCommon.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>

namespace DB
{
namespace DM
{

/// Use the latest rows. For rows with the same handle, only take the rows with biggest version and version <= version_limit.
static constexpr int DM_VERSION_FILTER_MODE_MVCC = 0;
/// Remove the outdated rows. For rows with the same handle, take all rows with version >= version_limit.
/// And if all of them are smaller than version_limit, then take the biggest one, if it is not deleted.
static constexpr int DM_VERSION_FILTER_MODE_COMPACT = 1;

template <int MODE>
class DMVersionFilterBlockInputStream : public IProfilingBlockInputStream
{
    static_assert(MODE == DM_VERSION_FILTER_MODE_MVCC || MODE == DM_VERSION_FILTER_MODE_COMPACT);

public:
    DMVersionFilterBlockInputStream(const BlockInputStreamPtr & input, const ColumnDefine & handle_define, UInt64 version_limit_)
        : version_limit(version_limit_),
          header(input->getHeader()),
          handle_col_pos(header.getPositionByName(handle_define.name)),
          version_col_pos(header.getPositionByName(VERSION_COLUMN_NAME)),
          delete_col_pos(header.getPositionByName(TAG_COLUMN_NAME)),
          filter(65536),
          log(&Logger::get("DMVersionFilterBlockInputStream<" + String(MODE == DM_VERSION_FILTER_MODE_MVCC ? "MVCC" : "COMPACT") + ">"))
    {
        children.push_back(input);
    }

    ~DMVersionFilterBlockInputStream()
    {
        LOG_INFO(log,
                 "Pass: " + DB::toString((Float64)passed_rows * 100 / total_rows, 2) + "%"
                     + ", complete pass: " + DB::toString((Float64)complete_passed * 100 / total_blocks, 2)
                     + ", complete not pass: " + DB::toString((Float64)complete_not_passed * 100 / total_blocks, 2));
    }

    String getName() const override { return "DeltaMergeVersionFilter"; }
    Block  getHeader() const override { return header; }

protected:
    Block readImpl() override;

    inline UInt8 checkWithNextIndex(size_t i)
    {
#define cur_handle (*handle_col_data)[i]
#define next_handle (*handle_col_data)[i + 1]
#define cur_version (*version_col_data)[i]
#define next_version (*version_col_data)[i + 1]
#define deleted (*delete_col_data)[i]
        if constexpr (MODE == DM_VERSION_FILTER_MODE_MVCC)
        {
            return !deleted && cur_version <= version_limit && (cur_handle != next_handle || next_version > version_limit);
        }
        else if constexpr (MODE == DM_VERSION_FILTER_MODE_COMPACT)
        {
            return cur_version >= version_limit || (cur_handle != next_handle && !deleted);
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

    IColumn::Filter filter;

    Block raw_block;

    PaddedPODArray<Handle> const * handle_col_data  = nullptr;
    PaddedPODArray<UInt64> const * version_col_data = nullptr;
    PaddedPODArray<UInt8> const *  delete_col_data  = nullptr;

    size_t total_blocks        = 0;
    size_t total_rows          = 0;
    size_t passed_rows         = 0;
    size_t complete_passed     = 0;
    size_t complete_not_passed = 0;

    Logger * log;
};
} // namespace DM
} // namespace DB