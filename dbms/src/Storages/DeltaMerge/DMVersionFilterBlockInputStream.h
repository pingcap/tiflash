#pragma once

#include <Columns/ColumnsCommon.h>
#include <DataStreams/IBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <common/logger_useful.h>

namespace DB
{
namespace DM
{

/// Use the latest rows. For rows with the same handle, only take the rows with biggest version and version <= version_limit.
static constexpr int DM_VERSION_FILTER_MODE_MVCC = 0;
/// Remove the outdated rows. For rows with the same handle, take
/// 1. rows with version >= version_limit are taken,
/// 2. for the rows with smaller verion than version_limit, then take the biggest one of them, if it is not deleted.
static constexpr int DM_VERSION_FILTER_MODE_COMPACT = 1;

template <int MODE>
class DMVersionFilterBlockInputStream : public IBlockInputStream
{
    static_assert(MODE == DM_VERSION_FILTER_MODE_MVCC || MODE == DM_VERSION_FILTER_MODE_COMPACT);

public:
    DMVersionFilterBlockInputStream(const BlockInputStreamPtr & input,
                                    const ColumnDefines &       read_columns,
                                    UInt64                      version_limit_,
                                    bool                        is_common_handle_,
                                    const String &              query_id_ = "")
        : version_limit(version_limit_),
          is_common_handle(is_common_handle_),
          header(toEmptyBlock(read_columns)),
          query_id(query_id_),
          log(&Logger::get("DMVersionFilterBlockInputStream<" + String(MODE == DM_VERSION_FILTER_MODE_MVCC ? "MVCC" : "COMPACT") + ">"))
    {
        children.push_back(input);

        auto input_header = input->getHeader();

        handle_col_pos  = input_header.getPositionByName(EXTRA_HANDLE_COLUMN_NAME);
        version_col_pos = input_header.getPositionByName(VERSION_COLUMN_NAME);
        delete_col_pos  = input_header.getPositionByName(TAG_COLUMN_NAME);
    }

    ~DMVersionFilterBlockInputStream()
    {
        LOG_DEBUG(log,
                  "Total rows: " << total_rows << ", pass: " << DB::toString((Float64)passed_rows * 100 / total_rows, 2)
                                 << "%, complete pass: " << DB::toString((Float64)complete_passed * 100 / total_blocks, 2)
                                 << "%, complete not pass: " << DB::toString((Float64)complete_not_passed * 100 / total_blocks, 2)
                                 << "%, not clean: " << DB::toString((Float64)not_clean_rows * 100 / passed_rows, 2)     //
                                 << "%, read tso: " << version_limit
                                 << ", query id: " << (query_id.empty() ? String("<non-query>") : query_id));
    }

    String getName() const override { return "DeltaMergeVersionFilter"; }
    Block  getHeader() const override { return header; }

    Block read() override
    {
        FilterPtr f;
        return read(f, false);
    }

    Block read(FilterPtr & res_filter, bool return_filter) override;

    size_t getNotCleanRows() const { return not_clean_rows; }

private:
    inline void checkWithNextIndex(size_t i)
    {
#define cur_handle rowkey_column->getRowKeyValue(i)
#define next_handle rowkey_column->getRowKeyValue(i + 1)
#define cur_version (*version_col_data)[i]
#define next_version (*version_col_data)[i + 1]
#define deleted (*delete_col_data)[i]
        if constexpr (MODE == DM_VERSION_FILTER_MODE_MVCC)
        {
            filter[i] = !deleted && cur_version <= version_limit && (compare(cur_handle, next_handle) != 0 || next_version > version_limit);
        }
        else if constexpr (MODE == DM_VERSION_FILTER_MODE_COMPACT)
        {
            filter[i]
                = cur_version >= version_limit || ((compare(cur_handle, next_handle) != 0 || next_version > version_limit) && !deleted);
            not_clean[i] = filter[i] && (compare(cur_handle, next_handle) == 0 || deleted);
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
        raw_block = ::DB::DM::readNextBlock(children.back());
        if (!raw_block)
        {
            rowkey_column    = nullptr;
            version_col_data = nullptr;
            delete_col_data  = nullptr;
            return false;
        }
        else
        {
            rowkey_column    = std::make_unique<RowKeyColumnContainer>(raw_block.getByPosition(handle_col_pos).column, is_common_handle);
            version_col_data = getColumnVectorDataPtr<UInt64>(raw_block, version_col_pos);
            delete_col_data  = getColumnVectorDataPtr<UInt8>(raw_block, delete_col_pos);
            return true;
        }
    }

private:
    const UInt64 version_limit;
    const bool   is_common_handle;
    const Block  header;
    const String query_id;

    size_t handle_col_pos;
    size_t version_col_pos;
    size_t delete_col_pos;

    IColumn::Filter filter{};
    // not_clean = selected & (handle equals with next || deleted)
    IColumn::Filter not_clean{};

    Block raw_block;

    //PaddedPODArray<Handle> const * handle_col_data  = nullptr;
    std::unique_ptr<RowKeyColumnContainer> rowkey_column    = nullptr;
    PaddedPODArray<UInt64> const *         version_col_data = nullptr;
    PaddedPODArray<UInt8> const *          delete_col_data  = nullptr;

    size_t total_blocks        = 0;
    size_t total_rows          = 0;
    size_t passed_rows         = 0;
    size_t complete_passed     = 0;
    size_t complete_not_passed = 0;
    size_t not_clean_rows      = 0;

    Poco::Logger * const log;
};
} // namespace DM
} // namespace DB
