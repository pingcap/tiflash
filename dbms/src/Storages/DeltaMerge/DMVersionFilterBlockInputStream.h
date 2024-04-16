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

#include <Columns/ColumnsCommon.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/SelectionByColumnIdTransformAction.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/ScanContext_fwd.h>
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
    static constexpr size_t UNROLL_BATCH = 64;
    static_assert(MODE == DM_VERSION_FILTER_MODE_MVCC || MODE == DM_VERSION_FILTER_MODE_COMPACT);

    constexpr static const char * MVCC_FILTER_NAME = "mode=MVCC";
    constexpr static const char * COMPACT_FILTER_NAME = "mode=COMPACT";

public:
    DMVersionFilterBlockInputStream(
        const BlockInputStreamPtr & input,
        const ColumnDefines & read_columns,
        UInt64 version_limit_,
        bool is_common_handle_,
        const String & tracing_id = "",
        const ScanContextPtr & scan_context_ = nullptr)
        : version_limit(version_limit_)
        , is_common_handle(is_common_handle_)
        , header(toEmptyBlock(read_columns))
        , select_by_colid_action(input->getHeader(), header)
        , scan_context(scan_context_)
        , log(Logger::get((MODE == DM_VERSION_FILTER_MODE_MVCC ? MVCC_FILTER_NAME : COMPACT_FILTER_NAME), tracing_id))
    {
        children.push_back(input);

        auto input_header = input->getHeader();

        handle_col_pos = input_header.getPositionByName(EXTRA_HANDLE_COLUMN_NAME);
        version_col_pos = input_header.getPositionByName(VERSION_COLUMN_NAME);
        delete_col_pos = input_header.getPositionByName(TAG_COLUMN_NAME);
    }

    ~DMVersionFilterBlockInputStream() override
    {
        LOG_DEBUG(
            log,
            "Total rows: {}, pass: {:.2f}%"
            ", complete pass: {:.2f}%, complete not pass: {:.2f}%"
            ", not clean: {:.2f}%, is deleted: {:.2f}%, effective: {:.2f}%"
            ", start_ts: {}",
            total_rows,
            passed_rows * 100.0 / total_rows,
            complete_passed * 100.0 / total_blocks,
            complete_not_passed * 100.0 / total_blocks,
            not_clean_rows * 100.0 / passed_rows,
            deleted_rows * 100.0 / passed_rows,
            effective_num_rows * 100.0 / passed_rows,
            version_limit);
    }

    void readPrefix() override;
    void readSuffix() override;

    String getName() const override { return "DeltaMergeVersionFilter"; }
    Block getHeader() const override { return header; }

    Block read() override
    {
        FilterPtr f;
        return read(f, false);
    }

    Block read(FilterPtr & res_filter, bool return_filter) override;

    size_t getEffectiveNumRows() const { return effective_num_rows; }
    size_t getNotCleanRows() const { return not_clean_rows; }
    size_t getDeletedRows() const { return deleted_rows; }
    UInt64 getGCHintVersion() const { return gc_hint_version; }

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
            filter[i] = !deleted && cur_version <= version_limit
                && (cur_handle != next_handle || next_version > version_limit);
        }
        else if constexpr (MODE == DM_VERSION_FILTER_MODE_COMPACT)
        {
            filter[i] = cur_version >= version_limit
                || ((cur_handle != next_handle || next_version > version_limit) && !deleted);
            not_clean[i] = filter[i] && (cur_handle == next_handle || deleted);
            is_deleted[i] = filter[i] && deleted;
            effective[i] = filter[i] && (cur_handle != next_handle);
            if (filter[i])
                gc_hint_version = std::min(
                    gc_hint_version,
                    calculateRowGcHintVersion(cur_handle, cur_version, next_handle, true, deleted));
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
            rowkey_column = nullptr;
            version_col_data = nullptr;
            delete_col_data = nullptr;
            return false;
        }
        else
        {
            rowkey_column = std::make_unique<RowKeyColumnContainer>(
                raw_block.getByPosition(handle_col_pos).column,
                is_common_handle);
            version_col_data = getColumnVectorDataPtr<UInt64>(raw_block, version_col_pos);
            delete_col_data = getColumnVectorDataPtr<UInt8>(raw_block, delete_col_pos);
            return true;
        }
    }

private:
    inline UInt64 calculateRowGcHintVersion(
        const RowKeyValueRef & cur_handle,
        UInt64 cur_version,
        const RowKeyValueRef & next_handle,
        bool next_handle_valid,
        bool deleted)
    {
        // The rules to calculate gc_hint_version of every pk,
        //     1. If the oldest version is delete, then the result is the oldest version.
        //     2. Otherwise, if the pk has just a single version, the result is UInt64_MAX(means just ignore this kind of pk).
        //     3. Otherwise, the result is the second oldest version.
        bool matched = false;
        if (is_first_oldest_version && deleted)
        {
            // rule 1
            matched = true;
        }
        else if (is_second_oldest_version && gc_hint_version_pending)
        {
            // rule 3
            matched = true;
        }
        gc_hint_version_pending = !matched;

        // update status variable for next row if need
        if (next_handle_valid)
        {
            if (cur_handle != next_handle)
            {
                is_first_oldest_version = true;
                is_second_oldest_version = false;
            }
            else if (is_first_oldest_version && cur_handle == next_handle)
            {
                is_first_oldest_version = false;
                is_second_oldest_version = true;
            }
            else
            {
                is_first_oldest_version = false;
                is_second_oldest_version = false;
            }
        }

        return matched ? cur_version : std::numeric_limits<UInt64>::max();
    }

    Block getNewBlock(const Block & block)
    {
        if (block.segmentRowIdCol() == nullptr)
        {
            return select_by_colid_action.transform(block);
        }
        else
        {
            // `DMVersionFilterBlockInputStream` is the last stage for generating segment row id.
            // In the way we use it, the other columns are not used subsequently.
            Block res;
            res.setSegmentRowIdCol(block.segmentRowIdCol());
            return res;
        }
    }

private:
    const UInt64 version_limit;
    const bool is_common_handle;
    // A sample block of `read` get
    const Block header;

    size_t handle_col_pos;
    size_t version_col_pos;
    size_t delete_col_pos;

    IColumn::Filter filter{};
    // effective = selected & handle not equals with next
    IColumn::Filter effective{};
    // not_clean = selected & (handle equals with next || deleted)
    IColumn::Filter not_clean{};
    // is_deleted = selected & deleted
    IColumn::Filter is_deleted{};

    // Calculate per block, when gc_safe_point exceed this version, there must be some data obsolete in this block
    // First calculate the gc_hint_version of every pk according to the following rules,
    //     see the comments in `calculateRowGcHintVersion` to see how to calculate it for every pk
    // Then the block's gc_hint_version is the minimum value of all pk's gc_hint_version
    UInt64 gc_hint_version = std::numeric_limits<UInt64>::max();

    // auxiliary variable for the calculation of gc_hint_version
    bool is_first_oldest_version = true;
    bool is_second_oldest_version = false;
    bool gc_hint_version_pending = true;

    Block raw_block;

    //PaddedPODArray<Handle> const * handle_col_data  = nullptr;
    std::unique_ptr<RowKeyColumnContainer> rowkey_column = nullptr;
    PaddedPODArray<UInt64> const * version_col_data = nullptr;
    PaddedPODArray<UInt8> const * delete_col_data = nullptr;

    size_t total_blocks = 0;
    size_t total_rows = 0;
    size_t passed_rows = 0;
    size_t complete_passed = 0;
    size_t complete_not_passed = 0;
    size_t not_clean_rows = 0;
    size_t effective_num_rows = 0;
    size_t deleted_rows = 0;

    SelectionByColumnIdTransformAction select_by_colid_action;

    const ScanContextPtr scan_context;

    const LoggerPtr log;
};
} // namespace DM
} // namespace DB
