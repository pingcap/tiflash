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

#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace DM
{
namespace tests
{
/**
 * This is similar to SegmentTestBasic, but is for the DeltaMergeStore.
 * It allows you to write tests easier based on the assumption that the PK is either Int or Int encoded in String.
 */
class SimplePKTestBasic : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override { reload(); }

    void TearDown() override { TiFlashStorageTestBasic::TearDown(); }

public:
    // Lightweight wrappers

    void fill(Int64 start_key, Int64 end_key);
    void fillDelete(Int64 start_key, Int64 end_key);
    void flush(Int64 start_key, Int64 end_key);
    void flush();
    void mergeDelta(Int64 start_key, Int64 end_key);
    void mergeDelta();
    bool merge(Int64 start_key, Int64 end_key);
    void deleteRange(Int64 start_key, Int64 end_key);

    struct IngestFilesOptions
    {
        std::pair<Int64, Int64> range;
        const std::vector<Block> & blocks;
        bool clear = false;
    };
    void ingestFiles(const IngestFilesOptions & options);

    size_t getRowsN() const;
    size_t getRowsN(Int64 start_key, Int64 end_key) const;
    size_t getRawRowsN() const;
    bool isFilled(Int64 start_key, Int64 end_key) const;

    struct FillBlockOptions
    {
        std::pair<Int64, Int64> range;
        bool is_deleted = false;
    };
    Block fillBlock(const FillBlockOptions & options);

public:
    SegmentPtr getSegmentAt(Int64 key) const;

    /**
     * Ensure segments in the store are split at the specified breakpoints.
     * This could be used to initialize segments as desired.
     */
    void ensureSegmentBreakpoints(const std::vector<Int64> & breakpoints, bool use_logical_split = false);

    /**
     * Returns the breakpoints of all segments in the store.
     *
     * Example:
     *
     * Segments                             | Expected Breakpoints
     * --------------------------------------------------
     * [-inf, +inf]                         | None
     * [-inf, 10), [10, +inf)               | 10
     * [-inf, 10), [10, 30), [30, +inf)     | 10, 30
     */
    std::vector<Int64> getSegmentBreakpoints() const;

    void debugDumpAllSegments() const;

protected:
    void reload();

    RowKeyValue buildRowKey(Int64 pk) const;

    RowKeyRange buildRowRange(Int64 start, Int64 end) const;

    std::pair<Int64, Int64> parseRange(const RowKeyRange & range) const;

protected:
    DeltaMergeStorePtr store;
    DMContextPtr dm_context;

    UInt64 version = 0;

    LoggerPtr logger = Logger::get();
    LoggerPtr logger_op = Logger::get("SimplePKTestBasicOperation");

protected:
    // Below are options
    bool is_common_handle = false;
};
} // namespace tests
} // namespace DM
} // namespace DB
