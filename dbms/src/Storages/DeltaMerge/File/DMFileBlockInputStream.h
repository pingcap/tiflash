// Copyright 2022 PingCAP, Ltd.
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

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/File/ColumnCache.h>
#include <Storages/DeltaMerge/File/DMFileReader.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>

namespace DB
{
namespace DM
{
class DMFileBlockInputStream : public SkippableBlockInputStream
{
public:
    DMFileBlockInputStream(const Context & context,
                           UInt64 max_read_version,
                           bool enable_clean_read,
                           UInt64 hash_salt,
                           const DMFilePtr & dmfile,
                           const ColumnDefines & read_columns,
                           const RowKeyRanges & rowkey_ranges,
                           const RSOperatorPtr & filter,
                           const ColumnCachePtr & column_cache_,
                           const IdSetPtr & read_packs,
                           size_t expected_size = DMFILE_READ_ROWS_THRESHOLD,
                           bool read_one_pack_every_time_ = false)
        : reader(dmfile,
                 read_columns,
                 // clean read
                 enable_clean_read,
                 max_read_version,
                 // filters
                 rowkey_ranges,
                 filter,
                 read_packs,
                 // caches
                 hash_salt,
                 context.getGlobalContext().getMarkCache(),
                 context.getGlobalContext().getMinMaxIndexCache(),
                 context.getSettingsRef().dt_enable_stable_column_cache,
                 column_cache_,
                 context.getSettingsRef().min_bytes_to_use_direct_io,
                 context.getSettingsRef().max_read_buffer_size,
                 context.getFileProvider(),
                 context.getReadLimiter(),
                 expected_size,
                 read_one_pack_every_time_)
    {
    }

    ~DMFileBlockInputStream() {}

    String getName() const override { return "DMFile"; }

    Block getHeader() const override { return reader.getHeader(); }

    bool getSkippedRows(size_t & skip_rows) override { return reader.getSkippedRows(skip_rows); }

    Block read() override { return reader.read(); }

private:
    DMFileReader reader;
};

using DMFileBlockInputStreamPtr = std::shared_ptr<DMFileBlockInputStream>;

/**
 * Create a simple stream that read all blocks on default.
 * @param context Database context.
 * @param file DMFile pointer.
 * @return A shared pointer of an input stream
 */
inline DMFileBlockInputStreamPtr createSimpleBlockInputStream(const DB::Context & context, const DMFilePtr & file)
{
    // disable clean read is needed, since we just want to read all data from the file, and we do not know about the column handle
    // enable read_one_pack_every_time_ is needed to preserve same block structure as the original file
    return std::make_shared<DMFileBlockInputStream>(context,
                                                    DB::DM::MAX_UINT64 /*< max_read_version */,
                                                    false /*< enable_clean_read */,
                                                    0 /*< hash_salt */,
                                                    file,
                                                    file->getColumnDefines(),
                                                    DB::DM::RowKeyRanges{},
                                                    DB::DM::RSOperatorPtr{},
                                                    DB::DM::ColumnCachePtr{},
                                                    DB::DM::IdSetPtr{},
                                                    DMFILE_READ_ROWS_THRESHOLD,
                                                    true /*< read_one_pack_every_time_ */);
}

} // namespace DM
} // namespace DB
