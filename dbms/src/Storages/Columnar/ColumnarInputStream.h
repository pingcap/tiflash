// Copyright 2026 PingCAP, Inc.
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

#include <Common/config.h> // for ENABLE_NEXT_GEN_COLUMNAR
#if ENABLE_NEXT_GEN_COLUMNAR
#include <Common/Logger.h>
#include <DataStreams/AddExtraTableIDColumnTransformAction.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/Columnar/ColumnarReader.h>

namespace DB
{

class ColumnarInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "ColumnarSource";

public:
    ~ColumnarInputStream() override;

    String getName() const override { return NAME; }
    Block getHeader() const override { return header; }
    void setHeader(const Block & header) { this->header = header; }
    Block read(FilterPtr & res_filter, bool return_filter) override;

protected:
    Block readImpl() override;
    Block readImpl(FilterPtr & res_filter, bool return_filter) override;

public:
    struct Options
    {
        const Context & context;
        LoggerPtr log;
        ColumnarReadTaskPoolPtr task;
        ColumnarReaderWorkPtr reader_work;
        const DM::ColumnDefines & columns_to_read;
        int extra_table_id_index;
        TableID table_id;
        const String & executor_id;
    };

    explicit ColumnarInputStream(const Options & options)
        : context(options.context)
        , log(options.log)
        , task(options.task)
        , fixed_reader_work(options.reader_work)
        , action(options.columns_to_read, options.extra_table_id_index)
        , table_id(options.table_id)
        , executor_id(options.executor_id)
    {
        // Keep header aligned with genNamesAndTypesForTableScan when TiDB requests _tidb_tid on partition scans.
        setHeader(action.getHeader());
    }

    static BlockInputStreamPtr create(const Options & options)
    {
        return std::make_shared<ColumnarInputStream>(options);
    }

private:
    bool ensureReader();
    void mergeReaderStats();
    void releaseReader();

    const Context & context;
    const LoggerPtr log;
    ColumnarReadTaskPoolPtr task;
    const ColumnarReaderWorkPtr fixed_reader_work;
    ColumnarReaderWorkPtr current_reader_work;
    std::optional<ColumnarReaderPtr> reader;
    AddExtraTableIDColumnTransformAction action;
    TableID table_id;
    const String executor_id;
    Block header;

    bool done = false;

    double duration_deserialize_sec = 0;
    double duration_read_sec = 0;
    UInt64 batch_size = 10240;
    UInt64 total_bytes = 0;
};

} // namespace DB
#endif
