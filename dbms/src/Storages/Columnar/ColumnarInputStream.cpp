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

#include <Common/config.h> // for ENABLE_NEXT_GEN_COLUMNAR
#if ENABLE_NEXT_GEN_COLUMNAR
#include <Common/Exception.h>
#include <Common/RedactHelpers.h>
#include <Common/Stopwatch.h>
#include <DataStreams/AddExtraTableIDColumnTransformAction.h>
#include <DataStreams/IBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <IO/Buffer/ReadBufferFromMemory.h>
#include <IO/IOThreadPools.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/Columnar/ColumnarInputStream.h>
#include <Storages/Columnar/ColumnarScanContext.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <tipb/executor.pb.h>

#include <ext/scope_guard.h>
#include <limits>

namespace DB
{

bool ColumnarInputStream::ensureReader()
{
    if (reader.has_value())
        return true;

    if (fixed_reader_work != nullptr)
    {
        current_reader_work = fixed_reader_work;
        reader.emplace(task->getOrCreateReader(fixed_reader_work));
        return true;
    }

    auto next_reader_work = task->tryAcquireReaderWork();
    if (!next_reader_work.has_value())
        return false;

    current_reader_work = next_reader_work.value();
    reader.emplace(task->getOrCreateReader(next_reader_work.value()));
    return true;
}

void ColumnarInputStream::releaseReader()
{
    mergeReaderStats();
    if (reader.has_value() && reader->inner.ptr != nullptr)
        RustGcHelper::instance().gcRustPtr(reader->inner.ptr, reader->inner.type);
    reader.reset();
    current_reader_work.reset();
}

void ColumnarInputStream::mergeReaderStats()
{
    if (!reader.has_value() || reader->inner.ptr == nullptr)
        return;

    const auto * dag_context = context.getDAGContext();
    if (dag_context == nullptr)
        return;

    auto scan_ctx_iter = dag_context->columnar_scan_context_map.find(executor_id);
    if (scan_ctx_iter == dag_context->columnar_scan_context_map.end() || !scan_ctx_iter->second)
        return;

    const auto & global_ctx = context.getGlobalContext();
    const TiFlashRaftProxyHelper * proxy_helper = global_ctx.getSharedContextDisagg()->getColumnarProxyHelper();
    if (proxy_helper == nullptr || proxy_helper->cloud_storage_engine_interfaces.fn_columnar_scan_stats == nullptr)
        return;

    const auto stats = proxy_helper->cloud_storage_engine_interfaces.fn_columnar_scan_stats(reader.value());
    scan_ctx_iter->second->merge(stats);
}

ColumnarInputStream::~ColumnarInputStream()
{
    SCOPE_EXIT({
        try
        {
            releaseReader();
        }
        catch (...)
        {}
    });
    try
    {
        const auto * dag_context = context.getDAGContext();
        const auto keyspace_id = dag_context != nullptr ? dag_context->getKeyspaceID() : NullspaceID;
        LOG_INFO(
            log,
            "Finished reading remote snapshot through columnar, keyspace_id={} rows={} bytes={} read_cost={:.3f}s "
            "deserialize_cost={:.3f}s",
            keyspace_id,
            action.totalRows(),
            total_bytes,
            duration_read_sec,
            duration_deserialize_sec);
        if (dag_context != nullptr)
        {
            if (auto it = dag_context->scan_context_map.find(executor_id); it != dag_context->scan_context_map.end())
            {
                if (it->second)
                {
                    std::optional<LACBytesCollector> lac_bytes_collector;
                    it->second->addUserReadBytes(total_bytes, DM::ReadTag::Query, lac_bytes_collector);
                }
            }
            if (auto it = dag_context->columnar_scan_context_map.find(executor_id);
                it != dag_context->columnar_scan_context_map.end() && it->second)
            {
                it->second->addUserReadBytes(total_bytes);
                it->second->addDeserializeBlockNs(static_cast<uint64_t>(duration_deserialize_sec * 1000000000.0));
            }
        }
    }
    catch (...)
    {
        // Destructors must not throw.
    }
}

Block ColumnarInputStream::read(FilterPtr & res_filter, bool return_filter)
{
    return readImpl(res_filter, return_filter);
}

Block ColumnarInputStream::readImpl()
{
    FilterPtr filter_ignored;
    return readImpl(filter_ignored, false);
}

Block ColumnarInputStream::readImpl([[maybe_unused]] FilterPtr & res_filter, [[maybe_unused]] bool return_filter)
{
    if (done)
        return {};
    const Context & global_ctx = context.getGlobalContext();
    const TiFlashRaftProxyHelper * proxy_helper = global_ctx.getSharedContextDisagg()->getColumnarProxyHelper();
    RUNTIME_CHECK_MSG(proxy_helper != nullptr, "columnar helper is not initialized");

    while (true)
    {
        if (!ensureReader())
        {
            done = true;
            return {};
        }

        Stopwatch w{CLOCK_MONOTONIC_COARSE};
        UInt64 rows = proxy_helper->cloud_storage_engine_interfaces.fn_read_block(reader.value(), batch_size);
        duration_read_sec += w.elapsedSecondsFromLastTime();
        LOG_DEBUG(log, "Read {} rows from columnar", rows);
        if (rows == std::numeric_limits<UInt64>::max())
        {
            LOG_WARNING(log, "Read block from columnar failed");
            throw Exception("read_block failed in columnar", ErrorCodes::LOGICAL_ERROR);
        }
        if (rows == 0)
        {
            releaseReader();
            if (fixed_reader_work != nullptr)
            {
                done = true;
                return {};
            }
            continue;
        }

        TableID physical_table_id = -1;
        Block header = getHeader();
        const ColumnsWithTypeAndName & col_type_and_name = header.getColumnsWithTypeAndName();
        // Construct block from columnar column data.
        MutableColumns columns = header.cloneEmptyColumns();
        for (UInt32 i = 0; i < col_type_and_name.size(); ++i)
        {
            LOG_DEBUG(
                log,
                "Read column id={} name={} type={}",
                col_type_and_name[i].column_id,
                col_type_and_name[i].name,
                col_type_and_name[i].type->getName());
            // Read column data from columnar
            Int64 col_id = col_type_and_name[i].column_id;
            if (col_id == MutSup::extra_handle_id)
            {
                RustStrWithView col_data = proxy_helper->cloud_storage_engine_interfaces.fn_read_handle(reader.value());
                SCOPE_EXIT({ RustGcHelper::instance().gcRustPtr(col_data.inner.ptr, col_data.inner.type); });
                physical_table_id = proxy_helper->cloud_storage_engine_interfaces.fn_physical_table_id(reader.value());
                ReadBufferFromMemory buf(col_data.buff.data, static_cast<size_t>(col_data.buff.len));
                auto & col = *columns[i];
                col_type_and_name[i].type->deserializeBinaryBulkWithMultipleStreams(
                    col,
                    [&](const IDataType::SubstreamPath &) { return &buf; },
                    rows,
                    -1.0, // avg_value_size_hint set to -1 to indicate Decimal format from columnar
                    true,
                    {});
            }
            else if (col_id == MutSup::extra_table_id_col_id)
            {
                continue;
            }
            else
            {
                RustStrWithView col_data
                    = proxy_helper->cloud_storage_engine_interfaces.fn_read_column(reader.value(), col_id);
                SCOPE_EXIT({ RustGcHelper::instance().gcRustPtr(col_data.inner.ptr, col_data.inner.type); });
                physical_table_id = proxy_helper->cloud_storage_engine_interfaces.fn_physical_table_id(reader.value());
                ReadBufferFromMemory buf(col_data.buff.data, static_cast<size_t>(col_data.buff.len));
                auto & col = *columns[i];
                col_type_and_name[i].type->deserializeBinaryBulkWithMultipleStreams(
                    col,
                    [&](const IDataType::SubstreamPath &) { return &buf; },
                    rows,
                    -1.0, // avg_value_size_hint set to -1 to indicate Decimal format from columnar
                    true,
                    {});
                LOG_DEBUG(log, "Read column data done, col size={}", col.size());
            }
        }
        duration_deserialize_sec += w.elapsedSecondsFromLastTime();

        Block block = header.cloneWithColumns(std::move(columns));
        LOG_DEBUG(log, "Read block rows={}, structure={}", block.rows(), block.dumpStructure());
        if (physical_table_id == -1)
        {
            LOG_WARNING(log, "physical_table_id is not set, use table_id {} instead", table_id);
            physical_table_id = table_id;
        }
        // Fill extra table id column.
        action.fill(block, physical_table_id);
        block.checkNumberOfRows();

        total_bytes += block.bytes();
        return block;
    }
}

} // namespace DB
#endif
