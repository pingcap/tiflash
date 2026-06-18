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

#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <common/types.h>
#include <fmt/format.h>
#include <tipb/executor.pb.h>

#include <atomic>

namespace DB
{
class ColumnarScanContext
{
public:
    std::atomic<UInt64> regions{0};
    std::atomic<UInt64> read_tasks{0};
    std::atomic<UInt64> physical_tables{0};
    std::atomic<UInt64> columns{0};
    std::atomic<UInt64> user_read_bytes{0};

    std::atomic<UInt64> mvcc_input_rows{0};
    std::atomic<UInt64> mvcc_input_bytes{0};
    std::atomic<UInt64> mvcc_output_rows{0};

    std::atomic<UInt64> total_read_block_ns{0};
    std::atomic<UInt64> total_serialize_block_ns{0};
    std::atomic<UInt64> total_init_reader_ns{0};
    std::atomic<UInt64> total_prefetch_ns{0};
    std::atomic<UInt64> total_deserialize_block_ns{0};

    std::atomic<UInt64> rough_check_total_packs{0};
    std::atomic<UInt64> rough_check_selected_packs{0};
    std::atomic<UInt64> rough_check_skipped_packs{0};
    std::atomic<UInt64> rough_check_unknown_packs{0};

    std::atomic<UInt64> remote_segments{0};
    std::atomic<UInt64> total_segments{0};

    void deserialize(const tipb::ColumnarScanContext & pb)
    {
        regions = pb.regions();
        read_tasks = pb.read_tasks();
        physical_tables = pb.physical_tables();
        columns = pb.columns();
        user_read_bytes = pb.user_read_bytes();
        mvcc_input_rows = pb.mvcc_input_rows();
        mvcc_input_bytes = pb.mvcc_input_bytes();
        mvcc_output_rows = pb.mvcc_output_rows();
        total_read_block_ns = pb.total_read_block_ms() * 1000000;
        total_serialize_block_ns = pb.total_serialize_block_ms() * 1000000;
        total_init_reader_ns = pb.total_init_reader_ms() * 1000000;
        total_prefetch_ns = pb.total_prefetch_ms() * 1000000;
        rough_check_total_packs = pb.rough_check_total_packs();
        rough_check_selected_packs = pb.rough_check_selected_packs();
        rough_check_skipped_packs = pb.rough_check_skipped_packs();
        rough_check_unknown_packs = pb.rough_check_unknown_packs();
        remote_segments = pb.remote_segments();
        total_segments = pb.total_segments();
        total_deserialize_block_ns = pb.total_deserialize_block_ms() * 1000000;
    }

    tipb::ColumnarScanContext serialize() const
    {
        tipb::ColumnarScanContext pb;
        pb.set_regions(regions.load());
        pb.set_read_tasks(read_tasks.load());
        pb.set_physical_tables(physical_tables.load());
        pb.set_columns(columns.load());
        pb.set_user_read_bytes(user_read_bytes.load());
        pb.set_mvcc_input_rows(mvcc_input_rows.load());
        pb.set_mvcc_input_bytes(mvcc_input_bytes.load());
        pb.set_mvcc_output_rows(mvcc_output_rows.load());
        pb.set_total_read_block_ms(total_read_block_ns.load() / 1000000);
        pb.set_total_serialize_block_ms(total_serialize_block_ns.load() / 1000000);
        pb.set_total_init_reader_ms(total_init_reader_ns.load() / 1000000);
        pb.set_total_prefetch_ms(total_prefetch_ns.load() / 1000000);
        pb.set_rough_check_total_packs(rough_check_total_packs.load());
        pb.set_rough_check_selected_packs(rough_check_selected_packs.load());
        pb.set_rough_check_skipped_packs(rough_check_skipped_packs.load());
        pb.set_rough_check_unknown_packs(rough_check_unknown_packs.load());
        pb.set_remote_segments(remote_segments.load());
        pb.set_total_segments(total_segments.load());
        pb.set_total_deserialize_block_ms(total_deserialize_block_ns.load() / 1000000);
        return pb;
    }

    void merge(const ColumnarScanContext & other)
    {
        regions += other.regions.load();
        read_tasks += other.read_tasks.load();
        mergeMax(physical_tables, other.physical_tables.load());
        mergeMax(columns, other.columns.load());
        user_read_bytes += other.user_read_bytes.load();
        mvcc_input_rows += other.mvcc_input_rows.load();
        mvcc_input_bytes += other.mvcc_input_bytes.load();
        mvcc_output_rows += other.mvcc_output_rows.load();
        total_read_block_ns += other.total_read_block_ns.load();
        total_serialize_block_ns += other.total_serialize_block_ns.load();
        total_init_reader_ns += other.total_init_reader_ns.load();
        total_prefetch_ns += other.total_prefetch_ns.load();
        total_deserialize_block_ns += other.total_deserialize_block_ns.load();
        rough_check_total_packs += other.rough_check_total_packs.load();
        rough_check_selected_packs += other.rough_check_selected_packs.load();
        rough_check_skipped_packs += other.rough_check_skipped_packs.load();
        rough_check_unknown_packs += other.rough_check_unknown_packs.load();
        remote_segments += other.remote_segments.load();
        total_segments += other.total_segments.load();
    }

    void merge(const tipb::ColumnarScanContext & other)
    {
        regions += other.regions();
        read_tasks += other.read_tasks();
        mergeMax(physical_tables, other.physical_tables());
        mergeMax(columns, other.columns());
        user_read_bytes += other.user_read_bytes();
        mvcc_input_rows += other.mvcc_input_rows();
        mvcc_input_bytes += other.mvcc_input_bytes();
        mvcc_output_rows += other.mvcc_output_rows();
        total_read_block_ns += other.total_read_block_ms() * 1000000;
        total_serialize_block_ns += other.total_serialize_block_ms() * 1000000;
        total_init_reader_ns += other.total_init_reader_ms() * 1000000;
        total_prefetch_ns += other.total_prefetch_ms() * 1000000;
        total_deserialize_block_ns += other.total_deserialize_block_ms() * 1000000;
        rough_check_total_packs += other.rough_check_total_packs();
        rough_check_selected_packs += other.rough_check_selected_packs();
        rough_check_skipped_packs += other.rough_check_skipped_packs();
        rough_check_unknown_packs += other.rough_check_unknown_packs();
        remote_segments += other.remote_segments();
        total_segments += other.total_segments();
    }

    void merge(const ColumnarScanStats & other)
    {
        mvcc_input_rows += other.mvcc_input_rows;
        mvcc_input_bytes += other.mvcc_input_bytes;
        mvcc_output_rows += other.mvcc_output_rows;
        total_read_block_ns += other.read_block_ns;
        total_serialize_block_ns += other.serialize_ns;
        total_init_reader_ns += other.init_reader_ns;
        total_prefetch_ns += other.prefetch_ns;
        rough_check_total_packs += other.rough_check_total_packs;
        rough_check_selected_packs += other.rough_check_selected_packs;
        rough_check_skipped_packs += other.rough_check_skipped_packs;
        rough_check_unknown_packs += other.rough_check_unknown_packs;
        remote_segments += other.remote_segments;
        total_segments += other.total_segments;
    }

    String toJson() const
    {
        static constexpr double NS_TO_MS_SCALE = 1'000'000.0;
        return fmt::format(
            R"({{"scan_type":"columnar","mvcc_input_rows":{},"mvcc_input_bytes":{},"mvcc_output_rows":{})"
            R"(,"regions":{},"read_tasks":{},"physical_tables":{},"columns":{})"
            R"(,"user_read_bytes":{},"read_block":"{:.3f}ms","serialize_block":"{:.3f}ms")"
            R"(,"init_reader":"{:.3f}ms","prefetch":"{:.3f}ms","deserialize_block":"{:.3f}ms")"
            R"(,"rough_check":{{"total":{},"selected":{},"skipped":{},"unknown":{}}})"
            R"(,"remote_segments":{},"total_segments":{}}})",
            mvcc_input_rows.load(),
            mvcc_input_bytes.load(),
            mvcc_output_rows.load(),
            regions.load(),
            read_tasks.load(),
            physical_tables.load(),
            columns.load(),
            user_read_bytes.load(),
            total_read_block_ns.load() / NS_TO_MS_SCALE,
            total_serialize_block_ns.load() / NS_TO_MS_SCALE,
            total_init_reader_ns.load() / NS_TO_MS_SCALE,
            total_prefetch_ns.load() / NS_TO_MS_SCALE,
            total_deserialize_block_ns.load() / NS_TO_MS_SCALE,
            rough_check_total_packs.load(),
            rough_check_selected_packs.load(),
            rough_check_skipped_packs.load(),
            rough_check_unknown_packs.load(),
            remote_segments.load(),
            total_segments.load());
    }

    void addUserReadBytes(size_t bytes) { user_read_bytes += bytes; }
    void addDeserializeBlockNs(UInt64 ns) { total_deserialize_block_ns += ns; }

private:
    static void mergeMax(std::atomic<UInt64> & target, UInt64 value)
    {
        auto current = target.load();
        while (current < value && !target.compare_exchange_weak(current, value))
        {
        }
    }
};
} // namespace DB
