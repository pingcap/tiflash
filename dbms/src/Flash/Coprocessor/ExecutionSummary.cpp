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

#include <Common/Exception.h>
#include <Flash/Coprocessor/ExecutionSummary.h>
#include <Flash/Statistics/BaseRuntimeStatistics.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <common/likely.h>

namespace DB
{
ExecutionSummary::ExecutionSummary()
    : scan_context(std::make_shared<DM::ScanContext>())
{}

void ExecutionSummary::merge(const ExecutionSummary & other)
{
    if (time_processed_ns < other.time_processed_ns)
    {
        time_processed_ns = other.time_processed_ns;
        time_minTSO_wait_ns = other.time_minTSO_wait_ns;
        time_pipeline_breaker_wait_ns = other.time_pipeline_breaker_wait_ns;
        time_pipeline_queue_ns = other.time_pipeline_queue_ns;
    }

    num_produced_rows += other.num_produced_rows;
    num_iterations += other.num_iterations;
    concurrency += other.concurrency;
    inner_zone_send_bytes += other.inner_zone_send_bytes;
    inner_zone_receive_bytes += other.inner_zone_receive_bytes;
    inter_zone_send_bytes += other.inter_zone_send_bytes;
    inter_zone_receive_bytes += other.inter_zone_receive_bytes;
    ru_consumption = mergeRUConsumption(ru_consumption, other.ru_consumption);
    scan_context->merge(*other.scan_context);
}

void ExecutionSummary::merge(const tipb::ExecutorExecutionSummary & other)
{
    if (time_processed_ns < other.time_processed_ns())
    {
        time_processed_ns = other.time_processed_ns();
        time_minTSO_wait_ns = other.tiflash_wait_summary().mintso_wait_ns();
        time_pipeline_breaker_wait_ns = other.tiflash_wait_summary().pipeline_breaker_wait_ns();
        time_pipeline_queue_ns = other.tiflash_wait_summary().pipeline_queue_wait_ns();
    }
    num_produced_rows += other.num_produced_rows();
    num_iterations += other.num_iterations();
    concurrency += other.concurrency();
    inner_zone_send_bytes += other.tiflash_network_summary().inner_zone_send_bytes();
    inner_zone_receive_bytes += other.tiflash_network_summary().inner_zone_receive_bytes();
    inter_zone_send_bytes += other.tiflash_network_summary().inter_zone_send_bytes();
    inter_zone_receive_bytes += other.tiflash_network_summary().inter_zone_send_bytes();
    ru_consumption = mergeRUConsumption(ru_consumption, parseRUConsumption(other));
    scan_context->merge(other.tiflash_scan_context());
}

void ExecutionSummary::fill(const BaseRuntimeStatistics & other)
{
    time_processed_ns = other.execution_time_ns;
    time_minTSO_wait_ns = other.minTSO_wait_time_ns;
    time_pipeline_breaker_wait_ns = other.pipeline_breaker_wait_time_ns;
    time_pipeline_queue_ns = other.queue_wait_time_ns;
    num_produced_rows = other.rows;
    num_iterations = other.blocks;
    concurrency = other.concurrency;
    inner_zone_send_bytes = other.inner_zone_send_bytes;
    inner_zone_receive_bytes = other.inner_zone_receive_bytes;
    inter_zone_send_bytes = other.inter_zone_send_bytes;
    inter_zone_receive_bytes = other.inter_zone_receive_bytes;
}

void ExecutionSummary::init(const tipb::ExecutorExecutionSummary & other)
{
    time_processed_ns = other.time_processed_ns();
    time_minTSO_wait_ns = other.tiflash_wait_summary().mintso_wait_ns();
    time_pipeline_breaker_wait_ns = other.tiflash_wait_summary().pipeline_breaker_wait_ns();
    time_pipeline_queue_ns = other.tiflash_wait_summary().pipeline_queue_wait_ns();
    num_produced_rows = other.num_produced_rows();
    num_iterations = other.num_iterations();
    concurrency = other.concurrency();
    inner_zone_send_bytes = other.tiflash_network_summary().inner_zone_send_bytes();
    inner_zone_receive_bytes = other.tiflash_network_summary().inner_zone_receive_bytes();
    inter_zone_send_bytes = other.tiflash_network_summary().inter_zone_send_bytes();
    inter_zone_receive_bytes = other.tiflash_network_summary().inter_zone_send_bytes();
    ru_consumption = parseRUConsumption(other);
    scan_context->deserialize(other.tiflash_scan_context());
}

resource_manager::Consumption parseRUConsumption(const tipb::ExecutorExecutionSummary & pb)
{
    resource_manager::Consumption ru_consumption;
    if (pb.has_ru_consumption())
    {
        RUNTIME_CHECK_MSG(
            ru_consumption.ParseFromString(pb.ru_consumption()),
            "failed to parse ru consumption from execution summary");
    }
    return ru_consumption;
}

resource_manager::Consumption mergeRUConsumption(
    const resource_manager::Consumption & left,
    const resource_manager::Consumption & right)
{
    // TiFlash only support read related RU for now.
    // So ignore merge other fields.
    resource_manager::Consumption sum;
    sum.set_r_r_u(left.r_r_u() + right.r_r_u());
    sum.set_read_bytes(left.read_bytes() + right.read_bytes());
    sum.set_total_cpu_time_ms(left.total_cpu_time_ms() + right.total_cpu_time_ms());
    return sum;
}
} // namespace DB
