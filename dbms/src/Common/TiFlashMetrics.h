#pragma once

#include <Common/TiFlashBuildInfo.h>
#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/gateway.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>

namespace DB
{

/// Central place to define metrics across all subsystems.
/// Refer to gtest_tiflash_metrics.cpp for more sample defines.
/// Usage:
/// GET_METRIC(context.getTiFlashMetrics(), tiflash_coprocessor_response_bytes).Increment(1);
/// GET_METRIC(context.getTiFlashMetrics(), tiflash_coprocessor_request_count, type_batch).Set(1);
/// Maintenance notes:
/// 1. Use same name prefix for metrics in same subsystem (coprocessor/schema/tmt/raft/etc.).
/// 2. Keep metrics with same prefix next to each other.
/// 3. Add metrics of new subsystems at tail.
/// 4. Keep it proper formatted using clang-format.
#define APPLY_FOR_METRICS(M, F)                                                                                                           \
    M(tiflash_coprocessor_request_count, "Total number of request", Counter, F(type_batch, {"type", "batch"}),                            \
        F(type_batch_cop, {"type", "batch_cop"}), F(type_cop, {"type", "cop"}), F(type_cop_dag, {"type", "cop_dag"}),                     \
        F(type_super_batch, {"type", "super_batch"}), F(type_super_batch_cop_dag, {"type", "super_batch_cop_dag"}),                       \
        F(type_dispatch_mpp_task, {"type", "dispatch_mpp_task"}), F(type_mpp_establish_conn, {"type", "mpp_establish_conn"}),             \
        F(type_cancel_mpp_task, {"type", "mpp_establish_conn"}))                                                                          \
    M(tiflash_coprocessor_handling_request_count, "Number of handling request", Gauge, F(type_batch, {"type", "batch"}),                  \
        F(type_batch_cop, {"type", "batch_cop"}), F(type_cop, {"type", "cop"}), F(type_cop_dag, {"type", "cop_dag"}),                     \
        F(type_super_batch, {"type", "super_batch"}), F(type_super_batch_cop_dag, {"type", "super_batch_cop_dag"}),                       \
        F(type_dispatch_mpp_task, {"type", "dispatch_mpp_task"}), F(type_mpp_establish_conn, {"type", "mpp_establish_conn"}),             \
        F(type_cancel_mpp_task, {"type", "mpp_establish_conn"}))                                                                          \
    M(tiflash_coprocessor_executor_count, "Total number of each executor", Counter, F(type_ts, {"type", "table_scan"}),                   \
        F(type_sel, {"type", "selection"}), F(type_agg, {"type", "aggregation"}), F(type_topn, {"type", "top_n"}),                        \
        F(type_limit, {"type", "limit"}), F(type_join, {"type", "join"}), F(type_exchange_sender, {"type", "exchange_sender"}),           \
        F(type_exchange_receiver, {"type", "exchange_receiver"}), F(type_projection, {"type", "projection"}))                             \
    M(tiflash_coprocessor_request_duration_seconds, "Bucketed histogram of request duration", Histogram,                                  \
        F(type_batch, {{"type", "batch"}}, ExpBuckets{0.0005, 2, 30}), F(type_cop, {{"type", "cop"}}, ExpBuckets{0.0005, 2, 30}),         \
        F(type_super_batch, {{"type", "super_batch"}}, ExpBuckets{0.0005, 2, 30}),                                                        \
        F(type_dispatch_mpp_task, {{"type", "dispatch_mpp_task"}}, ExpBuckets{0.0005, 2, 30}),                                            \
        F(type_mpp_establish_conn, {{"type", "mpp_establish_conn"}}, ExpBuckets{0.0005, 2, 30}),                                          \
        F(type_cancel_mpp_task, {{"type", "cancel_mpp_task"}}, ExpBuckets{0.0005, 2, 30}))                                                \
    M(tiflash_coprocessor_request_memory_usage, "Bucketed histogram of request memory usage", Histogram,                                  \
        F(type_cop, {{"type", "cop"}}, ExpBuckets{1024 * 1024, 2, 16}),                                                                   \
        F(type_super_batch, {{"type", "super_batch"}}, ExpBuckets{1024 * 1024, 2, 16}),                                                   \
        F(type_dispatch_mpp_task, {{"type", "mpp_dispatch_mpp_task"}}, ExpBuckets{1024 * 1024, 2, 16}))                                   \
    M(tiflash_coprocessor_request_error, "Total number of request error", Counter, F(reason_meet_lock, {"reason", "meet_lock"}),          \
        F(reason_region_not_found, {"reason", "region_not_found"}), F(reason_epoch_not_match, {"reason", "epoch_not_match"}),             \
        F(reason_kv_client_error, {"reason", "kv_client_error"}), F(reason_internal_error, {"reason", "internal_error"}),                 \
        F(reason_other_error, {"reason", "other_error"}))                                                                                 \
    M(tiflash_coprocessor_request_handle_seconds, "Bucketed histogram of request handle duration", Histogram,                             \
        F(type_batch, {{"type", "batch"}}, ExpBuckets{0.0005, 2, 30}), F(type_cop, {{"type", "cop"}}, ExpBuckets{0.0005, 2, 30}),         \
        F(type_super_batch, {{"type", "super_batch"}}, ExpBuckets{0.0005, 2, 30}),                                                        \
        F(type_dispatch_mpp_task, {{"type", "dispatch_mpp_task"}}, ExpBuckets{0.0005, 2, 30}),                                            \
        F(type_mpp_establish_conn, {{"type", "mpp_establish_conn"}}, ExpBuckets{0.0005, 2, 30}),                                          \
        F(type_cancel_mpp_task, {{"type", "cancel_mpp_task"}}, ExpBuckets{0.0005, 2, 30}))                                                \
    M(tiflash_coprocessor_response_bytes, "Total bytes of response body", Counter)                                                        \
    M(tiflash_schema_version, "Current version of tiflash cached schema", Gauge)                                                          \
    M(tiflash_schema_applying, "Whether the schema is applying or not (holding lock)", Gauge)                                             \
    M(tiflash_schema_apply_count, "Total number of each kinds of apply", Counter, F(type_diff, {"type", "diff"}),                         \
        F(type_full, {"type", "full"}), F(type_failed, {"type", "failed"}))                                                               \
    M(tiflash_schema_trigger_count, "Total number of each kinds of schema sync trigger", Counter, /**/                                    \
        F(type_timer, {"type", "timer"}), F(type_raft_decode, {"type", "raft_decode"}), F(type_cop_read, {"type", "cop_read"}))           \
    M(tiflash_schema_internal_ddl_count, "Total number of each kinds of internal ddl operations", Counter,                                \
        F(type_create_table, {"type", "create_table"}), F(type_create_db, {"type", "create_db"}),                                         \
        F(type_drop_table, {"type", "drop_table"}), F(type_drop_db, {"type", "drop_db"}), F(type_rename_table, {"type", "rename_table"}), \
        F(type_add_column, {"type", "add_column"}), F(type_drop_column, {"type", "drop_column"}),                                         \
        F(type_alter_column_tp, {"type", "alter_column_type"}), F(type_rename_column, {"type", "rename_column"}),                         \
        F(type_exchange_partition, {"type", "exchange_partition"}))                                                                       \
    M(tiflash_schema_apply_duration_seconds, "Bucketed histogram of ddl apply duration", Histogram,                                       \
        F(type_ddl_apply_duration, {{"req", "ddl_apply_duration"}}, ExpBuckets{0.0005, 2, 20}))                                           \
    M(tiflash_tmt_merge_count, "Total number of TMT engine merge", Counter)                                                               \
    M(tiflash_tmt_merge_duration_seconds, "Bucketed histogram of TMT engine merge duration", Histogram,                                   \
        F(type_tmt_merge_duration, {{"type", "tmt_merge_duration"}}, ExpBuckets{0.0005, 2, 20}))                                          \
    M(tiflash_tmt_write_parts_count, "Total number of TMT engine write parts", Counter)                                                   \
    M(tiflash_tmt_write_parts_duration_seconds, "Bucketed histogram of TMT engine write parts duration", Histogram,                       \
        F(type_tmt_write_duration, {{"type", "tmt_write_parts_duration"}}, ExpBuckets{0.0005, 2, 20}))                                    \
    M(tiflash_tmt_read_parts_count, "Total number of TMT engine read parts", Gauge)                                                       \
    M(tiflash_raft_read_index_count, "Total number of raft read index", Counter)                                                          \
    M(tiflash_raft_read_index_duration_seconds, "Bucketed histogram of raft read index duration", Histogram,                              \
        F(type_raft_read_index_duration, {{"type", "tmt_raft_read_index_duration"}}, ExpBuckets{0.0005, 2, 20}))                          \
    M(tiflash_raft_wait_index_duration_seconds, "Bucketed histogram of raft wait index duration", Histogram,                              \
        F(type_raft_wait_index_duration, {{"type", "tmt_raft_wait_index_duration"}}, ExpBuckets{0.0005, 2, 20}))                          \
    M(tiflash_storage_write_amplification, "The data write amplification in storage engine", Gauge)                                       \
    M(tiflash_storage_read_tasks_count, "Total number of storage engine read tasks", Counter)                                             \
    M(tiflash_storage_command_count, "Total number of storage's command, such as delete range / shutdown /startup", Counter,              \
        F(type_delete_range, {"type", "delete_range"}), F(type_ingest, {"type", "ingest"}))                                               \
    M(tiflash_storage_subtask_count, "Total number of storage's sub task", Counter, F(type_delta_merge, {"type", "delta_merge"}),         \
        F(type_delta_merge_fg, {"type", "delta_merge_fg"}), F(type_delta_merge_bg_gc, {"type", "delta_merge_bg_gc"}),                     \
        F(type_delta_compact, {"type", "delta_compact"}), F(type_delta_flush, {"type", "delta_flush"}),                                   \
        F(type_seg_split, {"type", "seg_split"}), F(type_seg_merge, {"type", "seg_merge"}),                                               \
        F(type_place_index_update, {"type", "place_index_update"}))                                                                       \
    M(tiflash_storage_subtask_duration_seconds, "Bucketed histogram of storage's sub task duration", Histogram,                           \
        F(type_delta_merge, {{"type", "delta_merge"}}, ExpBuckets{0.0005, 2, 20}),                                                        \
        F(type_delta_merge_fg, {{"type", "delta_merge_fg"}}, ExpBuckets{0.0005, 2, 20}),                                                  \
        F(type_delta_merge_bg_gc, {{"type", "delta_merge_bg_gc"}}, ExpBuckets{0.0005, 2, 20}),                                            \
        F(type_delta_compact, {{"type", "delta_compact"}}, ExpBuckets{0.0005, 2, 20}),                                                    \
        F(type_delta_flush, {{"type", "delta_flush"}}, ExpBuckets{0.0005, 2, 20}),                                                        \
        F(type_seg_split, {{"type", "seg_split"}}, ExpBuckets{0.0005, 2, 20}),                                                            \
        F(type_seg_merge, {{"type", "seg_merge"}}, ExpBuckets{0.0005, 2, 20}),                                                            \
        F(type_place_index_update, {{"type", "place_index_update"}}, ExpBuckets{0.0005, 2, 20}))                                          \
    M(tiflash_storage_throughput_bytes, "Calculate the throughput of tasks of storage in bytes", Gauge,           /**/                    \
        F(type_write, {"type", "write"}),                                                                         /**/                    \
        F(type_ingest, {"type", "ingest"}),                                                                       /**/                    \
        F(type_delta_merge, {"type", "delta_merge"}),                                                             /**/                    \
        F(type_split, {"type", "split"}),                                                                         /**/                    \
        F(type_merge, {"type", "merge"}))                                                                         /**/                    \
    M(tiflash_storage_throughput_rows, "Calculate the throughput of tasks of storage in rows", Gauge,             /**/                    \
        F(type_write, {"type", "write"}),                                                                         /**/                    \
        F(type_ingest, {"type", "ingest"}),                                                                       /**/                    \
        F(type_delta_merge, {"type", "delta_merge"}),                                                             /**/                    \
        F(type_split, {"type", "split"}),                                                                         /**/                    \
        F(type_merge, {"type", "merge"}))                                                                         /**/                    \
    M(tiflash_storage_write_stall_duration_seconds, "The write stall duration of storage, in seconds", Histogram, /**/                    \
        F(type_write, {{"type", "write"}}, ExpBuckets{0.0005, 2, 20}),                                            /**/                    \
        F(type_delete_range, {{"type", "delete_range"}}, ExpBuckets{0.0005, 2, 20}))                              /**/                    \
    M(tiflash_storage_page_gc_count, "Total number of page's gc execution.", Counter, F(type_exec, {"type", "exec"}),                     \
        F(type_low_write, {"type", "low_write"}))                                                                                         \
    M(tiflash_storage_page_gc_duration_seconds, "Bucketed histogram of page's gc task duration", Histogram,                               \
        F(type_exec, {{"type", "exec"}}, ExpBuckets{0.0005, 2, 20}), F(type_migrate, {{"type", "migrate"}}, ExpBuckets{0.0005, 2, 20}))   \
    M(tiflash_storage_rate_limiter_total_request_bytes, "RateLimiter total requested bytes", Counter)                                     \
    M(tiflash_storage_rate_limiter_total_alloc_bytes, "RateLimiter total allocated bytes", Counter)                                       \
    M(tiflash_raft_command_duration_seconds, "Bucketed histogram of some raft command: apply snapshot",                                   \
        Histogram, /* these command usually cost servel seconds, increase the start bucket to 50ms */                                     \
        F(type_ingest_sst, {{"type", "ingest_sst"}}, ExpBuckets{0.05, 2, 10}),                                                            \
        F(type_apply_snapshot_predecode, {{"type", "snapshot_predecode"}}, ExpBuckets{0.05, 2, 10}),                                      \
        F(type_apply_snapshot_flush, {{"type", "snapshot_flush"}}, ExpBuckets{0.05, 2, 10}))                                              \
    M(tiflash_raft_process_keys, "Total number of keys processed in some types of Raft commands", Counter,                                \
        F(type_apply_snapshot, {"type", "apply_snapshot"}), F(type_ingest_sst, {"type", "ingest_sst"}))                                   \
    M(tiflash_raft_apply_write_command_duration_seconds, "Bucketed histogram of applying write command Raft logs", Histogram,             \
        F(type_write, {{"type", "write"}}, ExpBuckets{0.0005, 2, 20}))                                                                    \
    M(tiflash_raft_write_data_to_storage_duration_seconds, "Bucketed histogram of writting region into storage layer", Histogram,         \
        F(type_decode, {{"type", "decode"}}, ExpBuckets{0.0005, 2, 20}), F(type_write, {{"type", "write"}}, ExpBuckets{0.0005, 2, 20}))   \
    M(tiflash_server_info, "Indicate the tiflash server info, and the value is the start timestamp (s).", Gauge,                          \
        F(start_time, {"version", TiFlashBuildInfo::getReleaseVersion()}, {"hash", TiFlashBuildInfo::getGitHash()}))


struct ExpBuckets
{
    const double start;
    const int base;
    const size_t size;
    inline operator prometheus::Histogram::BucketBoundaries() const &&
    {
        prometheus::Histogram::BucketBoundaries buckets(size);
        double current = start;
        std::for_each(buckets.begin(), buckets.end(), [&](auto & e) {
            e = current;
            current *= base;
        });
        return buckets;
    }
};

template <typename T>
struct MetricFamilyTrait
{
};
template <>
struct MetricFamilyTrait<prometheus::Counter>
{
    using ArgType = std::map<std::string, std::string>;
    static auto build() { return prometheus::BuildCounter(); }
    static auto & add(prometheus::Family<prometheus::Counter> & family, ArgType && arg) { return family.Add(std::forward<ArgType>(arg)); }
};
template <>
struct MetricFamilyTrait<prometheus::Gauge>
{
    using ArgType = std::map<std::string, std::string>;
    static auto build() { return prometheus::BuildGauge(); }
    static auto & add(prometheus::Family<prometheus::Gauge> & family, ArgType && arg) { return family.Add(std::forward<ArgType>(arg)); }
};
template <>
struct MetricFamilyTrait<prometheus::Histogram>
{
    using ArgType = std::tuple<std::map<std::string, std::string>, prometheus::Histogram::BucketBoundaries>;
    static auto build() { return prometheus::BuildHistogram(); }
    static auto & add(prometheus::Family<prometheus::Histogram> & family, ArgType && arg)
    {
        return family.Add(std::move(std::get<0>(arg)), std::move(std::get<1>(arg)));
    }
};

template <typename T>
struct MetricFamily
{
    using MetricTrait = MetricFamilyTrait<T>;
    using MetricArgType = typename MetricTrait::ArgType;

    MetricFamily(
        prometheus::Registry & registry, const std::string & name, const std::string & help, std::initializer_list<MetricArgType> args)
    {
        auto & family = MetricTrait::build().Name(name).Help(help).Register(registry);
        metrics.reserve(args.size() ? args.size() : 1);
        for (auto arg : args)
        {
            auto & metric = MetricTrait::add(family, std::forward<MetricArgType>(arg));
            metrics.emplace_back(&metric);
        }
        if (metrics.empty())
        {
            auto & metric = MetricTrait::add(family, MetricArgType{});
            metrics.emplace_back(&metric);
        }
    }

    T & get(size_t idx = 0) { return *(metrics[idx]); }

private:
    std::vector<T *> metrics;
};

/// Centralized registry of TiFlash metrics.
/// Cope with MetricsPrometheus by registering
/// profile events, current metrics and customized metrics (as individual member for caller to access) into registry ahead of being updated.
/// Asynchronous metrics will be however registered by MetricsPrometheus itself due to the life cycle difference.
class TiFlashMetrics
{
public:
    TiFlashMetrics();

private:
    static constexpr auto profile_events_prefix = "tiflash_system_profile_event_";
    static constexpr auto current_metrics_prefix = "tiflash_system_current_metric_";
    static constexpr auto async_metrics_prefix = "tiflash_system_asynchronous_metric_";

    std::shared_ptr<prometheus::Registry> registry = std::make_shared<prometheus::Registry>();

    std::vector<prometheus::Gauge *> registered_profile_events;
    std::vector<prometheus::Gauge *> registered_current_metrics;
    std::unordered_map<std::string, prometheus::Gauge *> registered_async_metrics;

public:
#define MAKE_METRIC_MEMBER_M(family_name, help, type, ...) \
    MetricFamily<prometheus::type> family_name = MetricFamily<prometheus::type>(*registry, #family_name, #help, {__VA_ARGS__});
#define MAKE_METRIC_MEMBER_F(field_name, ...) \
    {                                         \
        __VA_ARGS__                           \
    }
    APPLY_FOR_METRICS(MAKE_METRIC_MEMBER_M, MAKE_METRIC_MEMBER_F)

    friend class MetricsPrometheus;
};

#define MAKE_METRIC_ENUM_M(family_name, help, type, ...) \
    namespace family_name##_metrics                      \
    {                                                    \
        enum                                             \
        {                                                \
            invalid = -1,                                \
            ##__VA_ARGS__                                \
        };                                               \
    }
#define MAKE_METRIC_ENUM_F(field_name, ...) field_name
APPLY_FOR_METRICS(MAKE_METRIC_ENUM_M, MAKE_METRIC_ENUM_F)
#undef APPLY_FOR_METRICS

#define __GET_METRIC_MACRO(_1, _2, _3, NAME, ...) NAME
#define __GET_METRIC_0(ptr, family) (ptr)->family.get()
#define __GET_METRIC_1(ptr, family, metric) (ptr)->family.get(family##_metrics::metric)
#define GET_METRIC(...) __GET_METRIC_MACRO(__VA_ARGS__, __GET_METRIC_1, __GET_METRIC_0)(__VA_ARGS__)

} // namespace DB
