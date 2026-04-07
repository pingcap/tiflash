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

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionHelpers.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <dlfcn.h>
#include <filesystem>
#include <fmt/core.h>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace DB::tests
{
namespace
{

constexpr uint32_t EXECUTION_HOST_V2_ABI_VERSION = 4;
constexpr uint32_t PLAN_KIND_INNER_HASH_JOIN_UTF8_KEY_INT64_PAYLOAD = 2;
constexpr uint32_t INPUT_ID_BUILD = 0;
constexpr uint32_t INPUT_ID_PROBE = 1;
constexpr uint32_t STATUS_KIND_OK = 0;
constexpr uint32_t STATUS_CODE_NONE = 0;
constexpr uint32_t STATUS_CODE_MORE_OUTPUT_AVAILABLE = 29;
constexpr uint32_t PHYSICAL_TYPE_INT64 = 1;
constexpr uint32_t PHYSICAL_TYPE_UTF8 = 2;
constexpr uint32_t BATCH_OWNERSHIP_BORROW_WITHIN_CALL = 1;
constexpr uint32_t BATCH_OWNERSHIP_FOREIGN_RETAINABLE = 2;
constexpr uint32_t AMBIENT_REQUIREMENT_CHARSET = 1u << 2;
constexpr uint32_t AMBIENT_REQUIREMENT_DEFAULT_COLLATION = 1u << 3;
constexpr uint32_t SESSION_CHARSET_UTF8MB4 = 1;
constexpr uint32_t DEFAULT_COLLATION_UTF8MB4_BIN = 1;

struct TiforthExecutionBuildRequestV2
{
    uint32_t abi_version;
    uint32_t plan_kind;
    uint32_t ambient_requirement_mask;
    uint32_t sql_mode;
    uint32_t session_charset;
    uint32_t default_collation;
    bool decimal_precision_is_set;
    uint8_t decimal_precision;
    bool decimal_scale_is_set;
    int8_t decimal_scale;
    uint32_t max_block_size;
};

struct TiforthExecutionColumnViewV2
{
    uint32_t physical_type;
    const uint8_t * null_bitmap;
    uint32_t null_bitmap_bit_offset;
    uint32_t row_offset;
    const int64_t * values;
    const int32_t * offsets;
    const uint8_t * data;
    const void * decimal128_words;
    bool decimal_precision_is_set;
    uint8_t decimal_precision;
    bool decimal_scale_is_set;
    int8_t decimal_scale;
};

struct TiforthBatchViewV2
{
    uint32_t abi_version;
    uint32_t ownership_mode;
    uint32_t column_count;
    uint32_t row_count;
    const TiforthExecutionColumnViewV2 * columns;
};

struct TiforthStatusV2
{
    uint32_t abi_version;
    uint32_t kind;
    uint32_t code;
    uint32_t warning_count;
    char message[256];
};

struct TiforthExecutionExecutableHandleV2;
struct TiforthExecutionInstanceHandleV2;

struct TiforthExecutionHostV2Api
{
    using BuildFn = void (*) (
        const TiforthExecutionBuildRequestV2 *,
        TiforthStatusV2 *,
        TiforthExecutionExecutableHandleV2 **);
    using OpenFn = void (*) (
        const TiforthExecutionExecutableHandleV2 *,
        TiforthStatusV2 *,
        TiforthExecutionInstanceHandleV2 **);
    using DriveInputBatchFn = void (*) (
        TiforthExecutionInstanceHandleV2 *,
        uint32_t,
        const TiforthBatchViewV2 *,
        TiforthStatusV2 *,
        TiforthBatchViewV2 *);
    using DriveEndOfInputFn = void (*) (TiforthExecutionInstanceHandleV2 *, uint32_t, TiforthStatusV2 *);
    using ContinueOutputFn = void (*) (TiforthExecutionInstanceHandleV2 *, TiforthStatusV2 *, TiforthBatchViewV2 *);
    using FinishFn = void (*) (TiforthExecutionInstanceHandleV2 *, TiforthStatusV2 *);
    using ReleaseExecutableFn = void (*) (TiforthExecutionExecutableHandleV2 *);
    using ReleaseInstanceFn = void (*) (TiforthExecutionInstanceHandleV2 *);

    void * library_handle = nullptr;
    BuildFn build = nullptr;
    OpenFn open = nullptr;
    DriveInputBatchFn drive_input_batch = nullptr;
    DriveEndOfInputFn drive_end_of_input = nullptr;
    ContinueOutputFn continue_output = nullptr;
    FinishFn finish = nullptr;
    ReleaseExecutableFn release_executable = nullptr;
    ReleaseInstanceFn release_instance = nullptr;

    TiforthExecutionHostV2Api() = default;
    TiforthExecutionHostV2Api(const TiforthExecutionHostV2Api &) = delete;
    TiforthExecutionHostV2Api & operator=(const TiforthExecutionHostV2Api &) = delete;

    TiforthExecutionHostV2Api(TiforthExecutionHostV2Api && other) noexcept { *this = std::move(other); }

    TiforthExecutionHostV2Api & operator=(TiforthExecutionHostV2Api && other) noexcept
    {
        if (this == &other)
            return *this;
        close();
        library_handle = other.library_handle;
        build = other.build;
        open = other.open;
        drive_input_batch = other.drive_input_batch;
        drive_end_of_input = other.drive_end_of_input;
        continue_output = other.continue_output;
        finish = other.finish;
        release_executable = other.release_executable;
        release_instance = other.release_instance;

        other.library_handle = nullptr;
        other.build = nullptr;
        other.open = nullptr;
        other.drive_input_batch = nullptr;
        other.drive_end_of_input = nullptr;
        other.continue_output = nullptr;
        other.finish = nullptr;
        other.release_executable = nullptr;
        other.release_instance = nullptr;
        return *this;
    }

    ~TiforthExecutionHostV2Api() { close(); }

    void close()
    {
        if (library_handle != nullptr)
        {
            dlclose(library_handle);
            library_handle = nullptr;
        }
    }
};

template <typename Fn>
bool loadSymbol(void * library, const char * name, Fn & fn, String & error)
{
    dlerror();
    auto * raw = dlsym(library, name);
    const char * symbol_error = dlerror();
    if (symbol_error != nullptr)
    {
        error = fmt::format("failed to load symbol {}: {}", name, symbol_error);
        return false;
    }
    fn = reinterpret_cast<Fn>(raw);
    return true;
}

std::optional<TiforthExecutionHostV2Api> loadExecutionHostV2Api(const String & library_path, String & error)
{
    dlerror();
    auto * handle = dlopen(library_path.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (handle == nullptr)
    {
        const char * open_error = dlerror();
        error = fmt::format(
            "failed to open tiforth execution host-v2 library at {}: {}",
            library_path,
            open_error != nullptr ? open_error : "unknown error");
        return std::nullopt;
    }

    TiforthExecutionHostV2Api api;
    api.library_handle = handle;
    if (!loadSymbol(handle, "tiforth_execution_host_v2_build", api.build, error)
        || !loadSymbol(handle, "tiforth_execution_host_v2_open", api.open, error)
        || !loadSymbol(handle, "tiforth_execution_host_v2_drive_input_batch", api.drive_input_batch, error)
        || !loadSymbol(handle, "tiforth_execution_host_v2_drive_end_of_input", api.drive_end_of_input, error)
        || !loadSymbol(handle, "tiforth_execution_host_v2_continue_output", api.continue_output, error)
        || !loadSymbol(handle, "tiforth_execution_host_v2_finish", api.finish, error)
        || !loadSymbol(handle, "tiforth_execution_host_v2_release_executable", api.release_executable, error)
        || !loadSymbol(handle, "tiforth_execution_host_v2_release_instance", api.release_instance, error))
    {
        api.close();
        return std::nullopt;
    }
    return api;
}

std::optional<String> resolveExecutionHostV2LibraryPath()
{
    if (const char * configured = std::getenv("TIFORTH_FFI_C_DYLIB"); configured != nullptr)
    {
        if (std::filesystem::exists(configured))
            return String(configured);
        return std::nullopt;
    }

    return std::nullopt;
}

bool isValidRow(const TiforthExecutionColumnViewV2 & column, uint32_t row_count, size_t row)
{
    if (column.null_bitmap == nullptr || row_count == 0)
        return true;
    const size_t bit_index = static_cast<size_t>(column.null_bitmap_bit_offset) + row;
    return (column.null_bitmap[bit_index / 8] & (1u << (bit_index % 8))) != 0;
}

using JoinInputRow = std::pair<std::optional<String>, std::optional<int64_t>>;
using JoinOutputRow = std::pair<std::optional<int64_t>, std::optional<int64_t>>;

std::vector<JoinOutputRow> canonicalizeRows(std::vector<JoinOutputRow> rows)
{
    const auto rank_value = [](const std::optional<int64_t> & value) {
        return std::make_pair(value.has_value() ? 1 : 0, value.value_or(0));
    };

    std::sort(
        rows.begin(),
        rows.end(),
        [&](const JoinOutputRow & lhs, const JoinOutputRow & rhs) {
            const auto lhs_build = rank_value(lhs.first);
            const auto rhs_build = rank_value(rhs.first);
            if (lhs_build != rhs_build)
                return lhs_build < rhs_build;
            return rank_value(lhs.second) < rank_value(rhs.second);
        });
    return rows;
}

struct JoinBatchOwned
{
    std::vector<uint8_t> key_null_bitmap;
    std::vector<int32_t> key_offsets;
    String key_data;

    std::vector<int64_t> payload_values;
    std::vector<uint8_t> payload_null_bitmap;

    TiforthExecutionColumnViewV2 columns[2]{};
    TiforthBatchViewV2 batch{};

    JoinBatchOwned(const std::vector<JoinInputRow> & rows, uint32_t ownership_mode)
    {
        key_null_bitmap.assign(rows.empty() ? 0 : (rows.size() + 7) / 8, 0);
        key_offsets.reserve(rows.size() + 1);
        key_offsets.push_back(0);

        payload_values.reserve(rows.size());
        payload_null_bitmap.assign(rows.empty() ? 0 : (rows.size() + 7) / 8, 0);

        for (size_t i = 0; i < rows.size(); ++i)
        {
            if (rows[i].first.has_value())
            {
                key_null_bitmap[i / 8] |= static_cast<uint8_t>(1u << (i % 8));
                key_data.append(rows[i].first.value());
            }
            key_offsets.push_back(static_cast<int32_t>(key_data.size()));

            if (rows[i].second.has_value())
            {
                payload_null_bitmap[i / 8] |= static_cast<uint8_t>(1u << (i % 8));
                payload_values.push_back(rows[i].second.value());
            }
            else
            {
                payload_values.push_back(0);
            }
        }

        columns[0].physical_type = PHYSICAL_TYPE_UTF8;
        columns[0].null_bitmap = key_null_bitmap.empty() ? nullptr : key_null_bitmap.data();
        columns[0].null_bitmap_bit_offset = 0;
        columns[0].row_offset = 0;
        columns[0].values = nullptr;
        columns[0].offsets = key_offsets.data();
        columns[0].data = key_data.empty() ? nullptr : reinterpret_cast<const uint8_t *>(key_data.data());
        columns[0].decimal128_words = nullptr;
        columns[0].decimal_precision_is_set = false;
        columns[0].decimal_precision = 0;
        columns[0].decimal_scale_is_set = false;
        columns[0].decimal_scale = 0;

        columns[1].physical_type = PHYSICAL_TYPE_INT64;
        columns[1].null_bitmap = payload_null_bitmap.empty() ? nullptr : payload_null_bitmap.data();
        columns[1].null_bitmap_bit_offset = 0;
        columns[1].row_offset = 0;
        columns[1].values = payload_values.empty() ? nullptr : payload_values.data();
        columns[1].offsets = nullptr;
        columns[1].data = nullptr;
        columns[1].decimal128_words = nullptr;
        columns[1].decimal_precision_is_set = false;
        columns[1].decimal_precision = 0;
        columns[1].decimal_scale_is_set = false;
        columns[1].decimal_scale = 0;

        batch.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
        batch.ownership_mode = ownership_mode;
        batch.column_count = 2;
        batch.row_count = static_cast<uint32_t>(rows.size());
        batch.columns = columns;
    }
};

void appendJoinOutputRows(const TiforthBatchViewV2 & output, std::vector<JoinOutputRow> & rows)
{
    if (output.row_count == 0)
        return;

    ASSERT_EQ(output.column_count, 2u);

    const auto & build_payload = output.columns[0];
    const auto & probe_payload = output.columns[1];

    ASSERT_EQ(build_payload.physical_type, PHYSICAL_TYPE_INT64);
    ASSERT_EQ(probe_payload.physical_type, PHYSICAL_TYPE_INT64);

    ASSERT_NE(build_payload.values, nullptr);
    ASSERT_NE(probe_payload.values, nullptr);

    const auto * build_values = build_payload.values + build_payload.row_offset;
    const auto * probe_values = probe_payload.values + probe_payload.row_offset;

    for (size_t row = 0; row < output.row_count; ++row)
    {
        const auto build_value = isValidRow(build_payload, output.row_count, row)
            ? std::optional<int64_t>(build_values[row])
            : std::nullopt;
        const auto probe_value = isValidRow(probe_payload, output.row_count, row)
            ? std::optional<int64_t>(probe_values[row])
            : std::nullopt;
        rows.emplace_back(build_value, probe_value);
    }
}

std::vector<std::optional<int64_t>> readNullableInt64Column(const ColumnWithTypeAndName & column)
{
    const auto * nullable = checkAndGetColumn<ColumnNullable>(column.column.get());
    if (nullable == nullptr)
    {
        ADD_FAILURE() << "expected nullable int64 output column";
        return {};
    }

    const auto * nested = checkAndGetColumn<ColumnVector<Int64>>(nullable->getNestedColumnPtr().get());
    if (nested == nullptr)
    {
        ADD_FAILURE() << "expected int64 nested column";
        return {};
    }

    std::vector<std::optional<int64_t>> values;
    values.reserve(nullable->size());

    const auto & nested_data = nested->getData();
    for (size_t row = 0; row < nullable->size(); ++row)
    {
        if (nullable->isNullAt(row))
            values.push_back(std::nullopt);
        else
            values.push_back(nested_data[row]);
    }
    return values;
}

std::vector<JoinOutputRow> joinRowsFromColumns(const ColumnsWithTypeAndName & columns)
{
    EXPECT_EQ(columns.size(), 2u);
    if (columns.size() != 2)
        return {};

    auto build_payload = readNullableInt64Column(columns[0]);
    auto probe_payload = readNullableInt64Column(columns[1]);

    EXPECT_EQ(build_payload.size(), probe_payload.size());
    if (build_payload.size() != probe_payload.size())
        return {};

    std::vector<JoinOutputRow> rows;
    rows.reserve(build_payload.size());
    for (size_t row = 0; row < build_payload.size(); ++row)
        rows.emplace_back(build_payload[row], probe_payload[row]);

    return rows;
}

struct DonorRunResult
{
    std::vector<JoinOutputRow> rows;
    uint32_t warning_count = 0;
};

struct AdapterRunResult
{
    std::vector<JoinOutputRow> rows;
    uint32_t warning_count = 0;
};

class TestTiforthExecutionHostV2InnerHashJoin : public ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();

        context.addMockTable(
            "tiforth_host_v2",
            "build_input",
            {{"join_key", TiDB::TP::TypeString}, {"build_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<String>("join_key", {"k", "k", "x", {}}),
             toNullableVec<Int64>("build_payload", {10, 20, 30, 40})});

        context.addMockTable(
            "tiforth_host_v2",
            "probe_input",
            {{"join_key", TiDB::TP::TypeString}, {"probe_payload", TiDB::TP::TypeLongLong}},
            {toNullableVec<String>("join_key", {"k", "x", "z", {}}),
             toNullableVec<Int64>("probe_payload", {100, 200, 300, 400})});
    }

    DonorRunResult runDonorNativeInnerJoin(size_t concurrency)
    {
        getDAGContext().clearWarnings();
        auto request = context.scan("tiforth_host_v2", "probe_input")
                           .join(
                               context.scan("tiforth_host_v2", "build_input"),
                               tipb::JoinType::TypeInnerJoin,
                               {col("join_key")})
                           .project({"build_payload", "probe_payload"})
                           .build(context);

        DonorRunResult result;
        result.rows = canonicalizeRows(joinRowsFromColumns(executeStreams(request, concurrency)));
        result.warning_count = getDAGContext().getWarningCount();
        return result;
    }

    void runAdapterInnerJoin(
        TiforthExecutionHostV2Api & api,
        size_t partitions,
        uint32_t ownership_mode,
        AdapterRunResult & result)
    {
        TiforthExecutionBuildRequestV2 build_request{};
        build_request.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
        build_request.plan_kind = PLAN_KIND_INNER_HASH_JOIN_UTF8_KEY_INT64_PAYLOAD;
        build_request.ambient_requirement_mask =
            AMBIENT_REQUIREMENT_CHARSET | AMBIENT_REQUIREMENT_DEFAULT_COLLATION;
        build_request.sql_mode = 0;
        build_request.session_charset = SESSION_CHARSET_UTF8MB4;
        build_request.default_collation = DEFAULT_COLLATION_UTF8MB4_BIN;
        build_request.decimal_precision_is_set = false;
        build_request.decimal_precision = 0;
        build_request.decimal_scale_is_set = false;
        build_request.decimal_scale = 0;
        build_request.max_block_size = 1;

        TiforthStatusV2 status{};
        status.abi_version = EXECUTION_HOST_V2_ABI_VERSION;

        TiforthExecutionExecutableHandleV2 * executable = nullptr;
        api.build(&build_request, &status, &executable);
        ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
        ASSERT_EQ(status.code, STATUS_CODE_NONE) << status.message;
        ASSERT_NE(executable, nullptr);

        TiforthExecutionInstanceHandleV2 * instance = nullptr;
        api.open(executable, &status, &instance);
        ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
        ASSERT_EQ(status.code, STATUS_CODE_NONE) << status.message;
        ASSERT_NE(instance, nullptr);

        result.rows.clear();
        result.warning_count = 0;
        std::vector<JoinBatchOwned> retained_batches;
        if (ownership_mode == BATCH_OWNERSHIP_FOREIGN_RETAINABLE)
            retained_batches.reserve(8);

        auto drain_output = [&]() {
            while (status.code == STATUS_CODE_MORE_OUTPUT_AVAILABLE)
            {
                TiforthBatchViewV2 continued_output{};
                continued_output.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
                api.continue_output(instance, &status, &continued_output);

                ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
                result.warning_count += status.warning_count;
                appendJoinOutputRows(continued_output, result.rows);
            }
            ASSERT_EQ(status.code, STATUS_CODE_NONE) << status.message;
        };

        auto drive_input_rows = [&](const std::vector<JoinInputRow> & rows, uint32_t input_id) {
            const size_t chunk_size = std::max<size_t>(1, (rows.size() + partitions - 1) / partitions);
            for (size_t start = 0; start < rows.size(); start += chunk_size)
            {
                const size_t end = std::min(rows.size(), start + chunk_size);
                std::vector<JoinInputRow> chunk(
                    rows.begin() + static_cast<ptrdiff_t>(start),
                    rows.begin() + static_cast<ptrdiff_t>(end));

                const TiforthBatchViewV2 * input_batch = nullptr;
                std::optional<JoinBatchOwned> borrowed_batch;
                if (ownership_mode == BATCH_OWNERSHIP_FOREIGN_RETAINABLE)
                {
                    retained_batches.emplace_back(chunk, ownership_mode);
                    input_batch = &retained_batches.back().batch;
                }
                else
                {
                    borrowed_batch.emplace(chunk, ownership_mode);
                    input_batch = &borrowed_batch->batch;
                }

                TiforthBatchViewV2 output{};
                output.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
                api.drive_input_batch(instance, input_id, input_batch, &status, &output);

                ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
                result.warning_count += status.warning_count;
                appendJoinOutputRows(output, result.rows);
                drain_output();
            }
        };

        const std::vector<JoinInputRow> build_rows = {
            {String("k"), 10},
            {String("k"), 20},
            {String("x"), 30},
            {std::nullopt, 40},
        };
        const std::vector<JoinInputRow> probe_rows = {
            {String("k"), 100},
            {String("x"), 200},
            {String("z"), 300},
            {std::nullopt, 400},
        };

        drive_input_rows(build_rows, INPUT_ID_BUILD);

        api.drive_end_of_input(instance, INPUT_ID_BUILD, &status);
        ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
        drain_output();

        drive_input_rows(probe_rows, INPUT_ID_PROBE);

        api.drive_end_of_input(instance, INPUT_ID_PROBE, &status);
        ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
        drain_output();

        api.finish(instance, &status);
        ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
        ASSERT_EQ(status.code, STATUS_CODE_NONE) << status.message;

        api.release_instance(instance);
        api.release_executable(executable);

        result.rows = canonicalizeRows(std::move(result.rows));
    }
};

TEST_F(TestTiforthExecutionHostV2InnerHashJoin, InnerHashJoinPayloadParitySerialAndParallel)
{
    auto maybe_library = resolveExecutionHostV2LibraryPath();
    if (!maybe_library.has_value())
    {
        return;
    }

    String load_error;
    auto maybe_api = loadExecutionHostV2Api(maybe_library.value(), load_error);
    if (!maybe_api.has_value())
    {
        return;
    }

    auto api = std::move(maybe_api.value());

    auto donor_serial = runDonorNativeInnerJoin(1);
    auto donor_parallel = runDonorNativeInnerJoin(2);

    ASSERT_EQ(donor_serial.warning_count, donor_parallel.warning_count);
    ASSERT_EQ(donor_serial.rows, donor_parallel.rows);

    AdapterRunResult adapter_serial;
    runAdapterInnerJoin(api, 1, BATCH_OWNERSHIP_BORROW_WITHIN_CALL, adapter_serial);
    AdapterRunResult adapter_parallel;
    runAdapterInnerJoin(api, 2, BATCH_OWNERSHIP_FOREIGN_RETAINABLE, adapter_parallel);

    ASSERT_EQ(adapter_serial.warning_count, donor_serial.warning_count);
    ASSERT_EQ(adapter_parallel.warning_count, donor_serial.warning_count);

    ASSERT_EQ(adapter_serial.rows, donor_serial.rows);
    ASSERT_EQ(adapter_parallel.rows, donor_serial.rows);
}

} // namespace
} // namespace DB::tests
