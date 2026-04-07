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

#include <TestUtils/FunctionTestUtils.h>
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
#include <vector>

namespace DB::tests
{
namespace
{

constexpr uint32_t EXECUTION_HOST_V2_ABI_VERSION = 4;
constexpr uint32_t PLAN_KIND_CAST_UTF8_TO_DECIMAL = 1;
constexpr uint32_t INPUT_ID_SCALAR = 0;
constexpr uint32_t SQL_MODE_DEFAULT = 1;
constexpr uint32_t SESSION_CHARSET_UTF8MB4 = 1;
constexpr uint32_t DEFAULT_COLLATION_UTF8MB4_BIN = 1;
constexpr uint32_t STATUS_KIND_OK = 0;
constexpr uint32_t STATUS_KIND_PROTOCOL_ERROR = 5;
constexpr uint32_t STATUS_CODE_NONE = 0;
constexpr uint32_t STATUS_CODE_INSTANCE_FINISH_ONLY = 13;
constexpr uint32_t PHYSICAL_TYPE_UTF8 = 2;
constexpr uint32_t PHYSICAL_TYPE_DECIMAL128 = 4;
constexpr uint32_t BATCH_OWNERSHIP_BORROW_WITHIN_CALL = 1;
constexpr uint32_t BATCH_OWNERSHIP_FOREIGN_RETAINABLE = 2;
constexpr uint32_t AMBIENT_REQUIREMENT_SQL_MODE = 1u << 0;

constexpr const char * FUNC_NAME_TIDB_CAST = "tidb_cast";

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
    using BuildFn = void (*)(
        const TiforthExecutionBuildRequestV2 *,
        TiforthStatusV2 *,
        TiforthExecutionExecutableHandleV2 **);
    using OpenFn = void (*)(
        const TiforthExecutionExecutableHandleV2 *,
        TiforthStatusV2 *,
        TiforthExecutionInstanceHandleV2 **);
    using DriveInputBatchFn = void (*)(
        TiforthExecutionInstanceHandleV2 *,
        uint32_t,
        const TiforthBatchViewV2 *,
        TiforthStatusV2 *,
        TiforthBatchViewV2 *);
    using DriveEndOfInputFn = void (*)(TiforthExecutionInstanceHandleV2 *, uint32_t, TiforthStatusV2 *);
    using ContinueOutputFn = void (*)(TiforthExecutionInstanceHandleV2 *, TiforthStatusV2 *, TiforthBatchViewV2 *);
    using FinishFn = void (*)(TiforthExecutionInstanceHandleV2 *, TiforthStatusV2 *);
    using ReleaseExecutableFn = void (*)(TiforthExecutionExecutableHandleV2 *);
    using ReleaseInstanceFn = void (*)(TiforthExecutionInstanceHandleV2 *);

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

String uint128ToString(unsigned __int128 value)
{
    if (value == 0)
        return "0";

    String digits;
    while (value > 0)
    {
        const uint8_t digit = static_cast<uint8_t>(value % 10);
        digits.push_back(static_cast<char>('0' + digit));
        value /= 10;
    }
    std::reverse(digits.begin(), digits.end());
    return digits;
}

String formatDecimal128(__int128 value, int8_t scale)
{
    const bool negative = value < 0;
    unsigned __int128 abs_value;
    if (negative)
    {
        // avoid overflow when value is the smallest signed 128-bit number
        abs_value = static_cast<unsigned __int128>(-(value + 1));
        abs_value += 1;
    }
    else
    {
        abs_value = static_cast<unsigned __int128>(value);
    }

    if (scale == 0)
    {
        String rendered = uint128ToString(abs_value);
        if (negative)
            rendered.insert(rendered.begin(), '-');
        return rendered;
    }

    String digits = uint128ToString(abs_value);
    const size_t scale_digits = static_cast<size_t>(scale);
    if (digits.size() <= scale_digits)
        digits.insert(0, scale_digits + 1 - digits.size(), '0');

    const size_t split = digits.size() - scale_digits;
    String rendered = fmt::format("{}.{}", digits.substr(0, split), digits.substr(split));
    if (negative)
        rendered.insert(rendered.begin(), '-');
    return rendered;
}

struct Utf8BatchOwned
{
    std::vector<uint8_t> null_bitmap;
    std::vector<int32_t> offsets;
    String data;
    TiforthExecutionColumnViewV2 column{};
    TiforthBatchViewV2 batch{};

    Utf8BatchOwned(const std::vector<std::optional<String>> & rows, uint32_t ownership_mode)
    {
        null_bitmap.assign(rows.size() == 0 ? 0 : (rows.size() + 7) / 8, 0);
        offsets.reserve(rows.size() + 1);
        offsets.push_back(0);

        for (size_t i = 0; i < rows.size(); ++i)
        {
            if (rows[i].has_value())
            {
                null_bitmap[i / 8] |= static_cast<uint8_t>(1u << (i % 8));
                data.append(rows[i].value());
            }
            offsets.push_back(static_cast<int32_t>(data.size()));
        }

        column.physical_type = PHYSICAL_TYPE_UTF8;
        column.null_bitmap = null_bitmap.empty() ? nullptr : null_bitmap.data();
        column.null_bitmap_bit_offset = 0;
        column.row_offset = 0;
        column.values = nullptr;
        column.offsets = offsets.data();
        column.data = reinterpret_cast<const uint8_t *>(data.data());
        column.decimal128_words = nullptr;
        column.decimal_precision_is_set = false;
        column.decimal_precision = 0;
        column.decimal_scale_is_set = false;
        column.decimal_scale = 0;

        batch.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
        batch.ownership_mode = ownership_mode;
        batch.column_count = 1;
        batch.row_count = static_cast<uint32_t>(rows.size());
        batch.columns = &column;
    }
};

struct AdapterRunResult
{
    std::vector<std::optional<String>> output;
    uint32_t warning_count = 0;
};

class TestTiforthExecutionHostV2Cast : public FunctionTest
{
public:
    ColumnWithTypeAndName runDonorNativeCastAsString(const std::vector<std::optional<String>> & input)
    {
        getDAGContext().clearWarnings();
        auto input_column = createColumn<Nullable<String>>(input);
        auto decimal_column = executeFunction(
            FUNC_NAME_TIDB_CAST,
            {input_column, createConstColumn<String>(1, "Nullable(Decimal(10,3))")});
        return executeFunction(
            FUNC_NAME_TIDB_CAST,
            {decimal_column, createConstColumn<String>(1, "Nullable(String)")});
    }

    AdapterRunResult runAdapterCast(
        TiforthExecutionHostV2Api & api,
        const std::vector<std::optional<String>> & input,
        size_t partitions,
        uint32_t ownership_mode)
    {
        TiforthExecutionBuildRequestV2 build_request{};
        build_request.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
        build_request.plan_kind = PLAN_KIND_CAST_UTF8_TO_DECIMAL;
        build_request.ambient_requirement_mask = AMBIENT_REQUIREMENT_SQL_MODE;
        build_request.sql_mode = SQL_MODE_DEFAULT;
        build_request.session_charset = SESSION_CHARSET_UTF8MB4;
        build_request.default_collation = DEFAULT_COLLATION_UTF8MB4_BIN;
        build_request.decimal_precision_is_set = true;
        build_request.decimal_precision = 10;
        build_request.decimal_scale_is_set = true;
        build_request.decimal_scale = 3;
        build_request.max_block_size = 256;

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

        AdapterRunResult result;
        const size_t chunk_size = std::max<size_t>(1, (input.size() + partitions - 1) / partitions);

        for (size_t start = 0; start < input.size(); start += chunk_size)
        {
            const size_t end = std::min(input.size(), start + chunk_size);
            std::vector<std::optional<String>> batch_rows(input.begin() + static_cast<ptrdiff_t>(start), input.begin() + static_cast<ptrdiff_t>(end));
            Utf8BatchOwned batch(batch_rows, ownership_mode);

            TiforthBatchViewV2 output{};
            output.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
            api.drive_input_batch(instance, INPUT_ID_SCALAR, &batch.batch, &status, &output);

            ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
            ASSERT_EQ(status.code, STATUS_CODE_NONE) << status.message;
            result.warning_count = std::max(result.warning_count, status.warning_count);

            ASSERT_EQ(output.column_count, 1u);
            const auto & output_column = output.columns[0];
            ASSERT_EQ(output_column.physical_type, PHYSICAL_TYPE_DECIMAL128);

            const auto * decimal_values = reinterpret_cast<const __int128 *>(output_column.decimal128_words);
            ASSERT_NE(decimal_values, nullptr);
            decimal_values += output_column.row_offset;
            for (size_t row = 0; row < output.row_count; ++row)
            {
                if (!isValidRow(output_column, output.row_count, row))
                {
                    result.output.push_back(std::nullopt);
                    continue;
                }
                result.output.push_back(formatDecimal128(decimal_values[row], output_column.decimal_scale));
            }
        }

        api.drive_end_of_input(instance, INPUT_ID_SCALAR, &status);
        ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
        ASSERT_EQ(status.code, STATUS_CODE_NONE) << status.message;

        TiforthBatchViewV2 continued_output{};
        continued_output.abi_version = EXECUTION_HOST_V2_ABI_VERSION;
        api.continue_output(instance, &status, &continued_output);
        ASSERT_EQ(continued_output.row_count, 0u);
        ASSERT_EQ(continued_output.column_count, 0u);
        if (status.kind != STATUS_KIND_OK)
        {
            ASSERT_EQ(status.kind, STATUS_KIND_PROTOCOL_ERROR) << status.message;
            ASSERT_EQ(status.code, STATUS_CODE_INSTANCE_FINISH_ONLY) << status.message;
        }
        else
        {
            ASSERT_EQ(status.code, STATUS_CODE_NONE) << status.message;
        }

        api.finish(instance, &status);
        ASSERT_EQ(status.kind, STATUS_KIND_OK) << status.message;
        ASSERT_EQ(status.code, STATUS_CODE_NONE) << status.message;

        api.release_instance(instance);
        api.release_executable(executable);
        return result;
    }
};

TEST_F(TestTiforthExecutionHostV2Cast, CastUtf8ToDecimalParitySerialAndParallel)
{
    auto maybe_library = resolveExecutionHostV2LibraryPath();
    if (!maybe_library.has_value())
    {
        GTEST_SKIP() << "set TIFORTH_FFI_C_DYLIB to a built tiforth ffi/c shared library to run this donor adapter test";
    }

    String load_error;
    auto maybe_api = loadExecutionHostV2Api(maybe_library.value(), load_error);
    if (!maybe_api.has_value())
    {
        GTEST_SKIP() << load_error;
    }

    auto api = std::move(maybe_api.value());

    const std::vector<std::optional<String>> input = {
        String("12.345"),
        String("-7.800"),
        std::nullopt,
        String("0"),
        String("999.999"),
        String("1.2"),
    };

    auto donor_native = runDonorNativeCastAsString(input);
    const auto donor_warning_count = getDAGContext().getWarningCount();

    auto serial = runAdapterCast(api, input, 1, BATCH_OWNERSHIP_BORROW_WITHIN_CALL);
    auto parallel = runAdapterCast(api, input, 2, BATCH_OWNERSHIP_FOREIGN_RETAINABLE);

    ASSERT_EQ(serial.warning_count, donor_warning_count);
    ASSERT_EQ(parallel.warning_count, donor_warning_count);

    ASSERT_COLUMN_EQ(createColumn<Nullable<String>>(serial.output), donor_native);
    ASSERT_COLUMN_EQ(createColumn<Nullable<String>>(parallel.output), donor_native);
}

} // namespace
} // namespace DB::tests
