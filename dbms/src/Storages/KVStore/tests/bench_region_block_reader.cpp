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

#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/KVStore/Decode/RegionBlockReader.h>
#include <TiDB/Decode/DatumCodec.h>
#include <TiDB/tests/RowCodecTestUtils.h>
#include <benchmark/benchmark.h>
#include <gtest/gtest.h>

using TableInfo = TiDB::TableInfo;
namespace DB::tests
{
using ColumnIDs = std::vector<ColumnID>;
class RegionBlockReaderBenchTest : public benchmark::Fixture
{
protected:
    Int64 handle_value = 100;
    UInt8 del_mark_value = 0;
    UInt64 version_value = 100;

    RegionDataReadInfoList data_list_read;
    std::unordered_map<ColumnID, Field> fields_map;

    enum RowEncodeVersion
    {
        RowV1,
        RowV2
    };

protected:
    void SetUp(const benchmark::State & /*state*/) override
    {
        data_list_read.clear();
        fields_map.clear();
    }

    void encodeColumns(
        TableInfo & table_info,
        std::vector<Field> & fields,
        RowEncodeVersion row_version,
        size_t num_rows)
    {
        // for later check
        std::unordered_map<String, size_t> column_name_columns_index_map;
        for (size_t i = 0; i < table_info.columns.size(); i++)
        {
            fields_map.emplace(table_info.columns[i].id, fields[i]);
            column_name_columns_index_map.emplace(table_info.columns[i].name, i);
        }

        std::vector<Field> value_fields;
        std::vector<Field> pk_fields;
        for (size_t i = 0; i < table_info.columns.size(); i++)
        {
            if (!table_info.columns[i].hasPriKeyFlag())
                value_fields.emplace_back(fields[i]);
            else
                pk_fields.emplace_back(fields[i]);
        }

        // create PK
        WriteBufferFromOwnString pk_buf;
        if (table_info.is_common_handle)
        {
            auto & primary_index_info = table_info.getPrimaryIndexInfo();
            for (size_t i = 0; i < primary_index_info.idx_cols.size(); i++)
            {
                auto idx = column_name_columns_index_map[primary_index_info.idx_cols[i].name];
                DB::EncodeDatum(pk_fields[i], table_info.columns[idx].getCodecFlag(), pk_buf);
            }
        }
        else
        {
            DB::EncodeInt64(handle_value, pk_buf);
        }
        RawTiDBPK pk{std::make_shared<String>(pk_buf.releaseStr())};
        // create value
        WriteBufferFromOwnString value_buf;
        if (row_version == RowEncodeVersion::RowV1)
        {
            encodeRowV1(table_info, value_fields, value_buf);
        }
        else if (row_version == RowEncodeVersion::RowV2)
        {
            encodeRowV2(table_info, value_fields, value_buf);
        }
        else
        {
            throw Exception("Unknown row format " + std::to_string(row_version), ErrorCodes::LOGICAL_ERROR);
        }
        auto row_value = std::make_shared<const TiKVValue>(std::move(value_buf.str()));
        for (size_t i = 0; i < num_rows; i++)
            data_list_read.emplace_back(pk, del_mark_value, version_value, row_value);
    }

    bool decodeColumns(DecodingStorageSchemaSnapshotConstPtr decoding_schema, bool force_decode) const
    {
        RegionBlockReader reader{decoding_schema};
        Block block = createBlockSortByColumnID(decoding_schema);
        return reader.read(block, data_list_read, force_decode);
    }

    std::pair<TableInfo, std::vector<Field>> getNormalTableInfoFields(
        const ColumnIDs & handle_ids,
        bool is_common_handle) const
    {
        return getTableInfoAndFields(
            handle_ids,
            is_common_handle,
            ColumnIDValue(2, handle_value),
            ColumnIDValue(3, std::numeric_limits<UInt64>::max()),
            ColumnIDValue(4, std::numeric_limits<Float32>::min()),
            ColumnIDValue(9, String("aaa")),
            ColumnIDValue(10, DecimalField(ToDecimal<UInt64, Decimal64>(12345678910ULL, 4), 4)),
            ColumnIDValueNull<UInt64>(11));
    }
};

BENCHMARK_DEFINE_F(RegionBlockReaderBenchTest, CommonHandle)
(benchmark::State & state)
{
    size_t num_rows = state.range(0);
    auto [table_info, fields] = getNormalTableInfoFields({2, 3, 4}, true);
    encodeColumns(table_info, fields, RowEncodeVersion::RowV2, num_rows);
    auto decoding_schema = getDecodingStorageSchemaSnapshot(table_info);
    for (auto _ : state)
    {
        decodeColumns(decoding_schema, true);
    }
}


BENCHMARK_DEFINE_F(RegionBlockReaderBenchTest, PKIsNotHandle)
(benchmark::State & state)
{
    size_t num_rows = state.range(0);
    auto [table_info, fields] = getNormalTableInfoFields({MutSup::extra_handle_id}, false);
    encodeColumns(table_info, fields, RowEncodeVersion::RowV2, num_rows);
    auto decoding_schema = getDecodingStorageSchemaSnapshot(table_info);
    for (auto _ : state)
    {
        decodeColumns(decoding_schema, true);
    }
}

BENCHMARK_DEFINE_F(RegionBlockReaderBenchTest, PKIsHandle)
(benchmark::State & state)
{
    size_t num_rows = state.range(0);
    auto [table_info, fields] = getNormalTableInfoFields({2}, false);
    encodeColumns(table_info, fields, RowEncodeVersion::RowV2, num_rows);
    auto decoding_schema = getDecodingStorageSchemaSnapshot(table_info);
    for (auto _ : state)
    {
        decodeColumns(decoding_schema, true);
    }
}

constexpr size_t num_iterations_test = 1000;

BENCHMARK_REGISTER_F(RegionBlockReaderBenchTest, PKIsHandle)
    ->Iterations(num_iterations_test)
    ->Arg(1)
    ->Arg(10)
    ->Arg(100);
BENCHMARK_REGISTER_F(RegionBlockReaderBenchTest, CommonHandle)
    ->Iterations(num_iterations_test)
    ->Arg(1)
    ->Arg(10)
    ->Arg(100);
BENCHMARK_REGISTER_F(RegionBlockReaderBenchTest, PKIsNotHandle)
    ->Iterations(num_iterations_test)
    ->Arg(1)
    ->Arg(10)
    ->Arg(100);

} // namespace DB::tests
